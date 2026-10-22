"""Bridge existing private collectors into the research catalog; never crawl websites."""

import io
import json
import re
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime
from urllib.parse import urlsplit

from psycopg.types.json import Jsonb

from ofl.documents.collectors import link_title
from ofl.documents.funds import (
    FundSources,
    letter_fund,
    needs_first_page_for_fund,
    publishes_several_funds,
    titled_with_fund,
)
from ofl.documents.periods import (
    HEADER_CHARACTERS,
    LetterSources,
    Period,
    dated_title,
    letter_period,
    needs_first_page,
)

from .archive import index_text, item_identity, upsert_item
from .db import digest, lock_item

COLLECTION = re.compile(r"^[A-Za-z0-9_-]{1,80}$")
SHA = re.compile(r"^[a-f0-9]{64}$")
TIERS = {"official", "feed_fulltext", "headline_only"}


class LibrarySource:
    """Fixed server-configured buckets; request arguments never choose destinations."""

    def __init__(self, settings):
        import boto3

        from ofl.config import get_settings

        config = get_settings()
        self.settings = settings
        self.client = boto3.client(
            "s3",
            endpoint_url=config.minio_endpoint,
            aws_access_key_id=config.minio_user,
            aws_secret_access_key=config.minio_password,
            region_name=config.aws_region,
        )

    @property
    def news_tiers(self):
        from ofl.documents.news import load_sources

        return {source.id: source.tier for source in load_sources()}

    def read(self, key, maximum, *, snapshot=False):
        bucket = self.settings.snapshot_bucket if snapshot else self.settings.library_bucket
        result = self.client.get_object(Bucket=bucket, Key=key)
        try:
            if result["ContentLength"] > maximum:
                raise ValueError("source object exceeds bridge size limit")
            body = result["Body"].read(maximum + 1)
            if len(body) > maximum:
                raise ValueError("source object exceeds bridge size limit")
            return body
        finally:
            result["Body"].close()

    def objects(self, prefix):
        found = []
        for page in self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.settings.library_bucket,
            Prefix=prefix,
        ):
            for item in page.get("Contents", []):
                found.append(
                    {
                        "key": item["Key"],
                        "signature": item["ETag"],
                        "modified": str(item["LastModified"]),
                    }
                )
                if len(found) > self.settings.bridge_object_limit:
                    raise ValueError("library scope exceeds object limit; select a collection")
        return sorted(found, key=lambda row: (row["modified"], row["key"]), reverse=True)


def date_interval(raw):
    """Keep imprecise/unknown publication dates; never infer them from collection time."""
    text = str(raw or "").strip()
    try:
        if re.fullmatch(r"\d{4}", text):
            year = int(text)
            return date(year, 1, 1), date(year, 12, 31), "year", None
        if re.fullmatch(r"\d{4}-\d{2}", text):
            year, month = map(int, text.split("-"))
            return (
                date(year, month, 1),
                date(year, month, monthrange(year, month)[1]),
                "month",
                None,
            )
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            day = date.fromisoformat(text)
            return day, day, "day", None
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.date(), parsed.date(), "day", parsed.isoformat() if parsed.tzinfo else None
    except (ValueError, OverflowError):
        return None, None, "unknown", None


def source_url(value):
    url = str(value or "")
    parsed = urlsplit(url)
    if (
        len(url) > 2048
        or parsed.scheme not in {"https", "http"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
    ):
        raise ValueError("source record requires an HTTP(S) reference without credentials")
    return url


def _metadata(*, collection, title, published, url, tier, epistemic, source_key, sha):
    start, end, precision, timestamp = date_interval(published)
    return {
        "source_id": digest([collection, url]),
        "title": str(title or url)[:4000],
        "collection": collection,
        "url": url,
        "date_start": start.isoformat() if start else None,
        "date_end": end.isoformat() if end else None,
        "date_text": str(published or ""),
        "date_precision": precision,
        "published_at": timestamp,
        "content_tier": tier,
        "epistemic_status": epistemic,
        "collector_object": source_key,
        "collector_sha256": sha,
        "rights": "private collection; broader reuse not established by this bridge",
    }


def _asset(con, db, item_id, key, sha, name, media_type, *, process):
    asset_id = digest([item_id, sha])
    # Superseded sources stay preserved but must not contaminate current retrieval.
    con.execute("UPDATE assets SET active=false WHERE item_id=%s AND id<>%s", (item_id, asset_id))
    con.execute(
        "UPDATE passages SET active=false WHERE item_id=%s AND asset_id IS NOT NULL AND asset_id<>%s",
        (item_id, asset_id),
    )
    retired = con.execute(
        """UPDATE observations SET active=false WHERE active AND candidate_id IN
        (SELECT c.id FROM candidates c JOIN assets a ON a.id=c.asset_id WHERE a.item_id=%s AND a.id<>%s)""",
        (item_id, asset_id),
    )
    if retired.rowcount:
        db.enqueue_on(con, "build_observations", {}, "library")
    con.execute(
        """INSERT INTO assets(id,item_id,sha256,object_key,name,media_type,state)
        VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(item_id,sha256) DO UPDATE SET active=true""",
        (asset_id, item_id, sha, key, name, media_type, "registered" if process else "processed"),
    )
    existing = con.execute("SELECT state FROM assets WHERE id=%s", (asset_id,)).fetchone()
    if process and existing["state"] == "registered":
        db.enqueue_on(con, "process_asset", {"asset_id": asset_id}, "library")
        con.execute("UPDATE assets SET state='queued' WHERE id=%s", (asset_id,))
    return asset_id


def index_news(con, db, store, row, object_key, *, policies=None):
    """Reapply content-tier limits even if an old collector object contains extra text."""
    if not isinstance(row, dict):
        raise ValueError("news record must be an object")
    collection, tier = str(row.get("source", "")), row.get("tier")
    if not COLLECTION.fullmatch(collection) or tier not in TIERS:
        raise ValueError("news source or content tier is invalid")
    if not object_key.startswith(f"news/items/{collection}/"):
        raise ValueError("news source does not match its collector object")
    configured = (policies or {}).get(collection, "headline_only")
    if configured not in TIERS or "headline_only" in {tier, configured}:
        tier = "headline_only"
    url = source_url(row.get("link"))
    body = "" if tier == "headline_only" else str(row.get("body") or "")
    summary = str(row.get("summary") or "")[:300]
    if len(body) > 200000:
        raise ValueError("news text exceeds bridge limit")
    # No disallowed body is copied into the research original or metadata.
    captured = {
        k: row.get(k)
        for k in (
            "id",
            "source",
            "tier",
            "title",
            "link",
            "published",
            "author",
            "categories",
            "fetched_at",
        )
    }
    captured.update(tier=tier, summary=summary, body=body)
    encoded = json.dumps(captured, ensure_ascii=False, sort_keys=True).encode()
    key, sha, _ = store.put(io.BytesIO(encoded), max_bytes=db.settings.bridge_metadata_bytes)
    record = _metadata(
        collection=collection,
        title=row.get("title"),
        published=row.get("published"),
        url=url,
        tier=tier,
        epistemic="official_publication" if tier == "official" else "reported_claim",
        source_key=object_key,
        sha=sha,
    )
    record.update(item_type="news", description=summary, creators=str(row.get("author") or ""))
    identity = digest(["news", record["source_id"]])
    lock_item(con, identity)
    previous = con.execute("SELECT metadata FROM items WHERE id=%s", (identity,)).fetchone()
    observed_time = datetime.fromisoformat(str(row.get("fetched_at") or "").replace("Z", "+00:00"))
    if observed_time.tzinfo is None:
        raise ValueError("news collection timestamp requires timezone")
    from datetime import UTC

    observed = observed_time.astimezone(UTC).isoformat()
    # Old batches must not overwrite a newer captured revision on later syncs.
    if previous and previous["metadata"].get("collected_at", "") > observed:
        return False
    policy_fingerprint = digest([collection, configured])
    if (
        previous
        and previous["metadata"].get("collected_at") == observed
        and previous["metadata"].get("policy_fingerprint") == policy_fingerprint
        and previous["metadata"].get("collector_sha256") != sha
    ):
        raise ValueError("conflicting news records at the same collection timestamp")
    record["collected_at"] = observed
    record["policy_fingerprint"] = policy_fingerprint
    item_id = upsert_item(con, "news", record, record["source_id"])
    asset = _asset(
        con, db, item_id, key, sha, "captured-news.json", "application/json", process=False
    )
    index_text(
        con,
        item_id=item_id,
        asset_id=asset,
        revision=digest([sha, "collector-news-v1"]),
        text="\n".join([record["title"], summary, body]),
        method="collector_news",
        evidence={
            "sha256": sha,
            "source_url": url,
            "content_tier": tier,
            "epistemic_status": record["epistemic_status"],
        },
    )
    con.execute(
        "UPDATE passages SET active=false WHERE item_id=%s AND method='metadata'", (item_id,)
    )
    return True


def _letter_sources(row, url, first_page=None):
    listing_year = row.get("listing_year")
    if not isinstance(listing_year, int) or isinstance(listing_year, bool):
        listing_year = None
    return LetterSources(
        title=str(row.get("title") or ""),
        url=url,
        listing_year=listing_year,
        published=str(row.get("published") or ""),
        first_page=first_page,
    )


def _first_pages(db, letters):
    """The header of each letter's current original, keyed by item and checksum.

    letters holds (item_id, sha256) pairs; only those originals are read. Periods and
    funds are read from the header alone, so only its first characters are returned,
    plus one that tells whether the page goes on past the header."""
    if not letters:
        return {}
    rows = db.rows(
        """SELECT p.item_id, a.sha256,
        left(string_agg(p.text, ' ' ORDER BY p.method, p.ordinal), %s) AS text
        FROM passages p JOIN assets a ON a.id=p.asset_id AND a.active
        WHERE p.item_id=ANY(%s) AND p.active AND p.page=1 AND p.method<>'metadata'
        GROUP BY p.item_id, a.sha256""",
        (HEADER_CHARACTERS + 1, [item_id for item_id, _ in letters]),
    )
    return {(row["item_id"], row["sha256"]): row["text"] for row in rows}


def _manifest_letters(db, source, collection):
    """Every stored letter in the selected manifests, as (manager, url, row)."""
    letters = []
    manifests = source.objects("letters/manifest/" + (collection + ".jsonl" if collection else ""))
    for obj in manifests:
        manager = obj["key"].removeprefix("letters/manifest/").removesuffix(".jsonl")
        if not COLLECTION.fullmatch(manager) or (collection and manager != collection):
            continue
        raw = source.read(obj["key"], db.settings.bridge_metadata_bytes)
        for line in raw.decode().splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("status") != "stored":
                continue
            if not SHA.fullmatch(str(row.get("sha256", ""))):
                raise ValueError("stored letter requires a valid checksum")
            letters.append((manager, source_url(row.get("url")), row))
            if len(letters) > db.settings.bridge_object_limit:
                raise ValueError("letter scope exceeds object limit; select a manager")
    return letters


@dataclass
class _StoredLetter:
    """One stored letter on its way to a sync candidate."""

    manager: str
    url: str
    row: dict
    link_text: str
    period: Period | None = None
    first_page: str | None = None
    # Whether the first page was looked up, since an unprocessed original has none.
    first_page_loaded: bool = False

    @property
    def item_id(self):
        return item_identity("manager_letter", digest([self.manager, self.url]))

    def fund_sources(self):
        return FundSources(
            link_text=self.link_text,
            url=self.url,
            manifest_fund=str(self.row.get("fund") or ""),
            first_page=self.first_page,
        )


def _load_first_pages(db, letters):
    """Look up the first extracted page of each given letter not looked up yet."""
    waiting = [letter for letter in letters if not letter.first_page_loaded]
    pages = _first_pages(db, [(letter.item_id, letter.row["sha256"]) for letter in waiting])
    for letter in waiting:
        letter.first_page = pages.get((letter.item_id, letter.row["sha256"]))
        letter.first_page_loaded = True


def _dated_letters(db, source, collection, *, latest_year):
    """Every stored letter, with page-chrome titles replaced and its period derived."""
    letters = []
    for manager, url, row in _manifest_letters(db, source, collection):
        link_text = str(row.get("title") or "")
        titled = {**row, "title": link_title(link_text, url)}
        letters.append(_StoredLetter(manager, url, titled, link_text))
    # The first page is read only for letters no other source dates, and only once the
    # original has been processed; a new letter gets its year on a later sync.
    undated = [
        letter
        for letter in letters
        if needs_first_page(_letter_sources(letter.row, letter.url), latest_year=latest_year)
    ]
    _load_first_pages(db, undated)
    for letter in letters:
        sources = _letter_sources(letter.row, letter.url, letter.first_page)
        letter.period = letter_period(sources, latest_year=latest_year)
        letter.row["title"] = dated_title(letter.row["title"], letter.period)
    return letters


def _add_fund_names(db, letters):
    """Put the fund's name in the titles of collections that publish for several funds.

    The first page is read only for letters that no other source names a fund for."""
    unnamed = [
        letter
        for letter in letters
        if needs_first_page_for_fund(letter.fund_sources(), letter.manager)
    ]
    _load_first_pages(db, unnamed)
    by_manager = {}
    for letter in letters:
        by_manager.setdefault(letter.manager, []).append(letter)
    for manager, own in by_manager.items():
        funds = [letter_fund(letter.fund_sources(), manager) for letter in own]
        if not publishes_several_funds(funds):
            continue
        for letter, fund in zip(own, funds, strict=True):
            letter.row["title"] = titled_with_fund(letter.row["title"], fund)


def _letter_candidates(db, source, collection, *, latest_year):
    """One sync candidate per stored letter, with its derived title and period.

    Manifests written before page-chrome link texts were replaced still hold them, many
    titles name a month without its year, and a manager with several funds gives their
    letters for the same month the same title. Titles are therefore derived on every sync
    (see ofl/documents/periods.py and ofl/documents/funds.py). Besides the manifest, the
    result depends on the letter's first extracted page, on the other letters of its
    collection and on latest_year, so a letter can be retitled after its original is
    processed, when its manager starts publishing for a second fund, or when a new year
    begins. The derived title and period are part of the signature, so only letters they
    change are synced again."""
    letters = _dated_letters(db, source, collection, latest_year=latest_year)
    _add_fund_names(db, letters)
    return [
        _letter_candidate(letter.manager, letter.url, letter.row, letter.period)
        for letter in letters
    ]


def _letter_candidate(manager, url, row, period):
    # Collection activity does not change the semantic revision of a letter. The period
    # joins the signature only when there is one, so letters without one keep the
    # signature they had before periods were derived.
    stable = {k: row.get(k) for k in ("title", "published", "url", "sha256")}
    if period:
        stable["period"] = period.iso
    return {
        "key": f"letters/{manager}/{digest(url)}",
        "signature": digest(stable),
        "manager": manager,
        "row": row,
        "period": period.iso if period else "",
    }


def _save_entry(con, kind, key, signature, detail):
    con.execute(
        """INSERT INTO library_entries(kind,source_key,signature,detail) VALUES (%s,%s,%s,%s)
        ON CONFLICT(kind,source_key) DO UPDATE SET signature=excluded.signature,detail=excluded.detail,updated_at=now()""",
        (kind, key, signature, Jsonb(detail)),
    )


def sync_library(
    db, store, kind, *, limit=20, collection=None, source=None, heartbeat=lambda _: None
):
    if kind not in {"news", "letters"} or not 1 <= limit <= 200:
        raise ValueError("invalid library sync scope")
    if collection and not COLLECTION.fullmatch(collection):
        raise ValueError("invalid collection")
    source = source or LibrarySource(db.settings)
    done = {
        row["source_key"]: row
        for row in db.rows("SELECT * FROM library_entries WHERE kind=%s", (kind,))
    }
    result = {"kind": kind, "processed": 0, "items": 0, "unchanged": 0, "errors": 0, "remaining": 0}
    if kind == "news":
        prefix = "news/items/" + (collection + "/" if collection else "")
        candidates = source.objects(prefix)
    else:
        candidates = _letter_candidates(db, source, collection, latest_year=date.today().year)
    policies = getattr(source, "news_tiers", {}) if kind == "news" else {}
    for candidate in candidates:
        key, signature = candidate["key"], candidate["signature"]
        if kind == "news":
            publisher = key.removeprefix("news/items/").split("/", 1)[0]
            signature = digest(
                [signature, "collector-bridge-v1", policies.get(publisher, "headline_only")]
            )
        old = done.get(key)
        if old and old["signature"] == signature:
            result["unchanged"] += 1
            continue
        if result["processed"] + result["errors"] >= limit:
            result["remaining"] += 1
            continue
        try:
            if kind == "news":
                raw = source.read(key, db.settings.bridge_metadata_bytes)
                rows = [json.loads(line) for line in raw.decode().splitlines() if line.strip()]
                if len(rows) > 1000:
                    raise ValueError("split news batches into at most 1000 rows")
                with db.connect() as con:
                    count = sum(
                        index_news(con, db, store, row, key, policies=policies) for row in rows
                    )
                    _save_entry(con, kind, key, signature, {"items": count})
            else:
                row, manager = candidate["row"], candidate["manager"]
                source_key = f"letters/files/{manager}/{row['sha256']}.pdf"
                raw = source.read(source_key, db.settings.bridge_asset_bytes)
                if b"%PDF-" not in raw[:1024]:
                    raise ValueError("stored letter is not a PDF")
                stored_key, sha, _ = store.put(
                    io.BytesIO(raw), max_bytes=db.settings.bridge_asset_bytes
                )
                if sha != row["sha256"]:
                    raise ValueError("letter original checksum does not match manifest")
                # The upload date stays the letter's date when the manifest has one;
                # otherwise the month the letter covers dates it.
                record = _metadata(
                    collection=manager,
                    title=row.get("title"),
                    published=row.get("published") or candidate["period"],
                    url=source_url(row["url"]),
                    tier="private_original",
                    epistemic="attributed_opinion",
                    source_key=source_key,
                    sha=sha,
                )
                record.update(item_type="manager_letter", creators=manager)
                with db.connect() as con:
                    item_id = upsert_item(con, "manager_letter", record, record["source_id"])
                    _asset(
                        con,
                        db,
                        item_id,
                        stored_key,
                        sha,
                        f"{manager}.pdf",
                        "application/pdf",
                        process=True,
                    )
                    _save_entry(con, kind, key, signature, {"item_id": item_id})
                count = 1
            result["processed"] += 1
            result["items"] += count
        except (ValueError, OSError, LookupError):
            result["errors"] += 1
        heartbeat(result.copy())
    if result["errors"]:
        raise ValueError(
            f"library sync has {result['errors']} failed inputs; committed entries are resumable"
        )
    return result
