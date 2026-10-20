"""Bridge existing private collectors into the research catalog; never crawl websites."""

import io
import json
import re
from calendar import monthrange
from datetime import date, datetime
from urllib.parse import urlsplit

from psycopg.types.json import Jsonb

from .archive import index_text, lock_item, upsert_item
from .db import digest

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
        manifests = source.objects(
            "letters/manifest/" + (collection + ".jsonl" if collection else "")
        )
        candidates = []
        for obj in manifests:
            manager = obj["key"].removeprefix("letters/manifest/").removesuffix(".jsonl")
            if not COLLECTION.fullmatch(manager) or (collection and manager != collection):
                continue
            for line in (
                source.read(obj["key"], db.settings.bridge_metadata_bytes).decode().splitlines()
            ):
                if not line.strip():
                    continue
                row = json.loads(line)
                if row.get("status") != "stored":
                    continue
                sha = str(row.get("sha256", ""))
                if not SHA.fullmatch(sha):
                    raise ValueError("stored letter requires a valid checksum")
                url = source_url(row.get("url"))
                # Collection activity does not change the semantic revision of a letter.
                stable = {k: row.get(k) for k in ("title", "published", "url", "sha256")}
                candidates.append(
                    {
                        "key": f"letters/{manager}/{digest(url)}",
                        "signature": digest(stable),
                        "manager": manager,
                        "row": row,
                    }
                )
                if len(candidates) > db.settings.bridge_object_limit:
                    raise ValueError("letter scope exceeds object limit; select a manager")
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
                record = _metadata(
                    collection=manager,
                    title=row.get("title"),
                    published=row.get("published"),
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
