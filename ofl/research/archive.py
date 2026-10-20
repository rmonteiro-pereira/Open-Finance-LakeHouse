"""Supplied archival catalog/assets, with metadata revisions and page citations."""

import csv
import json
import mimetypes
import shutil
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

from psycopg.types.json import Jsonb

from . import jurisdiction
from .db import digest

VERSION = "archive-v1"


def chunks(text: str, words=jurisdiction.CHUNK_WORDS, overlap=jurisdiction.CHUNK_OVERLAP):
    """Page-bounded passages; embedding adapters enforce their tokenizer limit."""
    tokens = text.split()
    for start in range(0, len(tokens), words - overlap):
        yield " ".join(tokens[start : start + words])
        if start + words >= len(tokens):
            break


def index_text(con, *, item_id, revision, text, method, asset_id=None, page=None, evidence=None):
    # Only replace the appropriate metadata/page revision, never another asset.
    con.execute(
        """UPDATE passages SET active=false WHERE item_id=%s AND method=%s
        AND asset_id IS NOT DISTINCT FROM %s AND page IS NOT DISTINCT FROM %s""",
        (item_id, method, asset_id, page),
    )
    title = con.execute("SELECT title FROM items WHERE id=%s", (item_id,)).fetchone()["title"]
    passages = list(chunks(text))
    regions = jurisdiction.passage_regions(range(len(passages)), passages)
    for ordinal, passage in enumerate(passages):
        identity = digest([item_id, revision, ordinal])
        con.execute(
            """INSERT INTO passages(id,item_id,asset_id,page,ordinal,text,revision,method,evidence,search,
            jurisdiction,jurisdiction_evidence,jurisdiction_tagger)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,
            setweight(to_tsvector('simple',%s),'A') || setweight(to_tsvector('portuguese',%s),'B'),
            %s,%s,%s)
            ON CONFLICT(id) DO UPDATE SET active=true,jurisdiction=excluded.jurisdiction,
            jurisdiction_evidence=excluded.jurisdiction_evidence,
            jurisdiction_tagger=excluded.jurisdiction_tagger""",
            (
                identity,
                item_id,
                asset_id,
                page,
                ordinal,
                passage,
                revision,
                method,
                Jsonb(evidence or {}),
                passage if method == "metadata" else "",
                passage,
                *jurisdiction.columns(passage, title, **regions[ordinal]),
            ),
        )


def upsert_item(con, source, record, fallback_id):
    source_id = str(record.get("source_id") or fallback_id)
    identity = digest([source, source_id])
    title = str(record.get("title") or source_id)
    revision = digest(record)
    date_start, date_end = record.get("date_start") or None, record.get("date_end") or None
    if date_start and date_end and date_start > date_end:
        raise ValueError("catalog date interval reversed")
    con.execute(
        """INSERT INTO items(id,source,source_id,title,description,item_type,collection,
        date_start,date_end,date_text,date_precision,language,metadata,revision)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(source,source_id) DO UPDATE SET title=excluded.title,
        description=excluded.description,item_type=excluded.item_type,collection=excluded.collection,
        date_start=excluded.date_start,date_end=excluded.date_end,date_text=excluded.date_text,
        date_precision=excluded.date_precision,language=excluded.language,metadata=excluded.metadata,
        revision=excluded.revision,updated_at=now()""",
        (
            identity,
            source,
            source_id,
            title,
            str(record.get("description") or ""),
            record.get("item_type") or "document",
            record.get("collection") or "",
            date_start,
            date_end,
            record.get("date_text") or "",
            record.get("date_precision") or "unknown",
            record.get("language") or "por",
            Jsonb(record),
            revision,
        ),
    )
    text = "\n".join(
        str(record.get(k) or "")
        for k in ("title", "description", "subjects", "collection", "creators", "source_id")
    )
    index_text(con, item_id=identity, revision=revision, text=text, method="metadata")
    return identity


def attach(con, db, store, item_id, path, name=None):
    with path.open("rb") as stream:
        key, sha, _ = store.put(stream, max_bytes=db.settings.max_upload_bytes)
    identity = digest([item_id, sha])
    media_type = mimetypes.guess_type(name or path.name)[0] or "application/octet-stream"
    con.execute(
        """INSERT INTO assets(id,item_id,sha256,object_key,name,media_type)
        VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(item_id,sha256) DO UPDATE SET active=true""",
        (identity, item_id, sha, key, name or path.name, media_type),
    )
    existing = con.execute("SELECT state FROM assets WHERE id=%s", (identity,)).fetchone()
    if existing["state"] == "registered":
        db.enqueue_on(con, "process_asset", {"asset_id": identity}, "import")
        con.execute("UPDATE assets SET state='queued' WHERE id=%s", (identity,))


def _records(path, suffix, metadata):
    if suffix == ".jsonl":
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    yield json.loads(line)
    elif suffix == ".json":
        records = json.loads(path.read_text())
        yield from records if isinstance(records, list) else records["items"]
    elif suffix == ".csv":
        mapping = metadata.get("columns")
        if not mapping:
            raise ValueError("CSV catalog requires metadata.columns: canonical field -> CSV column")
        with path.open(encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                yield {target: row[column] for target, column in mapping.items()}


def import_archive(db, store, import_id, heartbeat=lambda _: None):
    with db.connect() as con:
        con.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (import_id,))
        item = con.execute("SELECT * FROM imports WHERE id=%s", (import_id,)).fetchone()
        if not item or item["state"] == "published":
            return
        source = store.path(item["object_key"])
        suffix = Path(item["name"]).suffix.lower()
        count = 0
        with tempfile.TemporaryDirectory(dir=store.root) as tmp:
            base = Path(tmp)
            if suffix == ".zip":
                with zipfile.ZipFile(source) as archive:
                    total = 0
                    seen = set()
                    if len(archive.infolist()) > 100_000:
                        raise ValueError("too many archive members")
                    for member in archive.infolist():
                        name = PurePosixPath(member.filename.replace("\\", "/"))
                        if name.is_absolute() or ".." in name.parts or member.flag_bits & 1:
                            raise ValueError("unsafe archive member")
                        if str(name) in seen:
                            raise ValueError("duplicate archive member")
                        seen.add(str(name))
                        if member.is_dir():
                            continue
                        total += member.file_size
                        if total > db.settings.max_uncompressed_bytes:
                            raise ValueError("archive exceeds uncompressed size limit")
                        store.check_space(member.file_size)
                        dest = base.joinpath(*name.parts)
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        with archive.open(member) as src, dest.open("wb") as out:
                            shutil.copyfileobj(src, out, 1024 * 1024)
                source, suffix = base / "catalog.jsonl", ".jsonl"
                if not source.exists():
                    if item["source"] != "b3_bulletin":
                        raise ValueError("MUB3 archive package requires catalog.jsonl at its root")
                    import re

                    from .imports import register

                    for path in sorted(base.rglob("*")):
                        if not path.is_file():
                            continue
                        if re.fullmatch(r"COTAHIST_[AMD]\d+\.(TXT|ZIP)", path.name.upper()):
                            register(db, store, path, "b3_cotahist")
                        else:
                            identity = upsert_item(
                                con,
                                item["source"],
                                {"title": path.name},
                                digest([item["sha256"], str(path.relative_to(base))]),
                            )
                            attach(con, db, store, identity, path)
                        count += 1
                        heartbeat({"package_files": count})
                    suffix = ".asset"
            elif suffix in {".jsonl", ".json", ".csv"} and not (
                suffix == ".csv"
                and item["source"] == "b3_bulletin"
                and not item["metadata"].get("columns")
            ):
                # External assets are supplied via ZIP or separate asset imports.
                base = source.parent
            else:
                record = {**item["metadata"], "title": item["metadata"].get("title", item["name"])}
                identity = upsert_item(con, item["source"], record, item["sha256"])
                attach(con, db, store, identity, source, item["name"])
                count = 1
                suffix = ".asset"
            if suffix in {".jsonl", ".json", ".csv"}:
                for ordinal, record in enumerate(_records(source, suffix, item["metadata"])):
                    if not isinstance(record, dict):
                        raise ValueError("catalog records must be JSON objects")
                    identity = upsert_item(
                        con, item["source"], record, digest([item["sha256"], ordinal])
                    )
                    if "assets" in record:
                        con.execute("UPDATE assets SET active=false WHERE item_id=%s", (identity,))
                    for asset in record.get("assets", []):
                        relative = PurePosixPath(asset["path"])
                        path = (base / relative).resolve()
                        if relative.is_absolute() or not path.is_relative_to(base.resolve()):
                            raise ValueError("asset path outside supplied package")
                        if Path(item["name"]).suffix.lower() != ".zip":
                            raise ValueError("supply local assets inside a catalog ZIP package")
                        attach(con, db, store, identity, path, asset.get("name") or path.name)
                    count += 1
                    if count % 100 == 0:
                        heartbeat({"catalog_items": count})
            heartbeat({"catalog_items": count})
            con.execute(
                "UPDATE imports SET state='published',counts=%s,error=NULL,updated_at=now() WHERE id=%s",
                (Jsonb({"items": count}), import_id),
            )


def import_csv_table(con, asset, path):
    """Unknown machine-readable bulletin tables enter review, never financial facts."""
    with path.open(encoding="utf-8-sig", newline="") as stream:
        sample = stream.read(8192)
        stream.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
        cells = []
        for row in csv.reader(stream, dialect):
            if len(cells) >= 5000:
                raise ValueError("split bulletin tables into files of at most 5000 rows")
            cells.append(row)
    add_candidate(con, asset["id"], 1, [0, 0, 1, 1], cells, "csv-v1")


def add_candidate(con, asset_id, page, bbox, cells, version, evidence=None):
    identity = digest([asset_id, page, bbox, version])
    con.execute(
        """INSERT INTO candidates(id,asset_id,page,bbox,cells,version,evidence)
        VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        (identity, asset_id, page, Jsonb(bbox), Jsonb(cells), version, Jsonb(evidence or {})),
    )
    return identity
