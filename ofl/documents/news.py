"""Collect news items from feeds into the archive.

What is kept depends on the source's tier (``sources/news.yml``):

``official``        public bodies; everything the feed carries.
``feed_fulltext``   outlets that put the full text in their own feed and whose terms do
                    not forbid automated collection (as far as was checked).
``headline_only``   outlets whose terms forbid it: title, link, date, categories and a
                    short summary. The body is dropped even when the feed carries it.

Layout::

    news/items/<source>/<YYYY-MM-DD>/<run>.jsonl   only the items new in that run
    news/state/<source>.json                       ids already archived (most recent first)
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import requests
import yaml
from pydantic import BaseModel

from ofl.documents import store
from ofl.documents.feeds import parse_feed, to_text
from ofl.documents.http import PoliteClient, RobotsDisallowed
from ofl.documents.sources import sources_dir
from ofl.platform.logging import get_logger

log = get_logger(__name__)

SUMMARY_CHARS = 300
STATE_KEEP = 5000


class NewsSource(BaseModel):
    id: str
    name: str
    tier: Literal["official", "feed_fulltext", "headline_only"]
    url: str
    enabled: bool = True


def load_sources(path: Path | None = None) -> list[NewsSource]:
    data = yaml.safe_load((path or sources_dir() / "news.yml").read_text(encoding="utf-8"))
    sources = [NewsSource(**s) for s in data["sources"]]
    ids = [s.id for s in sources]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate source id in news.yml")
    return sources


def _shape(raw: dict, source: NewsSource, now: str) -> dict:
    """Apply the tier: decide what of a feed item is kept."""
    summary = to_text(raw["summary_html"])
    body = to_text(raw["body_html"]) or summary
    row = {
        "id": hashlib.sha256(raw["link"].encode()).hexdigest()[:24],
        "source": source.id,
        "tier": source.tier,
        "title": raw["title"],
        "link": raw["link"],
        "published": raw["published"],
        "author": raw["author"],
        "categories": raw["categories"],
        "fetched_at": now,
    }
    if source.tier == "headline_only":
        row["summary"] = summary[:SUMMARY_CHARS]
    else:
        row["summary"] = summary[:SUMMARY_CHARS] if len(body) > len(summary) else ""
        row["body"] = body
    return row


def collect_source(source: NewsSource, *, backend: store.Backend, client: PoliteClient, now: datetime | None = None) -> dict:
    now = now or datetime.now(UTC)
    stamp = now.isoformat(timespec="seconds")
    try:
        resp = client.get(source.url)
        resp.raise_for_status()
        raw_items = parse_feed(resp.content)
    except (requests.RequestException, RobotsDisallowed, ValueError, SyntaxError) as exc:
        log.warning("news_feed_failed", source=source.id, error=repr(exc)[:200])
        return {"failed": 1}

    state_key = f"news/state/{source.id}.json"
    seen: list[str] = json.loads(backend.read(state_key) or b"[]")
    seen_set = set(seen)
    fresh = []
    for raw in raw_items:
        row = _shape(raw, source, stamp)
        if row["id"] not in seen_set:
            seen_set.add(row["id"])
            fresh.append(row)
    if fresh:
        key = f"news/items/{source.id}/{now:%Y-%m-%d}/{now:%H%M%S}.jsonl"
        body = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in fresh)
        backend.write(key, body.encode("utf-8"), "application/x-ndjson")
        seen = ([row["id"] for row in fresh] + seen)[:STATE_KEEP]
        backend.write(state_key, json.dumps(seen).encode(), "application/json")
    counts = {"in_feed": len(raw_items), "new": len(fresh)}
    log.info("news_collected", source=source.id, tier=source.tier, **counts)
    return counts
