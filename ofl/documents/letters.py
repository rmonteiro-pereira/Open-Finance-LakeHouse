"""Collect manager letters into the archive, and report coverage."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import requests

from ofl.documents import store
from ofl.documents.classify import is_letter
from ofl.documents.collectors import Candidate, list_candidates
from ofl.documents.http import PoliteClient, RobotsDisallowed
from ofl.documents.sources import Manager
from ofl.platform.logging import get_logger

log = get_logger(__name__)

MAX_BYTES = 60 * 1024 * 1024
SAVE_EVERY = 20
# A URL with one of these outcomes is settled; anything else is tried again next run.
FINAL = {"stored", "duplicate", "not_a_letter", "not_a_pdf", "too_large", "robots", "gone"}


def _fetch(client: PoliteClient, url: str) -> tuple[str, bytes | None, str]:
    """Return (status, body, detail) for one document URL."""
    try:
        resp = client.get(url, stream=True)
    except RobotsDisallowed:
        return "robots", None, "disallowed by robots.txt"
    except requests.RequestException as exc:
        return "error", None, type(exc).__name__
    try:
        if resp.status_code in {404, 410}:
            return "gone", None, f"http {resp.status_code}"
        if resp.status_code != 200:
            return "error", None, f"http {resp.status_code}"
        if int(resp.headers.get("content-length") or 0) > MAX_BYTES:
            return "too_large", None, resp.headers["content-length"]
        body = b""
        for chunk in resp.iter_content(1 << 16):
            body += chunk
            if len(body) > MAX_BYTES:
                return "too_large", None, f">{MAX_BYTES}"
    except requests.RequestException as exc:
        return "error", None, type(exc).__name__
    finally:
        resp.close()
    if not body.startswith(b"%PDF"):
        return "not_a_pdf", None, body[:20].decode("latin-1", "replace")
    return "ok", body, ""


def collect_manager(
    manager: Manager,
    *,
    backend: store.Backend,
    client: PoliteClient,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict:
    """Archive what is new for one manager. Returns counts by outcome."""
    manifest = store.load_manifest(backend, manager.id)
    known_hashes = {row["sha256"] for row in manifest.values() if row.get("sha256")}
    counts: dict[str, int] = {}
    fetched = 0

    def note(candidate: Candidate, status: str, **extra) -> None:
        counts[status] = counts.get(status, 0) + 1
        if dry_run:
            return
        manifest[candidate.url] = {
            "url": candidate.url,
            "title": candidate.title,
            "published": candidate.published,
            "status": status,
            "seen_at": datetime.now(UTC).isoformat(timespec="seconds"),
            **extra,
        }

    for collector in manager.collectors:
        try:
            candidates = list(list_candidates(collector, client))
        except (requests.RequestException, RobotsDisallowed, ValueError) as exc:
            log.warning("letters_listing_failed", manager=manager.id, kind=collector.kind, error=repr(exc)[:200])
            counts["listing_failed"] = counts.get("listing_failed", 0) + 1
            continue
        log.info("letters_listed", manager=manager.id, kind=collector.kind, candidates=len(candidates))

        for candidate in candidates:
            if manifest.get(candidate.url, {}).get("status") in FINAL:
                counts["already_seen"] = counts.get("already_seen", 0) + 1
                continue
            keep, reason = is_letter(
                candidate.url, candidate.title, listing_is_letters=candidate.listing_is_letters
            )
            if not keep:
                note(candidate, "not_a_letter", reason=reason)
                continue
            if dry_run:
                note(candidate, "would_fetch")
                continue
            if limit is not None and fetched >= limit:
                counts["deferred"] = counts.get("deferred", 0) + 1
                continue
            fetched += 1
            status, body, detail = _fetch(client, candidate.url)
            if body is None:
                note(candidate, status, reason=detail)
            else:
                sha = hashlib.sha256(body).hexdigest()
                if sha in known_hashes:
                    note(candidate, "duplicate", sha256=sha, bytes=len(body))
                else:
                    backend.write(store.file_key(manager.id, sha), body, "application/pdf")
                    known_hashes.add(sha)
                    note(candidate, "stored", sha256=sha, bytes=len(body), reason=reason)
            if fetched % SAVE_EVERY == 0:
                store.save_manifest(backend, manager.id, manifest)

    if not dry_run:
        store.save_manifest(backend, manager.id, manifest)
    log.info("letters_collected", manager=manager.id, **counts)
    return counts


def coverage(managers: list[Manager], backend: store.Backend) -> list[dict]:
    """One row per manager: what the archive holds for it."""
    rows = []
    for manager in managers:
        manifest = store.load_manifest(backend, manager.id)
        stored = [r for r in manifest.values() if r["status"] == "stored"]
        rows.append(
            {
                "manager": manager.id,
                "urls_seen": len(manifest),
                "letters": len(stored),
                "bytes": sum(r.get("bytes", 0) for r in stored),
                "errors": sum(1 for r in manifest.values() if r["status"] == "error"),
                "last_seen": max((r["seen_at"] for r in manifest.values()), default=""),
            }
        )
    return rows
