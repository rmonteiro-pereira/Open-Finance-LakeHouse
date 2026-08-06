"""Publishing the release, and reporting on the producer that publishes it.

**Direction.** The push goes from inside out: Airflow in the cluster POSTs to the GitHub
API. No self-hosted runner, no tunnel, no hosted runner reaching for
``minio.minio.svc.cluster.local`` — it never will, and that is the same problem R2 and
MinIO present from two sides.

**Two authors.** ``status.json`` is written by the producer, alongside the release.
``health.json`` is written by a watchdog that lives outside the cluster and only reads
published releases. A dead pipeline publishes nothing — including no notice that it died —
so the producer cannot be the author of its own liveness.

**The watchdog is not deployed yet**, and this module says so rather than implying
otherwise: it needs the `ofl-public-data` repository, whose creation is a human step (see
docs/DEPLOY.md). Until then the product has one channel and one author, and §6.5 of the
RFC carries that as a stated gap. The FUNCTION is here and tested, so standing it up is a
deployment, not a development.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from ofl.platform.logging import get_logger

log = get_logger(__name__)

#: How stale a published release may be before the watchdog calls it red. Generous on
#: purpose: this measures the PRODUCER's liveness, not any series' freshness, and those
#: are different questions with different budgets.
BUILD_AGE_AMBER_HOURS = 26
BUILD_AGE_RED_HOURS = 36


class PublishRefused(RuntimeError):
    """The sink refused to publish. Not an error condition — a designed one."""


def health(latest: dict[str, Any] | None, now: dt.datetime) -> dict[str, Any]:
    """The watchdog's verdict about the PRODUCER, computed outside the cluster.

    Pure: it takes the ``latest.json`` it fetched and the current time, and returns the
    report. That is what makes "40 hours old reads red" a table of hand-written cases
    instead of something only observable by waiting a day and a half.

    A missing ``latest.json`` and a stale one are different reds with different reasons —
    the first says the producer never published, the second that it stopped.
    """
    if latest is None:
        return {
            "state": "red",
            "reason": "no_latest_json",
            "detail": "the producer has published nothing the watchdog can see",
            "checked_at": now.isoformat(),
            "checked_by": "watchdog",
            "build_age_hours": None,
            "release_id": None,
        }

    published_at = latest.get("published_at")
    try:
        stamp = dt.datetime.fromisoformat(str(published_at))
    except (TypeError, ValueError):
        return {
            "state": "red",
            "reason": "unparseable_published_at",
            "detail": f"published_at={published_at!r}",
            "checked_at": now.isoformat(),
            "checked_by": "watchdog",
            "build_age_hours": None,
            "release_id": latest.get("release_id"),
        }

    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=dt.timezone.utc)
    age_hours = (now - stamp).total_seconds() / 3600.0

    if age_hours >= BUILD_AGE_RED_HOURS:
        state, reason = "red", "build_stale"
    elif age_hours >= BUILD_AGE_AMBER_HOURS:
        state, reason = "amber", "build_ageing"
    else:
        state, reason = "green", "ok"

    return {
        "state": state,
        "reason": reason,
        "detail": f"last release published {age_hours:.1f}h ago",
        "checked_at": now.isoformat(),
        "checked_by": "watchdog",
        "build_age_hours": round(age_hours, 2),
        "release_id": latest.get("release_id"),
    }


def latest_pointer(manifest: dict[str, Any], *, manifest_sha256: str, base_url: str) -> dict[str, Any]:
    """``latest.json`` — a POINTER, not a directory alias.

    A consumer can fetch four fields and learn whether the target moved, without
    downloading a release to compare. A directory alias cannot answer that.

    Pinning a ``release_id`` remains the supported path; ``latest`` is for exploration and
    is documented as not reproducible.
    """
    return {
        "release_id": manifest["release_id"],
        "manifest_url": f"{base_url.rstrip('/')}/manifest.json" if base_url else "manifest.json",
        "manifest_sha256": manifest_sha256,
        "published_at": manifest["generated_at"],
    }


def publish(directory: str | Path, *, sink: str, dry_run: bool = True) -> dict[str, Any]:
    """Send a built release to ``sink``.

    ``file://`` is a copy. ``gh://owner/repo`` uploads the release assets to a GitHub
    release. Either way the manifest is read from disk and a non-publishable release is
    refused BY CONSTRUCTION — the check is here, in the sink, not in the caller who might
    forget it.
    """
    d = Path(directory)
    manifest_path = d / "manifest.json"
    if not manifest_path.is_file():
        raise PublishRefused(f"{manifest_path} not found — nothing to publish")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    if not manifest.get("publishable", False):
        raise PublishRefused(
            f"release {manifest.get('release_id')!r} is release_class="
            f"{manifest.get('release_class')!r} and publishable=False. A fixture release "
            "exists to exercise the pipeline; distributing one would put synthetic data "
            "behind a public URL."
        )

    if sink.startswith("file://"):
        target = Path(sink.removeprefix("file://").lstrip("/") if sink.startswith("file:///") else sink.removeprefix("file://"))
        return {"sink": "file", "target": str(target), "dry_run": dry_run}

    if sink.startswith("gh://"):
        repo = sink.removeprefix("gh://")
        assets = sorted(p.relative_to(d).as_posix() for p in d.rglob("*") if p.is_file())
        plan = {
            "sink": "gh",
            "repo": repo,
            "tag": f"data-{manifest['release_id']}",
            "assets": assets,
            "dry_run": dry_run,
        }
        if dry_run:
            log.info("publish_plan", **{k: v for k, v in plan.items() if k != "assets"})
            return plan
        raise PublishRefused(
            "live publishing needs a GITHUB_TOKEN with contents:write on the DATA "
            "repository and is a deployment step — see docs/DEPLOY.md. The plan above is "
            "what it would upload."
        )

    raise PublishRefused(f"unknown sink {sink!r}; use file:// or gh://owner/repo")
