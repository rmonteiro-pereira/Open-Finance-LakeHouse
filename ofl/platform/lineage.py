"""OpenLineage emission — optional, env-gated.

If ``OPENLINEAGE_URL`` is set (e.g. the OpenMetadata OL endpoint), each CLI job
emits START/COMPLETE/FAIL run events. Otherwise it's a no-op, so local runs and
tests never depend on a lineage backend.
"""

from __future__ import annotations

import contextlib
import os
from datetime import datetime, timezone

from ofl.platform.logging import get_logger

log = get_logger(__name__)


@contextlib.contextmanager
def emit_run(job: str, *, namespace: str = "ofl"):
    url = os.getenv("OPENLINEAGE_URL")
    if not url:
        yield
        return

    try:
        from openlineage.client import OpenLineageClient
        from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
        from openlineage.client.uuid import generate_new_uuid
    except Exception as exc:  # noqa: BLE001 - lineage is best-effort
        log.warning("openlineage_unavailable", error=str(exc))
        yield
        return

    client = OpenLineageClient(url=url)
    run = Run(runId=str(generate_new_uuid()))
    job_ref = Job(namespace=namespace, name=job)

    def event(state: "RunState") -> "RunEvent":
        return RunEvent(
            eventType=state,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=run,
            job=job_ref,
            producer="https://github.com/rmonteiro-pereira/Open-Finance-LakeHouse",
        )

    client.emit(event(RunState.START))
    try:
        yield
    except Exception:
        with contextlib.suppress(Exception):
            client.emit(event(RunState.FAIL))
        raise
    with contextlib.suppress(Exception):
        client.emit(event(RunState.COMPLETE))
