"""Prove the silver job is idempotent under ``Trigger.AvailableNow`` — by measuring it.

    snapshot  ->  run 1 (AvailableNow)  ->  snapshot  ->  run 2 (AvailableNow)  ->  snapshot

The claim being tested is narrow and falsifiable: **two consecutive AvailableNow runs
over an unchanged source leave the silver table with identical row counts and
identical distinct-key counts.** Run 1 may write bars (it drains whatever bronze has
accumulated); run 2 must write none, because the checkpoint already records those
offsets and the watermark cannot advance without new event time.

Three choices make this evidence rather than decoration:

1. **Each run is a separate OS process.** A second call inside one Python process
   would reuse a warm ``SparkSession`` and could pass on in-memory state. Forking a
   fresh JVM means run 2 knows nothing except what the *checkpoint on disk* tells
   it — which is the thing under test.
2. **The counts are read with delta-rs, not with Spark.** The writer is not allowed
   to be the witness. ``ofl.streaming.metrics.table_metrics`` opens the Delta log
   independently, after the JVM has exited.
3. **The comparison is on the table, not on the run.** Run 1 and run 2 *should*
   differ in batches, input rows and throughput. Requiring those to match would be
   testing the wrong thing; requiring the table to match is testing the right one.

Exit code is 0 only when the two post-run states agree. Anything else — a mismatch,
a failed subprocess, a missing table — is a non-zero exit, so this is usable as a
CI gate and not only as a demo.

Usage::

    python tools/streaming_idempotence.py                       # two runs, prints a transcript
    python tools/streaming_idempotence.py --runs 3 --out t.txt  # more runs, saved transcript
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from ofl.streaming.metrics import (  # noqa: E402 — after sys.path, deliberately
    IDEMPOTENT_FIELDS,
    compare_states,
    silver_table_exists,
    table_metrics,
)

RULE = "=" * 78


def run_silver_available_now(label: str, *, window: str, watermark: str) -> dict:
    """One ``ofl stream-silver --available-now`` in its own process."""
    cmd = [
        sys.executable,
        "-m",
        "ofl.cli",
        "stream-silver",
        "--available-now",
        "--window",
        window,
        "--watermark",
        watermark,
        "--snapshot",
        f"silver-{label}",
    ]
    started = time.monotonic()
    proc = subprocess.run(  # noqa: S603 — fixed argv, no shell
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        # Explicit, because the transcript is committed: the default on Windows is
        # the console codepage, which mangles the job's UTF-8 log lines.
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return {
        "label": label,
        "returncode": proc.returncode,
        "elapsed_s": round(time.monotonic() - started, 1),
        # structlog renders JSON lines to stdout; Spark's own JVM chatter goes to
        # stderr and is only worth keeping when something failed.
        "log": [ln for ln in proc.stdout.splitlines() if _is_interesting(ln)],
        "stderr_tail": proc.stderr.splitlines()[-10:] if proc.returncode else [],
    }


def _is_interesting(line: str) -> bool:
    return any(
        marker in line
        for marker in (
            "silver_stream_started",
            "micro_batch",
            "last_progress",
            "silver_stream_stopped",
            "stream_silver_done",
            "metrics_snapshot",
        )
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--runs", type=int, default=2, help="consecutive AvailableNow runs")
    ap.add_argument("--window", default="1 minute")
    ap.add_argument("--watermark", default="2 minutes")
    ap.add_argument("--out", type=Path, help="also write the transcript here")
    args = ap.parse_args(argv)
    # The transcript is committed as documentation, so it is written UTF-8 either
    # way; without this the *console* copy is mangled on a cp1252 Windows terminal.
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    if not silver_table_exists():
        print("silver table does not exist yet — run `ofl stream-silver` once first")
        return 2

    lines: list[str] = [
        RULE,
        "streaming idempotence check — Trigger.AvailableNow, one process per run",
        f"started {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"window={args.window}  watermark={args.watermark}  runs={args.runs}",
        RULE,
        "",
        "--- before any run ---------------------------------------------------------",
        _fmt_state(table_metrics()),
        "",
    ]

    states: list[dict] = []
    failed = False
    for i in range(1, args.runs + 1):
        result = run_silver_available_now(f"availablenow-run{i}", window=args.window, watermark=args.watermark)
        state = table_metrics()
        states.append(state)
        lines += [
            f"--- run {i} — ofl stream-silver --available-now ------------------------------",
            f"exit={result['returncode']}  elapsed={result['elapsed_s']}s",
            *result["log"],
            "",
            f"--- silver table after run {i} ----------------------------------------------",
            _fmt_state(state),
            "",
        ]
        if result["returncode"] != 0:
            failed = True
            lines += [f"!! run {i} exited {result['returncode']}", *result["stderr_tail"], ""]

    diff = {}
    for i in range(1, len(states)):
        diff.update(compare_states(states[0], states[i]))

    lines += [
        RULE,
        "verdict",
        RULE,
        f"compared fields: {', '.join(IDEMPOTENT_FIELDS)}",
    ]
    if diff or failed:
        lines += [f"NOT IDEMPOTENT — {json.dumps(diff)}" if diff else "RUN FAILED"]
    else:
        first = states[0]
        lines += [
            f"IDEMPOTENT — {args.runs} consecutive AvailableNow runs left the table identical",
            f"  rows          {first['rows']}  (all runs)",
            f"  distinct keys {first['distinct_keys']}  (all runs)",
            f"  duplicate keys {first['duplicate_keys']}",
            f"  total trades  {first['total_trades']}",
        ]
    lines.append("")

    transcript = "\n".join(lines)
    print(transcript)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(transcript, encoding="utf-8")
    return 1 if (diff or failed) else 0


def _fmt_state(state: dict) -> str:
    return json.dumps(state, indent=2, default=str)


if __name__ == "__main__":
    raise SystemExit(main())
