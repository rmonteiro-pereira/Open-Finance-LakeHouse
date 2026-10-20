"""Benchmark jurisdiction rules against `off`; exit 1 when median overhead exceeds 20%.

    uv run python tools/jurisdiction_latency.py [--cases docs/product/retrieval-cases-heldout.json]
        [--modes keyword,hybrid] [--repetitions 15]

Uses OFL_RESEARCH_* settings; every database session is read-only. Hybrid needs the
pinned embedding model; query embeddings are excluded from timings.
"""

import argparse
import json
import sys
from pathlib import Path

from ofl.research.config import ResearchSettings
from ofl.research.db import Database
from ofl.research.retrieval_latency import measure, read_only

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--cases", type=Path, default=ROOT / "docs/product/retrieval-cases-heldout.json"
    )
    parser.add_argument("--modes", default="keyword,hybrid")
    parser.add_argument("--repetitions", type=int, default=15)
    args = parser.parse_args()
    if not 3 <= args.repetitions <= 200:
        raise SystemExit("--repetitions must be between 3 and 200")
    settings = ResearchSettings()
    settings.database_url = read_only(settings.database_url)
    db = Database(settings)
    passages = db.rows("SELECT count(*) AS n FROM passages WHERE active")[0]["n"]
    cases = json.loads(args.cases.read_text())["cases"]
    report = measure(db, cases, modes=tuple(args.modes.split(",")), repetitions=args.repetitions)
    print(json.dumps({"active_passages": passages, **report}, indent=2))
    sys.exit(0 if report["within_budget"] else 1)


if __name__ == "__main__":
    main()
