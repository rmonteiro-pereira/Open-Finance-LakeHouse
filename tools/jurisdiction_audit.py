"""Stratified private samples of jurisdiction tags for hand-checking, and their precision.

    uv run python tools/jurisdiction_audit.py sample /private/jurisdiction-sample.json --n 100
    uv run python tools/jurisdiction_audit.py assess /private/jurisdiction-sample.json

The sample holds private passage text: it is written once with mode 0600 and never
printed. Reviewers set each row's review.status to "assessed", mark every tag
correct/incorrect/uncertain and list codes the tagger missed. assess prints counts only.
"""

import argparse
import json
import secrets
from pathlib import Path

from ofl.research.jurisdiction_audit import assess, private_write, sample


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    sampler = commands.add_parser("sample")
    sampler.add_argument("output", type=Path, help="new private file (mode 0600)")
    sampler.add_argument("--n", type=int, default=100)
    sampler.add_argument("--source", help="e.g. news or manager_letter")
    sampler.add_argument("--seed", default=None)
    grader = commands.add_parser("assess")
    grader.add_argument("worksheet", type=Path)
    args = parser.parse_args()
    if args.command == "sample":
        from ofl.research.config import ResearchSettings
        from ofl.research.db import Database

        if not 5 <= args.n <= 1000:
            raise SystemExit("--n must be between 5 and 1000")
        result = sample(
            Database(ResearchSettings()), args.n, args.seed or secrets.token_hex(8), args.source
        )
        private_write(args.output, json.loads(json.dumps(result, default=str)))
        print(
            json.dumps(
                {
                    "status": "sampled_unassessed",
                    "rows": len(result["rows"]),
                    "population": result["population"],
                }
            )
        )
    else:
        if args.worksheet.stat().st_size > 64 * 1024**2:
            raise SystemExit("worksheet exceeds 64 MiB bound")
        print(json.dumps(assess(json.loads(args.worksheet.read_text())), indent=2))


if __name__ == "__main__":
    main()
