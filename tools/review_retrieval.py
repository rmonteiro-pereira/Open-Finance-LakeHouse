"""Collect private real-corpus candidate pools; grade manually edited worksheets."""

import argparse
import json
import os
from pathlib import Path

from ofl.research.config import ResearchSettings
from ofl.research.db import Database
from ofl.research.retrieval_review import assess, collect, worksheet


def read(path):
    if path.stat().st_size > 32 * 1024**2:
        raise ValueError("input exceeds 32 MiB bound")
    return json.loads(path.read_text())


def private_write(path, value):
    # Refuse overwrites/symlinks; no credentials or raw source text on stdout.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, ensure_ascii=False, default=str, indent=2)
        stream.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    collector = commands.add_parser("collect")
    collector.add_argument("specification", type=Path)
    collector.add_argument("output", type=Path, help="new private directory")
    collector.add_argument(
        "--jurisdiction",
        default="off",
        help="comma-separated rules pooled into one blinded worksheet, e.g. off,boost,exclude",
    )
    grader = commands.add_parser("assess")
    grader.add_argument("pool", type=Path)
    grader.add_argument("reviews", type=Path)
    grader.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.command == "collect":
        args.output.mkdir(mode=0o700, parents=False, exist_ok=False)
        # Normalize dates before hashing/export so reopening yields identical bytes' meaning.
        pool = json.loads(
            json.dumps(
                collect(
                    Database(ResearchSettings()),
                    read(args.specification),
                    args.jurisdiction.split(","),
                ),
                default=str,
            )
        )
        private_write(args.output / "pool.json", pool)
        private_write(args.output / "reviews.json", worksheet(pool))
        print(json.dumps({"status": "collected_unassessed", "cases": len(pool["cases"])}))
    else:
        private_write(args.output, assess(read(args.pool), read(args.reviews)))
        print(
            json.dumps({"status": "assessment_recorded", "reviewer_independence": "not_verified"})
        )


if __name__ == "__main__":
    main()
