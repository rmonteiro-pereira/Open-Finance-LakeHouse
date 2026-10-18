"""Grade private, manually reviewed artifacts against every planned case/condition."""
import argparse
import json
from pathlib import Path
from ofl.research.outcomes import assess

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("protocol", type=Path)
    parser.add_argument("trials", type=Path)
    parser.add_argument("assessments", type=Path)
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(assess(json.loads(args.protocol.read_text()), json.loads(args.trials.read_text())["trials"], json.loads(args.assessments.read_text()), args.artifacts), ensure_ascii=False, indent=2))
