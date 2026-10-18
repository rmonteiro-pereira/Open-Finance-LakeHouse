"""Public-safe descriptive finance demonstration; no external sources or advice."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ofl.research.analytics import portfolio

if __name__ == "__main__":
    source = Path(__file__).resolve().parents[1] / "docs/product/examples/portfolio.json"
    result = portfolio(json.loads(source.read_text()))
    assert result["total_value"] == "10000.00"
    assert result["scenarios"][0]["projected_value"] == "8800.00"
    print(
        json.dumps(
            {
                "scope": "synthetic descriptive calculation, not investment advice or a customer result",
                "result": result,
            },
            indent=2,
        )
    )
