"""Reproduce OFL's explicit persona-prioritization hypotheses; not a demand estimator."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
data = json.loads((ROOT / "docs/product/personas.json").read_text())
for profile, weights in data["profiles"].items():
    if sum(weights.values()) != 100:
        raise ValueError(f"{profile}: weights must sum to 100")
    scored = []
    for persona in data["personas"]:
        ratings = persona["ratings"]
        if set(ratings) != set(weights) or any(not 1 <= value <= 5 for value in ratings.values()):
            raise ValueError(f"invalid ratings for {persona['id']}")
        score = sum(weights[key] * ratings[key] / 5 for key in weights)
        scored.append((score, persona))
    scored.sort(key=lambda row: (-row[0], row[1]["id"]))
    print(f"\n{profile}: judgment-based hypotheses, NOT measured demand\n")
    print("| Rank | Persona | Domain | Hypothesis score / 100 |")
    print("| --- | --- | --- | --- |")
    for rank, (score, persona) in enumerate(scored, 1):
        print(f"| {rank} | {persona['name']} | {persona['domain']} | {score:g} |")
