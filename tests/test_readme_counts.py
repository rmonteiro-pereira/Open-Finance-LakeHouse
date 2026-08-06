"""The README's numbers, checked against the registry that produces them.

A stale number in a README is not a cosmetic defect. A portfolio audit put it exactly
right: once one published figure is wrong, no other figure in the document can be
trusted. This project's whole argument is that a number should carry its provenance, so
its own front page is the last place to leave a hand-maintained count.
"""

import re
from collections import Counter
from pathlib import Path

from ofl.providers import load_providers
from ofl.registry import load_registry

README = Path(__file__).resolve().parent.parent / "README.md"


def _actual_counts() -> Counter:
    providers = load_providers()
    counts: Counter = Counter()
    for series in load_registry().series.values():
        entry = providers.get(series.handler)
        counts[entry.state if entry else "unverified"] += 1
    return counts


def _claimed(state: str) -> int:
    """Pull `| `state` | **N** |` out of the licence table."""
    row = re.search(rf"\|\s*`{state}`\s*\|\s*\*\*(\d+)\*\*\s*\|", README.read_text(encoding="utf-8"))
    assert row, f"the README has no licence row for {state!r}"
    return int(row.group(1))


def test_the_licence_table_matches_the_registry():
    actual = _actual_counts()
    for state in ("open", "restricted", "unverified"):
        assert _claimed(state) == actual[state], (
            f"README claims {_claimed(state)} {state} series; the registry and "
            f"providers.yml produce {actual[state]}"
        )


def test_the_total_still_matches_the_headline():
    text = README.read_text(encoding="utf-8")
    headline = re.search(r"\*\*(\d+) registered series", text)
    assert headline
    assert int(headline.group(1)) == len(load_registry().series)


def test_the_states_partition_the_registry():
    """No series may fall outside the three states — an unclassified one would be
    invisible to the very table that decides what may be published."""
    actual = _actual_counts()
    assert sum(actual.values()) == len(load_registry().series)
    assert set(actual) <= {"open", "restricted", "unverified"}
