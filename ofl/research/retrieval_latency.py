"""Read-only latency of jurisdiction rules against `off` on named-jurisdiction queries.

Query embeddings are computed once per query and excluded from timings. Rules are
interleaved in rotating order on every repetition so drift affects all of them alike.
Reports counts and milliseconds only, never source text.
"""

import statistics
import time
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from . import jurisdiction as places
from .search import embed, search

RULES = ("off", "boost", "exclude")
BUDGET = 0.20


def read_only(url):
    """Every session of the run refuses writes."""
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query))
    options = query.get("options", "")
    query["options"] = (options + " -c default_transaction_read_only=on").strip()
    # libpq does not decode '+' as a space.
    return urlunsplit(parsed._replace(query=urlencode(query, quote_via=quote)))


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, round(fraction * (len(ordered) - 1))))]


def measure(db, cases, *, modes=("keyword", "hybrid"), repetitions=15, warmup=2):
    named = [case for case in cases if places.detect(case["query"])]
    if not named:
        raise ValueError("no case names a jurisdiction")
    vectors = {}
    if any(mode != "keyword" for mode in modes):
        vectors = {c["query"]: embed(db.settings, [c["query"]])[0] for c in named}
    timings = {mode: {rule: [] for rule in RULES} for mode in modes}
    per_case = {mode: {c["id"]: {rule: [] for rule in RULES} for c in named} for mode in modes}
    for repetition in range(warmup + repetitions):
        order = RULES[repetition % 3 :] + RULES[: repetition % 3]
        for mode in modes:
            for case in named:
                for rule in order:
                    started = time.perf_counter()
                    search(
                        db,
                        case["query"],
                        mode=mode,
                        vector=vectors.get(case["query"]),
                        limit=10,
                        jurisdiction=rule,
                        **case.get("filters", {}),
                    )
                    elapsed = (time.perf_counter() - started) * 1000
                    if repetition >= warmup:
                        timings[mode][rule].append(elapsed)
                        per_case[mode][case["id"]][rule].append(elapsed)
    report = {"cases": [c["id"] for c in named], "repetitions": repetitions, "modes": {}}
    passing = True
    for mode in modes:
        base = statistics.median(timings[mode]["off"])
        rows = {}
        for rule in RULES:
            values = timings[mode][rule]
            median = statistics.median(values)
            rows[rule] = {
                "median_ms": round(median, 1),
                "p95_ms": round(percentile(values, 0.95), 1),
                "median_overhead": round(median / base - 1, 3),
            }
            if rule != "off":
                passing &= median / base - 1 <= BUDGET
        rows["per_case_median_overhead"] = {
            case: {
                rule: round(
                    statistics.median(values[rule]) / statistics.median(values["off"]) - 1, 3
                )
                for rule in RULES[1:]
            }
            for case, values in per_case[mode].items()
        }
        report["modes"][mode] = rows
    report["budget_median_overhead"] = BUDGET
    report["within_budget"] = passing
    return report
