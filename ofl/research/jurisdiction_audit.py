"""Stratified private samples of passage jurisdiction tags and per-stratum tag precision.

Samples hold private passage text; callers write them with mode 0600 and never print them.
"""

import json
import os

from .db import digest
from .jurisdiction import VERSION

STRATA = ("BR", "US", "both", "none", "other")
CODES = {"BR", "US", "EA", "CN"}
VERDICTS = {"correct", "incorrect", "uncertain", "not_assessed"}
CAPTURED = (
    "passage_id",
    "item_id",
    "source",
    "title",
    "text",
    "stratum",
    "jurisdiction",
    "evidence",
)
STRATUM_SQL = """CASE WHEN p.jurisdiction @> ARRAY['BR','US'] THEN 'both'
    WHEN 'BR'=ANY(p.jurisdiction) THEN 'BR' WHEN 'US'=ANY(p.jurisdiction) THEN 'US'
    WHEN cardinality(p.jurisdiction)=0 THEN 'none' ELSE 'other' END"""


def stratum(codes):
    codes = set(codes)
    if {"BR", "US"} <= codes:
        return "both"
    if "BR" in codes or "US" in codes:
        return "BR" if "BR" in codes else "US"
    return "other" if codes else "none"


def quotas(n):
    base, extra = divmod(n, len(STRATA))
    return {name: base + (index < extra) for index, name in enumerate(STRATA)}


def sample(db, n, seed, source=None):
    with db.connect() as con:
        con.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        scope = "p.active AND p.jurisdiction_tagger=%s" + (" AND i.source=%s" if source else "")
        params = (VERSION, source) if source else (VERSION,)
        population = {
            row["stratum"]: row["n"]
            for row in con.execute(
                f"""SELECT {STRATUM_SQL} AS stratum,count(*) AS n FROM passages p
                JOIN items i ON i.id=p.item_id WHERE {scope} GROUP BY 1""",
                params,
            ).fetchall()
        }
        rows = []
        for name, quota in quotas(n).items():
            rows += con.execute(
                f"""SELECT p.id AS passage_id,p.item_id,i.source,i.title,p.text,
                {STRATUM_SQL} AS stratum,p.jurisdiction,p.jurisdiction_evidence AS evidence
                FROM passages p JOIN items i ON i.id=p.item_id
                WHERE {scope} AND {STRATUM_SQL}=%s ORDER BY md5(p.id||%s) LIMIT %s""",
                (*params, name, seed, quota),
            ).fetchall()
    return {
        "version": 1,
        "tagger": VERSION,
        "seed": seed,
        "source": source,
        "requested": n,
        "population": {name: population.get(name, 0) for name in STRATA},
        "rows": [
            {
                **{key: row[key] for key in CAPTURED},
                "capture_sha256": digest({key: row[key] for key in CAPTURED}),
                "review": {
                    "status": "not_assessed",
                    "tags": {code: "not_assessed" for code in row["jurisdiction"]},
                    "missed": [],
                    "note": "",
                },
            }
            for row in rows
        ],
        "qualification": "Private sample for hand-checking lexical jurisdiction evidence. "
        "Seeded random order within each stratum; strata are not population-weighted.",
    }


def assess(sheet):
    """Per-stratum tag precision over assessed rows; uncertain verdicts are not negatives."""
    if sheet.get("version") != 1 or not isinstance(sheet.get("rows"), list):
        raise ValueError("unsupported audit worksheet")
    report = {
        name: {
            "rows": 0,
            "assessed_rows": 0,
            "correct": 0,
            "incorrect": 0,
            "uncertain": 0,
            "rows_with_missed_tags": 0,
        }
        for name in STRATA
    }
    for row in sheet["rows"]:
        if digest({key: row.get(key) for key in CAPTURED}) != row.get("capture_sha256"):
            raise ValueError("captured passage or tags changed after sampling")
        if row["stratum"] != stratum(row["jurisdiction"]):
            raise ValueError("row stratum does not match its tags")
        review = row.get("review") or {}
        verdicts = review.get("tags")
        missed = review.get("missed")
        if (
            review.get("status") not in {"assessed", "not_assessed"}
            or not isinstance(verdicts, dict)
            or set(verdicts) != set(row["jurisdiction"])
            or not set(verdicts.values()) <= VERDICTS
            or not isinstance(missed, list)
            or not set(missed) <= CODES - set(row["jurisdiction"])
        ):
            raise ValueError("invalid review: tags must match, verdicts and missed codes bounded")
        counts = report[row["stratum"]]
        counts["rows"] += 1
        if review["status"] != "assessed":
            continue
        if "not_assessed" in verdicts.values():
            raise ValueError("an assessed row needs a verdict for every tag")
        counts["assessed_rows"] += 1
        counts["rows_with_missed_tags"] += bool(missed)
        for verdict in verdicts.values():
            counts[verdict] += 1
    for counts in report.values():
        judged = counts["correct"] + counts["incorrect"]
        counts["tag_precision"] = counts["correct"] / judged if judged else None
        counts["missed_rate"] = (
            counts["rows_with_missed_tags"] / counts["assessed_rows"]
            if counts["assessed_rows"]
            else None
        )
    return {
        "tagger": sheet.get("tagger"),
        "worksheet_sha256": digest(sheet),
        "strata": report,
        "qualification": "Precision over correct/incorrect verdicts only; uncertain and "
        "unassessed tags are excluded. The none stratum measures misses, not precision.",
    }


def private_write(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, ensure_ascii=False, default=str, indent=2)
        stream.write("\n")
