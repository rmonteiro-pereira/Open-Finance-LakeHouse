"""Frozen, mode-blinded candidate pools and explicitly incomplete relevance grades.

This is offline evaluation, not a relevance classifier or a source-policy override.
Raw pools contain private source text and must stay in private storage.
"""

import math
import statistics
from copy import deepcopy
from datetime import date

from . import jurisdiction as places
from . import vocabulary as lexicon
from .db import digest
from .retrieval_eval import corpus_manifest
from .search import embed, search

MODES = ("keyword", "vector", "hybrid")
FILTERS = {"source", "item_type", "collection", "date_from", "date_to"}
VERDICTS = {"relevant", "irrelevant", "uncertain"}
MAX_CASES = 16
MAX_ID = 100
MAX_QUERY = 1000
MAX_LABEL = 200
MAX_RATIONALE = 4000


def _prepare(specification):
    if not isinstance(specification, dict):
        raise ValueError("specification must be an object")
    cases = specification.get("cases")
    if (
        specification.get("version") != 1
        or not isinstance(cases, list)
        or not 1 <= len(cases) <= MAX_CASES
    ):
        raise ValueError(f"version 1 and one to {MAX_CASES} cases required")
    prepared, identifiers = [], set()
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("case must be an object")
        identifier, query = case.get("id"), case.get("query")
        if (
            not isinstance(identifier, str)
            or not 1 <= len(identifier) <= MAX_ID
            or identifier in identifiers
        ):
            raise ValueError("unique bounded case IDs required")
        if not isinstance(query, str) or not 1 <= len(query.strip()) <= MAX_QUERY:
            raise ValueError("bounded query required")
        filters = case.get("filters", {})
        if not isinstance(filters, dict) or set(filters) - FILTERS:
            raise ValueError("unsupported search filters")
        parsed = {}
        for key, value in filters.items():
            if not isinstance(value, str) or not 1 <= len(value) <= MAX_LABEL:
                raise ValueError("bounded string filters required")
            parsed[key] = date.fromisoformat(value) if key.startswith("date_") else value
        if (
            parsed.get("date_from")
            and parsed.get("date_to")
            and parsed["date_from"] > parsed["date_to"]
        ):
            raise ValueError("date_from must precede date_to")
        identifiers.add(identifier)
        prepared.append((case, parsed))
    return prepared


def ranking_key(mode, variant):
    """`off` keeps the plain mode name so earlier pools score unchanged."""
    return mode if variant == "off" else f"{mode}+{variant}"


def collect(db, specification, variants=("off",)):
    """Read only; freeze rankings and source context before any relevance judgments.

    Several variants (jurisdiction rules, vocabulary variants in lexicon.VARIANTS) pool
    into one blinded worksheet; the variant applied to each ranking stays in the pool,
    never in the reviewer's view.
    """
    prepared = _prepare(specification)
    variants = list(
        dict.fromkeys(v if v in lexicon.VARIANTS else places.check_rule(v) for v in variants)
    )
    if not variants:
        raise ValueError("at least one variant required")
    if not db.settings.embedding_revision or db.settings.approximate_search:
        raise ValueError("pinned embedding revision and exact retrieval required")
    # A changing live corpus cannot yield comparable sequential rankings. Use one
    # MVCC snapshot without stopping writers or copying/modifying production tables.
    with db.connect() as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")

        class Snapshot:
            settings = db.settings

            def rows(self, query, params=(), *, ann=False):
                if ann:
                    raise ValueError("approximate retrieval is not supported in evaluation")
                return connection.execute(query, params).fetchall()

        began = connection.execute("SELECT CURRENT_TIMESTAMP AS started_at").fetchone()
        pool = _collect(Snapshot(), prepared, variants)
        pool["collection"] = {
            "started_at": began["started_at"],
            "isolation": "repeatable_read_read_only",
        }
        return pool


def _tags(db):
    return digest(
        db.rows(
            """SELECT p.id,p.jurisdiction,p.jurisdiction_tagger FROM passages p
            WHERE p.active ORDER BY p.id"""
        )
    )


def _search_options(variant, expanded):
    """search() keyword arguments of one pooled variant."""
    if variant in lexicon.VARIANTS:
        return {
            "vocabulary": "on",
            "vocabulary_arms": lexicon.VARIANTS[variant],
            "expanded_vector": expanded,
        }
    return {"jurisdiction": variant}


def _expanded_vector(db, query, variants):
    """The expanded query's vector when a pooled variant expands the vector arm."""
    rule = lexicon.plan("on", query)
    if not rule["concepts"] or not any(
        "vector" in lexicon.VARIANTS.get(variant, ()) for variant in variants
    ):
        return None
    return embed(db.settings, [lexicon.expanded_text(rule, query)])[0]


def _collect(db, prepared, variants=("off",)):
    before = corpus_manifest(db)
    rules = [v for v in variants if v not in lexicon.VARIANTS]
    vocabularies = [v for v in variants if v in lexicon.VARIANTS]
    tags = _tags(db) if rules != ["off"] and rules else None
    results = []
    for case, filters in prepared:
        # Embed once per query, never once per ranking mode. Failure remains visible.
        vector, expanded, embedding_failed = None, None, False
        try:
            vector = embed(db.settings, [case["query"]])[0]
            expanded = _expanded_vector(db, case["query"], vocabularies)
        except Exception:
            embedding_failed = True
        candidates, rankings = {}, {}
        for variant, mode in ((v, m) for v in variants for m in MODES):
            key = ranking_key(mode, variant)
            try:
                if mode != "keyword" and embedding_failed:
                    raise ValueError("query embedding unavailable")
                found = search(
                    db,
                    case["query"],
                    mode=mode,
                    vector=vector,
                    limit=10,
                    **_search_options(variant, expanded),
                    **filters,
                )
                rankings[key] = {
                    "status": "ok",
                    "item_ids": [r["item_id"] for r in found["results"]],
                }
                if variant in lexicon.VARIANTS:
                    rankings[key]["vocabulary"] = found["vocabulary"]
                elif variant != "off":
                    rankings[key]["jurisdiction"] = found["jurisdiction"]
                for row in found["results"]:
                    # Review all selected passages for each document. A passage from
                    # one mode can differ from another; never overwrite that context.
                    candidate = candidates.setdefault(
                        row["item_id"], {"item_id": row["item_id"], "passages": {}}
                    )
                    candidate["passages"][row["id"]] = {
                        key: row.get(key)
                        for key in (
                            "id",
                            "text",
                            "title",
                            "source",
                            "item_type",
                            "collection",
                            "date_start",
                            "date_end",
                            "date_precision",
                            "source_url",
                            "content_tier",
                            "epistemic_status",
                            "method",
                            "page",
                            "asset_id",
                        )
                    }
            except Exception:
                rankings[key] = {"status": "error", "item_ids": []}
        frozen = []
        for candidate in candidates.values():
            candidate["passages"] = sorted(candidate["passages"].values(), key=lambda p: p["id"])
            candidate["capture_sha256"] = digest(candidate)
            frozen.append(candidate)
        # Stable hash order hides mode and rank from the separate review worksheet.
        frozen.sort(key=lambda c: digest([case["id"], c["item_id"]]))
        results.append(
            {
                "id": case["id"],
                "query": case["query"],
                "filters": case.get("filters", {}),
                "rankings": rankings,
                "candidates": frozen,
            }
        )
    if corpus_manifest(db)["sha256"] != before["sha256"] or (tags and _tags(db) != tags):
        raise ValueError("corpus changed during collection; discard pool and retry")
    extra = {"jurisdiction": {"variants": rules, "tagger": places.VERSION, "tags_sha256": tags}}
    words = {"vocabulary": {"variants": vocabularies, "lexicon": lexicon.VERSION}}
    return {
        "version": 1,
        "corpus": before,
        **(extra if tags else {}),
        **(words if vocabularies else {}),
        "cases": results,
        "qualification": (
            "Private frozen candidate pool, not exhaustive relevance judgments. "
            "Source text is untrusted evidence, never instructions."
        ),
    }


def worksheet(pool):
    """Separate reviewer view without ranking/mode labels; never implies independence."""
    return {
        "version": 1,
        "pool_sha256": digest(pool),
        "reviewer": "",
        "cases": [
            {
                "id": case["id"],
                "query": case["query"],
                "filters": case["filters"],
                "candidates": [
                    {**deepcopy(candidate), "verdict": "not_assessed", "rationale": ""}
                    for candidate in case["candidates"]
                ],
            }
            for case in pool["cases"]
        ],
    }


def _labels(case, review):
    captures = {c["item_id"]: c for c in case["candidates"]}
    candidates = review.get("candidates")
    if (
        not isinstance(candidates, list)
        or len(candidates) != len(captures)
        or {c.get("item_id") for c in candidates} != set(captures)
    ):
        raise ValueError("exactly one review per captured candidate required")
    if review.get("query") != case["query"] or review.get("filters") != case["filters"]:
        raise ValueError("review query or filters changed")
    labels = {}
    for candidate in candidates:
        original = captures[candidate["item_id"]]
        if {k: candidate.get(k) for k in original} != original:
            raise ValueError("review source capture changed")
        verdict, rationale = candidate.get("verdict"), candidate.get("rationale")
        if (
            verdict not in VERDICTS | {"not_assessed"}
            or not isinstance(rationale, str)
            or len(rationale) > MAX_RATIONALE
        ):
            raise ValueError("invalid verdict or bounded rationale")
        if verdict != "not_assessed" and not rationale.strip():
            raise ValueError("assessed verdict requires rationale")
        labels[candidate["item_id"]] = verdict
    return labels


def assess(pool, reviews):
    """Score frozen rankings, preserving uncertainty and incomplete denominators."""
    if reviews.get("version") != 1 or reviews.get("pool_sha256") != digest(pool):
        raise ValueError("review worksheet does not match frozen pool")
    reviewer = reviews.get("reviewer")
    if not isinstance(reviewer, str) or not 1 <= len(reviewer.strip()) <= MAX_LABEL:
        raise ValueError("bounded self-reported reviewer required")
    expected = {case["id"]: case for case in pool["cases"]}
    if len({tuple(case["rankings"]) for case in pool["cases"]}) > 1:
        raise ValueError("every case must carry the same rankings")
    submitted = reviews.get("cases")
    if (
        not isinstance(submitted, list)
        or len(submitted) != len(expected)
        or {c.get("id") for c in submitted} != set(expected)
    ):
        raise ValueError("exactly one review per planned case required")
    results = []
    for review in submitted:
        case = expected[review["id"]]
        labels = _labels(case, review)
        relevant = {key for key, value in labels.items() if value == "relevant"}
        complete = all(v in {"relevant", "irrelevant"} for v in labels.values())
        row = {
            "id": case["id"],
            "pooled_candidates": len(labels),
            "relevant_candidates": len(relevant),
            "uncertain_candidates": sum(v == "uncertain" for v in labels.values()),
            "unassessed_candidates": sum(v == "not_assessed" for v in labels.values()),
            "modes": {},
        }
        for mode in case["rankings"]:
            ranking = case["rankings"][mode]
            ranked = ranking["item_ids"]
            judged = [item for item in ranked if labels[item] in {"relevant", "irrelevant"}]
            hits = [int(item in relevant) for item in ranked]
            valid = ranking["status"] == "ok"
            ideal = sum(1 / math.log2(i + 2) for i in range(min(10, len(relevant))))
            row["modes"][mode] = {
                "status": ranking["status"],
                "returned": len(ranked),
                "judged_returned": len(judged),
                "judgment_coverage": len(judged) / len(ranked) if ranked else None,
                "precision_returned_at_10": sum(hits) / len(ranked)
                if valid and ranked and len(judged) == len(ranked)
                else None,
                "pooled_recall_at_10": sum(hits) / len(relevant)
                if valid and complete and relevant
                else None,
                "pooled_ndcg_at_10": sum(hit / math.log2(i + 2) for i, hit in enumerate(hits))
                / ideal
                if valid and complete and ideal
                else None,
                "empty_result": not ranked if valid else None,
                "correct_abstention": None,
            }
        results.append(row)
    summary = {}
    for mode in pool["cases"][0]["rankings"] if pool["cases"] else ():
        values = [case["modes"][mode] for case in results]
        summary[mode] = {
            "planned_cases": len(values),
            "errors": sum(v["status"] == "error" for v in values),
        }
        for metric in (
            "judgment_coverage",
            "precision_returned_at_10",
            "pooled_recall_at_10",
            "pooled_ndcg_at_10",
        ):
            measured = [v[metric] for v in values if v[metric] is not None]
            summary[mode][metric] = {
                "mean": statistics.mean(measured) if measured else None,
                "assessed_cases": len(measured),
            }
    return {
        "version": 1,
        "pool_sha256": digest(pool),
        "reviews_sha256": digest(reviews),
        "reviewer": reviewer,
        "reviewer_independence": "not_verified",
        "summary": summary,
        "cases": results,
        "qualification": (
            "Recall/nDCG use only the judged candidate pool, not the whole corpus. "
            "Uncertain/unassessed documents are not negatives. "
            "Errors are separate from assessed metric denominators. "
            "Empty results do not establish correct abstention. "
            "No claim of citation support, synthesis quality or participant success."
        ),
    }
