"""Judged document retrieval on a frozen corpus; no database or source writes.

This measures keyword/vector/hybrid retrieval, not analytical or participant outcomes.
The existing pilot module separately compares vector storage engines.
"""

import hashlib
import json
import math
import statistics
import time
from datetime import date
from pathlib import Path

from .db import digest
from .search import embed, index_coverage, scope_conditions, search


def corpus_manifest(db, maximum=100_000):
    where, _ = scope_conditions()
    rows = db.rows(
        f"""SELECT p.id,p.item_id,p.text,p.revision,i.revision AS item_revision,
        i.source,i.item_type,i.collection,i.date_start,i.date_end,i.date_precision,
        e.embedding::text AS vector FROM passages p JOIN items i ON i.id=p.item_id
        LEFT JOIN embeddings e ON e.passage_id=p.id AND e.model=%s AND e.revision=%s
        WHERE {where} ORDER BY p.id LIMIT %s""",
        (db.settings.embedding_model, db.settings.embedding_revision, maximum + 1),
    )
    if len(rows) > maximum:
        raise ValueError("evaluation corpus exceeds bound; use a dedicated snapshot")
    records = [{**r, "text": hashlib.sha256(r["text"].encode()).hexdigest()} for r in rows]
    return {
        "sha256": digest(records),
        "passages": len(rows),
        "model": db.settings.embedding_model,
        "model_revision": db.settings.embedding_revision,
        "coverage": index_coverage(db),
    }


def retrieval_metrics(ranked, relevant, k=10):
    relevant = set(relevant)
    ranked = list(dict.fromkeys(ranked))[:k]
    hits = [int(item in relevant) for item in ranked]
    dcg = sum(hit / math.log2(rank + 2) for rank, hit in enumerate(hits))
    ideal = sum(1 / math.log2(rank + 2) for rank in range(min(k, len(relevant))))
    return {
        "recall_at_10": sum(hits) / len(relevant) if relevant else None,
        "ndcg_at_10": dcg / ideal if ideal else None,
        "reciprocal_rank": next((1 / (i + 1) for i, hit in enumerate(hits) if hit), 0)
        if relevant
        else None,
    }


def evaluate(db, path: Path, *, maximum=1000):
    if path.stat().st_size > 2 * 1024**2:
        raise ValueError("judgments manifest exceeds size bound")
    manifest = json.loads(path.read_text())
    if not isinstance(manifest, dict):
        raise ValueError("judgments manifest must be an object")
    if not db.settings.embedding_revision or db.settings.approximate_search:
        raise ValueError("evaluation requires a pinned embedding model and exact retrieval")
    if manifest.get("version") != 1 or manifest.get("split") not in {"development", "held_out"}:
        raise ValueError("version 1 and an explicit development/held_out split required")
    if not isinstance(manifest.get("judge"), str) or not manifest["judge"].strip():
        raise ValueError("record who supplied relevance judgments")
    cases = manifest.get("cases")
    if not isinstance(cases, list) or not cases or len(cases) > min(maximum, 1000):
        raise ValueError("bounded nonempty judged cases required")
    before = corpus_manifest(db)
    if (
        any(manifest.get(key) != before[key] for key in ("model", "model_revision"))
        or manifest.get("corpus_sha256") != before["sha256"]
    ):
        raise ValueError("model or corpus differs from judged snapshot")
    ids = set()
    prepared = []
    for case in cases:
        if (
            not isinstance(case, dict)
            or not isinstance(case.get("id"), str)
            or not case["id"]
            or case["id"] in ids
        ):
            raise ValueError("unique case IDs required")
        ids.add(case["id"])
        if not isinstance(case.get("query"), str) or not 1 <= len(case["query"]) <= 1000:
            raise ValueError("bounded query required")
        relevant = case.get("relevant_item_ids")
        if (
            not isinstance(relevant, list)
            or any(not isinstance(v, str) or not v or len(v) > 128 for v in relevant)
            or len(relevant) != len(set(relevant))
        ):
            raise ValueError("unique relevant document IDs required")
        if (
            not relevant
            and case.get("expected_empty") is not True
            or relevant
            and case.get("expected_empty")
        ):
            raise ValueError("positive judgments or an explicit expected-empty case required")
        filters = case.get("filters", {})
        if not isinstance(filters, dict) or set(filters) - {
            "source",
            "item_type",
            "collection",
            "date_from",
            "date_to",
        }:
            raise ValueError("unsupported search filters")
        filters = dict(filters)
        for key, value in filters.items():
            if not isinstance(value, str) or not 1 <= len(value) <= 200:
                raise ValueError("bounded string filters required")
            if key.startswith("date_"):
                filters[key] = date.fromisoformat(value)
        if (
            filters.get("date_from")
            and filters.get("date_to")
            and filters["date_from"] > filters["date_to"]
        ):
            raise ValueError("date_from must precede date_to")
        where, params = scope_conditions(**filters)
        eligible = {
            r["id"]
            for r in db.rows(
                f"SELECT DISTINCT i.id FROM passages p JOIN items i ON i.id=p.item_id WHERE {where}",
                params,
            )
        }
        prepared.append((case, filters, set(relevant) <= eligible))
    results = []
    for case, filters, eligible in prepared:
        row = {"id": case["id"], "eligible": eligible, "modes": {}}
        results.append(row)
        if not eligible:
            row["reason"] = "judged_documents_outside_active_filtered_corpus"
            continue
        where, params = scope_conditions(**filters)
        scoped_coverage = index_coverage(db, where=where, params=params)
        began = time.perf_counter()
        try:
            vector = (
                embed(db.settings, [case["query"]])[0]
                if scoped_coverage["indexed_passages"]
                else []
            )
            row["query_embedding_ms"] = (time.perf_counter() - began) * 1000
        except Exception:
            vector = None
            row["query_embedding_error"] = True
        for mode in ("keyword", "vector", "hybrid"):
            began = time.perf_counter()
            try:
                if mode != "keyword" and vector is None and scoped_coverage["indexed_passages"]:
                    raise ValueError("query embedding unavailable")
                found = search(db, case["query"], mode=mode, vector=vector, limit=10, **filters)
                ranked = [r["item_id"] for r in found["results"]]
                expected_empty = case.get("expected_empty") is True
                row["modes"][mode] = {
                    "status": "ok",
                    "ranked_item_ids": ranked,
                    **retrieval_metrics(ranked, case["relevant_item_ids"]),
                    "expected_empty_correct": not ranked if expected_empty else None,
                    "coverage": found["index_coverage"],
                }
            except Exception:
                row["modes"][mode] = {
                    "status": "error",
                    "ranked_item_ids": [],
                    **retrieval_metrics([], case["relevant_item_ids"]),
                    "expected_empty_correct": False if case.get("expected_empty") else None,
                }
            row["modes"][mode]["retrieval_ms"] = (time.perf_counter() - began) * 1000
    if corpus_manifest(db)["sha256"] != before["sha256"]:
        raise ValueError(
            "corpus changed during evaluation; discard measurements and rerun on a frozen snapshot"
        )
    summary = {}
    for mode in ("keyword", "vector", "hybrid"):
        modes = [r["modes"][mode] for r in results if r["eligible"]]
        summary[mode] = {
            "eligible": len(modes),
            "errors": sum(r["status"] == "error" for r in modes),
        }
        for metric in ("recall_at_10", "ndcg_at_10", "reciprocal_rank", "expected_empty_correct"):
            measured = [r[metric] for r in modes if r[metric] is not None]
            summary[mode][metric] = statistics.mean(measured) if measured else None
    return {
        "version": 1,
        "status": "measured",
        "unit": "document",
        "k": 10,
        "judgments_sha256": digest(manifest),
        "corpus": before,
        "split": manifest["split"],
        "judge": manifest["judge"],
        "planned_cases": len(results),
        "ineligible_cases": sum(not r["eligible"] for r in results),
        "query_embedding_reused_between_modes": True,
        "summary": summary,
        "cases": results,
        "qualification": "Judged retrieval on this frozen corpus only. Latency excludes separately reported query embedding. Errors remain in eligible denominators. A held_out label does not establish independence, synthesis quality or user success.",
    }
