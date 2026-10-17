"""Comparable exact-pgvector / Qdrant retrieval using identical stored embeddings.

Input JSONL: {query, embedding, relevant: [passage_id,...], filters: {...}}.
The Qdrant index is temporary and local; do not send private passages to a cloud.
"""

import json
import math
import statistics
import time
import uuid
from pathlib import Path

from .search import vector_literal


def metrics(ranked, relevant, k=10):
    relevant = set(relevant)
    hits = [int(identity in relevant) for identity in ranked[:k]]
    dcg = sum(hit / math.log2(rank + 2) for rank, hit in enumerate(hits))
    ideal = sum(1 / math.log2(rank + 2) for rank in range(min(k, len(relevant))))
    return {
        "recall_at_10": sum(hits) / len(relevant) if relevant else 0,
        "ndcg_at_10": dcg / ideal if ideal else 0,
    }


def evaluate(db, queries: Path, *, maximum=10_000):
    from qdrant_client import QdrantClient, models

    settings = db.settings
    rows = db.rows(
        """SELECT p.id,e.embedding::text AS embedding,i.source,i.item_type,i.collection,
        i.date_start,i.date_end FROM embeddings e JOIN passages p ON p.id=e.passage_id
        JOIN items i ON i.id=p.item_id WHERE p.active AND e.model=%s AND e.revision=%s
        AND (p.asset_id IS NULL OR EXISTS(SELECT 1 FROM assets a WHERE a.id=p.asset_id AND a.active)) ORDER BY p.id LIMIT %s""",
        (settings.embedding_model, settings.embedding_revision, maximum + 1),
    )
    if not rows or len(rows) > maximum:
        raise ValueError(
            "pilot requires a nonempty corpus within --maximum; use a dedicated pilot database"
        )
    samples = [json.loads(line) for line in queries.read_text().splitlines() if line.strip()]
    if not samples:
        raise ValueError("pilot requires judged queries")
    identities = {row["id"] for row in rows}
    for sample in samples:
        if not sample.get("relevant") or not set(sample["relevant"]) <= identities:
            raise ValueError("each query needs judged relevant passage IDs in the pilot corpus")
        vector_literal(sample["embedding"], settings.embedding_dimensions)
    client = QdrantClient(":memory:")
    client.create_collection(
        "pilot",
        vectors_config=models.VectorParams(
            size=settings.embedding_dimensions, distance=models.Distance.COSINE
        ),
    )
    client.upload_points(
        "pilot",
        [
            models.PointStruct(
                id=str(uuid.UUID(row["id"][:32])),
                vector=json.loads(row["embedding"]),
                payload={"id": row["id"]},
            )
            for row in rows
        ],
    )
    scores = {"pgvector_exact": [], "qdrant_exact": []}
    latencies = {key: [] for key in scores}
    for sample in samples:
        filters = sample.get("filters", {})
        if set(filters) - {"source", "item_type", "collection"}:
            raise ValueError("pilot supports source/item_type/collection filters")
        eligible = [row for row in rows if all(row[k] == value for k, value in filters.items())]
        ids = [row["id"] for row in eligible]
        started = time.perf_counter()
        pg = db.rows(
            """SELECT passage_id AS id FROM embeddings WHERE model=%s AND revision=%s
            AND passage_id=ANY(%s) ORDER BY embedding <=> %s::vector,passage_id LIMIT 10""",
            (
                settings.embedding_model,
                settings.embedding_revision,
                ids,
                vector_literal(sample["embedding"], settings.embedding_dimensions),
            ),
        )
        latencies["pgvector_exact"].append((time.perf_counter() - started) * 1000)
        started = time.perf_counter()
        found = (
            client.query_points(
                "pilot",
                query=sample["embedding"],
                limit=10,
                query_filter=models.Filter(
                    must=[models.HasIdCondition(has_id=[str(uuid.UUID(i[:32])) for i in ids])]
                ),
            ).points
            if ids
            else []
        )
        latencies["qdrant_exact"].append((time.perf_counter() - started) * 1000)
        for key, ranked in (
            ("pgvector_exact", [r["id"] for r in pg]),
            ("qdrant_exact", [r.payload["id"] for r in found]),
        ):
            scores[key].append(metrics(ranked, sample["relevant"]))
    client.close()
    return {
        "corpus_passages": len(rows),
        "queries": len(samples),
        "qualification": "Exact retrieval correctness pilot. Qdrant runs in-process; timings are not comparable server benchmarks. No ANN or graph winner selected.",
        "backends": {
            key: {
                "recall_at_10": statistics.mean(r["recall_at_10"] for r in scores[key]),
                "ndcg_at_10": statistics.mean(r["ndcg_at_10"] for r in scores[key]),
                "p95_ms": sorted(latencies[key])[max(0, math.ceil(len(samples) * 0.95) - 1)],
            }
            for key in scores
        },
    }
