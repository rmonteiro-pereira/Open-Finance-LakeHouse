"""Revision-aware keyword/vector fusion. pgvector is a measured baseline, not a lock-in."""

import math
from functools import lru_cache

from .db import digest


def fuse(rankings, k=60):
    scores = {}
    records = {}
    for ranking in rankings:
        for rank, row in enumerate(ranking, 1):
            scores[row["id"]] = scores.get(row["id"], 0) + 1 / (k + rank)
            records[row["id"]] = row
    return [
        {**records[key], "score": scores[key]}
        for key in sorted(scores, key=lambda key: (-scores[key], key))
    ]


def vector_literal(values, dimensions):
    if len(values) != dimensions or not all(math.isfinite(float(v)) for v in values):
        raise ValueError(f"embedding must have {dimensions} finite dimensions")
    return "[" + ",".join(str(float(v)) for v in values) + "]"


@lru_cache(maxsize=2)
def _model(name, revision):
    if not revision:
        raise ValueError("pin OFL_RESEARCH_EMBEDDING_REVISION before enabling semantic search")
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(name, revision=revision, trust_remote_code=False, device="cpu")


def embed(settings, texts):
    if settings.embedding_url:
        import requests

        base = settings.embedding_url.rstrip("/")
        # The model tag is mutable. Refuse a changed digest rather than mixing spaces.
        response = requests.get(f"{base}/api/tags", timeout=15)
        response.raise_for_status()
        models = response.json()["models"]
        matching = [m for m in models if m["name"] == settings.embedding_model]
        if not matching or matching[0]["digest"] != settings.embedding_revision:
            raise ValueError("Ollama embedding model digest does not match pinned revision")
        response = requests.post(
            f"{base}/api/embed",
            json={
                "model": settings.embedding_model,
                "input": texts,
                "truncate": False,
                "keep_alive": -1,
            },
            timeout=120,
        )
        response.raise_for_status()
        vectors = response.json()["embeddings"]
        if len(vectors) != len(texts):
            raise ValueError("embedding response count does not match input")
        for vector in vectors:
            vector_literal(vector, settings.embedding_dimensions)
        return vectors
    model = _model(settings.embedding_model, settings.embedding_revision)
    for text in texts:
        if len(model.tokenizer.encode(text)) > model.max_seq_length:
            raise ValueError(
                "passage exceeds model token limit; rechunk rather than silently truncate"
            )
    vectors = model.encode(texts, normalize_embeddings=True, batch_size=8, show_progress_bar=False)
    for vector in vectors:
        vector_literal(vector, settings.embedding_dimensions)
    return vectors.tolist()


def embed_pending(db, limit=1000, heartbeat=lambda _: None):
    settings = db.settings
    if not settings.embedding_revision:
        raise ValueError("embedding revision is not pinned")
    rows = db.rows(
        """SELECT p.id,p.text FROM passages p LEFT JOIN embeddings e
        ON e.passage_id=p.id AND e.model=%s AND e.revision=%s WHERE p.active AND e.passage_id IS NULL
        AND (p.asset_id IS NULL OR EXISTS(SELECT 1 FROM assets a WHERE a.id=p.asset_id AND a.active))
        ORDER BY p.id LIMIT %s""",
        (settings.embedding_model, settings.embedding_revision, limit),
    )
    # CPU-backed remote models can exceed the request deadline on eight long
    # passages. Persist each result independently so retries retain progress.
    batch_size = 1 if settings.embedding_url else 8
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        vectors = embed(settings, [row["text"] for row in batch])
        with db.connect() as con:
            for row, vector in zip(batch, vectors, strict=True):
                con.execute(
                    """INSERT INTO embeddings(passage_id,model,revision,embedding)
                    VALUES (%s,%s,%s,%s::vector) ON CONFLICT DO NOTHING""",
                    (
                        row["id"],
                        settings.embedding_model,
                        settings.embedding_revision,
                        vector_literal(vector, settings.embedding_dimensions),
                    ),
                )
        heartbeat({"embedded": min(start + batch_size, len(rows)), "total": len(rows)})
    return len(rows)


def scope_conditions(*, source=None, item_type=None, collection=None, date_from=None, date_to=None):
    conditions = [
        "p.active",
        "(p.asset_id IS NULL OR EXISTS(SELECT 1 FROM assets a WHERE a.id=p.asset_id AND a.active))",
    ]
    params = []
    for column, value in (("source", source), ("item_type", item_type), ("collection", collection)):
        if value:
            conditions.append(f"i.{column}=%s")
            params.append(value)
    if date_from:
        conditions.append("COALESCE(i.date_end,i.date_start)>=%s")
        params.append(date_from)
    if date_to:
        conditions.append("i.date_start<=%s")
        params.append(date_to)
    return " AND ".join(conditions), params


def index_coverage(db, *, where=None, params=()):
    """Point-in-time eligible passage coverage, never a relevance or truth score."""
    if where is None:
        where, params = scope_conditions()
    settings = db.settings
    configured = bool(settings.embedding_revision)
    row = db.rows(
        f"""SELECT count(*) AS eligible_passages, count(e.passage_id) AS indexed_passages
        FROM passages p JOIN items i ON i.id=p.item_id LEFT JOIN embeddings e
        ON e.passage_id=p.id AND e.model=%s AND e.revision=%s WHERE {where}""",
        (settings.embedding_model, settings.embedding_revision or "", *params),
    )[0]
    total, indexed = (
        int(row["eligible_passages"]),
        int(row["indexed_passages"]) if configured else 0,
    )
    return {
        "configured": configured,
        "model": settings.embedding_model if configured else None,
        "model_revision": settings.embedding_revision if configured else None,
        "eligible_passages": total,
        "indexed_passages": indexed,
        "pending_passages": total - indexed,
        "fraction": indexed / total if configured and total else None,
        "status": "disabled"
        if not configured
        else "empty"
        if not total
        else "complete"
        if indexed == total
        else "partial",
        "qualification": "Current active passages within source/publication filters; query terms are not a coverage filter. This is not relevance, historical completeness or truth.",
    }


def search(
    db,
    text,
    *,
    mode="keyword",
    source=None,
    item_type=None,
    collection=None,
    date_from=None,
    date_to=None,
    limit=20,
    vector=None,
):
    if mode not in {"keyword", "vector", "hybrid"}:
        raise ValueError("unknown search mode")
    where, params = scope_conditions(
        source=source,
        item_type=item_type,
        collection=collection,
        date_from=date_from,
        date_to=date_to,
    )
    coverage = index_coverage(db, where=where, params=params)
    projection = """p.id,p.item_id,p.asset_id,p.page,p.text,p.method,p.evidence,
        i.title,i.source,i.item_type,i.collection,i.date_start,i.date_end,i.date_precision,
        i.metadata->>'url' AS source_url,i.metadata->>'content_tier' AS content_tier,
        i.metadata->>'epistemic_status' AS epistemic_status"""
    rankings = []
    candidates = max(100, limit * 5)
    if mode in {"keyword", "hybrid"}:
        rankings.append(
            db.rows(
                f"""SELECT {projection},ts_rank_cd(p.search,
            websearch_to_tsquery('portuguese',%s) || websearch_to_tsquery('simple',%s)) AS score
            FROM passages p JOIN items i ON i.id=p.item_id WHERE {where}
            AND p.search @@ (websearch_to_tsquery('portuguese',%s) || websearch_to_tsquery('simple',%s))
            ORDER BY score DESC,p.id LIMIT %s""",
                (text, text, *params, text, text, candidates),
            )
        )
    if mode in {"vector", "hybrid"} and not coverage["configured"]:
        raise ValueError("semantic search requires a pinned embedding model")
    if mode in {"vector", "hybrid"} and not coverage["indexed_passages"]:
        rankings.append([])
    if mode in {"vector", "hybrid"} and coverage["indexed_passages"]:
        if vector is None:
            vector = embed(db.settings, [text])[0]
        literal = vector_literal(vector, db.settings.embedding_dimensions)
        expression = (
            f"e.embedding::vector({db.settings.embedding_dimensions})"
            if db.settings.approximate_search
            else "e.embedding"
        )
        rankings.append(
            db.rows(
                f"""SELECT {projection},1-({expression} <=> %s::vector) AS score
            FROM passages p JOIN items i ON i.id=p.item_id JOIN embeddings e ON e.passage_id=p.id
            WHERE {where} AND e.model=%s AND e.revision=%s
            ORDER BY {expression} <=> %s::vector,p.id LIMIT %s""",
                (
                    literal,
                    *params,
                    db.settings.embedding_model,
                    db.settings.embedding_revision,
                    literal,
                    candidates,
                ),
                ann=db.settings.approximate_search,
            )
        )
    labels = ["keyword", "vector"] if mode == "hybrid" else [mode]
    contributions = {}
    for label, ranking in zip(labels, rankings, strict=True):
        for position, row in enumerate(ranking, 1):
            contributions.setdefault(row["id"], {})[label] = {
                "rank": position,
                "raw_score": float(row["score"]),
                "rrf_contribution": 1 / (60 + position) if mode == "hybrid" else None,
            }
    combined = fuse(rankings) if len(rankings) > 1 else rankings[0]
    # Group by document without allowing one long scanned volume to occupy every hit.
    results = []
    seen = set()
    for row in combined:
        if row["item_id"] in seen:
            continue
        seen.add(row["item_id"])
        results.append(
            {
                **row,
                "snippet": row["text"][:600],
                "ranking": contributions.get(row["id"], {}),
                "search_mode": mode,
            }
        )
        if len(results) >= limit:
            break
    return {
        "mode": mode,
        "model": db.settings.embedding_model if mode != "keyword" else None,
        "model_revision": db.settings.embedding_revision if mode != "keyword" else None,
        "approximate": db.settings.approximate_search if mode != "keyword" else False,
        "index_coverage": coverage,
        "results": results,
    }


def create_vector_index(db):
    """Opt-in ANN after comparing filtered recall against the exact baseline."""
    from psycopg import sql

    settings = db.settings
    if not settings.embedding_revision:
        raise ValueError("pin an embedding revision first")
    name = (
        "research_vectors_" + digest([settings.embedding_model, settings.embedding_revision])[:12]
    )
    statement = sql.SQL("""CREATE INDEX IF NOT EXISTS {} ON embeddings USING hnsw
        ((embedding::vector({})) vector_cosine_ops) WHERE model={} AND revision={} """).format(
        sql.Identifier(name),
        sql.Literal(settings.embedding_dimensions),
        sql.Literal(settings.embedding_model),
        sql.Literal(settings.embedding_revision),
    )
    with db.connect() as con:
        con.execute(statement)
    return name
