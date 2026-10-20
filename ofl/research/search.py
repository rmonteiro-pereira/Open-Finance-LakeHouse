"""Revision-aware keyword/vector fusion. pgvector is a measured baseline, not a lock-in."""

import hashlib
import math
from functools import lru_cache

from . import jurisdiction as places
from .db import digest

SEMANTIC_CONTENT = (
    "(p.method<>'metadata' OR COALESCE(i.metadata->>'content_tier','')<>'private_original')"
)
# A rejection holds only for the exact passage text and model space it was made for.
REJECTED = """embedding_rejections r ON r.passage_id=p.id AND r.model=%s AND r.revision=%s
    AND r.passage_revision=p.revision AND r.text_sha256=encode(sha256(convert_to(p.text,'UTF8')),'hex')"""


class ContextLengthExceeded(ValueError):
    pass


def _context_refusal(response):
    # Only Ollama's exact refusal; a proxy or outage body quoting it must not reject passages.
    try:
        body = response.json()
    except ValueError:
        return False
    error = body.get("error") if isinstance(body, dict) else None
    return isinstance(error, str) and error.strip() == "the input length exceeds the context length"


RRF_K = 60


def fuse(rankings, k=RRF_K):
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
                "options": {"num_thread": settings.embedding_threads},
            },
            timeout=120,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as error:
            too_large = response.status_code == requests.codes.request_entity_too_large
            if too_large and _context_refusal(response):
                raise ContextLengthExceeded(
                    "passage exceeds model context length; rechunk rather than silently truncate"
                ) from error
            raise
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


def _embed_isolating(settings, rows):
    """Bisect only context-length refusals; any other provider error fails the job."""
    try:
        return list(zip(rows, embed(settings, [row["text"] for row in rows]), strict=True)), []
    except ContextLengthExceeded as error:
        if len(rows) == 1:
            return [], [(rows[0], str(error))]
        halves = [
            _embed_isolating(settings, part)
            for part in (rows[: len(rows) // 2], rows[len(rows) // 2 :])
        ]
        return halves[0][0] + halves[1][0], halves[0][1] + halves[1][1]


def embed_pending(db, limit=1000, heartbeat=lambda _: None):
    settings = db.settings
    if not settings.embedding_revision:
        raise ValueError("embedding revision is not pinned")
    space = (settings.embedding_model, settings.embedding_revision)
    rows = db.rows(
        f"""SELECT p.id,p.text,p.revision FROM passages p JOIN items i ON i.id=p.item_id LEFT JOIN embeddings e
        ON e.passage_id=p.id AND e.model=%s AND e.revision=%s LEFT JOIN {REJECTED}
        WHERE p.active AND e.passage_id IS NULL AND r.passage_id IS NULL
        AND (p.asset_id IS NULL OR EXISTS(SELECT 1 FROM assets a WHERE a.id=p.asset_id AND a.active))
        AND {SEMANTIC_CONTENT}
        ORDER BY p.id LIMIT %s""",
        (*space, *space, limit),
    )
    rejected_total = 0
    # CPU-backed remote models can exceed the request deadline on eight long
    # passages. Keep that safe default; measured GPU workers can opt into bounded batches.
    # Each committed batch survives retries, with progress after every commit.
    batch_size = settings.embedding_batch_size if settings.embedding_url else 8
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        embedded, rejected = _embed_isolating(settings, batch)
        with db.connect() as con:
            con.cursor().executemany(
                """INSERT INTO embeddings(passage_id,model,revision,embedding)
                VALUES (%s,%s,%s,%s::vector) ON CONFLICT DO NOTHING""",
                [
                    (row["id"], *space, vector_literal(vector, settings.embedding_dimensions))
                    for row, vector in embedded
                ],
            )
            con.cursor().executemany(
                """INSERT INTO embedding_rejections(passage_id,model,revision,passage_revision,text_sha256,reason)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(passage_id,model,revision) DO UPDATE SET
                passage_revision=EXCLUDED.passage_revision,text_sha256=EXCLUDED.text_sha256,
                reason=EXCLUDED.reason,created_at=now()""",
                [
                    (
                        row["id"],
                        *space,
                        row["revision"],
                        hashlib.sha256(row["text"].encode()).hexdigest(),
                        reason,
                    )
                    for row, reason in rejected
                ],
            )
        rejected_total += len(rejected)
        done = min(start + batch_size, len(rows))
        heartbeat(
            {"embedded": done - rejected_total, "total": len(rows)}
            | ({"rejected": rejected_total} if rejected_total else {})
        )
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
    where = f"({where}) AND {SEMANTIC_CONTENT}"
    settings = db.settings
    space = (settings.embedding_model, settings.embedding_revision or "")
    row = db.rows(
        f"""SELECT count(*) AS eligible_passages, count(e.passage_id) AS indexed_passages,
        count(r.passage_id) FILTER (WHERE e.passage_id IS NULL) AS rejected_passages
        FROM passages p JOIN items i ON i.id=p.item_id LEFT JOIN embeddings e
        ON e.passage_id=p.id AND e.model=%s AND e.revision=%s LEFT JOIN {REJECTED} WHERE {where}""",
        (*space, *space, *params),
    )[0]
    return _coverage(settings, row)


def _coverage(settings, row):
    configured = bool(settings.embedding_revision)
    total, indexed, rejected = (
        int(row["eligible_passages"]),
        int(row["indexed_passages"]) if configured else 0,
        int(row["rejected_passages"]) if configured else 0,
    )
    return {
        "configured": configured,
        "model": settings.embedding_model if configured else None,
        "model_revision": settings.embedding_revision if configured else None,
        "eligible_passages": total,
        "indexed_passages": indexed,
        "rejected_passages": rejected,
        "pending_passages": total - indexed - rejected,
        "fraction": indexed / total if configured and total else None,
        "status": "disabled"
        if not configured
        else "empty"
        if not total
        else "complete"
        if indexed == total
        else "partial",
        "qualification": "Current active semantic passages within source/publication filters; catalog-only private-original metadata is excluded. Query terms are not a coverage filter. Rejected passages exceed the model context, stay eligible but unindexed and are not pending until their content changes. This is not relevance, historical completeness or truth.",
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
    jurisdiction="off",
):
    if mode not in {"keyword", "vector", "hybrid"}:
        raise ValueError("unknown search mode")
    rule = places.plan(jurisdiction, text)
    where, params = scope_conditions(
        source=source,
        item_type=item_type,
        collection=collection,
        date_from=date_from,
        date_to=date_to,
    )
    disclosure, coverage = _jurisdiction_scope(db, rule, where, params)
    # Both arms and the coverage denominator see the same (for exclude, narrowed) scope.
    where, params = _narrowed_scope(rule, where, params)
    if coverage is None:
        coverage = index_coverage(db, where=where, params=params)
    projection = _projection(rule)
    rankings = []
    # Extracted bodies have only Portuguese lexemes; applying a simple NOT
    # against that vector can silently bypass exclusions (`tesouro` vs `tesour`).
    # Metadata also has simple lexemes, useful for short identifiers. Keep that
    # fallback for positive queries only: exclusions use Portuguese consistently,
    # including inflected metadata words. Inspect PostgreSQL's parsed query rather
    # than interpreting minus signs inside identifiers/quoted source text ourselves.
    candidates = max(100, limit * 5)
    if mode in {"keyword", "hybrid"}:
        rankings.append(
            db.rows(
                f"""WITH query AS (
                SELECT websearch_to_tsquery('portuguese',%s) AS pt,
                    websearch_to_tsquery('simple',%s) AS literal
                ) SELECT {projection},ts_rank_cd(p.search,
                    CASE WHEN p.method='metadata' AND strpos(query.literal::text,'!')=0
                    THEN query.pt || query.literal ELSE query.pt END) AS score
                FROM passages p JOIN items i ON i.id=p.item_id CROSS JOIN query
                WHERE {where} AND (p.search @@ query.pt OR
                    (p.method='metadata' AND strpos(query.literal::text,'!')=0
                    AND p.search @@ query.literal))
                ORDER BY score DESC,p.id LIMIT %s""",
                (text, text, *params, candidates),
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
            WHERE {where} AND {SEMANTIC_CONTENT} AND e.model=%s AND e.revision=%s
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
    rankings, labels, boosted = _boost(rule, rankings, labels)
    contributions = _contributions(labels, rankings)
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
    response = {
        "mode": mode,
        "model": db.settings.embedding_model if mode != "keyword" else None,
        "model_revision": db.settings.embedding_revision if mode != "keyword" else None,
        "approximate": db.settings.approximate_search if mode != "keyword" else False,
        "index_coverage": coverage,
        "results": results,
    }
    if disclosure is not None:
        response["jurisdiction"] = _boost_disclosure(disclosure, boosted, results)
    return response


PROJECTION = """p.id,p.item_id,p.asset_id,p.page,p.text,p.method,p.evidence,
    i.title,i.source,i.item_type,i.collection,i.date_start,i.date_end,i.date_precision,
    i.metadata->>'url' AS source_url,i.metadata->>'content_tier' AS content_tier,
    i.metadata->>'epistemic_status' AS epistemic_status"""
JURISDICTION_COLUMNS = ",p.jurisdiction,p.jurisdiction_evidence,p.jurisdiction_tagger"
DISCLOSURE_QUALIFICATION = (
    "Lexical evidence of a named place, institution, indicator or currency symbol, not "
    "topic classification. Applied only when the query names a jurisdiction and every "
    "named jurisdiction is in the precision-gated set (BR, US, EA); otherwise the query "
    "is searched without the rule and ungated names are listed as ignored. Untagged and "
    "evidence-free passages are never excluded."
)


def _projection(rule):
    """Result columns; jurisdiction evidence only when a rule other than off was asked."""
    return PROJECTION + (JURISDICTION_COLUMNS if rule["rule"] != "off" else "")


def _narrowed_scope(rule, where, params):
    """Scope both arms search: narrowed only for an active exclude rule."""
    if rule["rule"] != "exclude" or not rule["active"]:
        return where, params
    condition, extra = places.exclusion(rule["active"])
    return f"{where} AND {condition}", [*params, *extra]


def _boost(rule, rankings, labels):
    """One more list in the same RRF (k=60) for an active boost rule.

    It holds only candidates the arms already retrieved, in their fused order, so each
    passage gains at most 1/61. Returns rankings, labels and the boosted candidate count
    (None when no boost applies).
    """
    if rule["rule"] != "boost" or not rule["active"]:
        return rankings, labels, None
    base = fuse(rankings) if len(rankings) > 1 else rankings[0]
    extra = places.boosted(base, rule["active"])
    return [*rankings, extra], [*labels, "jurisdiction_boost"], len(extra)


def _contributions(labels, rankings):
    """Per-passage rank, raw score and RRF share of each fused list."""
    contributions = {}
    for label, ranking in zip(labels, rankings, strict=True):
        for position, row in enumerate(ranking, 1):
            contributions.setdefault(row["id"], {})[label] = {
                "rank": position,
                "raw_score": float(row["score"]),
                "rrf_contribution": 1 / (RRF_K + position) if len(rankings) > 1 else None,
            }
    return contributions


def _boost_disclosure(disclosure, boosted, results):
    if boosted is None:
        return disclosure
    return disclosure | {
        "boosted_candidates": boosted,
        "boosted_results": sum("jurisdiction_boost" in row["ranking"] for row in results),
    }


def _disclosure(rule):
    """Base disclosure for a rule other than off; None for off."""
    if rule["rule"] == "off":
        return None
    return {
        "rule": rule["rule"],
        "tagger": places.VERSION,
        "named": rule["named"],
        "applied": bool(rule["active"]),
        "ignored": rule["ignored"],
        "qualification": DISCLOSURE_QUALIFICATION,
    }


def _jurisdiction_scope(db, rule, where, params):
    """Disclosure for a non-off rule and, when the rule is active, the index coverage.

    One scan of the filtered scope yields both: coverage aggregates are restricted to the
    searched (semantic, and for exclude narrowed) passages, disclosure counts are not.
    """
    disclosure = _disclosure(rule)
    if disclosure is None or not rule["active"]:
        return disclosure, None
    query, values = _scope_scan(rule, where, params, db.settings)
    row = db.rows(query, values)[0]
    return disclosure | _scope_counts(rule, row), _coverage(db.settings, row)


def _scope_scan(rule, where, params, settings):
    """SQL and parameters of the one scope scan behind coverage and disclosure counts."""
    condition, extra = places.exclusion(rule["active"])
    exclude = rule["rule"] == "exclude"
    searched = f"{SEMANTIC_CONTENT} AND {condition}" if exclude else SEMANTIC_CONTENT
    narrowed = extra if exclude else []
    space = (settings.embedding_model, settings.embedding_revision or "")
    query = f"""SELECT count(*) FILTER (WHERE {searched}) AS eligible_passages,
        count(e.passage_id) FILTER (WHERE {searched}) AS indexed_passages,
        count(r.passage_id) FILTER (WHERE e.passage_id IS NULL AND {searched}) AS rejected_passages,
        count(*) AS scope,count(*) FILTER (WHERE p.jurisdiction IS NULL) AS untagged,
        count(*) FILTER (WHERE p.jurisdiction_tagger IS DISTINCT FROM %s
            AND p.jurisdiction IS NOT NULL) AS older_tagger,
        count(*) FILTER (WHERE p.jurisdiction && %s::text[]) AS named_evidence,
        count(*) FILTER (WHERE NOT {condition}) AS conflicting
        FROM passages p JOIN items i ON i.id=p.item_id LEFT JOIN embeddings e
        ON e.passage_id=p.id AND e.model=%s AND e.revision=%s LEFT JOIN {REJECTED} WHERE {where}"""
    values = (
        *narrowed,
        *narrowed,
        *narrowed,
        places.VERSION,
        rule["active"],
        *extra,
        *space,
        *space,
        *params,
    )
    return query, values


def _scope_counts(rule, row):
    return {
        "scope_passages": row["scope"],
        "passages_with_named_evidence": row["named_evidence"],
        "conflicting_passages": row["conflicting"],
        "excluded_passages": row["conflicting"] if rule["rule"] == "exclude" else 0,
        "untagged_passages": row["untagged"],
        "older_tagger_passages": row["older_tagger"],
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
