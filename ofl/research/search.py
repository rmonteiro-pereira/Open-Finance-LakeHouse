"""Revision-aware keyword/vector fusion. pgvector is a measured baseline, not a lock-in."""

import contextvars
import hashlib
import math
import threading
import time
from collections import OrderedDict
from contextlib import nullcontext
from functools import lru_cache

from . import jurisdiction as places
from . import lexical
from . import vocabulary as lexicon
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
    """Reciprocal-rank fusion; a row's `fusion_weight` (default 1) scales its share."""
    scores = {}
    records = {}
    for ranking in rankings:
        for rank, row in enumerate(ranking, 1):
            share = row.get("fusion_weight", 1) / (k + rank)
            scores[row["id"]] = scores.get(row["id"], 0) + share
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


# Scope coverage reused by search (coverage_cache_seconds), and the scopes being refreshed.
_COVERAGE = OrderedDict()
_COVERAGE_LIMIT = 256
_COVERAGE_REFRESHING = set()
_COVERAGE_REFRESH_LIMIT = 2
_COVERAGE_LOCK = threading.Lock()


def _coverage_key(db, where, params):
    settings = db.settings
    space = (settings.embedding_model, settings.embedding_revision)
    return (settings.database_url, *space, where, tuple(params))


def _store_coverage(key, value):
    with _COVERAGE_LOCK:
        _COVERAGE[key] = (time.monotonic(), value)
        _COVERAGE.move_to_end(key)
        while len(_COVERAGE) > _COVERAGE_LIMIT:
            _COVERAGE.popitem(last=False)


def _refresh_coverage(db, key, where, params):
    try:
        _store_coverage(key, index_coverage(db, where=where, params=params))
    finally:
        with _COVERAGE_LOCK:
            _COVERAGE_REFRESHING.discard(key)


def search_coverage(db, where, params):
    """index_coverage of a search scope, reused for coverage_cache_seconds (ttl).

    Up to ttl old, the cached value is returned. Between ttl and 2 x ttl it is still
    returned while a background thread recomputes it (at most _COVERAGE_REFRESH_LIMIT
    at a time). Older, missing, or after a failed refresh, the search scans. So a value
    is never more than 2 x ttl old. 0 (the default) scans on every search.
    """
    ttl = getattr(db.settings, "coverage_cache_seconds", 0)
    if not ttl:
        return index_coverage(db, where=where, params=params)
    key = _coverage_key(db, where, params)
    with _COVERAGE_LOCK:
        entry = _COVERAGE.get(key)
        age = None if entry is None else time.monotonic() - entry[0]
        usable = age is not None and age <= 2 * ttl
        refresh = (
            usable
            and age > ttl
            and key not in _COVERAGE_REFRESHING
            and len(_COVERAGE_REFRESHING) < _COVERAGE_REFRESH_LIMIT
        )
        if refresh:
            _COVERAGE_REFRESHING.add(key)
    if not usable:
        value = index_coverage(db, where=where, params=params)
        _store_coverage(key, value)
        return value
    if refresh:
        # A fresh context: the refresh must not borrow this request's session connection.
        threading.Thread(
            target=contextvars.Context().run,
            args=(_refresh_coverage, db, key, where, params),
            daemon=True,
        ).start()
    return entry[1]


def search(db, text, **options):
    """Search on one database connection (Database.session) when the database offers it."""
    session = getattr(db, "session", None)
    with session() if session else nullcontext():
        return _search(db, text, **options)


def _search(
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
    vocabulary="off",
    vocabulary_arms=None,
    expanded_vector=None,
):
    if mode not in {"keyword", "vector", "hybrid"}:
        raise ValueError("unknown search mode")
    rule = places.plan(jurisdiction, text)
    expansion = lexicon.plan(vocabulary, text)
    arms = _arms(expansion, mode, vocabulary_arms)
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
        coverage = search_coverage(db, where, params)
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
        terms = _keyword_terms(db, text, expansion, arms)
        query, head, minimum = _keyword_query(projection, where, text, terms)
        rows = db.rows(query, (*head, *params, *_keyword_tail(minimum, candidates)))
        count = len(terms["operands"])
        rankings.append(_keyword_fusion(rows, count) if mode == "hybrid" else rows)
    if mode in {"vector", "hybrid"} and not coverage["configured"]:
        raise ValueError("semantic search requires a pinned embedding model")
    if mode in {"vector", "hybrid"} and not coverage["indexed_passages"]:
        rankings.append([])
        arms = [arm for arm in arms if arm != "vector"]
    if mode in {"vector", "hybrid"} and coverage["indexed_passages"]:
        vectors = _query_vectors(
            db.settings, text, expansion, arms, vector=vector, expanded=expanded_vector
        )
        rankings += [
            _vector_rows(db, (projection, where, params), value, candidates) for value in vectors
        ]
    labels = ["keyword", "vector"] if mode == "hybrid" else [mode]
    labels += ["vocabulary_vector"] if "vector" in arms else []
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
    if expansion["setting"] != "off":
        response["vocabulary"] = lexicon.disclosure(expansion, arms)
    return response


def _arms(expansion, mode, requested=None):
    """Arms the expansion acts on in this mode: none when no concept was expanded."""
    if not expansion["concepts"]:
        return []
    allowed = {"keyword": mode != "vector", "vector": mode != "keyword"}
    arms = lexicon.ARMS if requested is None else requested
    if set(arms) - set(allowed):
        raise ValueError("vocabulary arms must be keyword and/or vector")
    return [arm for arm in ("keyword", "vector") if arm in arms and allowed[arm]]


PARSE = f"""SELECT array_agg(websearch_to_tsquery('{lexical.CONFIG}',research_search_text(t))::text
    ORDER BY n) AS parsed FROM unnest(%s::text[]) WITH ORDINALITY AS u(t,n)"""


def _keyword_terms(db, text, expansion, arms):
    """Parsed tsqueries of the keyword arm (lexical module docstring).

    `ranked` is the typed query with its distinct positive top-level operands joined by OR
    and its negations kept; `operands` are what coverage counts, each distinct operand
    once (repeated words and words that stem alike are one). With keyword expansion, each
    vocabulary concept is one operand (the words as written or any expansion), the rest
    of the query supplies the others, and `expanded` matches any of them.
    """
    expanded = "keyword" in arms and bool(expansion["concepts"])
    pieces = [text]
    if expanded:
        pieces.append(expansion["remainder"])
        for concept in expansion["concepts"]:
            pieces += [concept["matched"], *(lexicon.term(t)[0] for t in concept["expansions"])]
    parsed = db.rows(PARSE, (pieces,))[0]["parsed"]
    positives, negatives = lexical.split(lexical.operands(parsed[0]))
    positives = lexical.distinct(positives)
    ranked = lexical.disjunction(positives, negatives) or parsed[0]
    terms = {
        "strict": parsed[0],
        "ranked": ranked,
        "matched": ranked,
        "operands": positives,
        "expanded": None,
    }
    if not expanded:
        return terms
    rest, negated = lexical.split(lexical.operands(parsed[1]))
    groups, at = [], 2
    for concept in expansion["concepts"]:
        size = 1 + len(concept["expansions"])
        alternatives = lexical.distinct([a for a in parsed[at : at + size] if a])
        at += size
        if alternatives:
            groups.append(" | ".join(f"( {a} )" for a in alternatives))
    operands = lexical.distinct(rest + groups)
    combined = lexical.disjunction(operands, negated)
    return terms | {"operands": operands, "expanded": combined, "matched": combined or ranked}


# An item title's vector at weight A. schema.sql stores it in item_headings with this
# same expression; a missing row is computed here.
HEADING = "setweight(to_tsvector('{config}',research_search_text(COALESCE(i.title,''))),'A')"
# Cost notes, because every hit is scored on a slow host:
# - titles come from item_headings, not one to_tsvector per hit;
# - `OFFSET 0` keeps each lateral from being inlined, so each rank is computed once;
# - the expanded rank is skipped when its weight is 0 (no keyword expansion);
# - `floor` drops items that cannot reach the result: the score is coverage plus a part
#   below 1, so when `limit` items cover at least c operands, an item covering fewer
#   scores below all of them. Coverage tiers below the minimum never count;
# - the projection, with passage text, is read only for the returned rows.
KEYWORD = """WITH query AS (
                SELECT %s::tsquery AS strict, %s::tsquery AS ranked, %s::tsquery AS matched,
                    %s::text[]::tsquery[] AS operands, %s::float8 AS weight,
                    %s::tsquery AS expanded,
                    websearch_to_tsquery('{literal}',research_search_text(%s)) AS literal
                ), hits AS (
                SELECT p.id AS hit, p.item_id AS hit_item, p.search AS vector, p.method,
                    COALESCE(t.heading,{heading}) AS heading,
                    NOT p.search @@ query.matched AS literal_only
                FROM passages p JOIN items i ON i.id=p.item_id
                LEFT JOIN item_headings t ON t.item_id=p.item_id CROSS JOIN query
                WHERE {where} AND (p.search @@ query.matched OR
                    (p.method='metadata' AND strpos(query.literal::text,'!')=0
                    AND p.search @@ query.literal))
                ), coverage AS (
                SELECT h.hit_item, count(DISTINCT o.n) AS covered FROM hits h CROSS JOIN query
                CROSS JOIN LATERAL unnest(query.operands) WITH ORDINALITY AS o(operand,n)
                WHERE h.vector @@ o.operand OR h.heading @@ o.operand GROUP BY h.hit_item
                ), floor AS (
                SELECT COALESCE(max(t.covered),0) AS covered FROM (SELECT covered,
                    count(*) OVER (ORDER BY covered DESC) AS reach FROM coverage) t
                WHERE t.covered >= %s AND t.reach >= %s
                ), scored AS (
                SELECT DISTINCT ON (h.hit_item) h.hit AS id,COALESCE(c.covered,0) AS covered,
                    COALESCE(c.covered,0)+0.9*r.strict/(1+r.strict)+0.1*r.loose/(1+r.loose) AS score
                FROM hits h CROSS JOIN query LEFT JOIN coverage c ON c.hit_item=h.hit_item
                CROSS JOIN LATERAL (SELECT h.heading || h.vector AS document,
                    strpos(query.literal::text,'!')=0 AND h.method='metadata' AS literal_rank
                    OFFSET 0) d
                CROSS JOIN LATERAL (SELECT ts_rank_cd(d.document, CASE WHEN d.literal_rank
                    THEN query.strict || query.literal ELSE query.strict END) AS strict,
                    ts_rank_cd(d.document, CASE WHEN d.literal_rank
                    THEN query.ranked || query.literal ELSE query.ranked END)
                    + CASE WHEN query.weight=0 THEN 0
                    ELSE query.weight*ts_rank_cd(d.document,query.expanded) END AS loose
                    OFFSET 0) r
                WHERE (COALESCE(c.covered,0) >= %s OR h.literal_only)
                    AND COALESCE(c.covered,0) >= (SELECT covered FROM floor)
                ORDER BY h.hit_item,score DESC,h.hit
                ), best AS (
                SELECT * FROM scored ORDER BY score DESC,id LIMIT %s
                ) SELECT {projection},best.covered,best.score
                FROM best JOIN passages p ON p.id=best.id JOIN items i ON i.id=p.item_id
                ORDER BY best.score DESC,best.id"""


# How hybrid search fuses the keyword list: `plain` (every candidate at full RRF weight),
# `weighted` (each candidate's share times its coverage fraction) or `floor` (only
# candidates within one operand of the best coverage). Chosen on the development cases.
FUSION = "plain"
FUSIONS = ("plain", "weighted", "floor")


def _keyword_fusion(rows, count, mode=None):
    """The keyword list as hybrid search fuses it (FUSION)."""
    mode = FUSION if mode is None else mode
    if mode not in FUSIONS:
        raise ValueError("keyword fusion must be plain, weighted or floor")
    if mode == "plain" or not count or not rows:
        return rows
    if mode == "weighted":
        return [{**row, "fusion_weight": row["covered"] / count} for row in rows]
    best = max(row["covered"] for row in rows)
    return [row for row in rows if row["covered"] >= best - 1]


def _keyword_query(projection, where, text, terms, mode=None):
    """Keyword SQL, its leading parameters (before the scope's) and the minimum coverage.

    One passage per document, its best: candidates are documents, not passages. Score:
    item coverage (an integer) plus a part below 1, so coverage orders first. Within a
    coverage, the rank of the typed query with all its terms weighs 0.9 and the rank of
    any of its operands 0.1, both as rank/(1+rank); a passage that holds every term keeps
    its earlier order. Expansion-only matches add EXPANSION_WEIGHT times their rank to
    the second part, a discount.
    """
    sql = KEYWORD.format(
        projection=projection,
        where=where,
        heading=HEADING.format(config=lexical.CONFIG),
        literal=lexical.LITERAL,
    )
    weight = lexicon.EXPANSION_WEIGHT if terms["expanded"] else 0.0
    head = (
        terms["strict"],
        terms["ranked"],
        terms["matched"],
        terms["operands"],
        weight,
        terms["expanded"] or terms["ranked"],
        text,
    )
    return sql, head, lexical.min_should_match(len(terms["operands"]), mode)


def _keyword_tail(minimum, candidates):
    """KEYWORD parameters after the scope's: the floor's minimum and reach, the
    minimum coverage and the row limit."""
    return (minimum, candidates, minimum, candidates)


# Recent query embeddings (query_vector_cache), keyed by model space and text.
_QUERY_VECTORS = OrderedDict()
_QUERY_VECTORS_LOCK = threading.Lock()


def _embed_queries(settings, texts):
    """embed() for query texts, reusing vectors of texts embedded before in this process.

    The model space is pinned by digest, so the same text has the same vector."""
    size = getattr(settings, "query_vector_cache", 0)
    if not size:
        return embed(settings, texts)
    space = (settings.embedding_url, settings.embedding_model, settings.embedding_revision)
    with _QUERY_VECTORS_LOCK:
        known = {t: _QUERY_VECTORS[(space, t)] for t in texts if (space, t) in _QUERY_VECTORS}
    missing = [t for t in dict.fromkeys(texts) if t not in known]
    if missing:
        known |= dict(zip(missing, embed(settings, missing), strict=True))
    with _QUERY_VECTORS_LOCK:
        for t in texts:
            _QUERY_VECTORS[(space, t)] = known[t]
            _QUERY_VECTORS.move_to_end((space, t))
        while len(_QUERY_VECTORS) > size:
            _QUERY_VECTORS.popitem(last=False)
    return [list(known[t]) for t in texts]


def _query_vectors(settings, text, expansion, arms, *, vector=None, expanded=None):
    """The query vector, then the expanded query's when the vector arm is expanded.

    Missing vectors are embedded in one request; given ones are never recomputed.
    """
    texts = [text] if vector is None else []
    if "vector" in arms and expanded is None:
        texts.append(lexicon.expanded_text(expansion, text))
    embedded = _embed_queries(settings, texts) if texts else []
    vector = embedded.pop(0) if vector is None else vector
    if "vector" not in arms:
        return [vector]
    return [vector, embedded.pop(0) if expanded is None else expanded]


def _vector_rows(db, scope, vector, candidates):
    """Exact (or opted-in approximate) nearest passages to one query vector."""
    projection, where, params = scope
    literal = vector_literal(vector, db.settings.embedding_dimensions)
    expression = (
        f"e.embedding::vector({db.settings.embedding_dimensions})"
        if db.settings.approximate_search
        else "e.embedding"
    )
    return db.rows(
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
                "rrf_contribution": row.get("fusion_weight", 1) / (RRF_K + position)
                if len(rankings) > 1
                else None,
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
