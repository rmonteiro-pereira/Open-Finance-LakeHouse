"""Deterministic jurisdiction evidence for passages and explicitly named query jurisdictions.

Evidence is a lexical observation with provenance, never a classification of what a
document is about. Strong markers name one jurisdiction unambiguously (a place, demonym,
institution, indicator or currency symbol). Weak markers ("real", "CPI", "Treasury",
"Banco Central", "euro") are recorded for audit but never create a tag, alone or together.
Language is not a marker: Portuguese text does not imply Brazil. Matches inside
related-link/navigation text ("Leia também: ...", "Tags: ...") are recorded as `related`
and never create a tag. A passage may carry several jurisdictions or none (unknown).
"""

import hashlib
import re

from psycopg.types.json import Jsonb

VERSION = "jurisdiction-v2"
RULES = ("off", "boost", "exclude")
WEIGHTED = ("title", "body")
MAX_MATCHES = 40
RELATED_WORDS = 30
# Must equal ofl.research.archive.chunks: passages are 350-word windows overlapping by 40.
CHUNK_WORDS = 350
CHUNK_OVERLAP = 40

# (code, strength, pattern). `(?-i:...)` marks case-sensitive acronyms.
MARKERS = [
    ("BR", "strong", r"brasil"),
    ("BR", "strong", r"brasileir[oa]s?"),
    ("BR", "strong", r"bras[ií]lia"),
    ("BR", "strong", r"(?-i:IPCA|IGP-M|IGP-DI|INPC|IBGE|BCB|COPOM|Copom|CVM|CMN|Caged|CAGED)"),
    ("BR", "strong", r"selic|ibovespa|tesouro nacional"),
    ("BR", "strong", r"(?<!\w)(?-i:R)\$"),
    ("BR", "weak", r"real|reais|banco central|tesouro"),
    ("US", "strong", r"estados unidos|united states|(?-i:EUA|U\.S\.|USA)"),
    ("US", "strong", r"(?<![\w-])american[oa]s?(?!\w)|(?<!\w)norte-american[oa]s?(?!\w)"),
    ("US", "strong", r"(?-i:Fed|FOMC)|federal reserve|wall street|casa branca|white house"),
    ("US", "strong", r"(?-i:Nasdaq|NASDAQ)|dow jones|washington"),
    ("US", "weak", r"(?-i:CPI)|treasury|treasuries|payroll"),
    ("US", "weak", r"(?<!\w)(?-i:US)\$"),
    ("EA", "strong", r"zona do euro|[áa]rea do euro|eurozona|euro ?zone|euro area"),
    ("EA", "strong", r"banco central europeu|european central bank|(?-i:BCE|ECB|HICP)"),
    ("EA", "weak", r"euros?|europa|europe|europeu|europeia|europeus|europeias|european"),
    ("CN", "strong", r"china|chin[eê]s|chines[ae]s?|pequim|beijing|xangai|shanghai"),
    ("CN", "strong", r"yuan|renminbi|(?-i:PBoC|PBOC)"),
]
PATTERNS = [
    (
        code,
        strength,
        re.compile(pattern if pattern.startswith("(?<") else rf"(?<!\w)(?:{pattern})(?!\w)", re.I),
    )
    for code, strength, pattern in MARKERS
]
RELATED = re.compile(
    r"(?<!\w)(?:leia (?:também|mais|a seguir)|veja (?:também|mais)|saiba mais|confira (?:também|mais)"
    r"|mais lidas|últimas notícias|(?:notícias|matérias|conteúdos?) relacionad[ao]s|read (?:more|also)"
    r"|related(?: articles| stories| news)?|(?:tags|tópicos|temas|assuntos)\s*:)(?!\w)",
    re.I,
)
END = re.compile(r"[.!?](?=\s|$)")


def check_rule(rule):
    if rule not in RULES:
        raise ValueError("jurisdiction rule must be off, boost or exclude")
    return rule


def related_spans(text):
    """Related-link/navigation regions: a marker up to its sentence end, at most 30 words."""
    spans = []
    for marker in RELATED.finditer(text):
        start = marker.end()
        words = list(re.finditer(r"\S+", text[start:]))
        limit = start + words[RELATED_WORDS - 1].end() if len(words) >= RELATED_WORDS else len(text)
        stop = END.search(text, start)
        spans.append((marker.start(), min(limit, stop.end() if stop else len(text))))
    return spans


def find(text):
    """Non-overlapping marker matches; strong and longer matches win overlaps."""
    found = [
        (match.start(), match.end(), code, strength)
        for code, strength, pattern in PATTERNS
        for match in pattern.finditer(text)
    ]
    found.sort(key=lambda m: (m[3] != "strong", m[0] - m[1], m[0]))
    kept = []
    for match in found:
        if all(match[1] <= other[0] or match[0] >= other[1] for other in kept):
            kept.append(match)
    return sorted(kept)


def _word_spans(words):
    spans, offset = [], 0
    for word in words:
        spans.append((offset, offset + len(word)))
        offset += len(word) + 1
    return spans


def _merge(spans):
    merged = []
    for a, b in spans:
        if merged and a <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], b)
        else:
            merged.append((a, b))
    return merged


def document(texts):
    """Rebuild a document's words from its chunk sequence, or None if they do not chain."""
    words = [text.split() for text in texts]
    step = CHUNK_WORDS - CHUNK_OVERLAP
    for previous, current in zip(words, words[1:]):
        if len(previous) != CHUNK_WORDS or current[:CHUNK_OVERLAP] != previous[step:]:
            return None
    return words[0] + [word for chunk in words[1:] for word in chunk[CHUNK_OVERLAP:]]


def passage_regions(ordinals, texts):
    """Related-region provenance per passage, computed on the whole document when possible.

    Without the complete chunk chain (ordinals 0..n), each passage falls back to its own
    text and its first 30 words, which a region opened in an earlier chunk could still
    cover, are `unclassified` evidence that never creates a tag.
    """
    words = document(texts) if list(ordinals) == list(range(len(texts))) else None
    if words is None:
        regions = []
        for ordinal, text in zip(ordinals, texts, strict=True):
            spans = _word_spans(text.split())[:RELATED_WORDS] if ordinal else []
            regions.append(
                {
                    "related": related_spans(text),
                    "unclassified": [(spans[0][0], spans[-1][1])] if spans else [],
                    "context": "passage",
                }
            )
        return regions
    joined = _word_spans(words)
    inside = {
        index
        for a, b in related_spans(" ".join(words))
        for index, (start, end) in enumerate(joined)
        if start >= a and end <= b
    }
    step = CHUNK_WORDS - CHUNK_OVERLAP
    return [
        {
            "related": _merge(
                span
                for index, span in enumerate(_word_spans(text.split()))
                if ordinal * step + index in inside
            ),
            "unclassified": [],
            "context": "document",
        }
        for ordinal, text in zip(ordinals, texts, strict=True)
    ]


def tag(text, title="", related=None, unclassified=(), context="passage"):
    """Passage tags from strong title/body matches, with every match's provenance.

    `related`/`unclassified` are spans of `text`; without them, related regions come
    from the passage text alone.
    """
    title = title or ""
    flat = " ".join(title.split())
    shift = len(flat) if flat and text.startswith(flat) else 0
    body = text[shift:]
    related = related_spans(text) if related is None else related
    matches = [(*m, "title") for m in find(title)]
    for start, end, code, strength in find(body):
        where = "body"
        if any(start + shift >= a and end + shift <= b for a, b in related):
            where = "related"
        elif any(start + shift < b and end + shift > a for a, b in unclassified):
            where = "unclassified"
        matches.append((start, end, code, strength, where))
    counts = {}
    for start, end, code, strength, where in matches:
        key = where if strength == "strong" else "weak"
        counts.setdefault(
            code, {"title": 0, "body": 0, "related": 0, "unclassified": 0, "weak": 0}
        )[key] += 1
    codes = sorted(c for c, n in counts.items() if n["title"] + n["body"])
    return {
        "jurisdiction": codes,
        "evidence": {
            "tagger": VERSION,
            "context": context,
            "title_sha256": hashlib.sha256(title.encode()).hexdigest(),
            "counts": counts,
            "matches": [
                {
                    "code": code,
                    "term": (title if where == "title" else body)[start:end],
                    "where": where,
                    "strength": strength,
                    "offset": start,
                }
                for start, end, code, strength, where in matches[:MAX_MATCHES]
            ],
        },
    }


def negated_spans(query):
    """Spans PostgreSQL websearch_to_tsquery negates: `-word`, `- word` or `-"a phrase"`.

    A hyphen inside a word (`Brasil-EUA`) or inside quotes is not negation. An unclosed
    quote runs to the end of the query. `--x` is treated as negated (conservative).
    """
    spans, index, size = [], 0, len(query)
    while index < size:
        if query[index].isspace():
            index += 1
            continue
        negated = query[index] == "-"
        start = index + 1 if negated else index
        while negated and start < size and (query[start].isspace() or query[start] == "-"):
            start += 1
        if start < size and query[start] == '"':
            close = query.find('"', start + 1)
            end = size if close < 0 else close + 1
        else:
            end = start
            while end < size and not query[end].isspace() and query[end] != '"':
                end += 1
        if negated:
            spans.append((start, end))
        index = max(end, index + 1)
    return spans


def detect(query):
    """Jurisdictions the query names with a strong marker outside negated terms."""
    negated = negated_spans(query)
    return sorted(
        {
            code
            for start, end, code, strength in find(query)
            if strength == "strong" and not any(a <= start and end <= b for a, b in negated)
        }
    )


def exclusion(codes):
    """Keep untagged, unknown, any-named, or only-named passages; drop other-only evidence."""
    return (
        "(p.jurisdiction IS NULL OR p.jurisdiction <@ %s::text[] OR p.jurisdiction && %s::text[])",
        [list(codes), list(codes)],
    )


def boosted(ranking, codes):
    """The extra fused list: base-order candidates with evidence of a named jurisdiction."""
    named = set(codes)
    return [row for row in ranking if named & set(row.get("jurisdiction") or ())]


def columns(text, title, **regions):
    result = tag(text, title, **regions)
    return result["jurisdiction"], Jsonb(result["evidence"]), VERSION


PENDING = """p.active AND (p.jurisdiction_tagger IS DISTINCT FROM %s
    OR p.jurisdiction_evidence->>'title_sha256' IS DISTINCT FROM
    encode(sha256(convert_to(i.title,'UTF8')),'hex'))"""


def backfill(db, *, batch=1000, dry_run=False, progress=lambda _: None):
    """Re-tag pending active passages without re-embedding.

    Each passage is tagged in the context of its document, rebuilt from all passages of
    the same item revision (active or not), exactly as at ingestion. When the chain is
    incomplete, regions it cannot classify are marked `unclassified` and do not count.
    `batch` bounds document groups per transaction.
    """
    pending = db.rows(
        f"SELECT count(*) AS n FROM passages p JOIN items i ON i.id=p.item_id WHERE {PENDING}",
        (VERSION,),
    )[0]["n"]
    result = {"tagger": VERSION, "pending": pending, "tagged": 0, "partial_context": 0}
    if dry_run:
        return result
    last = ("", "")
    while True:
        groups = db.rows(
            f"""SELECT DISTINCT p.item_id,p.revision FROM passages p JOIN items i ON i.id=p.item_id
            WHERE {PENDING} AND (p.item_id,p.revision)>(%s,%s) ORDER BY 1,2 LIMIT %s""",
            (VERSION, *last, batch),
        )
        if not groups:
            return result
        rows = db.rows(
            f"""SELECT p.id,p.item_id,p.revision,p.ordinal,p.text,i.title,{PENDING} AS pending
            FROM passages p JOIN items i ON i.id=p.item_id
            WHERE (p.item_id,p.revision) IN (SELECT * FROM unnest(%s::text[],%s::text[]))
            ORDER BY p.item_id,p.revision,p.ordinal""",
            (VERSION, [g["item_id"] for g in groups], [g["revision"] for g in groups]),
        )
        updates, chains = [], {}
        for row in rows:
            chains.setdefault((row["item_id"], row["revision"]), []).append(row)
        for chain in chains.values():
            regions = passage_regions([r["ordinal"] for r in chain], [r["text"] for r in chain])
            for row, region in zip(chain, regions, strict=True):
                if row["pending"]:
                    updates.append((*columns(row["text"], row["title"], **region), row["id"]))
                    result["partial_context"] += region["context"] == "passage"
        with db.connect() as con:
            con.cursor().executemany(
                """UPDATE passages SET jurisdiction=%s,jurisdiction_evidence=%s,
                jurisdiction_tagger=%s WHERE id=%s""",
                updates,
            )
        last = (groups[-1]["item_id"], groups[-1]["revision"])
        result["tagged"] += len(updates)
        progress(dict(result))
