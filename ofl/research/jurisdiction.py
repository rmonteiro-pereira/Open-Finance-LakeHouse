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

VERSION = "jurisdiction-v1"
RULES = ("off", "boost", "exclude")
WEIGHTED = ("title", "body")
MAX_MATCHES = 40
RELATED_WORDS = 30

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


def tag(text, title=""):
    """Passage tags from strong title/body matches, with every match's provenance."""
    title = title or ""
    flat = " ".join(title.split())
    body = text[len(flat) :] if flat and text.startswith(flat) else text
    related = related_spans(body)
    matches = [(*m, "title") for m in find(title)]
    for start, end, code, strength in find(body):
        inside = any(start >= a and end <= b for a, b in related)
        matches.append((start, end, code, strength, "related" if inside else "body"))
    counts = {}
    for start, end, code, strength, where in matches:
        key = where if strength == "strong" else "weak"
        counts.setdefault(code, {"title": 0, "body": 0, "related": 0, "weak": 0})[key] += 1
    codes = sorted(c for c, n in counts.items() if n["title"] + n["body"])
    source = {"title": title, "body": body, "related": body}
    return {
        "jurisdiction": codes,
        "evidence": {
            "tagger": VERSION,
            "title_sha256": hashlib.sha256(title.encode()).hexdigest(),
            "counts": counts,
            "matches": [
                {
                    "code": code,
                    "term": source[where][start:end],
                    "where": where,
                    "strength": strength,
                    "offset": start,
                }
                for start, end, code, strength, where in matches[:MAX_MATCHES]
            ],
        },
    }


def detect(query):
    """Jurisdictions the query names with a strong marker; negated (-term) mentions ignored."""
    codes = set()
    for start, _, code, strength in find(query):
        negated = start and query[start - 1] == "-" and (start == 1 or query[start - 2].isspace())
        if strength == "strong" and not negated:
            codes.add(code)
    return sorted(codes)


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


def columns(text, title):
    result = tag(text, title)
    return result["jurisdiction"], Jsonb(result["evidence"]), VERSION


PENDING = """p.active AND (p.jurisdiction_tagger IS DISTINCT FROM %s
    OR p.jurisdiction_evidence->>'title_sha256' IS DISTINCT FROM
    encode(sha256(convert_to(i.title,'UTF8')),'hex'))"""


def backfill(db, *, batch=1000, dry_run=False, progress=lambda _: None):
    """Tag active passages with a missing/older tagger or changed title. Embeddings untouched."""
    pending = db.rows(
        f"SELECT count(*) AS n FROM passages p JOIN items i ON i.id=p.item_id WHERE {PENDING}",
        (VERSION,),
    )[0]["n"]
    result = {"tagger": VERSION, "pending": pending, "tagged": 0}
    if dry_run:
        return result
    last = ""
    while True:
        rows = db.rows(
            f"""SELECT p.id,p.text,i.title FROM passages p JOIN items i ON i.id=p.item_id
            WHERE {PENDING} AND p.id>%s ORDER BY p.id LIMIT %s""",
            (VERSION, last, batch),
        )
        if not rows:
            return result
        with db.connect() as con:
            con.cursor().executemany(
                """UPDATE passages SET jurisdiction=%s,jurisdiction_evidence=%s,
                jurisdiction_tagger=%s WHERE id=%s""",
                [(*columns(row["text"], row["title"]), row["id"]) for row in rows],
            )
        last = rows[-1]["id"]
        result["tagged"] += len(rows)
        progress(dict(result))
