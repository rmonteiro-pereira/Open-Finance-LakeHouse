"""Deterministic jurisdiction evidence for passages and explicitly named query jurisdictions.

Evidence is a lexical observation with provenance, never a classification of what a
document is about. Two tiers:

- strong: a country or currency-area name in substantive position (`Brasil`, `EUA`,
  `zona do euro`, `China`), a national institution or indicator (`IPCA`, `Copom`, `Fed`,
  `BCE`, `PBoC`) or a national market (`Ibovespa`, `Wall Street`, `Nasdaq`);
- weak: demonyms (`brasileiro`, `americana`, `chinês`), cities (`Brasília`,
  `Washington`, `Xangai`), currencies and symbols (`real`, `R$`, `US$`, `euro`, `yuan`)
  state banks (`Banco do Brasil`, `Bank of China`) and generic terms (`Banco Central`,
  `CPI`, `Treasury`). Weak evidence is recorded but never creates a tag.

State banks are national evidence but, like company news generally, not a statement
about the country, so they are weak: recorded, never decisive. Reviewed multiword
entities that contain a country word without referring to the country (publisher,
brand, road, sports names: `Lojas Americanas`, `CNN Brasil`, `Rodovia Washington Luís`,
`futebol americano`) mask their words and count as nothing.

In queries, strong markers and national demonyms name a jurisdiction; European
demonyms do not name the euro area, which needs an explicit name or institution.

A jurisdiction is tagged with a strong title mention or at least two strong body
mentions. When another jurisdiction has at least three times its strong evidence, it
needs at least two strong mentions. Matches inside related-link/navigation text are
`related` and never count. Portuguese text does not imply Brazil. A passage may carry
several jurisdictions or none (unknown).
"""

import hashlib
import re

from psycopg.types.json import Jsonb

VERSION = "jurisdiction-v4"
RULES = ("off", "boost", "exclude")
# Query-side rules act only for jurisdictions whose v4 tags passed the hand-check
# precision gate (>= 0.90; BR 40/40, US 38-39/40, EA 3/3). CN (17/21 = 0.81) failed it:
# CN passages are still tagged and stored, but a query naming China applies no rule.
GATED_JURISDICTIONS = frozenset({"BR", "US", "EA"})
MAX_MATCHES = 40
RELATED_WORDS = 30
DOMINANCE = 3
# Must equal ofl.research.archive.chunks: passages are 350-word windows overlapping by 40.
CHUNK_WORDS = 350
CHUNK_OVERLAP = 40

# (code, tier, kind, pattern). `(?-i:...)` marks case-sensitive acronyms and names.
MARKERS = [
    ("BR", "strong", "name", r"brasil"),
    ("BR", "strong", "institution", r"(?-i:IPCA|IGP-M|IGP-DI|INPC|IBGE|BCB|COPOM|Copom|CVM|CMN)"),
    ("BR", "strong", "institution", r"(?-i:Caged|CAGED)|selic|tesouro nacional"),
    ("BR", "strong", "market", r"ibovespa"),
    ("BR", "weak", "demonym", r"brasileir[oa]s?"),
    ("BR", "weak", "city", r"bras[ií]lia"),
    ("BR", "weak", "currency", r"(?<!\w)(?-i:R)\$"),
    ("BR", "weak", "state_bank", r"banco do brasil"),
    ("BR", "weak", "generic", r"real|reais|banco central|tesouro"),
    ("US", "strong", "name", r"estados unidos|united states|(?-i:EUA|U\.S\.|USA)"),
    ("US", "strong", "institution", r"(?-i:Fed|FOMC)|federal reserve|casa branca|white house"),
    ("US", "strong", "market", r"wall street|(?-i:Nasdaq|NASDAQ)|dow jones|s&p ?500"),
    (
        "US",
        "weak",
        "demonym",
        r"(?<![\w-])american[oa]s?(?!\w)|(?<!\w)norte-american[oa]s?(?!\w)",
    ),
    ("US", "weak", "city", r"washington|nova york|new york"),
    ("US", "weak", "currency", r"(?<!\w)(?-i:US)\$"),
    ("US", "weak", "generic", r"(?-i:CPI)|treasury|treasuries|payroll"),
    ("EA", "strong", "name", r"zona do euro|[áa]rea do euro|eurozona|euro ?zone|euro area"),
    (
        "EA",
        "strong",
        "institution",
        r"banco central europeu|european central bank|eurostat|(?-i:BCE|ECB|HICP)",
    ),
    # European, not euro-area: never names EA in a query.
    ("EA", "weak", "regional", r"europeu|europeia|europeus|europeias|european"),
    ("EA", "weak", "currency", r"euros?"),
    ("EA", "weak", "generic", r"europa|europe"),
    ("CN", "strong", "name", r"china"),
    ("CN", "strong", "institution", r"(?-i:PBoC|PBOC)"),
    ("CN", "strong", "market", r"bolsa de (?:xangai|shanghai)"),
    ("CN", "weak", "demonym", r"chin[eê]s|chines[ae]s?"),
    ("CN", "weak", "city", r"pequim|beijing|xangai|shanghai"),
    ("CN", "weak", "state_bank", r"bank of china|china construction bank"),
    ("CN", "weak", "currency", r"yuan|renminbi"),
]
# Reviewed entities whose country word does not refer to the country.
ENTITIES = [
    r"(?:rodovia |avenida |av\. )?washington lu[ií]s",
    r"washington post|wall street journal|new york times",
    r"lojas americanas|americanas s\.?a\.?|(?-i:Americanas)",
    r"(?:cnn|tv|agência|agencia|rádio|radio) brasil|brasil paralelo",
    r"copa do brasil|(?:gp|grande prêmio|grande premio) do brasil|seleção brasileira",
    r"futebol americano|(?:moeda|divisa) (?:americana|norte-americana)|sonho americano",
    r"china mobile|air china|china evergrande",
]
PATTERNS = [
    (
        code,
        tier,
        kind,
        re.compile(pattern if pattern.startswith("(?<") else rf"(?<!\w)(?:{pattern})(?!\w)", re.I),
    )
    for code, tier, kind, pattern in MARKERS
] + [(None, "entity", "entity", re.compile(rf"(?<!\w)(?:{p})(?!\w)", re.I)) for p in ENTITIES]
PRIORITY = {"entity": 0, "state_bank": 0, "strong": 1, "weak": 2}
RELATED = re.compile(
    r"(?<!\w)(?:leia (?:também|mais|a seguir)|veja (?:também|mais)|saiba mais|confira (?:também|mais)"
    r"|assista (?:também|mais)|mais lidas|últimas notícias|mais notícias|recomendad[ao]s"
    r"|(?:notícias|matérias|conteúdos?|reportagens) relacionad[ao]s|read (?:more|also)"
    r"|related(?: articles| stories| news)?|(?:tags|tópicos|temas|assuntos)\s*:)(?!\w)",
    re.I,
)
FOOTER = re.compile(r"(?<!\w)the post\s.{0,600}?\sappeared first on\s[^.]{0,80}\.?", re.I | re.S)
END = re.compile(r"[.!?…](?=\s|$)")
ABBREVIATIONS = {
    "sr",
    "sra",
    "srs",
    "dr",
    "dra",
    "prof",
    "profa",
    "av",
    "vs",
    "nº",
    "jr",
    "mr",
    "mrs",
    "ms",
    "inc",
    "ltd",
    "corp",
    "gov",
    "dep",
    "sen",
    "pres",
    "min",
    "etc",
}
INITIALS = re.compile(r"(?:\w\.)+")


def check_rule(rule):
    if rule not in RULES:
        raise ValueError("jurisdiction rule must be off, boost or exclude")
    return rule


def sentence_end(text, start):
    """End of the sentence from `start`; abbreviations (U.S., E.U.A., Sr.) do not end it."""
    for stop in END.finditer(text, start):
        token = re.search(r"\S*$", text[max(start, stop.start() - 40) : stop.end()]).group()
        bare = token.strip("()[]\"'“”«»")
        if stop.group() == "." and (INITIALS.fullmatch(bare) or bare[:-1].lower() in ABBREVIATIONS):
            continue
        return stop.end()
    return None


def related_spans(text):
    """Related-link/navigation regions, only where a cue marks them.

    A marker runs to its sentence end, at most 30 words. Feed footers ("The post ...
    appeared first on ...") are related. Unmarked text, even a closing fragment without
    punctuation, keeps body provenance: stored passages have no line structure that
    would show a link list.
    """
    words = [w.span() for w in re.finditer(r"\S+", text)]
    spans = []
    for marker in RELATED.finditer(text):
        after = [w for w in words if w[0] >= marker.end()]
        limit = after[RELATED_WORDS - 1][1] if len(after) >= RELATED_WORDS else len(text)
        stop = sentence_end(text, marker.end())
        spans.append((marker.start(), min(limit, len(text) if stop is None else stop)))
    spans += [m.span() for m in FOOTER.finditer(text)]
    return sorted(spans)


def find(text):
    """Non-overlapping matches; entities and state banks, then strong, then longer, then
    earlier win (`Banco do Brasil` is not also a strong `Brasil`)."""
    found = [
        (PRIORITY.get(kind, PRIORITY[tier]), match.start(), match.end(), code, tier)
        for code, tier, kind, pattern in PATTERNS
        for match in pattern.finditer(text)
    ]
    found.sort(key=lambda m: (m[0], m[1] - m[2], m[1]))
    kept = []
    for match in found:
        if all(match[2] <= other[1] or match[1] >= other[2] for other in kept):
            kept.append(match)
    return sorted(m[1:] for m in kept)


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


def decide(counts):
    """Tag decision per jurisdiction from strong title/body counts, with its reason."""
    totals = {code: n["strong"]["title"] + n["strong"]["body"] for code, n in counts.items()}
    decisions = {}
    for code, n in counts.items():
        strong, total = n["strong"], totals[code]
        if strong["title"] or strong["body"] >= 2:
            dominated = total < 2 and any(
                other != code and totals[other] >= DOMINANCE * total for other in totals
            )
            decisions[code] = "dominated" if dominated else "title" if strong["title"] else "body"
        else:
            decisions[code] = "single_body" if strong["body"] else "no_strong_evidence"
    return decisions


def tag(text, title="", related=None, unclassified=(), context="passage"):
    """Passage tags with every match's provenance, per-tier counts and decision.

    `related`/`unclassified` are spans of `text`; without them, related regions come
    from `text` alone, read as a whole document.
    """
    title = title or ""
    flat = " ".join(title.split())
    shift = len(flat) if flat and text.startswith(flat) else 0
    body = text[shift:]
    related = related_spans(text) if related is None else related
    matches = [(*m, "title") for m in find(title)]
    for start, end, code, tier in find(body):
        where = "body"
        if any(start + shift >= a and end + shift <= b for a, b in related):
            where = "related"
        elif any(start + shift < b and end + shift > a for a, b in unclassified):
            where = "unclassified"
        matches.append((start, end, code, tier, where))
    counts, entities = {}, 0
    for _, _, code, tier, where in matches:
        if tier == "entity":
            entities += 1
            continue
        empty = {"title": 0, "body": 0, "related": 0, "unclassified": 0}
        counts.setdefault(code, {"strong": dict(empty), "weak": dict(empty)})[tier][where] += 1
    decisions = decide(counts)
    return {
        "jurisdiction": sorted(c for c, reason in decisions.items() if reason in {"title", "body"}),
        "evidence": {
            "tagger": VERSION,
            "context": context,
            "title_sha256": hashlib.sha256(title.encode()).hexdigest(),
            "counts": counts,
            "decisions": decisions,
            "entities": entities,
            "matches": [
                {
                    "code": code,
                    "term": (title if where == "title" else body)[start:end],
                    "where": where,
                    "strength": tier,
                    "offset": start,
                }
                for start, end, code, tier, where in matches[:MAX_MATCHES]
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
    """Jurisdictions the query names: strong markers or demonyms outside negated terms.

    A marker overlapping a negated term at all (`-Estados Unidos` negates `Estados`) is
    ignored; entity names (`futebol americano`) never name a jurisdiction.
    """
    negated = negated_spans(query)
    demonyms = {
        match.span()
        for _, _, kind, pattern in PATTERNS
        if kind == "demonym"
        for match in pattern.finditer(query)
    }
    return sorted(
        {
            code
            for start, end, code, tier in find(query)
            if (tier == "strong" or (tier == "weak" and (start, end) in demonyms))
            and not any(start < b and end > a for a, b in negated)
        }
    )


def plan(rule, query):
    """The rule a search applies: `active` names the jurisdictions it acts on.

    `off` never reads the query. When the query names any jurisdiction outside
    GATED_JURISDICTIONS, the whole query is searched as `off` (no partial rule) and the
    ungated codes are reported as `ignored`.
    """
    rule = check_rule(rule)
    named = detect(query) if rule != "off" else []
    ignored = sorted(set(named) - GATED_JURISDICTIONS)
    return {"rule": rule, "named": named, "active": [] if ignored else named, "ignored": ignored}


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
