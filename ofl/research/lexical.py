"""Lexical search text: accent-insensitive, locale-independent vectors and OR+coverage queries.

Index and query side use the same text search configurations, created by the migration:
`portuguese_unaccent` (unaccent, then the Portuguese stemmer) for bodies and
`simple_unaccent` (unaccent, no stemming) for catalog metadata. Both read text through
`research_search_text()`, which turns typographic quotes, dashes, ellipses and bullets into
ASCII quotes or spaces. With unaccent before the stemmer, a database whose LC_CTYPE is C
lowercases the folded letters too, so vectors no longer depend on the database locale
(`ministro”` and `É` were stored fused and unfolded under C).

The keyword arm matches passages that contain ANY positive top-level operand of the
websearch query (a word, a quoted phrase or a vocabulary concept group) and none of its
negated operands. Coverage is the number of distinct positive operands matched anywhere in
the passage's item (any active passage of the scope, or the title); ranking is coverage
first, then ts_rank_cd of the passage with the title at weight A. MIN_SHOULD_MATCH sets the
minimum coverage.
"""

import math

VERSION = "lexical-v1"
CONFIG = "portuguese_unaccent"
LITERAL = "simple_unaccent"
# Vectors built before lexical-v1 (search_version NULL or LEGACY): raw text, Portuguese
# bodies and simple metadata. Only a rollback writes them (rebuild target LEGACY).
LEGACY = "legacy"
LEGACY_CONFIG = "portuguese"
# Characters research_search_text() maps (schema.sql): quotes to '"', the rest to spaces.
QUOTES = "“”„‟«»"
SPACES = "‘’‚‛‹›—–‐‑‒―…•·"
# `none`: any one operand matches; `ceil60`: ceil(0.6 n) of n >= 3 operands.
MIN_SHOULD_MATCH = "none"
MODES = ("none", "ceil60")
DOCUMENT = (
    f"setweight(to_tsvector('{LITERAL}',research_search_text(%s)),'A') || "
    f"setweight(to_tsvector('{CONFIG}',research_search_text(%s)),'B')"
)
DOCUMENTS = {
    VERSION: DOCUMENT,
    LEGACY: f"setweight(to_tsvector('simple',%s),'A') || "
    f"setweight(to_tsvector('{LEGACY_CONFIG}',%s),'B')",
}


def document_params(text, method):
    """Parameters of DOCUMENT: metadata text is also indexed literally (weight A)."""
    return (text if method == "metadata" else "", text)


def operands(parsed):
    """Top-level operands of a tsquery's text form, split at `&` and `|` outside
    parentheses and quoted lexemes (`'a' & 'b' <-> 'c' | !'d'` -> three operands)."""
    parts, depth, quoted, start, index = [], 0, False, 0, 0
    while index < len(parsed):
        character = parsed[index]
        if quoted:
            # An escaped quote ('') closes and reopens the lexeme: the state is the same.
            quoted = character != "'"
        elif character == "'":
            quoted = True
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif depth == 0 and character in "&|":
            parts.append(parsed[start:index])
            start = index + 1
        index += 1
    parts.append(parsed[start:])
    return [part.strip() for part in parts if part.strip()]


def distinct(parts):
    """Operands without repeats, in first-seen order: equal parsed text is one term, so
    `selic selic` and words that stem alike count once."""
    return list(dict.fromkeys(parts))


def split(parts):
    """Positive and negated operands."""
    return [p for p in parts if not p.startswith("!")], [p for p in parts if p.startswith("!")]


def disjunction(positives, negatives):
    """Any positive operand and no negated one; None without positives."""
    if not positives:
        return None
    return " & ".join(["(" + " | ".join(f"( {p} )" for p in positives) + ")", *negatives])


def min_should_match(count, mode=None):
    """Minimum item coverage for `count` positive operands."""
    mode = MIN_SHOULD_MATCH if mode is None else mode
    if mode not in MODES:
        raise ValueError("min-should-match must be none or ceil60")
    if mode == "ceil60" and count >= 3:
        return math.ceil(0.6 * count)
    return 1 if count else 0


# A passage is pending when its vector is not in the target version (NULL is legacy).
PENDING = "COALESCE(search_version,'legacy') IS DISTINCT FROM %s"


def rebuild(db, *, batch=1000, dry_run=False, target=VERSION, progress=lambda _: None):
    """Rewrite passage vectors into the `target` version, in committed batches.

    `target` is VERSION, or LEGACY to roll back before running code that predates
    lexical-v1. Resumable: each batch commits; rows locked by a concurrent writer are
    skipped and stay pending for the next pass. Every batch waits at most 5 s for a lock.
    """
    if target not in DOCUMENTS:
        raise ValueError("rebuild target must be lexical-v1 or legacy")
    pending = db.rows(f"SELECT count(*) AS n FROM passages WHERE {PENDING}", (target,))[0]["n"]
    result = {"lexical": target, "pending": pending, "rebuilt": 0}
    if dry_run:
        return result
    expression = (
        DOCUMENTS[target]
        .replace("%s", "{}")
        .format("CASE WHEN p.method='metadata' THEN p.text ELSE '' END", "p.text")
    )
    while True:
        with db.connect() as con:
            con.execute("SET LOCAL lock_timeout = '5s'")
            done = con.execute(
                f"""WITH batch AS (SELECT id FROM passages WHERE {PENDING}
                ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED)
                UPDATE passages p SET search={expression}, search_version=%s
                FROM batch WHERE p.id=batch.id""",
                (target, batch, target),
            ).rowcount
        if not done:
            result["remaining"] = db.rows(
                f"SELECT count(*) AS n FROM passages WHERE {PENDING}", (target,)
            )[0]["n"]
            return result
        result["rebuilt"] += done
        progress(dict(result))
