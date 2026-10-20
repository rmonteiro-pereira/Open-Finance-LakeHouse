"""Curated domain vocabulary: optional, disclosed query expansion for research search.

Users name economic concepts in plain words (`inflação brasileira`, `juros básicos`,
`bolsa`, `desemprego`, `PIB`); documents name them by institution or indicator (`IPCA`,
`Selic`, `Copom`, `Ibovespa`, `PNAD`, `Caged`, `IBC-Br`). Each lexicon entry maps a
concept, written in Portuguese or English, to such terms, with the reason it holds.

Expansion is query side only and never changes stored passages. With `on`:

- keyword arm: each matched concept becomes a disjunction of the concept as written
  and its expansions (`(inflação & brasil) | ipca | ...`), combined with the rest of the
  query by AND. Rank is the original query's rank plus EXPANSION_WEIGHT times the rank
  of the expanded query, so passages that match only an expansion rank below those
  that also match the words the user typed;
- vector arm (when in ARMS): one more list in the reciprocal-rank fusion (k=60) from an
  embedding of the query followed by its expansions, so each passage gains at most 1/61.

Concepts inside a negated term or a quoted phrase are never expanded. A query that uses
the `or` operator is searched without expansion (rewriting it would change the
operator's grouping). Entries with a home jurisdiction (Brazilian indicators for an
unqualified `desemprego`) are skipped when the query names or mentions any other
jurisdiction. Matching uses the canonical, accent-folded query form of
ofl.research.jurisdiction, case-insensitively and on whole words.
"""

import re

from . import jurisdiction as places

VERSION = "vocab-v1"
SETTINGS = ("off", "on")
# Arms `on` expands. Evaluation variants pass their own arms (VARIANTS).
ARMS = ("keyword",)
VARIANTS = {"vocab-k": ("keyword",), "vocab-kv": ("keyword", "vector")}
# Design choice, not a measured optimum: expansion-only matches score half the
# keyword rank they would score as an original-term match.
EXPANSION_WEIGHT = 0.5

# Jurisdiction modifiers that qualify a concept next to it (`inflação no Brasil`,
# `Brazilian inflation`, `juros americanos`), on accent-folded text.
BR = r"(?:(?:no|do|da|de|em|in|of) )?(?:brasil|brazil)|brasileir[oa]s?|brazilian"
US = (
    r"(?:(?:nos|dos|nas|das|em|in|of|in the|of the) )?(?:eua|estados unidos|united states|u\.s\.|usa|us)"
    r"|american[oa]s?|norte-american[oa]s?|american"
)
EA = r"(?:(?:na|da|em|in|of|in the|of the) )?(?:zona do euro|area do euro|eurozona|euro ?zone|euro area)"

INFLATION = r"inflacao|inflation"
RATES = r"juros|taxas? de juros|interest rates?"
STOCKS = r"bolsas?(?: de valores)?|mercado de acoes|acoes|stock market|stocks|equities"
CENTRAL_BANK = r"banco central|bc|central bank"
UNEMPLOYMENT = r"desemprego|desempregados?|unemployment(?: rate)?|jobless(?:ness)?"


def around(concept, modifier):
    """The concept followed or preceded by a jurisdiction modifier."""
    return rf"(?:{concept}) (?:{modifier})|(?:{modifier}) (?:{concept})"


# (concept, pattern on folded text, expansions, home jurisdiction or None, rationale)
LEXICON = [
    (
        "inflation-br",
        around(INFLATION, BR),
        (
            '"IPCA" -"Tesouro IPCA"',
            "IPCA-15",
            "INPC",
            "IGP-M",
            "índice de preços ao consumidor amplo",
        ),
        None,
        "IBGE's IPCA is the official inflation-target index; Brazilian sources report "
        "inflation by index name (IPCA, IPCA-15, INPC) and FGV's IGP-M. `Tesouro IPCA+` "
        "names an inflation-linked bond, not an inflation reading.",
    ),
    (
        "inflation-us",
        around(INFLATION, US),
        ("PCE", "índice de preços ao consumidor", "inflação ao consumidor"),
        None,
        "US inflation is reported as the BLS consumer price index and the PCE price index "
        "the Federal Reserve targets. The acronym CPI is left out: in Portuguese it also "
        "names a parliamentary inquiry (Comissão Parlamentar de Inquérito).",
    ),
    (
        "inflation-ea",
        around(INFLATION, EA),
        ("HICP", "Eurostat", "índice harmonizado de preços"),
        None,
        "Euro-area inflation is Eurostat's harmonised index of consumer prices (HICP).",
    ),
    (
        "inflation",
        INFLATION,
        ("índice de preços", "preços ao consumidor", "desinflação"),
        None,
        "Unqualified inflation: neutral price-index wording only, no national index.",
    ),
    (
        "policy-rate-br",
        around(RATES, BR) + r"|juros basicos|taxa basica(?: de juros)?",
        ("Selic", "Copom"),
        None,
        "Brazil's policy rate is the Selic, set by the central bank's Copom.",
    ),
    (
        "policy-rate-us",
        around(RATES, US) + r"|fed funds",
        ("Fed", "FOMC", "Federal Reserve", "fed funds"),
        None,
        "The US policy rate is the fed funds target set by the Federal Reserve's FOMC.",
    ),
    (
        "policy-rate-ea",
        around(RATES, EA),
        ("BCE", "ECB", "Banco Central Europeu"),
        None,
        "Euro-area policy rates are set by the European Central Bank (BCE/ECB).",
    ),
    (
        "monetary-policy-br",
        around(r"politica monetaria|monetary policy", BR),
        ("Copom", "Selic", "BCB"),
        None,
        "Brazilian monetary policy is the Copom's Selic decision by the BCB.",
    ),
    (
        "monetary-policy",
        r"politica monetaria|monetary policy",
        ("banco central", "bancos centrais", "aperto monetário", "afrouxamento monetário"),
        None,
        "Unqualified monetary policy: neutral central-bank wording, no named institution.",
    ),
    (
        "central-bank-br",
        around(CENTRAL_BANK, BR),
        ("BCB", "Copom"),
        None,
        "Brazil's central bank is the BCB; its rate committee is the Copom.",
    ),
    (
        "central-bank-us",
        around(CENTRAL_BANK, US),
        ("Fed", "Federal Reserve", "FOMC"),
        None,
        "The US central bank is the Federal Reserve (Fed); its rate committee is the FOMC.",
    ),
    (
        "stocks-br",
        around(STOCKS, BR) + r"|bolsa paulista|bolsa de sao paulo|bovespa",
        ("Ibovespa", "B3"),
        None,
        "Brazil's stock exchange is B3; its benchmark index is the Ibovespa.",
    ),
    (
        "stocks-us",
        around(STOCKS, US) + r"|bolsas? de nova (?:york|iorque)",
        ("Wall Street", "S&P 500", "Nasdaq", "Dow Jones"),
        None,
        "US equities are reported as Wall Street and its indices (S&P 500, Nasdaq, Dow Jones).",
    ),
    (
        "stocks",
        r"bolsas?(?: de valores)?(?! (?:familia|de estudos?|verde|escola|atleta))"
        r"|mercado de acoes|stock market|stock exchange",
        ("Ibovespa", "B3"),
        "BR",
        "An unqualified stock exchange in this Brazilian corpus is B3 (Ibovespa); "
        "`Bolsa Família` and other grants are not exchanges.",
    ),
    (
        "fx-br",
        r"cambio|taxa de cambio|exchange rate|dolar|brazilian real"
        r"|real (?:x|contra o|frente ao) dolar|dolar (?:x|contra o|frente ao) real",
        ("dólar", "PTAX", "taxa de câmbio"),
        "BR",
        "The Brazilian exchange rate is quoted as reais per US dollar; the BCB's "
        "reference rate is the PTAX.",
    ),
    (
        "unemployment-us",
        around(UNEMPLOYMENT, US),
        ("payroll", "relatório de emprego", "seguro-desemprego"),
        None,
        "US labour data are the monthly payroll (employment situation) report and weekly "
        "jobless claims.",
    ),
    (
        "unemployment-br",
        UNEMPLOYMENT,
        ("taxa de desocupação", "desocupação", "PNAD", "Caged", "mercado de trabalho"),
        "BR",
        "IBGE reports unemployment as the `taxa de desocupação` of the PNAD Contínua; "
        "formal jobs come from the Novo Caged.",
    ),
    (
        "formal-jobs-br",
        r"empregos? formais?|emprego formal|vagas formais|geracao de empregos"
        r"|criacao de (?:empregos|vagas)|formal jobs|job creation",
        ("Caged", "Novo Caged", "carteira assinada", "saldo de vagas"),
        "BR",
        "Formal job creation in Brazil is the Novo Caged net balance of hires and dismissals.",
    ),
    (
        "gdp-br",
        r"pib|produto interno bruto|gdp|crescimento economico|economic growth"
        r"|atividade economica|economic activity",
        ("PIB", "produto interno bruto", "IBC-Br", "atividade econômica"),
        "BR",
        "IBGE publishes GDP (PIB) quarterly; the BCB's IBC-Br is its monthly proxy.",
    ),
    (
        "fiscal-br",
        r"contas publicas|situacao fiscal|politica fiscal|risco fiscal|deficit publico"
        r"|rombo fiscal|fiscal policy|public finances|fiscal deficit",
        ("resultado primário", "arcabouço fiscal", "meta fiscal", "Tesouro Nacional"),
        "BR",
        "Brazilian fiscal results are reported as the primary result against the fiscal "
        "framework's target (arcabouço fiscal), published by the Tesouro Nacional.",
    ),
    (
        "debt-br",
        r"divida publica|divida do governo|endividamento publico|public debt|government debt",
        ("dívida bruta", "DBGG", "dívida líquida", "Tesouro Nacional"),
        "BR",
        "Brazilian public debt is reported as gross general government debt (DBGG) and "
        "net public-sector debt.",
    ),
    (
        "bonds-br",
        r"titulos publicos|titulos do governo|government bonds",
        ("Tesouro Direto", "NTN-B", "LTN", "títulos do Tesouro"),
        "BR",
        "Brazilian government bonds are the Tesouro's LTN and NTN-B, sold retail through "
        "the Tesouro Direto.",
    ),
    (
        "rate-curve-br",
        r"juros futuros|curva de juros|yield curve|curva de rendimentos?",
        ("DI", "contratos de DI", "taxas de DI", "DI futuro"),
        "BR",
        "Brazil's rate curve trades as B3's interbank deposit (DI) futures.",
    ),
    (
        "expectations-br",
        r"expectativas? (?:do|de) mercado|projecoes do mercado|expectativas de inflacao"
        r"|market expectations|inflation expectations",
        ("Boletim Focus", "Relatório Focus", "Focus"),
        "BR",
        "The BCB's weekly Focus survey is the reference for Brazilian market expectations.",
    ),
    (
        "retail-br",
        r"vendas (?:no|do) varejo|vendas varejistas|retail sales",
        ("PMC", "Pesquisa Mensal de Comércio", "comércio varejista"),
        "BR",
        "IBGE's monthly trade survey (PMC) measures Brazilian retail sales.",
    ),
    (
        "industry-br",
        r"producao industrial|industrial production|industrial output",
        ("PIM", "Pesquisa Industrial Mensal", "produção da indústria"),
        "BR",
        "IBGE's monthly industrial survey (PIM) measures Brazilian industrial production.",
    ),
    (
        "services-br",
        r"(?:volume|setor) de servicos|services sector",
        ("PMS", "Pesquisa Mensal de Serviços"),
        "BR",
        "IBGE's monthly services survey (PMS) measures the Brazilian services sector.",
    ),
    (
        "trade-br",
        r"balanca comercial|trade balance|comercio exterior",
        ("Secex", "superávit comercial", "déficit comercial", "exportações"),
        "BR",
        "Brazil's trade balance is published by Secex as a surplus or deficit of exports "
        "over imports.",
    ),
    (
        "fuel-br",
        r"precos? (?:dos?|da) (?:combustiveis|gasolina|diesel)|fuel prices?|gasoline prices?",
        ("Petrobras", "ANP", "gasolina", "diesel"),
        "BR",
        "Brazilian fuel prices follow Petrobras refinery prices and are surveyed by the ANP.",
    ),
    (
        "oil",
        r"(?:preco|precos|cotacao) do petroleo|petroleo|oil prices?|crude oil",
        ("Brent", "WTI", "barril"),
        None,
        "Oil prices are quoted per barrel as the Brent and WTI benchmarks.",
    ),
    (
        "cost-of-living",
        r"custo de vida|cost of living|poder de compra|purchasing power",
        ("inflação", "orçamento familiar", "orçamento doméstico", "alta de preços"),
        None,
        "Household cost of living is discussed as inflation and family budgets; neutral "
        "wording, no national index.",
    ),
]
PATTERNS = [
    (concept, re.compile(rf"(?<![\w-])(?:{pattern})(?![\w-])", re.I), expansions, home)
    for concept, pattern, expansions, home, _ in LEXICON
]
OPERATOR = re.compile(r"(?<!\S)or(?!\S)", re.I)


def check_setting(setting):
    if setting not in SETTINGS:
        raise ValueError("vocabulary must be off or on")
    return setting


def quoted_spans(query):
    """Spans inside double quotes (with the quotes); an unclosed quote runs to the end."""
    spans, start = [], None
    for index, character in enumerate(query):
        if character != '"':
            continue
        if start is None:
            start = index
        else:
            spans.append((start, index + 1))
            start = None
    if start is not None:
        spans.append((start, len(query)))
    return spans


def matches(query):
    """Non-overlapping concept matches on the canonical query: (start, end, entry index).

    Longer matches win, then earlier ones, then earlier lexicon entries.
    """
    folded, index = places._folded(query)
    found = [
        (index[m.start()], index[m.end() - 1] + 1, position)
        for position, (_, pattern, _, _) in enumerate(PATTERNS)
        for m in pattern.finditer(folded)
    ]
    found.sort(key=lambda m: (m[0] - m[1], m[0], m[2]))
    kept = []
    for match in found:
        if all(match[1] <= other[0] or match[0] >= other[1] for other in kept):
            kept.append(match)
    return sorted(kept)


def _others(query):
    """Whether the query names or mentions any jurisdiction other than Brazil."""
    return bool(set(places.detect(query)) - {"BR"} or places.foreign(query))


def plan(setting, query):
    """The expansions a search applies, and those it skipped with the reason.

    `off` never reads the query. `remainder` is the canonical query without the
    expanded concepts, searched by AND with each concept's disjunction.
    """
    setting = check_setting(setting)
    result = {"setting": setting, "lexicon": VERSION, "concepts": [], "skipped": []}
    if setting == "off":
        return result
    query = places.canonical(query)
    quotes = quoted_spans(query)
    blocked = places.negated_spans(query) + quotes
    unquoted = "".join(
        " " if any(a <= i < b for a, b in quotes) else c for i, c in enumerate(query)
    )
    operator = bool(OPERATOR.search(unquoted))
    others = None
    kept, remainder, last = [], [], 0
    for start, end, position in matches(query):
        concept, _, expansions, home = PATTERNS[position]
        entry = {"concept": concept, "matched": query[start:end]}
        if any(start < b and end > a for a, b in blocked):
            result["skipped"].append(entry | {"reason": "negated_or_quoted"})
            continue
        if operator:
            result["skipped"].append(entry | {"reason": "or_operator"})
            continue
        if home is not None:
            others = _others(query) if others is None else others
            if others:
                result["skipped"].append(entry | {"reason": "other_jurisdiction"})
                continue
        kept.append(entry | {"expansions": list(expansions)})
        remainder.append(query[last:start])
        last = end
    remainder.append(query[last:])
    result["concepts"] = kept
    result["remainder"] = " ".join(" ".join(remainder).split())
    return result


def term(expansion):
    """websearch_to_tsquery text of an expansion and the phrase it names.

    A plain expansion is searched as a quoted phrase. One that starts with a quote is
    already websearch syntax: its first quoted phrase, minus negated phrases
    (`"IPCA" -"Tesouro IPCA"`).
    """
    if not expansion.startswith('"'):
        return f'"{expansion}"', expansion
    return expansion, expansion.split('"')[1]


def tsquery(rule):
    """SQL expression and parameters of the expanded Portuguese tsquery, None if unused."""
    if not rule["concepts"]:
        return None
    parse = "websearch_to_tsquery('portuguese',%s)"
    parts, params = [parse], [rule["remainder"]]
    for concept in rule["concepts"]:
        terms = [concept["matched"], *(term(t)[0] for t in concept["expansions"])]
        parts.append("(" + " || ".join([parse] * len(terms)) + ")")
        params += terms
    return " && ".join(parts), params


def expanded_text(rule, query):
    """Text embedded for the extra vector list: the query followed by its expansions."""
    terms = [term(t)[1] for concept in rule["concepts"] for t in concept["expansions"]]
    return f"{places.canonical(query)} ({', '.join(terms)})"


def disclosure(rule, arms):
    """Response disclosure for `on`; None for `off`."""
    if rule["setting"] == "off":
        return None
    return {
        "setting": rule["setting"],
        "lexicon": rule["lexicon"],
        "applied": bool(rule["concepts"]),
        "arms": list(arms) if rule["concepts"] else [],
        "concepts": rule["concepts"],
        "skipped": rule["skipped"],
        "qualification": QUALIFICATION,
    }


QUALIFICATION = (
    "Curated query expansion: each matched concept also searches the listed institutional "
    "terms, ranked below matches of the words as written. Concepts in negated terms or "
    "quoted phrases are never expanded; queries using OR are searched as written; "
    "Brazil-default entries are skipped when the query names another jurisdiction. "
    "Expansion is lexical, not topic classification."
)
