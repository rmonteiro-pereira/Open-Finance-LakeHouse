"""The fund a manager letter is about, read only from sources that state it.

A manager that runs several funds often publishes one letter per fund each period, and
its listing page labels each letter with the month alone, so two letters end up with the
same title ("Dezembro 2024"). This module finds the fund's name in the letter's link
text, its file name, its manifest record or the header of its first page, and puts it in
the title when the collection publishes letters for more than one fund.

It never guesses. A name must carry a fund-type code written in capitals (FIA, FIM, FIC,
...), the way Brazilian fund names are registered. It must have at least one word besides
the code ("Alfa FIA", "Delta FIA"). It must be set apart from the text around it by a
separator, a document word ("Carta"), a month or a number. A source gives nothing when a
name sits inside running text, runs longer than a name can be or is a bare code. It also
gives nothing when it names two different funds, unless the two are joined with "+" as in
a letter written for both.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal
from urllib.parse import unquote, urlparse

from ofl.documents.periods import HEADER_CHARACTERS, fold, is_month_word

# Fund-type codes from the names funds are registered under. Only the capitalised form
# counts, because "fim" is also an ordinary Portuguese word.
FUND_TYPE_CODES = frozenset(
    {
        "FIA",
        "FIAGRO",
        "FIC",
        "FICFIA",
        "FICFIM",
        "FIDC",
        "FIE",
        "FIF",
        "FII",
        "FIM",
        "FIP",
        "FIQ",
        "FIRF",
    }
)
# Words that name the document or address its readers ("Carta Mensal", "Prezados
# Cotistas"). A fund name is often written next to them, so they end a name cleanly.
_DOCUMENT_WORDS = frozenset(
    {
        "anual",
        "annual",
        "apresentacao",
        "cara",
        "caro",
        "caros",
        "carta",
        "cartas",
        "comentario",
        "comentarios",
        "cotistas",
        "desempenho",
        "fundo",
        "gerencial",
        "gestao",
        "gestor",
        "gestora",
        "informe",
        "investidores",
        "letter",
        "mensal",
        "monthly",
        "newsletter",
        "performance",
        "prezado",
        "prezados",
        "quarterly",
        "relatorio",
        "relatorios",
        "report",
        "resultado",
        "resultados",
        "semestral",
        "senhores",
        "trimestral",
    }
)
# Words that join a sentence. A name next to one of them sits inside running text, as in
# "UM BOM FIM DE ANO" or "Alfa Fundo de Fundos FIM", so where it starts or ends is not
# known and the text names no fund.
_SENTENCE_WORDS = frozenset(
    {
        "a",
        "ao",
        "aos",
        "as",
        "ate",
        "com",
        "da",
        "das",
        "de",
        "do",
        "dos",
        "e",
        "em",
        "na",
        "nas",
        "nesta",
        "neste",
        "no",
        "nos",
        "o",
        "os",
        "para",
        "pela",
        "pelo",
        "por",
        "que",
        "sem",
        "um",
        "uma",
    }
)
# Words that may follow the code and still belong to the name ("Gama FIF Ações", "Alfa
# FIDC NP"). Any other word after the code starts the text around the name.
_QUALIFIER_WORDS = frozenset(
    {
        "acoes",
        "cp",
        "credito",
        "ie",
        "master",
        "multimercado",
        "np",
        "prev",
        "previdenciario",
        "privado",
        "rf",
        "rl",
    }
)
# A name has at most this many words before its code; a longer run is a sentence.
MAX_NAME_WORDS = 6
# Words after the code that still belong to the name ("Gama FIF Ações").
MAX_TRAILING_WORDS = 2
# In an all-capital header, words this short are kept in capitals: they are usually
# initials ("RE", "XP") rather than words.
MAX_INITIALS_LETTERS = 3

# The one code that is also an ordinary Portuguese word ("fim", end).
_AMBIGUOUS_CODE = "FIM"
_JOIN = "+"
# Stands for the rest of a text that was cut short; it neither is nor bounds a name.
_CUT_OFF = "\N{HORIZONTAL ELLIPSIS}"
_BOUNDARY = "|"
_TOKEN = re.compile(r"[^\W_]+|\s[-–—]\s|[–—+|:/()\[\],;\n]", re.UNICODE)
_CAMEL_CASE_BOUNDARY = re.compile(r"(?<=[a-z])(?=[A-Z])")
_CODE_AND_NUMBER = re.compile(r"([A-Z]+)(\d+)")


FundSource = Literal["link_text", "file_name", "manifest", "first_page"]


@dataclass(frozen=True)
class Fund:
    """A fund name and the source that stated it."""

    name: str
    source: FundSource

    @property
    def key(self) -> str:
        """The name compared without case, accents or punctuation."""
        return _words_key(self.name)


@dataclass(frozen=True)
class FundSources:
    """What is known about one letter that could name its fund."""

    link_text: str
    url: str
    manifest_fund: str = ""
    first_page: str | None = None


def _words_key(text: str) -> str:
    """The words of a text, folded and separated by single spaces, with one space at each
    end so that a key matches whole words only."""
    return " " + " ".join(re.findall(r"[^\W_]+", fold(text))) + " "


def _tokens(text: str) -> list[str]:
    """Words of a text, with every separator that ends a name as one boundary token.

    A code written together with a number ("FIA026") is split from it."""
    tokens = []
    for match in _TOKEN.finditer(text):
        token = match.group(0)
        code_and_number = _CODE_AND_NUMBER.fullmatch(token)
        if code_and_number and _is_code(code_and_number.group(1)):
            tokens.extend(code_and_number.groups())
        elif token == _JOIN or token[0].isalnum():
            tokens.append(token)
        else:
            tokens.append(_BOUNDARY)
    return tokens


def _is_code(token: str) -> bool:
    return token in FUND_TYPE_CODES


def _is_name_word(token: str, manager_words: tuple[str, ...]) -> bool:
    """True for a word that can be part of a fund name.

    Names are written with capitals ("Beta Hedge"). A word that starts with a digit
    counts only as the manager's own name ("7G"), since otherwise it is a date or a
    sequence number."""
    if token in (_JOIN, _BOUNDARY) or fold(token) in _DOCUMENT_WORDS | _SENTENCE_WORDS:
        return False
    if is_month_word(token):
        return False
    if token[0].isdigit():
        return fold(token) in manager_words
    return token[0].isupper()


def _bounds_name(token: str, manager_words: tuple[str, ...]) -> bool:
    """True for a token that can sit right next to a fund name: a separator, a document
    word, a month, a number or the manager's own name."""
    if token in (_JOIN, _BOUNDARY):
        return True
    if fold(token) in _DOCUMENT_WORDS or fold(token) in manager_words:
        return True
    return is_month_word(token) or token[0].isdigit()


def _is_delimited(tokens: list[str], start: int, end: int, manager_words: tuple[str, ...]) -> bool:
    """True when the words on both sides of tokens[start:end] end the name cleanly."""
    if start > 0 and not _bounds_name(tokens[start - 1], manager_words):
        return False
    return end == len(tokens) or _bounds_name(tokens[end], manager_words)


def _code_chain_end(tokens: list[str], start: int) -> int:
    """The token after a run of adjacent codes ("FIC FIA") that begins at start."""
    end = start
    while end < len(tokens) and _is_code(tokens[end]):
        end += 1
    return end


def _name_start(tokens: list[str], chain_start: int, floor: int, manager_words: tuple[str, ...]):
    """The first word of the name before a code chain, never before floor, or None when
    the name runs past MAX_NAME_WORDS and so has no reliable start.

    Another code ends the name: two codes apart are two funds ("Alfa FIA Beta FIM")."""

    def belongs(token):
        return not _is_code(token) and _is_name_word(token, manager_words)

    start = chain_start
    while start > floor and belongs(tokens[start - 1]):
        if chain_start - start == MAX_NAME_WORDS:
            return None
        start -= 1
    return start


def _is_trailing_word(token: str, manager_words: tuple[str, ...], *, has_own_words: bool) -> bool:
    """True for a word after the code that still belongs to the name.

    After a name such as "Alfa FIA" only qualifiers such as "Ações" or "NP" belong to it;
    the next capitalised word starts the text ("ALFA FIA PREZADOS COTISTAS"). A name that
    starts with its code ("FII Alfa Malls") takes the words that follow instead."""
    if _is_code(token) or any(char.isdigit() for char in token):
        return False
    if fold(token) in _QUALIFIER_WORDS:
        return True
    return not has_own_words and _is_name_word(token, manager_words)


def _name_end(tokens: list[str], chain_end: int, manager_words: tuple[str, ...], *, has_own_words):
    """The token after the name's last word, or None when the name runs past its limit:
    a cut name could make two funds ("FII Alfa Renda Logística", "FII Alfa Renda
    Corporativa") look like one."""
    limit = MAX_TRAILING_WORDS if has_own_words else MAX_NAME_WORDS
    end = chain_end
    while end < len(tokens):
        if not _is_trailing_word(tokens[end], manager_words, has_own_words=has_own_words):
            break
        if end - chain_end == limit:
            return None
        end += 1
    return end


def _has_own_words(words: list[str], manager_words: tuple[str, ...]) -> bool:
    return any(fold(word) not in manager_words for word in words)


def _without_manager_name(words: list[str], manager_words: tuple[str, ...]) -> list[str]:
    """The name without the manager's whole name in front, when the fund has a name of
    its own.

    "7G Alfa FIA" becomes "Alfa FIA" and "7G FII Alfa Malls" becomes "FII Alfa Malls";
    "7G FIA" stays as it is. Only the manager's whole
    name, in order, is dropped: for the manager "alfa-capital", "Capital Beta FIA" keeps
    "Capital"."""
    prefix_length = len(manager_words)
    has_prefix = tuple(fold(word) for word in words[:prefix_length]) == manager_words
    remaining = words[prefix_length:] if has_prefix else words
    if _own_words(remaining):
        return remaining
    return words


def _display(words: list[str]) -> str:
    """The name as written, with all-capital headers ("BETA HEDGE FIM") in title case."""
    letters = "".join(word for word in words if not _is_code(word))
    if not letters.isupper():
        return " ".join(words)
    shown = []
    for word in words:
        keep = _is_code(word) or len(word) <= MAX_INITIALS_LETTERS
        shown.append(word if keep else word.capitalize())
    return " ".join(shown)


def _own_words(words: list[str]) -> list[str]:
    """The words of a name that are neither codes nor qualifiers."""
    return [word for word in words if not _is_code(word) and fold(word) not in _QUALIFIER_WORDS]


def _names_a_fund(words: list[str], *, in_file_name: bool) -> bool:
    """True when a name says which fund it is.

    A code alone, or with qualifiers only ("Alfa FIA + FIM CP"), names some fund without
    saying which. In text a lone "FIM" may also be the word for "end" ("Desejamos bom
    FIM"), so outside file names it counts only next to another code ("Alfa FIC FIM")."""
    if not _own_words(words):
        return False
    codes = [word for word in words if _is_code(word)]
    return in_file_name or codes != [_AMBIGUOUS_CODE]


def _fund_runs(tokens: list[str], manager_words: tuple[str, ...], *, in_file_name: bool):
    """Every fund name in a token list, as (first token, token after the last, name).

    The name is None for a run whose start or end is not reliable."""
    runs = []
    floor = 0
    index = 0
    while index < len(tokens):
        if not _is_code(tokens[index]):
            index += 1
            continue
        chain_end = _code_chain_end(tokens, index)
        start = _name_start(tokens, index, floor, manager_words)
        own_words = start is not None and _has_own_words(tokens[start:index], manager_words)
        end = _name_end(tokens, chain_end, manager_words, has_own_words=own_words)
        reliable = start is not None and end is not None
        if not reliable or not _is_delimited(tokens, start, end, manager_words):
            runs.append((index, chain_end, None))
            floor = index = chain_end
            continue
        words = _without_manager_name(tokens[start:end], manager_words)
        named = _names_a_fund(words, in_file_name=in_file_name)
        runs.append((start, end, _display(words) if named else None))
        floor = index = end
    return runs


def _joined_names(tokens: list[str], runs) -> list[str | None]:
    """Names, with runs joined only by "+" merged into one: a letter for both funds."""
    names: list[str | None] = []
    previous_end = None
    for start, end, name in runs:
        joined = previous_end is not None and tokens[previous_end:start] == [_JOIN]
        if joined and name is not None and names[-1] is not None:
            names[-1] = f"{names[-1]} + {name}"
        else:
            names.append(name)
        previous_end = end
    return names


def stated_fund(
    text: str, collection: str, *, in_file_name: bool = False, cut_off: bool = False
) -> str | None:
    """The one fund a text names, or None when it names none or several.

    collection is the manager's id ("7g"), whose words are dropped from the front of a
    name: the manager's name says nothing about which of its funds a letter covers.
    in_file_name says the text is a file name, where a lone "FIM" is always the code.
    cut_off says the text is the start of a longer one, so a name that reaches its end may
    continue past it and is not used."""
    manager_words = tuple(fold(word) for word in re.split(r"[-_]", collection) if word)
    tokens = _tokens(text)
    if cut_off:
        tokens.append(_CUT_OFF)
    runs = _fund_runs(tokens, manager_words, in_file_name=in_file_name)
    names = _joined_names(tokens, runs)
    if None in names:
        return None
    distinct = {_words_key(name): name for name in names}
    if len(distinct) != 1:
        return None
    return next(iter(distinct.values()))


def file_name_fund(url: str, collection: str) -> str | None:
    """The fund a document's file name names: "7G_Alfa_FIA-Carta_do_Gestor-Junho_2024.pdf"
    gives "Alfa FIA"."""
    name = unquote(urlparse(url).path.rsplit("/", 1)[-1])
    stem = name.rsplit(".", 1)[0] if "." in name else name
    return stated_fund(_CAMEL_CASE_BOUNDARY.sub(" ", stem), collection, in_file_name=True)


def _may_name_an_unconfirmed_fund(text: str, collection: str) -> bool:
    """True when a text would name a fund if its lone "FIM" were the code.

    Such a text ("Alfa Macro FIM", but also "Desejamos bom FIM") may name the letter's
    fund, so a later source must not name a different one in its place."""
    return stated_fund(text, collection, in_file_name=True) is not None


def _fund_before_first_page(sources: FundSources, collection: str) -> tuple[Fund | None, bool]:
    """The fund from the link text, the file name or the manifest, in that order, and
    whether the search has to stop there, named or not."""
    texts: list[tuple[FundSource, str]] = [
        ("link_text", sources.link_text),
        ("file_name", sources.url),
        ("manifest", sources.manifest_fund),
    ]
    for source, text in texts:
        if source == "file_name":
            name = file_name_fund(text, collection)
        else:
            name = stated_fund(text, collection)
        if name:
            return Fund(name, source), True
        if source != "file_name" and _may_name_an_unconfirmed_fund(text, collection):
            return None, True
    return None, False


def needs_first_page_for_fund(sources: FundSources, collection: str) -> bool:
    """True when only the first page could still name this letter's fund."""
    return not _fund_before_first_page(sources, collection)[1]


def letter_fund(sources: FundSources, collection: str) -> Fund | None:
    """The fund a letter covers, from the first source that states it.

    The order is the link text, the file name, the manifest's ``fund`` field and the
    header of the letter's first extracted page (its first 300 characters, where the
    letterhead is). Every source follows the same naming rules, the manifest included.
    A source that may name a fund it cannot confirm (a lone "FIM" in text) ends the
    search with no fund, so a later source cannot attribute the letter to another one."""
    found, settled = _fund_before_first_page(sources, collection)
    if settled or not sources.first_page:
        return found
    header = sources.first_page[:HEADER_CHARACTERS]
    cut_off = len(sources.first_page) > HEADER_CHARACTERS
    from_header = stated_fund(header, collection, cut_off=cut_off)
    return Fund(from_header, "first_page") if from_header else None


def publishes_several_funds(funds: Iterable[Fund | None]) -> bool:
    """True when the letters of one collection name more than one fund."""
    return len({fund.key for fund in funds if fund}) > 1


def titled_with_fund(title: str, fund: Fund | None) -> str:
    """The title with the fund's name in front, unless it already names the fund:
    "Dezembro 2024" becomes "Alfa FIA - Dezembro 2024"."""
    if fund is None or fund.key in _words_key(title):
        return title
    return f"{fund.name} - {title}"
