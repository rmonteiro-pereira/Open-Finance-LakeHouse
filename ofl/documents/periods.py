"""The month and year a manager letter covers, read only from sources that state it.

Many listing pages label a letter with a bare month ("Jun", "Junho") and leave the year
to the page layout. This module finds the year in the letter's file name, its listing
page, its upload date or its first page, and gives the letter a title that names both.
It never guesses: a source that is missing, ambiguous or contradicts the title's month
gives nothing, and the title stays as it was.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from urllib.parse import unquote, urlparse

MONTH_NAMES = (
    "Janeiro",
    "Fevereiro",
    "Março",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro",
)
_ABBREVIATIONS = (
    "jan",
    "fev",
    "mar",
    "abr",
    "mai",
    "jun",
    "jul",
    "ago",
    "set",
    "out",
    "nov",
    "dez",
)

# Portuguese month names by their lower-case spelling; the letters come from Brazilian
# managers, and English names such as "may" are also ordinary words. Accent-free spellings are found by
# folding, except "marco": it is also a common Portuguese word ("marco regulatório"), so a
# title must spell "março".
_FULL_NAMES = {name.casefold(): number for number, name in enumerate(MONTH_NAMES, 1)}
_SHORT_NAMES = {name: number for number, name in enumerate(_ABBREVIATIONS, 1)}

_WORD = re.compile(r"[^\W\d_]+", re.UNICODE)
_YEAR = r"(?:19|20)\d\d"
_TITLE_YEAR = re.compile(rf"(?<!\d)({_YEAR})(0[1-9]|1[0-2])?(?!\d)")
_NUMERIC_PERIOD = re.compile(rf"(?<![0-9A-Za-z])({_YEAR})[-_ .]?(0[1-9]|1[0-2])(?![0-9A-Za-z])")
_FILE_NAME_TOKEN = re.compile(r"[^\W\d_]+|\d+", re.UNICODE)
_CAMEL_CASE_BOUNDARY = re.compile(r"(?<=[a-z])(?=[A-Z])")
_MONTH_TEXT = "|".join(
    sorted(
        {*map(str.casefold, MONTH_NAMES), "marco", *_ABBREVIATIONS},
        key=len,
        reverse=True,
    )
)
_TEXT_PERIOD = re.compile(
    rf"(?<![^\W\d_])({_MONTH_TEXT})\.?\s*(?:de\s+|/\s*|-\s*)?({_YEAR})(?!\d)",
    re.IGNORECASE,
)
# Only the start of the first page is read: a letter names its own month in its header,
# and the rest of the page discusses other months.
FIRST_PAGE_CHARACTERS = 4000
EARLIEST_YEAR = 1900


@dataclass(frozen=True)
class Period:
    """One calendar month."""

    year: int
    month: int

    @property
    def iso(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def label(self) -> str:
        return f"{MONTH_NAMES[self.month - 1]} {self.year}"


def _fold(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    return decomposed.encode("ascii", "ignore").decode().casefold()


def _month_of_word(word: str, *, allow_short: bool, allow_marco: bool) -> int | None:
    lowered = word.casefold()
    if lowered in _FULL_NAMES:
        return _FULL_NAMES[lowered]
    folded = _fold(word)
    if folded == "marco":
        return 3 if allow_marco else None
    if folded in _FULL_NAMES:
        return _FULL_NAMES[folded]
    if allow_short and folded in _SHORT_NAMES:
        return _SHORT_NAMES[folded]
    return None


def _single(values: set[int]) -> int | None:
    return next(iter(values)) if len(values) == 1 else None


def is_bare_month(title: str) -> bool:
    """True when a title is only a month name or its abbreviation, such as "Jun" or "Junho"."""
    words = _WORD.findall(title)
    if len(words) != 1 or re.search(r"\d", title):
        return False
    return _month_of_word(words[0], allow_short=True, allow_marco=True) is not None


def _title_months(title: str) -> set[int]:
    if is_bare_month(title):
        month = _month_of_word(_WORD.findall(title)[0], allow_short=True, allow_marco=True)
        return {month} if month else set()
    months = set()
    for word in _WORD.findall(title):
        month = _month_of_word(word, allow_short=False, allow_marco=False)
        if month:
            months.add(month)
    numeric = {int(match.group(2)) for match in _TITLE_YEAR.finditer(title) if match.group(2)}
    return months | numeric


def _title_years(title: str) -> set[int]:
    return {int(match.group(1)) for match in _TITLE_YEAR.finditer(title)}


def title_month(title: str) -> int | None:
    """The one month a title names, or None when it names none or several.

    Abbreviations count only when they are the whole title, because "set" and "out" are
    also ordinary words."""
    return _single(_title_months(title))


def title_year(title: str) -> int | None:
    """The one year a title names (also inside "202301"), or None."""
    return _single(_title_years(title))


def plausible_year(year: int, latest_year: int) -> bool:
    """True for a year a letter can cover; a later one is a typing or reading error."""
    return EARLIEST_YEAR <= year <= latest_year


def _month_years(tokens: list[str], index: int) -> set[int]:
    """Four-digit years next to the month word at tokens[index], also across "de"."""
    before = tokens[max(index - 1, 0) : index]
    after = tokens[index + 1 : index + 2]
    if after and after[0].casefold() == "de":
        after = tokens[index + 2 : index + 3]
    return {int(token) for token in before + after if re.fullmatch(_YEAR, token)}


def file_name_period(url: str) -> Period | None:
    """The month a document's file name names: "Carta_Abril_2020.pdf", "Carta_2025_04.pdf",
    "Carta-Junho-de-2023.pdf".

    A month word counts only next to a four-digit year. A name with several different
    months, or with another year anywhere in it, gives nothing."""
    name = unquote(urlparse(url).path.rsplit("/", 1)[-1])
    stem = name.rsplit(".", 1)[0] if "." in name else name
    periods = {
        Period(int(match.group(1)), int(match.group(2))) for match in _NUMERIC_PERIOD.finditer(stem)
    }
    tokens = _FILE_NAME_TOKEN.findall(_CAMEL_CASE_BOUNDARY.sub(" ", stem))
    for index, token in enumerate(tokens):
        month = _month_of_word(token, allow_short=True, allow_marco=True)
        if month:
            periods.update(Period(year, month) for year in _month_years(tokens, index))
    if len(periods) != 1:
        return None
    period = next(iter(periods))
    stated_years = {int(token) for token in tokens if re.fullmatch(_YEAR, token)}
    return period if stated_years <= {period.year} else None


def date_period(published: str) -> Period | None:
    """The month of an ISO date or month ("2024-03-15", "2024-03"), or None."""
    match = re.fullmatch(rf"({_YEAR})-(0[1-9]|1[0-2])(?:-\d\d.*)?", published.strip())
    return Period(int(match.group(1)), int(match.group(2))) if match else None


def first_page_year(text: str, month: int, *, latest_year: int) -> int | None:
    """The year the start of a letter's first page gives its month ("Junho/2023").

    Years after latest_year are reading errors and are ignored. Two different years for
    the same month give nothing."""
    years = set()
    for match in _TEXT_PERIOD.finditer(text[:FIRST_PAGE_CHARACTERS]):
        named = _month_of_word(match.group(1), allow_short=True, allow_marco=True)
        year = int(match.group(2))
        if named == month and plausible_year(year, latest_year):
            years.add(year)
    return _single(years)


@dataclass(frozen=True)
class LetterSources:
    """What is known about one letter, from the collector manifest and the catalog."""

    title: str
    url: str
    listing_year: int | None = None
    published: str = ""
    first_page: str | None = None


def _agrees(period: Period | None, month: int | None, year: int | None) -> bool:
    if period is None:
        return False
    if month is not None and period.month != month:
        return False
    return year is None or period.year == year


def _year_only_period(sources: LetterSources, month: int, *, latest_year: int) -> Period | None:
    """The period from sources that give only a year for the month the title names."""
    listing_year = sources.listing_year
    if listing_year and plausible_year(listing_year, latest_year):
        return Period(listing_year, month)
    uploaded = date_period(sources.published)
    if uploaded and uploaded.month == month and plausible_year(uploaded.year, latest_year):
        return uploaded
    if not sources.first_page:
        return None
    found = first_page_year(sources.first_page, month, latest_year=latest_year)
    return Period(found, month) if found else None


def letter_period(sources: LetterSources, *, latest_year: int) -> Period | None:
    """The month a letter covers, from the first source that states it.

    The order is the title itself, the file name, the listing page's year heading, the
    upload date and the first page. A title that names several months or several years
    gives nothing. A source must agree with the month and year the title names, and a
    year after latest_year is a typing or reading error. The listing year, upload date
    and first page only give a year, so they are used only when the title names the month
    and no year."""
    months = _title_months(sources.title)
    years = _title_years(sources.title)
    if len(months) > 1 or len(years) > 1:
        return None
    month = _single(months)
    year = _single(years)
    if month and year:
        return Period(year, month)
    from_file_name = file_name_period(sources.url)
    plausible = from_file_name is not None and plausible_year(from_file_name.year, latest_year)
    if plausible and _agrees(from_file_name, month, year):
        return from_file_name
    if month is None or year is not None:
        return None
    return _year_only_period(sources, month, latest_year=latest_year)


def needs_first_page(sources: LetterSources, *, latest_year: int) -> bool:
    """True when only the first page could still give this letter's year."""
    if _title_years(sources.title) or title_month(sources.title) is None:
        return False
    return letter_period(sources, latest_year=latest_year) is None


def dated_title(title: str, period: Period | None) -> str:
    """The title with the letter's month and year: "Jun" becomes "Junho 2023",
    "Carta Mensal Julho" becomes "Carta Mensal Julho 2025"."""
    if period is None or title_year(title):
        return title
    if is_bare_month(title):
        return period.label
    if title_month(title) == period.month:
        return f"{title} {period.year}"
    return f"{title} - {period.label}"
