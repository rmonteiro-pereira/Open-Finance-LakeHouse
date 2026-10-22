"""The month and year a letter covers, read from its title, file name, listing and text."""

import pytest

from ofl.documents.periods import (
    FIRST_PAGE_CHARACTERS,
    LetterSources,
    Period,
    dated_title,
    file_name_period,
    first_page_year,
    is_bare_month,
    letter_period,
    needs_first_page,
    title_month,
    title_year,
)

LATEST = 2026
OPAQUE_URL = "https://example.org/_files/ugd/0a1b2c_3d4e5f60718293a4b5c6d7e8f9012345.pdf"


@pytest.mark.parametrize(
    ("url", "period"),
    [
        ("https://example.org/Fundo_X-Carta_do_Gestor-Abril_2020.pdf", Period(2020, 4)),
        ("https://example.org/Fundo-Carta-Mar%C3%A7o_2021-Crise.pdf", Period(2021, 3)),
        ("https://example.org/Gestora%20Carta%20Mensal%202025_04.pdf", Period(2025, 4)),
        ("https://example.org/CartaMensal_202301.pdf", Period(2023, 1)),
        ("https://example.org/CartaDezembro2019.pdf", Period(2019, 12)),
        ("https://example.org/Carta-outubro-22-1.pdf", None),
        ("https://example.org/Carta-Junho-de-2023.pdf", Period(2023, 6)),
        ("https://example.org/Carta-Junho_2021-e-Dezembro_2021.pdf", None),
        ("https://example.org/Carta-Junho-2023-revisada-2024.pdf", None),
        ("https://example.org/Carta-Junho-2023-revisada-20240115.pdf", None),
        ("https://example.org/Carta-Junho-2023-Julho.pdf", None),
        ("https://example.org/Carta-Junho-2023-Jul.pdf", None),
        ("https://example.org/Carta-Junho-2023-revisada-20230715.pdf", None),
        ("https://example.org/Carta-Junho-2023-revisada-20230630.pdf", Period(2023, 6)),
        ("https://example.org/Carta%20Mensal%20Julho.pdf", None),
        ("https://example.org/Relatorio_20230303_final.pdf", None),
        (OPAQUE_URL, None),
    ],
)
def test_file_name_period(url, period):
    assert file_name_period(url) == period


@pytest.mark.parametrize(
    ("title", "month"),
    [
        ("Jun", 6),
        ("set", 9),
        ("Junho", 6),
        ("Carta Mensal Março", 3),
        ("Carta Mensal - Abril 2025", 4),
        ("Carta Mensal 202302", 2),
        ("Sell out", None),
        ("Novo marco regulatório", None),
        ("Janeiro a Março", None),
        ("COVID", None),
    ],
)
def test_title_month(title, month):
    assert title_month(title) == month


@pytest.mark.parametrize(
    ("title", "year"),
    [
        ("Carta 1Tri2010 Detalhamento", 2010),
        ("Carta Mensal 202301", 2023),
        ("Carta outubro 22", None),
        ("Carta 2019 e 2020", None),
    ],
)
def test_title_year_reads_four_digit_years_only(title, year):
    assert title_year(title) == year


def test_bare_month_is_a_month_word_alone():
    assert is_bare_month("Dez")
    assert is_bare_month("Março")
    assert not is_bare_month("Carta Mensal Março")
    assert not is_bare_month("Jun/2021")


@pytest.mark.parametrize(
    ("text", "month", "year"),
    [
        ("Gestora Exemplo | Carta Mensal | Junho/2023 | Em maio de 2023 os juros", 6, 2023),
        ("Gestora Exemplo | Carta Mensal | Junho/2023 | Em maio de 2023 os juros", 7, None),
        ("Carta jun. 2024", 6, 2024),
        # A year after the latest one is a reading error, not a second year.
        ("Fevereiro 2071 Fevereiro de 2021", 2, 2021),
        ("Junho 2023 versus junho de 2022", 6, None),
    ],
)
def test_first_page_year_takes_the_one_year_given_to_the_title_month(text, month, year):
    assert first_page_year(text, month, latest_year=LATEST) == year


def test_first_page_year_reads_only_the_start_of_the_page():
    text = "x" * FIRST_PAGE_CHARACTERS + " Junho 2023"
    assert first_page_year(text, 6, latest_year=LATEST) is None


def test_title_that_names_month_and_year_needs_no_other_source():
    sources = LetterSources(title="Carta Mensal - Abril 2025", url=OPAQUE_URL)
    assert letter_period(sources, latest_year=LATEST) == Period(2025, 4)


def test_file_name_comes_before_listing_upload_date_and_first_page():
    sources = LetterSources(
        title="Abr",
        url="https://example.org/Carta-Abril_2020.pdf",
        listing_year=2021,
        published="2022-04-10",
        first_page="Abril 2023",
    )
    assert letter_period(sources, latest_year=LATEST) == Period(2020, 4)


def test_file_name_that_contradicts_the_title_month_is_not_used():
    sources = LetterSources(
        title="Agosto", url="https://example.org/Carta-Julho_2020.pdf", listing_year=2021
    )
    assert letter_period(sources, latest_year=LATEST) == Period(2021, 8)


def test_file_name_year_after_the_latest_year_is_not_used():
    sources = LetterSources(title="Abr", url="https://example.org/Carta-Abril_2062.pdf")
    assert letter_period(sources, latest_year=LATEST) is None


@pytest.mark.parametrize(
    ("url", "period"),
    [
        ("https://example.org/Carta-1Semestre_2021.pdf", Period(2021, 6)),
        ("https://example.org/Carta-Junho-revisada-2023.pdf", Period(2023, 6)),
        # Another month, two years or a later year: the listing year decides instead.
        ("https://example.org/Carta-Julho-2023.pdf", Period(2024, 6)),
        ("https://example.org/Carta-2022-e-2023.pdf", Period(2024, 6)),
        ("https://example.org/Carta-2062.pdf", Period(2024, 6)),
        ("https://example.org/Carta_202307.pdf", Period(2024, 6)),
        ("https://example.org/Carta_2023_07.pdf", Period(2024, 6)),
        ("https://example.org/Carta_20230715.pdf", Period(2024, 6)),
    ],
)
def test_year_alone_in_the_file_name_comes_before_the_listing_year(url, period):
    sources = LetterSources(title="Jun", url=url, listing_year=2024)
    assert letter_period(sources, latest_year=LATEST) == period


@pytest.mark.parametrize(
    "url",
    [
        "https://example.org/Carta_202307.pdf",
        "https://example.org/Carta-Julho-2023.pdf",
        "https://example.org/Carta_20230715.pdf",
    ],
)
def test_file_name_with_another_month_dates_nothing_alone(url):
    assert letter_period(LetterSources(title="Jun", url=url), latest_year=LATEST) is None


def test_title_with_an_implausible_year_is_left_as_it_is():
    sources = LetterSources(
        title="Junho 2062",
        url="https://example.org/Carta-Junho_2023.pdf",
        first_page="Junho 2023",
    )
    assert letter_period(sources, latest_year=LATEST) is None


def test_file_name_with_de_comes_before_a_listing_year():
    sources = LetterSources(
        title="Junho", url="https://example.org/Carta-Junho-de-2023.pdf", listing_year=2025
    )
    assert letter_period(sources, latest_year=LATEST) == Period(2023, 6)


def test_file_name_with_a_second_year_falls_through_to_the_listing_year():
    sources = LetterSources(
        title="Junho",
        url="https://example.org/Carta-Junho-2023-revisada-2024.pdf",
        listing_year=2023,
    )
    assert letter_period(sources, latest_year=LATEST) == Period(2023, 6)


@pytest.mark.parametrize("title", ["Junho 2022 e 2023", "Junho e Julho", "Carta Junho 202307"])
def test_title_naming_two_years_or_months_is_left_as_it_is(title):
    sources = LetterSources(
        title=title,
        url="https://example.org/Carta-Junho_2025.pdf",
        listing_year=2025,
        first_page="Junho 2025",
    )
    assert letter_period(sources, latest_year=LATEST) is None
    assert not needs_first_page(sources, latest_year=LATEST)


@pytest.mark.parametrize(
    ("listing_year", "published"), [(2062, ""), (1850, ""), (None, "2062-06-10")]
)
def test_implausible_listing_or_upload_year_falls_through_to_the_first_page(
    listing_year, published
):
    sources = LetterSources(
        title="Junho",
        url=OPAQUE_URL,
        listing_year=listing_year,
        published=published,
        first_page="Carta Junho/2023",
    )
    assert letter_period(sources, latest_year=LATEST) == Period(2023, 6)


def test_upload_date_counts_only_in_the_title_month():
    in_month = LetterSources(title="Junho", url=OPAQUE_URL, published="2024-06-30")
    next_month = LetterSources(title="Junho", url=OPAQUE_URL, published="2024-07-05")
    assert letter_period(in_month, latest_year=LATEST) == Period(2024, 6)
    assert letter_period(next_month, latest_year=LATEST) is None


def test_first_page_dates_a_bare_month_when_nothing_else_does():
    sources = LetterSources(title="Junho", url=OPAQUE_URL, first_page="Carta Junho/2023")
    assert letter_period(sources, latest_year=LATEST) == Period(2023, 6)


def test_year_only_sources_need_the_title_to_name_the_month():
    sources = LetterSources(title="COVID", url=OPAQUE_URL, listing_year=2020)
    assert letter_period(sources, latest_year=LATEST) is None


def test_needs_first_page_only_when_the_title_month_has_no_year_yet():
    assert needs_first_page(LetterSources(title="Junho", url=OPAQUE_URL), latest_year=LATEST)
    dated_by_listing = LetterSources(title="Junho", url=OPAQUE_URL, listing_year=2024)
    assert not needs_first_page(dated_by_listing, latest_year=LATEST)
    assert not needs_first_page(LetterSources(title="COVID", url=OPAQUE_URL), latest_year=LATEST)
    with_year = LetterSources(title="Junho 2020", url=OPAQUE_URL)
    assert not needs_first_page(with_year, latest_year=LATEST)


@pytest.mark.parametrize(
    ("title", "period", "dated"),
    [
        ("Jun", Period(2023, 6), "Junho 2023"),
        ("Carta Mensal Julho", Period(2025, 7), "Carta Mensal Julho 2025"),
        ("COVID", Period(2020, 3), "COVID - Março 2020"),
        ("Carta Mensal - Abril 2025", Period(2025, 4), "Carta Mensal - Abril 2025"),
        ("Junho", None, "Junho"),
    ],
)
def test_dated_title(title, period, dated):
    assert dated_title(title, period) == dated


def test_period_gives_an_iso_month_for_the_catalog_date():
    assert Period(2023, 6).iso == "2023-06"
