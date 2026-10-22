"""The month, year and fund a letter covers, read from its title, file name, listing and
text."""

import pytest

from ofl.documents.funds import (
    Fund,
    FundSources,
    file_name_fund,
    letter_fund,
    needs_first_page_for_fund,
    publishes_several_funds,
    stated_fund,
    titled_with_fund,
)
from ofl.documents.periods import (
    HEADER_CHARACTERS,
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
        # The number before the year is the letter's sequence number, not its month.
        ("https://example.org/Carta-Gestora-10-2013.pdf", None),
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
        # The header names another month; the title's month appears only as a comparison.
        ("Carta Mensal Julho 2024 Comparado com junho de 2023, o fundo", 6, None),
        # No header date: a month in the body is not the letter's own.
        ("Carta Mensal " + "texto " * 60 + "comparado com junho de 2023", 6, None),
        # A year after the latest one is a reading error, and nothing else is trusted.
        ("Fevereiro 2071 Fevereiro de 2021", 2, None),
        # A header date in numbers counts the same way.
        ("Carta 06/2024 Comparado com maio de 2024", 6, 2024),
        ("07/2024 Comparado com junho de 2023, o fundo", 6, None),
        ("Carta 2024-07 Comparado com junho de 2023", 6, None),
        ("Carta 2024-06 Comparado com junho de 2023", 6, 2024),
    ],
)
def test_first_page_year_reads_the_header_date_only(text, month, year):
    assert first_page_year(text, month, latest_year=LATEST) == year


def test_first_page_year_reads_only_the_header():
    text = "x" * HEADER_CHARACTERS + " Junho 2023"
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
        ("https://example.org/Carta-07-2023.pdf", Period(2024, 6)),
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
        "https://example.org/Carta-07-2023.pdf",
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


@pytest.mark.parametrize(
    ("url", "fund"),
    [
        ("https://example.org/a/7G_Alfa_FIA-Carta_do_Gestor-Dezembro_2024.pdf", "Alfa FIA"),
        (
            "https://example.org/a/7G_Beta_Hedge_FIM-Carta_do_Gestor-Maio_2019.pdf",
            "Beta Hedge FIM",
        ),
        (
            "https://example.org/a/7G_Beta_Hedge_FIM+7G_XY_FIA-Carta_do_Gestor-Junho_2020.pdf",
            "Beta Hedge FIM + XY FIA",
        ),
        (
            "https://example.org/a/7G%20Gama%20FIF%20A%C3%A7%C3%B5es%20-%20Carta%20-%202025.pdf",
            "Gama FIF Ações",
        ),
        ("https://example.org/a/Carta-Alfa-Prev-FIC-FIM-Dez-2024.pdf", "Alfa Prev FIC FIM"),
        # The manager's name alone is kept when the fund has no name of its own.
        ("https://example.org/a/7G_FIA-Carta-Junho_2024.pdf", "7G FIA"),
        ("https://example.org/a/Carta_do_Gestor-Dezembro_2024.pdf", None),
        # Lower-case "fim" is the Portuguese word for "end", not a fund-type code.
        ("https://example.org/a/carta-de-fim-de-ano-2024.pdf", None),
        ("https://example.org/a/Carta-FIM-2024.pdf", None),
        (OPAQUE_URL, None),
    ],
)
def test_file_name_fund(url, fund):
    assert file_name_fund(url, "7g") == fund


@pytest.mark.parametrize(
    ("text", "fund"),
    [
        ("CARTA DO GESTOR 7G BETA HEDGE FIA DEZEMBRO 2024", "Beta Hedge FIA"),
        ("Prezados Cotistas, Alfa Macro FIA Junho 2024", "Alfa Macro FIA"),
        # A name inside running text has no known start or end.
        ("Prezados cotistas do Alfa Macro FIA, em junho", None),
        ("DESEJAMOS UM BOM FIM DE ANO", None),
        ("Alfa Fundo de Fundos FIA", None),
        # At most six words before the code and two qualifiers after it.
        ("Alfa Beta Gama Delta Zeta Teta FIA", "Alfa Beta Gama Delta Zeta Teta FIA"),
        ("Iota Alfa Beta Gama Delta Zeta Teta FIA", None),
        ("Alfa FIC FIM CP IE", "Alfa FIC FIM CP IE"),
        ("Alfa FIC FIM CP IE Prev", None),
        # A bare code names some fund without saying which.
        ("Alfa FIA + FII", None),
        ("Carta FII Junho 2024", None),
        ("Carta FIDC NP Junho 2024", None),
        ("Alfa FIA + FIDC NP", None),
        # Outside file names a lone FIM may be the word "fim"; another code settles it.
        ("DESEJAMOS BOM FIM", None),
        ("Desejamos Bom FIM", None),
        ("7G FII Alfa Malls", "FII Alfa Malls"),
        ("ALFA MACRO FIC FIM JUNHO 2024", "Alfa Macro FIC FIM"),
        # A heading that names a fund and its former name names two funds.
        ("7G GAMA FIA (antigo 7G ALFA FIA)", None),
        ("Alfa Macro FII | Beta Ações FIA", None),
        ("No fim do ano os juros caíram", None),
        ("Dez", None),
        # A number after the code is a share class or reference, not part of the name.
        ("Value Q FIC FIA026", "Value Q FIC FIA"),
        ("Desempenho Gestora Value FIA", "Value FIA"),
        ("Relatório Gerencial FII Alfa Malls", "FII Alfa Malls"),
        # After a name, a capitalised word that is not a qualifier starts the text.
        ("ALFA FIA PREZADOS COTISTAS", "Alfa FIA"),
        ("Alfa Master FIDC NP Carta", "Alfa Master FIDC NP"),
        ("Value Q FIC FIA Junho", "Value Q FIC FIA"),
        # Two codes apart are two funds, so the text names no single fund.
        ("Alfa FIA Beta FII", None),
        # Sentence words around a code are not a name.
        ("ATÉ O FIA DO ANO", None),
        # A name longer than the limit has no reliable end, so funds that differ only in
        # their last words are not read as one.
        ("FII Alfa Renda Urbana Corporativa Sul Norte Leste", None),
        ("FII Alfa Renda Logística", "FII Alfa Renda Logística"),
    ],
)
def test_stated_fund_needs_exactly_one_named_fund(text, fund):
    assert stated_fund(text, "7g") == fund


def test_a_lone_fim_counts_as_the_code_in_a_file_name_only():
    url = "https://example.org/a/Alfa_Macro_FIM-Carta-Junho_2024.pdf"
    assert file_name_fund(url, "gestora") == "Alfa Macro FIM"
    assert stated_fund("Alfa Macro FIM - Junho 2024", "gestora") is None


@pytest.mark.parametrize("text", ["Alfa Macro FIM - Junho 2024", "Desejamos Bom FIM"])
def test_a_text_that_may_name_a_fund_with_fim_stops_the_search(text):
    # The text may name the letter's fund, so no later source may name another one.
    later_sources = FundSources(
        link_text=text,
        url="https://example.org/a/Beta_FIA-Carta-Junho_2024.pdf",
        first_page="Gama FIA Carta",
    )
    assert letter_fund(later_sources, "gestora") is None
    assert not needs_first_page_for_fund(later_sources, "gestora")
    as_manifest = FundSources(
        link_text="Jun", url=OPAQUE_URL, manifest_fund=text, first_page="Gama FIA Carta"
    )
    assert letter_fund(as_manifest, "gestora") is None
    assert not needs_first_page_for_fund(as_manifest, "gestora")


def test_only_the_managers_whole_name_is_dropped_from_a_fund_name():
    assert stated_fund("Alfa Capital Beta FIA", "alfa-capital") == "Beta FIA"
    assert stated_fund("Capital Beta FIA", "alfa-capital") == "Capital Beta FIA"


def test_letter_fund_takes_the_first_source_that_names_one():
    named_by_file = "https://example.org/a/Beta_Renda_FIA-Carta-Junho_2024.pdf"
    every_source = FundSources(
        link_text="Alfa Macro FIA - Junho",
        url=named_by_file,
        manifest_fund="Gama FIA",
        first_page="Delta Ações FIA Carta Mensal",
    )
    assert letter_fund(every_source, "gestora") == Fund("Alfa Macro FIA", "link_text")
    header = "Delta Ações FIA Carta Mensal"
    by_file = FundSources(
        link_text="Jun", url=named_by_file, manifest_fund="Gama FIA", first_page=header
    )
    assert letter_fund(by_file, "gestora") == Fund("Beta Renda FIA", "file_name")
    by_manifest = FundSources(
        link_text="Jun", url=OPAQUE_URL, manifest_fund="Gama FIA", first_page=header
    )
    assert letter_fund(by_manifest, "gestora") == Fund("Gama FIA", "manifest")
    # The manifest follows the same rules: no fund-type code, no fund.
    without_code = FundSources(link_text="Jun", url=OPAQUE_URL, manifest_fund="Gama")
    assert letter_fund(without_code, "gestora") is None
    by_header = FundSources(link_text="Jun", url=OPAQUE_URL, first_page="Delta Ações FIA Carta")
    assert letter_fund(by_header, "gestora") == Fund("Delta Ações FIA", "first_page")
    assert letter_fund(FundSources(link_text="Jun", url=OPAQUE_URL), "gestora") is None


@pytest.mark.parametrize(
    "link_text",
    ["Alfa FIA Beta FII", "Alfa FIA + FII", "ATÉ O FIM DO ANO", "Carta Mensal - Junho"],
)
def test_a_link_text_naming_no_single_fund_falls_through_to_the_file_name(link_text):
    sources = FundSources(
        link_text=link_text,
        url="https://example.org/a/Gama_FIA-Carta-Junho_2024.pdf",
    )
    assert letter_fund(sources, "gestora") == Fund("Gama FIA", "file_name")


def test_first_page_names_the_fund_only_in_its_header():
    body_mention = "Carta Mensal Junho 2024 " + "x" * HEADER_CHARACTERS + " | Alfa Macro FIA"
    sources = FundSources(link_text="Jun", url=OPAQUE_URL, first_page=body_mention)
    assert letter_fund(sources, "gestora") is None


def test_a_name_cut_by_the_header_limit_is_not_used():
    name = "FII Alfa Renda Logística"
    # The header ends inside the name, after "FII Alfa Renda Log".
    padding = "x" * (HEADER_CHARACTERS - len(" | FII Alfa Renda Log"))
    page = padding + " | " + name + " Junho 2024"
    assert page[:HEADER_CHARACTERS].endswith(" | FII Alfa Renda Log")
    sources = FundSources(link_text="Jun", url=OPAQUE_URL, first_page=page)
    assert letter_fund(sources, "gestora") is None
    whole = FundSources(link_text="Jun", url=OPAQUE_URL, first_page="Carta | " + name)
    assert letter_fund(whole, "gestora") == Fund(name, "first_page")


def test_several_funds_means_two_different_names():
    alfa = Fund("Alfa FIA", "file_name")
    alfa_header = Fund("ALFA FIA", "first_page")
    beta = Fund("Beta Hedge FIM", "file_name")
    assert publishes_several_funds([alfa, None, beta])
    assert not publishes_several_funds([alfa, alfa_header, None])
    assert not publishes_several_funds([None, None])


@pytest.mark.parametrize(
    ("title", "fund", "titled"),
    [
        ("Dezembro 2024", Fund("Alfa FIA", "file_name"), "Alfa FIA - Dezembro 2024"),
        (
            "Carta Alfa FIA Junho 2024",
            Fund("Alfa FIA", "file_name"),
            "Carta Alfa FIA Junho 2024",
        ),
        # The name is compared word by word, so punctuation in the title does not matter.
        (
            "Alfa Master FIDC-NP - Junho",
            Fund("Alfa Master FIDC NP", "link_text"),
            "Alfa Master FIDC-NP - Junho",
        ),
        ("Dezembro 2024", None, "Dezembro 2024"),
    ],
)
def test_titled_with_fund(title, fund, titled):
    assert titled_with_fund(title, fund) == titled
