import json

import pytest

from ofl.documents import letters, store
from ofl.documents.classify import is_letter
from ofl.documents.collectors import (
    is_page_chrome,
    link_title,
    list_candidates,
    listing_years,
    page_links,
    wp_media,
)
from ofl.documents.http import PoliteClient
from ofl.documents.sources import Collector, Manager, load_managers

PDF = b"%PDF-1.7 fake letter body"


class _Resp:
    def __init__(self, status=200, text="", body=b"", headers=None, payload=None):
        self.status_code, self.text, self._body, self._payload = status, text, body, payload
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(str(self.status_code))

    def iter_content(self, _size):
        yield self._body

    def close(self):
        pass


class _Session:
    """Answers from a dict of URL -> response; records what was asked."""

    def __init__(self, routes):
        self.routes, self.headers, self.asked = routes, {}, []

    def get(self, url, params=None, **_kw):
        self.asked.append(url)
        if url.endswith("/robots.txt"):
            return self.routes.get(url, _Resp(404))
        key = url if not params else f"{url}?page={params['page']}"
        return self.routes.get(key, _Resp(404))


def _client(routes):
    session = _Session(routes)
    return PoliteClient(min_interval=0, session=session, sleep=lambda _s: None), session


@pytest.mark.parametrize(
    ("url", "title", "keep"),
    [
        ("https://x/wp-content/uploads/2026/10/Carta-Mensal-Setembro-2026.pdf", "", True),
        ("https://x/uploads/Relatorio-de-Gestao_Set26.pdf", "", True),
        ("https://x/uploads/Regulamento-Fundo-X-FIA.pdf", "", False),
        ("https://x/uploads/L%C3%A2mina-Fundo-X.pdf", "", False),
        ("https://x/uploads/Politica-de-Voto.pdf", "Política de voto", False),
        ("https://x/uploads/0405c7_06b8fc.pdf", "", False),
    ],
)
def test_classifier(url, title, keep):
    assert is_letter(url, title)[0] is keep


def test_opaque_name_is_kept_when_the_page_is_a_letters_listing():
    assert is_letter("https://x/_files/ugd/0405c7_06b8fc.pdf", listing_is_letters=True)[0] is True
    assert is_letter("https://x/regulamento.pdf", listing_is_letters=True)[0] is False


def test_wp_media_pages_until_the_last_page_and_resolves_relative_urls():
    base = "https://site.example"
    api = f"{base}/wp-json/wp/v2/media"
    client, _ = _client(
        {
            f"{api}?page=1": _Resp(
                payload=[
                    {
                        "date": "2026-10-02T10:00:00",
                        "source_url": "/wp-content/a.pdf",
                        "title": {"rendered": "Carta &amp; Call"},
                    }
                ],
                headers={"X-WP-TotalPages": "2"},
            ),
            f"{api}?page=2": _Resp(
                payload=[
                    {
                        "date": "2026-09-01T10:00:00",
                        "source_url": f"{base}/wp-content/b.pdf",
                        "title": {"rendered": "B"},
                    }
                ],
                headers={"X-WP-TotalPages": "2"},
            ),
        }
    )
    got = list(wp_media(base, client))
    assert [c.url for c in got] == [f"{base}/wp-content/a.pdf", f"{base}/wp-content/b.pdf"]
    assert got[0].title == "Carta & Call" and got[0].published == "2026-10-02"


def test_page_links_keeps_pdf_anchors_once_with_their_text():
    page = "https://site.example/cartas/"
    html = '<a href="docs/c1.pdf"><span>Carta 1</span></a> <a href="/about">x</a> <a href="docs/c1.pdf">again</a>'
    client, _ = _client({page: _Resp(text=html)})
    got = list(page_links([page], client))
    assert [(c.url, c.title) for c in got] == [
        ("https://site.example/cartas/docs/c1.pdf", "Carta 1")
    ]
    assert got[0].listing_is_letters


@pytest.mark.parametrize(
    "link_text",
    ["Mais informações", "MAIS INFORMACOES", "LEIA AQUI", "Saiba mais »", "PDF", "Download", ""],
)
def test_generic_button_texts_are_page_chrome(link_text):
    assert is_page_chrome(link_text)


@pytest.mark.parametrize(
    "link_text", ["Carta Mensal - Abril 2025", "Junho", "Mais informações 2024"]
)
def test_descriptive_link_texts_are_not_page_chrome(link_text):
    assert not is_page_chrome(link_text)


@pytest.mark.parametrize(
    ("link_text", "url", "title"),
    [
        (
            "Mais informações",
            "https://x.example/content/view/1/file/Carta%20Mensal%20Julho.pdf?version=1",
            "Carta Mensal Julho",
        ),
        ("PDF", "https://x.example/uploads/CartaMensal_202301.pdf", "Carta Mensal 202301"),
        (
            "",
            "https://x.example/a/Fund-Carta_do_Gestor+Maio_2024.pdf",
            "Fund Carta do Gestor Maio 2024",
        ),
        ("Carta Anual 2024", "https://x.example/uploads/ca24.pdf", "Carta Anual 2024"),
        ("Leia aqui", "https://x.example/", "Leia aqui"),
    ],
)
def test_link_title_replaces_page_chrome_with_the_file_name(link_text, url, title):
    assert link_title(link_text, url) == title


def test_page_links_titles_button_links_from_the_file_name():
    page = "https://site.example/cartas/"
    html = (
        '<p>Resumo do mes. <a href="/files/Carta%20Mensal%20Maio.pdf">'
        "<strong>LEIA AQUI</strong></a></p>"
        '<a href="/files/Carta-Anual.pdf">Carta anual de gestão</a>'
    )
    client, _ = _client({page: _Resp(text=html)})
    got = list(page_links([page], client))
    assert [c.title for c in got] == ["Carta Mensal Maio", "Carta anual de gestão"]


def test_robots_disallow_is_respected():
    robots = _Resp(
        text="User-agent: *\nDisallow: /wp-json/\n", headers={"content-type": "text/plain"}
    )
    client, session = _client({"https://site.example/robots.txt": robots})
    manager = Manager(
        id="m", name="M", collectors=[Collector(kind="wp_media", base="https://site.example")]
    )
    counts = letters.collect_manager(
        manager, backend=store.LocalBackend("/nonexistent"), client=client, dry_run=True
    )
    assert counts == {"listing_failed": 1}
    assert not any("wp-json" in url for url in session.asked)


def test_collect_stores_once_dedups_and_resumes(tmp_path):
    page = "https://site.example/cartas/"
    html = '<a href="a.pdf">Carta A</a><a href="copy-of-a.pdf">Carta A again</a><a href="regulamento.pdf">Reg</a><a href="broken.pdf">B</a>'
    routes = {
        page: _Resp(text=html),
        f"{page}a.pdf": _Resp(body=PDF),
        f"{page}copy-of-a.pdf": _Resp(body=PDF),
        f"{page}broken.pdf": _Resp(body=b"<html>login</html>"),
    }
    manager = Manager(id="m", name="M", collectors=[Collector(kind="page_links", urls=[page])])
    backend = store.LocalBackend(tmp_path)

    client, _ = _client(routes)
    first = letters.collect_manager(manager, backend=backend, client=client)
    assert first == {"stored": 1, "duplicate": 1, "not_a_letter": 1, "not_a_pdf": 1}
    assert len(list((tmp_path / "letters/files/m").glob("*.pdf"))) == 1
    rows = [
        json.loads(line)
        for line in (tmp_path / "letters/manifest/m.jsonl").read_text().splitlines()
    ]
    assert {r["status"] for r in rows} == {"stored", "duplicate", "not_a_letter", "not_a_pdf"}

    client, session = _client(routes)
    second = letters.collect_manager(manager, backend=backend, client=client)
    assert second == {"already_seen": 4}
    assert not any(url.endswith(".pdf") for url in session.asked)

    cov = letters.coverage([manager], backend)[0]
    assert (cov["letters"], cov["urls_seen"], cov["bytes"]) == (1, 4, len(PDF))


SECTIONED_LISTING = """
<a href="destaque/Carta%20Julho.pdf">Carta Julho</a>
<h3>2026</h3><a href="2026/Carta%20Julho.pdf">Carta Julho</a><a href="2026/Carta%20Junho.pdf">Junho</a>
<h3>2025</h3><a href="2025/Carta%20Julho.pdf">Carta Julho</a><a href="pagina.html">Mais</a>
"""
# A tab bar lists every year before the panels, so no link sits under its own year.
TABBED_LISTING = """
<div role="tab">2026</div><div role="tab">2025</div>
<a href="a.pdf">Julho</a><a href="b.pdf">Julho</a>
"""


def test_listing_years_follow_the_year_heading_above_each_link():
    page = "https://site.example/cartas/"
    years = listing_years(page, SECTIONED_LISTING)
    assert years == {
        f"{page}2026/Carta%20Julho.pdf": 2026,
        f"{page}2026/Carta%20Junho.pdf": 2026,
        f"{page}2025/Carta%20Julho.pdf": 2025,
    }


@pytest.mark.parametrize(
    "page_html",
    [
        TABBED_LISTING,
        '<p>2026</p><a href="a.pdf">Julho</a><a href="b.pdf">Junho</a>',
        '<h3>2025</h3><a href="a.pdf">Julho</a><h3>2026</h3><a href="b.pdf">Julho</a>'
        '<h3>2024</h3><a href="c.pdf">Julho</a>',
    ],
)
def test_listing_years_are_not_read_from_pages_not_sectioned_by_year(page_html):
    assert listing_years("https://site.example/cartas/", page_html) == {}


def test_link_listed_under_two_years_gets_no_year():
    page_html = '<h3>2026</h3><a href="a.pdf">Julho</a><h3>2025</h3><a href="a.pdf">Julho</a>'
    page_html += '<a href="b.pdf">Junho</a>'
    years = listing_years("https://site.example/cartas/", page_html)
    assert years == {"https://site.example/cartas/b.pdf": 2025}


def test_listing_year_is_recorded_and_refreshed_without_a_new_download(tmp_path):
    page = "https://site.example/cartas/"
    letter = f"{page}2025/Carta%20Julho.pdf"
    before = _Resp(text='<a href="2025/Carta%20Julho.pdf">Carta Julho</a>')
    sectioned = _Resp(text='<h3>2026</h3><a href="x.pdf">Junho</a>' + "<h3>2025</h3>" + before.text)
    manager = Manager(id="m", name="M", collectors=[Collector(kind="page_links", urls=[page])])
    backend = store.LocalBackend(tmp_path)
    manifest_file = tmp_path / "letters/manifest/m.jsonl"

    def manifest_years():
        rows = [json.loads(line) for line in manifest_file.read_text().splitlines()]
        return {row["url"]: row["listing_year"] for row in rows}

    client, _ = _client({page: before, letter: _Resp(body=PDF)})
    assert letters.collect_manager(manager, backend=backend, client=client) == {"stored": 1}
    assert manifest_years() == {letter: None}

    client, session = _client(
        {page: sectioned, letter: _Resp(body=PDF), f"{page}x.pdf": _Resp(404)}
    )
    counts = letters.collect_manager(manager, backend=backend, client=client)
    assert counts == {"already_seen": 1, "listing_year_updated": 1, "gone": 1}
    assert manifest_years() == {letter: 2025, f"{page}x.pdf": 2026}
    assert letter not in session.asked

    # A page that stops grouping by year keeps the year already recorded.
    client, _ = _client({page: before})
    assert letters.collect_manager(manager, backend=backend, client=client) == {"already_seen": 1}
    assert manifest_years() == {letter: 2025, f"{page}x.pdf": 2026}


def test_page_links_carry_the_listing_year():
    page = "https://site.example/cartas/"
    client, _ = _client({page: _Resp(text=SECTIONED_LISTING)})
    collector = Collector(kind="page_links", urls=[page])
    by_url = {c.url: c.listing_year for c in list_candidates(collector, client)}
    assert by_url == {
        f"{page}destaque/Carta%20Julho.pdf": None,
        f"{page}2026/Carta%20Julho.pdf": 2026,
        f"{page}2026/Carta%20Junho.pdf": 2026,
        f"{page}2025/Carta%20Julho.pdf": 2025,
    }


def test_seed_list_loads():
    managers = load_managers()
    assert len(managers) >= 30
    assert all(m.collectors for m in managers)


def test_source_lists_follow_the_registry_folder_in_the_image(tmp_path, monkeypatch):
    from ofl.documents.sources import sources_dir

    monkeypatch.setenv("OFL_REGISTRY", str(tmp_path / "registry.yml"))
    assert sources_dir() == tmp_path
    monkeypatch.delenv("OFL_REGISTRY")
    assert (sources_dir() / "letters.yml").exists()
