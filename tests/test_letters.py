import json

import pytest

from ofl.documents import letters, store
from ofl.documents.classify import is_letter
from ofl.documents.collectors import page_links, wp_media
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
                payload=[{"date": "2026-10-02T10:00:00", "source_url": "/wp-content/a.pdf", "title": {"rendered": "Carta &amp; Call"}}],
                headers={"X-WP-TotalPages": "2"},
            ),
            f"{api}?page=2": _Resp(
                payload=[{"date": "2026-09-01T10:00:00", "source_url": f"{base}/wp-content/b.pdf", "title": {"rendered": "B"}}],
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
    assert [(c.url, c.title) for c in got] == [("https://site.example/cartas/docs/c1.pdf", "Carta 1")]
    assert got[0].listing_is_letters


def test_robots_disallow_is_respected():
    robots = _Resp(text="User-agent: *\nDisallow: /wp-json/\n", headers={"content-type": "text/plain"})
    client, session = _client({"https://site.example/robots.txt": robots})
    manager = Manager(id="m", name="M", collectors=[Collector(kind="wp_media", base="https://site.example")])
    counts = letters.collect_manager(manager, backend=store.LocalBackend("/nonexistent"), client=client, dry_run=True)
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
    rows = [json.loads(line) for line in (tmp_path / "letters/manifest/m.jsonl").read_text().splitlines()]
    assert {r["status"] for r in rows} == {"stored", "duplicate", "not_a_letter", "not_a_pdf"}

    client, session = _client(routes)
    second = letters.collect_manager(manager, backend=backend, client=client)
    assert second == {"already_seen": 4}
    assert not any(url.endswith(".pdf") for url in session.asked)

    cov = letters.coverage([manager], backend)[0]
    assert (cov["letters"], cov["urls_seen"], cov["bytes"]) == (1, 4, len(PDF))


def test_seed_list_loads():
    managers = load_managers()
    assert len(managers) >= 30
    assert all(m.collectors for m in managers)
