import json
from datetime import UTC, datetime

from ofl.documents import news, store
from ofl.documents.feeds import parse_date, parse_feed, to_text
from ofl.documents.http import PoliteClient

RSS = b"""<?xml version="1.0"?><rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/"
 xmlns:dc="http://purl.org/dc/elements/1.1/"><channel><title>x</title>
<item><title>Copom mant&#233;m a Selic</title><link>https://site.example/a</link>
<pubDate>Wed, 07 Oct 2026 18:31:37 -0300</pubDate><dc:creator>Ana</dc:creator><category>Juros</category>
<description><![CDATA[<p>Resumo curto.</p>]]></description>
<content:encoded><![CDATA[<p>Primeiro par&aacute;grafo.</p><script>x()</script><p>Segundo.</p>]]></content:encoded></item>
<item><title>Sem link</title></item></channel></rss>"""

ATOM = b"""\xef\xbb\xbf<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
<entry><title>Nota</title><link rel="alternate" href="https://bcb.example/n1"/>
<updated>2026-10-08T12:00:00Z</updated><author><name>BCB</name></author>
<content type="html">&lt;p&gt;Texto da nota.&lt;/p&gt;</content></entry></feed>"""


class _Resp:
    def __init__(self, content=b"", status=200):
        self.content, self.status_code, self.headers, self.text = content, status, {}, ""

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(str(self.status_code))


class _Session:
    def __init__(self, routes):
        self.routes, self.headers = routes, {}

    def get(self, url, **_kw):
        return self.routes.get(url, _Resp(status=404))


def _client(routes):
    return PoliteClient(min_interval=0, session=_Session(routes), sleep=lambda _s: None)


def test_rss_item_is_parsed_and_items_without_link_are_dropped():
    (item,) = parse_feed(RSS)
    assert item["title"] == "Copom mantém a Selic"
    assert item["published"] == "2026-10-07T21:31:37+00:00"
    assert (item["author"], item["categories"]) == ("Ana", ["Juros"])
    assert to_text(item["body_html"]) == "Primeiro parágrafo.\n\nSegundo."


def test_atom_entry_with_bom_is_parsed():
    (item,) = parse_feed(ATOM)
    assert (item["link"], item["author"]) == ("https://bcb.example/n1", "BCB")
    assert to_text(item["body_html"]) == "Texto da nota."


def test_portuguese_dates():
    assert parse_date("Qua, 07 Out 2026 18:31:37 -0300") == "2026-10-07T21:31:37+00:00"
    assert parse_date("not a date") == ""


def test_headline_tier_drops_the_body_and_a_second_run_adds_nothing(tmp_path):
    url = "https://site.example/feed"
    backend = store.LocalBackend(tmp_path)
    now = datetime(2026, 10, 9, 3, 0, 0, tzinfo=UTC)

    headline = news.NewsSource(id="h", name="H", tier="headline_only", url=url)
    assert news.collect_source(headline, backend=backend, client=_client({url: _Resp(RSS)}), now=now) == {"in_feed": 1, "new": 1}
    (row,) = [json.loads(line) for line in (tmp_path / "news/items/h/2026-10-09/030000.jsonl").read_text().splitlines()]
    assert "body" not in row and row["summary"] == "Resumo curto." and row["title"].startswith("Copom")

    assert news.collect_source(headline, backend=backend, client=_client({url: _Resp(RSS)}), now=now) == {"in_feed": 1, "new": 0}

    full = news.NewsSource(id="f", name="F", tier="feed_fulltext", url=url)
    news.collect_source(full, backend=backend, client=_client({url: _Resp(RSS)}), now=now)
    (row,) = [json.loads(line) for line in (tmp_path / "news/items/f/2026-10-09/030000.jsonl").read_text().splitlines()]
    assert row["body"] == "Primeiro parágrafo.\n\nSegundo."


def test_a_broken_feed_is_reported_not_raised(tmp_path):
    source = news.NewsSource(id="b", name="B", tier="official", url="https://site.example/feed")
    out = news.collect_source(source, backend=store.LocalBackend(tmp_path), client=_client({source.url: _Resp(b"<html>oops")}))
    assert out == {"failed": 1}


def test_source_list_loads_and_keeps_forbidden_outlets_to_headlines():
    sources = {s.id: s for s in news.load_sources()}
    assert len(sources) >= 25
    for outlet in ("valor", "oglobo-economia", "estadao-economia", "exame", "brazil-journal", "neofeed", "folha-mercado"):
        assert sources[outlet].tier == "headline_only"
    assert not any("reuters" in s for s in sources)
