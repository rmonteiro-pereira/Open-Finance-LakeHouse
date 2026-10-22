"""List the documents a manager has published. Listing only: nothing is downloaded here."""

from __future__ import annotations

import html
import re
import unicodedata
from collections.abc import Iterator
from dataclasses import dataclass
from urllib.parse import unquote, urljoin, urlparse

from ofl.documents.http import PoliteClient
from ofl.documents.sources import Collector

_ANCHOR = re.compile(r"<a\b[^>]*?href\s*=\s*[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.I | re.S)
_TAG = re.compile(r"<[^>]+>")
MAX_PAGES = 200  # 20,000 media items; a site with more is not a letters archive

# Link texts that name an action on the page, not the document behind the link. They are
# compared without accents, case or trailing punctuation.
PAGE_CHROME_LINK_TEXTS = frozenset(
    {
        "abrir",
        "acesse",
        "acesse aqui",
        "aqui",
        "baixar",
        "baixe",
        "baixe aqui",
        "clique aqui",
        "confira",
        "download",
        "fazer download",
        "leia aqui",
        "leia mais",
        "mais informacoes",
        "pdf",
        "saiba mais",
        "ver mais",
        "veja mais",
        "visualizar",
        "click here",
        "here",
        "learn more",
        "read more",
    }
)
_FILE_NAME_SEPARATORS = re.compile(r"[_+\-]+")
_CAMEL_CASE_BOUNDARY = re.compile(r"(?<=[a-z])(?=[A-Z])")


@dataclass(frozen=True)
class Candidate:
    url: str
    title: str = ""
    published: str = ""  # upload date when the source gives one; NOT the reference period
    listing_is_letters: bool = False


def _is_pdf_url(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".pdf")


def is_page_chrome(link_text: str) -> bool:
    """True when a link text is empty or a generic action such as "Saiba mais" or "PDF"."""
    folded = unicodedata.normalize("NFKD", link_text).encode("ascii", "ignore").decode()
    words = " ".join(re.sub(r"[^\w\s]", " ", folded.casefold()).split())
    return not words or words in PAGE_CHROME_LINK_TEXTS


def file_name_title(url: str) -> str:
    """A readable title from a document URL's file name: "CartaMensal_202301.pdf" gives
    "Carta Mensal 202301"."""
    name = unquote(urlparse(url).path.rsplit("/", 1)[-1])
    stem = name.rsplit(".", 1)[0] if "." in name else name
    spaced = _CAMEL_CASE_BOUNDARY.sub(" ", _FILE_NAME_SEPARATORS.sub(" ", stem))
    return " ".join(spaced.split())


def link_title(link_text: str, url: str) -> str:
    """The title a listing link gives its document.

    Some listing pages label every letter with the same button text ("Mais informações",
    "PDF"). That text says nothing about the letter, so the file name is used instead.
    """
    if not is_page_chrome(link_text):
        return link_text
    return file_name_title(url) or link_text


def wp_media(base: str, client: PoliteClient) -> Iterator[Candidate]:
    """Every PDF in a WordPress media library, newest first."""
    base = base.rstrip("/")
    for page in range(1, MAX_PAGES + 1):
        resp = client.get(
            f"{base}/wp-json/wp/v2/media",
            params={
                "mime_type": "application/pdf",
                "per_page": 100,
                "page": page,
                "_fields": "date,source_url,title",
            },
        )
        if resp.status_code == 400 and page > 1:  # WordPress answers 400 past the last page
            return
        resp.raise_for_status()
        items = resp.json()
        if not items:
            return
        for item in items:
            source = item.get("source_url")
            if not source:
                continue
            title = item.get("title") or {}
            yield Candidate(
                url=urljoin(base + "/", source),
                title=html.unescape(title.get("rendered", "") if isinstance(title, dict) else str(title)),
                published=(item.get("date") or "")[:10],
            )
        if page >= int(resp.headers.get("X-WP-TotalPages", page)):
            return


def page_links(urls: list[str], client: PoliteClient) -> Iterator[Candidate]:
    """PDF links on letters listing pages; the anchor text is the title unless it is
    generic page chrome (see link_title)."""
    seen: set[str] = set()
    for page_url in urls:
        resp = client.get(page_url)
        resp.raise_for_status()
        for href, inner in _ANCHOR.findall(resp.text):
            url = urljoin(page_url, html.unescape(href.strip()))
            if not _is_pdf_url(url) or url in seen:
                continue
            seen.add(url)
            link_text = " ".join(html.unescape(_TAG.sub(" ", inner)).split())
            yield Candidate(url=url, title=link_title(link_text, url), listing_is_letters=True)


def list_candidates(collector: Collector, client: PoliteClient) -> Iterator[Candidate]:
    if collector.kind == "wp_media":
        if not collector.base:
            raise ValueError("wp_media collector needs 'base'")
        return wp_media(collector.base, client)
    return page_links(collector.urls, client)
