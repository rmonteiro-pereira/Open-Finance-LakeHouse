"""List the documents a manager has published. Listing only: nothing is downloaded here."""

from __future__ import annotations

import html
import re
from collections.abc import Iterator
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

from ofl.documents.http import PoliteClient
from ofl.documents.sources import Collector

_ANCHOR = re.compile(r"<a\b[^>]*?href\s*=\s*[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.I | re.S)
_TAG = re.compile(r"<[^>]+>")
MAX_PAGES = 200  # 20,000 media items; a site with more is not a letters archive


@dataclass(frozen=True)
class Candidate:
    url: str
    title: str = ""
    published: str = ""  # upload date when the source gives one; NOT the reference period
    listing_is_letters: bool = False


def _is_pdf_url(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".pdf")


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
    """PDF links on letters listing pages; the anchor text is the title."""
    seen: set[str] = set()
    for page_url in urls:
        resp = client.get(page_url)
        resp.raise_for_status()
        for href, inner in _ANCHOR.findall(resp.text):
            url = urljoin(page_url, html.unescape(href.strip()))
            if not _is_pdf_url(url) or url in seen:
                continue
            seen.add(url)
            title = " ".join(html.unescape(_TAG.sub(" ", inner)).split())
            yield Candidate(url=url, title=title, listing_is_letters=True)


def list_candidates(collector: Collector, client: PoliteClient) -> Iterator[Candidate]:
    if collector.kind == "wp_media":
        if not collector.base:
            raise ValueError("wp_media collector needs 'base'")
        return wp_media(collector.base, client)
    return page_links(collector.urls, client)
