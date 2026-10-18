"""Read-only stdio MCP adapter over the authenticated private OFL REST API."""

from datetime import date
from typing import Literal

import httpx
from mcp.server.fastmcp import FastMCP

from .config import ResearchSettings


def create_mcp(settings: ResearchSettings, *, transport=None):
    if not settings.api_token:
        raise ValueError("OFL_RESEARCH_API_TOKEN is required for MCP")
    server = FastMCP("OFL research")

    def read(path, params=None):
        with httpx.Client(
            base_url=settings.api_url.rstrip("/") + "/api/v1/",
            headers={"Authorization": f"Bearer {settings.api_token}"},
            timeout=60,
            follow_redirects=False,
            transport=transport,
        ) as client:
            result = client.get(
                path, params={k: v for k, v in (params or {}).items() if v is not None}
            )
            if result.status_code != 200:
                raise ValueError(
                    f"OFL request failed ({result.status_code}); check access or query"
                )
            return result.json()

    @server.tool()
    def coverage() -> dict:
        """Actual published coverage; no assumption of complete historical ingestion."""
        return read("coverage")

    @server.tool()
    def search_archive(
        q: str,
        mode: Literal["keyword", "vector", "hybrid"] = "keyword",
        source: Literal["b3_bulletin", "mub3", "news", "manager_letter"] | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int = 8,
    ) -> dict:
        """Document evidence with passage/item/asset/page IDs. OCR is not approved numeric data."""
        if not 1 <= len(q) <= 1000 or not 1 <= limit <= 20:
            raise ValueError("query length or result limit outside bounds")
        if date_from and date_to and date_from > date_to:
            raise ValueError("reversed date interval")
        return read("search", locals())

    @server.tool()
    def quotes(
        symbol: str,
        market: str,
        date_from: date,
        date_to: date,
        currency: str | None = None,
        release: str | None = None,
        limit: int = 30,
    ) -> dict:
        """Original unadjusted B3 prices. Preserve decimal strings, currency and quotation factor."""
        if not symbol or len(symbol) > 32 or not market.isdigit() or len(market) != 3:
            raise ValueError("explicit symbol and three-digit market required")
        if date_from > date_to or not 1 <= limit <= 100:
            raise ValueError("invalid date interval or row limit")
        return read("quotes", locals())

    @server.tool()
    def item(item_id: str) -> dict:
        """Get an item returned by search, its active originals and page references."""
        if (
            not item_id
            or len(item_id) > 128
            or not all(c.isascii() and (c.isalnum() or c in "_-") for c in item_id)
        ):
            raise ValueError("invalid item ID")
        return read(f"items/{item_id}")

    @server.tool()
    def releases(limit: int = 30) -> list:
        """Published immutable release identifiers and metadata."""
        if not 1 <= limit <= 100:
            raise ValueError("limit outside bounds")
        return read("releases", {"limit": limit})

    @server.tool()
    def data_catalog(q: str | None = None, snapshot_id: str | None = None) -> dict:
        """Discover open macro series with units, frequencies, validated coverage and snapshot ID."""
        if q and len(q) > 200:
            raise ValueError("catalog query is too long")
        return read("data/catalog", {"q": q, "snapshot_id": snapshot_id})

    @server.tool()
    def macro_series(
        series_id: str,
        snapshot_id: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int = 100,
    ) -> dict:
        """Pinned macro observations; values preserve source numeric representation, not historical publication availability."""
        from .macro import SERIES

        if not SERIES.fullmatch(series_id) or not 1 <= limit <= 1000:
            raise ValueError("invalid macro series query")
        if date_from and date_to and date_from > date_to:
            raise ValueError("reversed date interval")
        return read(
            f"data/series/{series_id}",
            {
                "snapshot_id": snapshot_id,
                "date_from": date_from,
                "date_to": date_to,
                "limit": limit,
            },
        )

    @server.tool()
    def passage(passage_id: str) -> dict:
        """Inspect a cited passage, its source/date/content qualifications and revision state."""
        if (
            not passage_id
            or len(passage_id) > 128
            or not all(c.isascii() and (c.isalnum() or c in "_-") for c in passage_id)
        ):
            raise ValueError("invalid passage ID")
        return read(f"passages/{passage_id}")

    return server
