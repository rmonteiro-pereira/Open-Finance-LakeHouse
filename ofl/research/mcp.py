"""Read-only stdio MCP adapter over the authenticated private OFL REST API."""

from datetime import date
from typing import Literal
from urllib.parse import quote, unquote

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.types import ResourceLink

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

    def read_bundle(specification):
        with httpx.Client(
            base_url=settings.api_url.rstrip("/") + "/api/v1/",
            headers={"Authorization": f"Bearer {settings.api_token}"},
            timeout=60,
            follow_redirects=False,
            transport=transport,
        ) as client:
            with client.stream(
                "GET", "datasets/bundle", params={"specification": specification}
            ) as result:
                if (
                    result.status_code != 200
                    or result.headers.get("content-type", "").split(";")[0] != "application/zip"
                ):
                    raise ValueError("OFL bundle request failed; check access or recipe")
                chunks, size = [], 0
                for chunk in result.iter_bytes():
                    size += len(chunk)
                    if size > 64 * 1024**2:
                        raise ValueError("bundle exceeds MCP transfer bound")
                    chunks.append(chunk)
                return b"".join(chunks)

    @server.resource("ofl://datasets/reproduction/{recipe}", mime_type="application/zip")
    def reproduction_resource(recipe: str) -> bytes:
        """Download a pinned offline reproduction bundle using this caller's service identity."""
        from .analytics import DatasetSpec

        if len(recipe) > 24000:
            raise ValueError("recipe URI exceeds bound")
        spec = DatasetSpec.model_validate_json(unquote(recipe))
        return read_bundle(spec.model_dump_json())

    @server.tool(structured_output=False)
    def dataset_reproduction(specification: str) -> list[ResourceLink]:
        """Prepare a pinned dataset and return a ZIP resource link.

        Includes captured inputs and a Python stdlib offline reproducer.
        Fetch the resource to download; no saved state is changed.
        """
        from .analytics import DatasetSpec

        if len(specification) > 8000:
            raise ValueError("specification exceeds bound")
        spec = DatasetSpec.model_validate_json(specification)
        read("datasets/preview", {"specification": spec.model_dump_json()})
        return [
            ResourceLink(
                type="resource_link",
                uri="ofl://datasets/reproduction/" + quote(spec.model_dump_json(), safe=""),
                name="ofl-dataset-reproduction.zip",
                mimeType="application/zip",
                description=(
                    "Captured inputs, manifest, CSV and offline reproducer; "
                    "no server required after download."
                ),
            )
        ]

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
        jurisdiction: Literal["off", "exclude"] = "off",
    ) -> dict:
        """Document evidence with passage/item/asset/page IDs. OCR is not approved numeric data.

        jurisdiction (experimental): `exclude` drops passages whose evidence names only
        jurisdictions other than the ones the query names. It applies only when every named
        jurisdiction is precision-gated (Brazil, United States, euro area); a query naming
        another one (e.g. China) is searched without it. The response's `jurisdiction`
        field discloses what was applied and what was ignored.
        """
        if not 1 <= len(q) <= 1000 or not 1 <= limit <= 20:
            raise ValueError("query length or result limit outside bounds")
        if date_from and date_to and date_from > date_to:
            raise ValueError("reversed date interval")
        if jurisdiction not in {"off", "exclude"}:
            raise ValueError("jurisdiction must be off or exclude")
        if jurisdiction == "off":
            jurisdiction = None
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

    @server.tool()
    def prepare_dataset(specification: str) -> dict:
        """Read-only deterministic multi-series preparation. JSON specification pins snapshot, alignment and transformations; no imputation or arbitrary SQL."""
        from .analytics import DatasetSpec

        spec = DatasetSpec.model_validate_json(specification)
        return read("datasets/preview", {"specification": spec.model_dump_json()})

    @server.tool()
    def compare_prices(
        symbol: str, market: str, date_from: date, date_to: date, release: str
    ) -> dict:
        """Exact observed unadjusted price change with compatible instrument identity and quotation basis. Not total return."""
        if (
            not 1 <= len(symbol) <= 32
            or len(market) != 3
            or not market.isdigit()
            or date_from >= date_to
            or len(release) > 128
        ):
            raise ValueError("explicit bounded instrument, dates and release required")
        return read("quotes/compare", locals())

    @server.tool()
    def compare_releases(
        left: str, right: str, symbol: str, market: str, date_from: date, date_to: date
    ) -> dict:
        """Scoped record-level differences between immutable quote releases."""
        if (
            not 1 <= len(symbol) <= 32
            or len(market) != 3
            or not market.isdigit()
            or date_from > date_to
            or max(len(left), len(right)) > 128
        ):
            raise ValueError("invalid release comparison scope")
        return read("releases/diff", locals())

    @server.tool()
    def saved_work(workspace_id: str, revision: int | None = None) -> dict:
        """Read a caller-owned dossier, portfolio, dataset recipe or check. Credentials must represent the artifact owner. No edit capability."""
        import re

        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", workspace_id) or (
            revision is not None and revision < 1
        ):
            raise ValueError("invalid owned artifact reference")
        return read(f"workspaces/{workspace_id}", {"revision": revision})

    @server.tool()
    def list_saved_work(
        kind: Literal["investigation", "portfolio", "dataset", "watchlist", "check"] | None = None,
    ) -> list:
        """List only work owned by the authenticated credential principal."""
        return read("workspaces", {"kind": kind})

    return server
