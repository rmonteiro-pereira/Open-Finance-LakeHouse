"""Authentication and MCP contract checks without external models or production data."""

import asyncio
import json
from datetime import date

import httpx
import pytest
from fastapi.testclient import TestClient

from ofl.research.api import create_app
from ofl.research.config import ResearchSettings
from ofl.research.mcp import create_mcp

TOKEN = "local-test-service-token-" + "x" * 32


def client(tmp_path, **overrides):
    return TestClient(
        create_app(
            ResearchSettings(
                root=tmp_path,
                gateway_token="gateway",
                roles=json.dumps({"ana": "admin", "bia": "reader"}),
                service_tokens=json.dumps({"ana": TOKEN}),
                **overrides,
            )
        )
    )


def test_service_identity_is_bound_to_token_and_read_only(tmp_path):
    api = client(tmp_path)
    headers = {"Authorization": f"Bearer {TOKEN}", "X-OFL-Identity": "bia"}
    result = api.get("/api/v1/me", headers=headers)
    assert result.status_code == 200
    assert result.json()["identity"] == "ana"
    assert result.json()["role"] == "reader"
    assert api.post("/api/v1/jobs", json={"action": "embed"}, headers=headers).status_code == 403
    assert api.get("/api/v1/candidates/x", headers=headers).status_code == 403
    assert api.get("/api/v1/me", headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_private_schema_and_gateway_remain_private(tmp_path):
    api = client(tmp_path)
    assert api.get("/api/v1/openapi").status_code == 401
    assert api.get("/docs").status_code == 404
    assert api.get("/openapi.json").status_code == 404
    result = api.get("/api/v1/openapi", headers={"Authorization": f"Bearer {TOKEN}"})
    assert result.status_code == 200
    assert "/api/v1/releases" in result.json()["paths"]
    assert (
        api.get(
            "/api/v1/me", headers={"X-OFL-Gateway-Token": "gateway", "X-OFL-Identity": "ana"}
        ).json()["role"]
        == "admin"
    )
    assert (
        api.get(
            "/api/v1/me", headers={"X-OFL-Gateway-Token": "forged", "X-OFL-Identity": "ana"}
        ).status_code
        == 401
    )


def test_unconfigured_gateway_fails_closed(tmp_path):
    api = TestClient(create_app(ResearchSettings(root=tmp_path)))
    assert api.get("/api/v1/me").status_code == 401


def test_mcp_requires_service_credential():
    with pytest.raises(ValueError, match="API_TOKEN"):
        create_mcp(ResearchSettings())


def test_mcp_tools_preserve_exact_values_and_bound_network_access():
    calls = []

    def source(request):
        calls.append(request)
        assert request.headers["authorization"] == f"Bearer {TOKEN}"
        assert "x-ofl-gateway-token" not in request.headers
        assert request.method == "GET"
        return httpx.Response(
            200, json={"release": "r1", "rows": [{"close": "9007199254740993.123456789012"}]}
        )

    server = create_mcp(ResearchSettings(api_token=TOKEN), transport=httpx.MockTransport(source))

    async def exercise():
        tools = await server.list_tools()
        assert {tool.name for tool in tools} == {
            "coverage",
            "quotes",
            "search_archive",
            "item",
            "releases",
        }
        result = await server.call_tool(
            "quotes",
            {
                "symbol": "PETR4",
                "market": "010",
                "date_from": str(date(2026, 1, 5)),
                "date_to": "2026-01-05",
            },
        )
        assert "9007199254740993.123456789012" in str(result)
        count = len(calls)
        with pytest.raises(Exception):
            await server.call_tool("item", {"item_id": "../../jobs"})
        with pytest.raises(Exception):
            await server.call_tool(
                "quotes",
                {
                    "symbol": "PETR4",
                    "market": "010",
                    "date_from": "2026-01-06",
                    "date_to": "2026-01-05",
                },
            )
        assert len(calls) == count

    asyncio.run(exercise())


def test_real_stdio_mcp_initialization_and_discovery():
    import sys

    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    async def exercise():
        server = StdioServerParameters(
            command=sys.executable,
            args=["-m", "ofl.cli", "research", "mcp"],
            env={"OFL_RESEARCH_API_TOKEN": TOKEN},
        )
        async with stdio_client(server) as (reader, writer):
            async with ClientSession(reader, writer) as session:
                initialized = await session.initialize()
                assert initialized.serverInfo.name == "OFL research"
                discovered = await session.list_tools()
                assert "quotes" in {tool.name for tool in discovered.tools}

    asyncio.run(exercise())
