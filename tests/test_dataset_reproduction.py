"""Recompute exported data with a separate stdlib implementation, outside OFL."""

import asyncio
import hashlib
import io
import json
import subprocess
import sys
import zipfile
from datetime import date

import pytest

pytest.importorskip("psycopg")
pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from ofl.research.api import create_app
from ofl.research.dataset_bundle import bundle
from ofl.research.dataset_replay import replay
from ofl.research.macro import sync_macro
from tests.test_research import research  # noqa: F401
from tests.test_workbench import snapshot


def setup(research):
    db, store = research
    values = [
        {"series_id": "ipca", "date": date(2026, i + 1, 1), "value": value}
        for i, value in enumerate([0.0, 1.0, None, 4.0, 8.0])
    ] + [
        {"series_id": "rates", "date": date(2026, 2, 1), "value": 10.0},
        {"series_id": "rates", "date": date(2026, 4, 1), "value": 12.0},
    ]
    source = snapshot(values=values)
    catalog = json.loads(source.blobs["snapshots/s1/catalog.json"])
    catalog["series"].append(
        {**catalog["series"][0], "key": "rates", "name": "Rates", "frequency": "irregular"}
    )
    source.blobs["snapshots/s1/catalog.json"] = json.dumps(catalog).encode()
    sync_macro(db, store, source=source)
    return db, store


@pytest.mark.parametrize("alignment", ["inner", "outer"])
@pytest.mark.parametrize(
    "operation", ["level", "difference", "percent_change", "index_100", "rolling_mean"]
)
def test_offline_reproduction_matches_server_for_null_calendars_and_transforms(
    research, tmp_path, alignment, operation
):
    db, store = setup(research)
    spec = {
        "snapshot_id": "s1",
        "alignment": alignment,
        "series": [
            {"series_id": "ipca", "operation": operation, "window": 2},
            {"series_id": "rates"},
        ],
    }
    raw = bundle(db, store, spec)
    assert raw == bundle(db, store, spec)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        assert set(archive.namelist()) == {
            "inputs.json",
            "manifest.json",
            "receipt.json",
            "dataset.csv",
            "reproduce.py",
            "README.txt",
        }
        archive.extractall(tmp_path)
    # -I -S removes site packages and PYTHONPATH; this is not a second call to the
    # server's calculation routine and cannot accidentally use the editable OFL.
    completed = subprocess.run(
        [sys.executable, "-I", "-S", str(tmp_path / "reproduce.py")],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
    assert (tmp_path / "dataset.reproduced.csv").read_bytes() == (
        tmp_path / "dataset.csv"
    ).read_bytes()
    assert json.loads(completed.stdout)["status"] == "reproduced"
    # A second invocation must not overwrite an existing output.
    again = subprocess.run(
        [sys.executable, "-I", "-S", str(tmp_path / "reproduce.py")],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert again.returncode != 0


def test_reproduction_rejects_tampering_and_recomputes_after_rehashed_inputs(research, tmp_path):
    db, store = setup(research)
    raw = bundle(db, store, {"snapshot_id": "s1", "series": [{"series_id": "ipca"}]})
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        archive.extractall(tmp_path)
    inputs = tmp_path / "inputs.json"
    sources = json.loads(inputs.read_text())
    sources[0]["rows"][1]["value"] = "123456"
    inputs.write_text(json.dumps(sources))
    with pytest.raises(ValueError, match="checksum mismatch"):
        replay(tmp_path)
    receipt = json.loads((tmp_path / "receipt.json").read_text())
    receipt["sha256"]["inputs.json"] = hashlib.sha256(inputs.read_bytes()).hexdigest()
    (tmp_path / "receipt.json").write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="recomputed result differs"):
        replay(tmp_path)


def test_bundle_api_respects_identity_origin_and_read_only_service_access(research):
    db, _ = setup(research)
    db.settings.service_tokens = json.dumps({"reader": "service-secret-for-bundle-test-0123456789"})
    client = TestClient(create_app(db.settings))
    headers = {
        "X-OFL-Gateway-Token": "test-gateway",
        "X-OFL-Identity": "reader",
        "Origin": db.settings.origin,
    }
    spec = {"snapshot_id": "s1", "series": [{"series_id": "ipca"}]}
    response = client.post("/api/v1/datasets/bundle", headers=headers, json=spec)
    assert response.status_code == 200 and response.headers["content-type"] == "application/zip"
    assert client.post("/api/v1/datasets/bundle", json=spec).status_code == 401
    assert (
        client.post(
            "/api/v1/datasets/bundle", headers={**headers, "Origin": "https://evil.test"}, json=spec
        ).status_code
        == 403
    )
    machine = {"Authorization": "Bearer service-secret-for-bundle-test-0123456789"}
    exported = client.get(
        "/api/v1/datasets/bundle", headers=machine, params={"specification": json.dumps(spec)}
    )
    assert exported.status_code == 200 and exported.content == response.content
    assert client.post("/api/v1/datasets/bundle", headers=machine, json=spec).status_code == 403


def test_mcp_downloads_reproduction_resource_without_embedding_zip_in_tool_text(research, tmp_path):
    import httpx

    from ofl.research.mcp import create_mcp

    db, _ = setup(research)
    secret = "service-secret-for-bundle-test-0123456789"
    db.settings.service_tokens = json.dumps({"reader": secret})
    db.settings.api_token = secret
    api = TestClient(create_app(db.settings))
    calls = []

    def route(request):
        calls.append(request)
        assert request.method == "GET"
        response = api.get(
            request.url.path, params=dict(request.url.params), headers=dict(request.headers)
        )
        return httpx.Response(
            response.status_code, content=response.content, headers=dict(response.headers)
        )

    server = create_mcp(db.settings, transport=httpx.MockTransport(route))

    async def exercise():
        result = await server.call_tool(
            "dataset_reproduction",
            {"specification": json.dumps({"snapshot_id": "s1", "series": [{"series_id": "ipca"}]})},
        )
        assert len(result) == 1 and result[0].type == "resource_link"
        assert secret not in str(result[0].uri)
        content = list(await server.read_resource(result[0].uri))
        assert content[0].mime_type == "application/zip"
        with zipfile.ZipFile(io.BytesIO(content[0].content)) as archive:
            archive.extractall(tmp_path)
        assert replay(tmp_path) == (tmp_path / "dataset.csv").read_bytes()
        templates = await server.list_resource_templates()
        assert len(templates) == 1

    asyncio.run(exercise())
    assert len(calls) == 2
