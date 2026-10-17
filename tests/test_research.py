"""Offline parser tests and PostgreSQL tests in unique disposable schemas."""

import hashlib
import json
import os
import zipfile
from decimal import Decimal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pytest

pytest.importorskip("psycopg")

from ofl.research import cotahist
from ofl.research.config import ResearchSettings
from ofl.research.db import Database, uid
from ofl.research.storage import Store


def record(
    market="010",
    bdi="02",
    symbol="PETR4",
    day="20240305",
    close=3880,
    currency="R$",
    factor=1,
    distribution="100",
):
    raw = bytearray(b" " * 245)
    raw[:2], raw[2:10] = b"01", day.encode()
    values = {
        "codbdi": bdi,
        "tpmerc": market,
        "symbol": symbol,
        "issuer_name": "PETROBRÁS",
        "specification": "PN",
        "forward_term": "",
        "currency": currency,
        "correction_indicator": "0",
        "expiry_raw": "99991231",
        "isin": "BRPETRACNPR6",
        "distribution": distribution,
    }
    for key, (start, length) in cotahist.TEXT_FIELDS.items():
        raw[start : start + length] = values[key].ljust(length).encode("latin-1")
    for key, (start, length) in cotahist.MONEY_FIELDS.items():
        raw[start : start + length] = str(close).zfill(length).encode()
    for key, (start, length) in cotahist.INTEGER_FIELDS.items():
        raw[start : start + length] = (
            str(factor if key == "quotation_factor" else 123).zfill(length).encode()
        )
    raw[217:230] = b"0000000123456"
    return bytes(raw)


def file_bytes(rows, generated="20240306", year="2024", wrong_count=False):
    header = ("00" + f"COTAHIST.{year}" + "BOVESPA " + generated).ljust(245).encode()
    trailer = (
        (
            "99"
            + f"COTAHIST.{year}"
            + "BOVESPA "
            + generated
            + str(len(rows) + (3 if wrong_count else 2)).zfill(11)
        )
        .ljust(245)
        .encode()
    )
    return b"\r\n".join([header, *rows, trailer]) + b"\r\n"


def write_input(folder, rows, name="COTAHIST_A2024.TXT", **kwargs):
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_bytes(file_bytes(rows, **kwargs))
    return path


def test_market_identity_and_exact_values(tmp_path):
    rows = [
        record(market=m, bdi="70" if m == "050" else "71" if m == "060" else "02")
        for m in cotahist.MARKETS
    ]
    path = write_input(tmp_path, rows)
    result = cotahist.stage(path, tmp_path / "stage", "hash", batch_rows=2, input_name=path.name)
    assert result["counts"] == {
        "records": 10,
        "accepted": 8,
        "excluded_futures": 2,
        "quarantined": 0,
    }
    import pyarrow.parquet as pq

    quotes = pq.ParquetDataset(tmp_path / "stage").read().to_pylist()
    assert len({row["record_id"] for row in quotes}) == 8
    assert quotes[0]["close"] == Decimal("38.80")
    assert quotes[0]["exercise_points"] == Decimal(".123456")
    assert quotes[0]["expiry"] is None


@pytest.mark.parametrize(
    "rows,kwargs,message",
    [
        ([record()[:-1]], {}, "record length"),
        ([record(market="050")], {}, "conflicting futures"),
        ([record(market="999")], {}, "unknown market"),
        ([record()], {"wrong_count": True}, "count mismatch"),
        ([record(day="20230305")], {}, "coverage"),
    ],
)
def test_rejects_bad_files(tmp_path, rows, kwargs, message):
    path = write_input(tmp_path, rows, **kwargs)
    with pytest.raises(ValueError, match=message):
        cotahist.stage(path, tmp_path / "stage", "hash")


def test_historical_currency_factor_and_distribution():
    old = cotahist.parse_record(record(day="19860305", currency="Cz$", factor=1000), "hash", 2)
    changed = cotahist.parse_record(
        record(day="19860305", currency="Cz$", factor=1000, distribution="101"), "hash", 3
    )
    assert old["currency"] == "Cz$" and old["quotation_factor"] == 1000
    assert old["record_id"] != changed["record_id"]


def test_zip_path_and_expansion_limits(tmp_path):
    path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("../escape.TXT", file_bytes([record()]))
    with pytest.raises(ValueError, match="unsafe"):
        cotahist.stage(path, tmp_path / "stage", "hash")


def test_observed_market_021_keeps_its_code_without_inferred_classification(tmp_path):
    path = write_input(tmp_path, [record(market="021", bdi="93")])
    result = cotahist.stage(path, tmp_path / "stage", "hash")
    assert result["counts"]["accepted"] == 1
    assert cotahist.MARKETS["021"] == "unclassified_021"
    invalid = write_input(tmp_path / "invalid", [record(market="021", bdi="02")])
    with pytest.raises(ValueError, match="unrecognized BDI"):
        cotahist.stage(invalid, tmp_path / "invalid-stage", "hash")


@pytest.fixture
def research(tmp_path):
    url = os.getenv("OFL_RESEARCH_TEST_DATABASE_URL")
    if not url:
        pytest.skip("set OFL_RESEARCH_TEST_DATABASE_URL to disposable PostgreSQL with pgvector")
    import psycopg
    from psycopg import sql

    schema = "research_test_" + uid()
    with psycopg.connect(url, autocommit=True) as con:
        con.execute("CREATE EXTENSION IF NOT EXISTS vector")
        con.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query))
    query["options"] = f"-csearch_path={schema},public"
    settings = ResearchSettings(
        database_url=urlunsplit(parsed._replace(query=urlencode(query))),
        root=tmp_path / "store",
        gateway_token="test-gateway",
        roles=json.dumps({"owner": "admin", "reader": "reader"}),
        embedding_dimensions=3,
        embedding_model="test-model",
        embedding_revision="test-revision",
    )
    db = Database(settings)
    db.migrate()
    store = Store(settings.root)
    try:
        yield db, store
    finally:
        with psycopg.connect(url, autocommit=True) as con:
            con.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def import_and_run(db, store, path, source="b3_cotahist", metadata=None):
    from ofl.research.imports import register
    from ofl.research.worker import run

    result = register(db, store, path, source, metadata)
    run(db, store, drain=True)
    job = db.rows("SELECT * FROM jobs WHERE id=%s", (result["job_id"],))[0]
    assert job["state"] == "done", job["error"]
    return result


@pytest.mark.parametrize("publication_days", [1, 31])
def test_import_replay_precedence_and_correction(research, tmp_path, publication_days):
    from ofl.research.imports import register
    from ofl.research.query import quote_query

    db, store = research
    db.settings.publication_days = publication_days
    annual = write_input(
        tmp_path / "annual", [record(), record(market="020"), record(day="20240306")]
    )
    first = import_and_run(db, store, annual)
    assert len(quote_query(db)["rows"]) == 3
    assert register(db, store, annual, "b3_cotahist")["job_id"] == first["job_id"]
    daily = write_input(tmp_path / "daily", [record(close=4000)], "COTAHIST_D05032024.TXT")
    import_and_run(db, store, daily)
    rows = quote_query(db)["rows"]
    assert len(rows) == 2 and rows[0]["close"] == Decimal("40.00")
    annual2 = write_input(
        tmp_path / "annual2",
        [record(close=4100), record(day="20240306", close=4200)],
        generated="20240307",
    )
    import_and_run(db, store, annual2)
    rows = quote_query(db)["rows"]
    assert [r["close"] for r in rows] == [Decimal("40.00"), Decimal("42.00")]


def test_equal_priority_requires_explicit_supersession(research, tmp_path):
    from ofl.research.imports import register
    from ofl.research.query import quote_query
    from ofl.research.worker import run

    db, store = research
    first = import_and_run(db, store, write_input(tmp_path / "a", [record(), record(market="020")]))
    second = register(db, store, write_input(tmp_path / "b", [record(close=4400)]), "b3_cotahist")
    run(db, store, drain=True)
    assert (
        db.rows("SELECT state FROM jobs WHERE id=%s", (second["job_id"],))[0]["state"] == "failed"
    )
    assert len(quote_query(db)["rows"]) == 2
    with db.connect() as con:
        from psycopg.types.json import Jsonb

        con.execute(
            "UPDATE imports SET metadata=%s WHERE id=%s",
            (Jsonb({"supersedes": first["import_id"]}), second["import_id"]),
        )
        con.execute("UPDATE jobs SET state='queued' WHERE id=%s", (second["job_id"],))
    run(db, store, drain=True)
    assert len(quote_query(db)["rows"]) == 1


def test_conflicting_duplicate_never_publishes(research, tmp_path):
    from ofl.research.imports import register
    from ofl.research.worker import run

    db, store = research
    result = register(
        db, store, write_input(tmp_path, [record(), record(close=4200)]), "b3_cotahist"
    )
    run(db, store, drain=True)
    assert (
        db.rows("SELECT state FROM jobs WHERE id=%s", (result["job_id"],))[0]["state"] == "failed"
    )
    assert not db.rows("SELECT * FROM pointers")
    assert not store.path("silver/fact_b3_quote").exists()
    error = db.rows("SELECT error FROM jobs WHERE id=%s", (result["job_id"],))[0]["error"]
    assert "source lines 2 and 3" in error


def test_catalog_search_and_revisions(research, tmp_path):
    from ofl.research.search import search

    db, store = research
    path = tmp_path / "catalog.jsonl"
    path.write_text(
        json.dumps(
            {
                "source_id": "photo-1",
                "title": "Pregão histórico",
                "item_type": "photograph",
                "description": "Negociação de ações",
            }
        )
    )
    import_and_run(db, store, path, "mub3")
    assert search(db, "negociação")["results"][0]["title"] == "Pregão histórico"
    path.write_text(
        json.dumps(
            {"source_id": "photo-1", "title": "Arquivo atualizado", "item_type": "photograph"}
        )
    )
    import_and_run(db, store, path, "mub3")
    assert not search(db, "negociação")["results"]
    assert len(db.rows("SELECT * FROM items")) == 1


def test_review_approval_and_supersession(research, tmp_path):
    from ofl.research.archive import add_candidate, attach, upsert_item
    from ofl.research.review import submit_review

    db, store = research
    path = tmp_path / "table.csv"
    path.write_text("date;value\n2024-03-05;38,80\n")
    with db.connect() as con:
        item = upsert_item(con, "b3_bulletin", {"title": "Bulletin"}, "one")
        attach(con, db, store, item, path)
        asset = con.execute("SELECT id FROM assets").fetchone()["id"]
        candidate = add_candidate(
            con, asset, 1, [0, 0, 1, 1], [["date", "value"], ["2024-03-05", "38,80"]], "test-v1"
        )
    mapping = {
        "date": {"column": 0},
        "value": {"column": 1},
        "subject": {"constant": "PETR4"},
        "market": {"constant": "010"},
        "metric": {"constant": "close"},
        "unit": {"constant": "price"},
        "currency": {"constant": "BRL"},
        "decimal_separator": ",",
    }
    cells = [["date", "value"], ["2024-03-05", "38,80"]]
    assert not db.rows("SELECT * FROM observations")
    submit_review(
        db,
        candidate,
        actor="owner",
        expected_revision=0,
        cells=cells,
        mapping=mapping,
        state="approved",
    )
    assert db.rows("SELECT value FROM observations WHERE active")[0]["value"] == Decimal("38.80")
    with pytest.raises(FileExistsError):
        submit_review(
            db,
            candidate,
            actor="owner",
            expected_revision=0,
            cells=cells,
            mapping=mapping,
            state="approved",
        )
    submit_review(
        db,
        candidate,
        actor="owner",
        expected_revision=1,
        cells=cells,
        mapping=mapping,
        state="rejected",
    )
    assert not db.rows("SELECT * FROM observations WHERE active")


def test_private_api_roles_upload_replay_and_exact_decimals(research):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from ofl.research.api import create_app
    from ofl.research.worker import run

    db, store = research
    client = TestClient(create_app(db.settings))
    assert client.get("/api/v1/coverage").status_code == 401
    headers = {
        "X-OFL-Gateway-Token": "test-gateway",
        "X-OFL-Identity": "owner",
        "Origin": "http://localhost:3000",
        "Idempotency-Key": "upload-one",
    }
    assert client.get("/api/v1/me", headers=headers).json()["pipeline_enabled"] is False
    assert (
        client.post(
            "/api/v1/jobs",
            headers=headers,
            json={"action": "pipeline", "dag": "ofl_silver"},
        ).status_code
        == 503
    )
    assert (
        client.post(
            "/api/v1/jobs",
            headers={**headers, "X-OFL-Identity": "reader"},
            json={"action": "embed"},
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/api/v1/jobs",
            headers={**headers, "Origin": "https://attacker.invalid"},
            json={"action": "embed"},
        ).status_code
        == 403
    )
    raw = file_bytes([record()])
    body = {
        "name": "COTAHIST_A2024.TXT",
        "source": "b3_cotahist",
        "size": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }
    first = client.post("/api/v1/uploads", headers=headers, json=body)
    assert first.status_code == 201, first.text
    assert client.post("/api/v1/uploads", headers=headers, json=body).json() == first.json()
    assert (
        client.post("/api/v1/uploads", headers=headers, json={**body, "size": 1}).status_code == 409
    )
    upload = first.json()["upload_id"]
    assert (
        client.put(f"/api/v1/uploads/{upload}?offset=0", headers=headers, content=raw).status_code
        == 200
    )
    assert (
        client.put(f"/api/v1/uploads/{upload}?offset=0", headers=headers, content=raw).status_code
        == 409
    )
    assert client.post(f"/api/v1/uploads/{upload}/complete", headers=headers).status_code == 202
    run(db, store, drain=True)
    result = client.get("/api/v1/quotes", headers=headers)
    assert result.status_code == 200, result.text
    assert result.json()["rows"][0]["close"] == "38.80"


def test_vector_filtering_and_inactive_revision(research, tmp_path):
    from ofl.research.search import search

    db, store = research
    path = tmp_path / "catalog.jsonl"
    path.write_text(
        "\n".join(
            json.dumps({"source_id": str(i), "title": name, "collection": collection})
            for i, name, collection in (
                (1, "Mercado de ações", "a"),
                (2, "Coleção de objetos", "b"),
            )
        )
    )
    import_and_run(db, store, path, "mub3")
    with db.connect() as con:
        for row in con.execute(
            "SELECT p.id,i.collection FROM passages p JOIN items i ON i.id=p.item_id"
        ).fetchall():
            vector = "[1,0,0]" if row["collection"] == "a" else "[0,1,0]"
            con.execute(
                "INSERT INTO embeddings VALUES (%s,'test-model','test-revision',%s::vector)",
                (row["id"], vector),
            )
    found = search(db, "ações", mode="hybrid", vector=[1, 0, 0], collection="a")["results"]
    assert len(found) == 1 and found[0]["collection"] == "a"
    from ofl.research.search import create_vector_index

    create_vector_index(db)
    db.settings.approximate_search = True
    assert (
        search(db, "ações", mode="vector", vector=[1, 0, 0], collection="a")["results"][0]["id"]
        == found[0]["id"]
    )
    with db.connect() as con:
        con.execute("UPDATE passages SET active=false WHERE id=%s", (found[0]["id"],))
    assert not search(db, "ações", mode="vector", vector=[1, 0, 0], collection="a")["results"]


def test_unpublished_delta_commit_is_restored(research, tmp_path):
    from deltalake import DeltaTable, write_deltalake

    from ofl.research.query import quote_query

    db, store = research
    import_and_run(db, store, write_input(tmp_path / "a", [record(), record(day="20240306")]))
    release = db.rows("SELECT * FROM releases WHERE kind='quotes'")[0]
    delta = DeltaTable(release["delta_uri"])
    table = delta.to_pyarrow_table()
    write_deltalake(release["delta_uri"], table.slice(0, 0), mode="overwrite")
    # API readers still pin the committed release despite the orphan commit.
    assert len(quote_query(db)["rows"]) == 2
    daily = write_input(tmp_path / "b", [record(close=4500)], "COTAHIST_D05032024.TXT")
    import_and_run(db, store, daily)
    assert len(quote_query(db)["rows"]) == 2


def test_correction_can_remove_an_entire_date(research, tmp_path):
    from ofl.research.query import quote_query

    db, store = research
    import_and_run(db, store, write_input(tmp_path / "a", [record(), record(day="20240306")]))
    import_and_run(
        db, store, write_input(tmp_path / "b", [record(close=4500)], generated="20240307")
    )
    assert len(quote_query(db)["rows"]) == 1


def test_expired_job_lease_can_be_reclaimed(research):
    db, _ = research
    job = db.enqueue("build_observations", {})
    assert db.claim("first")["id"] == job
    assert db.claim("second") is None
    with db.connect() as con:
        con.execute("UPDATE jobs SET lease_until=now()-interval '1 second' WHERE id=%s", (job,))
    assert db.claim("second")["id"] == job
    with pytest.raises(RuntimeError, match="lease lost"):
        db.heartbeat(job, "first")


def test_bulletin_csv_is_a_reviewable_asset(research, tmp_path):
    db, store = research
    path = tmp_path / "bulletin.csv"
    path.write_text("date;value\n2024-03-05;38,80\n")
    import_and_run(db, store, path, "b3_bulletin")
    assert len(db.rows("SELECT * FROM candidates")) == 1
    assert not db.rows("SELECT * FROM observations")


def test_qdrant_and_postgres_pilot_agree_on_fixed_vectors(research, tmp_path):
    pytest.importorskip("qdrant_client")
    from ofl.research.pilot import evaluate

    db, store = research
    path = tmp_path / "catalog.jsonl"
    path.write_text(json.dumps({"source_id": "one", "title": "Mercado de ações"}))
    import_and_run(db, store, path, "mub3")
    passage = db.rows("SELECT id FROM passages WHERE active")[0]["id"]
    with db.connect() as con:
        con.execute(
            "INSERT INTO embeddings VALUES (%s,'test-model','test-revision','[1,0,0]')", (passage,)
        )
    queries = tmp_path / "judged.jsonl"
    queries.write_text(
        json.dumps({"query": "ações", "embedding": [1, 0, 0], "relevant": [passage]})
    )
    result = evaluate(db, queries)
    assert all(score["recall_at_10"] == 1 for score in result["backends"].values())
    assert "not comparable" in result["qualification"]


def test_derived_unit_price_uses_decimal_arithmetic():
    from ofl.research.query import per_unit

    assert per_unit(Decimal("99999999999.99"), 1000) == Decimal("99999999.999990000000")
    assert per_unit(Decimal("38.80"), 0) is None


def test_historical_observation_period_preserves_precision():
    from datetime import date

    from ofl.research.review import observation_period

    assert observation_period("02/1952", "%m/%Y") == (date(1952, 2, 1), date(1952, 2, 29), "month")
    assert observation_period("1986", "%Y") == (date(1986, 1, 1), date(1986, 12, 31), "year")


def test_removed_catalog_asset_does_not_remain_searchable(research, tmp_path):
    from ofl.research.search import search

    db, store = research
    archive = tmp_path / "catalog.zip"
    with zipfile.ZipFile(archive, "w") as package:
        package.writestr(
            "catalog.jsonl",
            json.dumps(
                {"source_id": "one", "title": "Documento", "assets": [{"path": "files/one.txt"}]}
            ),
        )
        package.writestr("files/one.txt", "Regulamento de negociacao do pregao")
    import_and_run(db, store, archive, "mub3")
    assert search(db, "regulamento")["results"]
    update = tmp_path / "update.jsonl"
    update.write_text(json.dumps({"source_id": "one", "title": "Documento", "assets": []}))
    import_and_run(db, store, update, "mub3")
    assert not search(db, "regulamento")["results"]
    assert db.rows("SELECT active FROM assets")[0]["active"] is False


def test_empty_approved_observation_release_is_valid(research):
    from deltalake import DeltaTable

    from ofl.research.worker import build_observations

    db, store = research
    result = build_observations(db, store)
    assert result["observations"] == 0
    release = db.rows("SELECT * FROM releases WHERE id=%s", (result["release"],))[0]
    assert DeltaTable(release["delta_uri"]).to_pyarrow_table().num_rows == 0


@pytest.mark.parametrize("suffix", ["png", "pdf"])
def test_real_ocr_and_ruled_table_extraction(research, tmp_path, suffix):
    import shutil

    if not all(shutil.which(command) for command in ("tesseract", "pdftoppm")):
        pytest.skip("requires the research OCR image's Tesseract/Poppler tools")
    pytest.importorskip("cv2")
    from PIL import Image, ImageDraw, ImageFont

    db, store = research
    image = Image.new("RGB", (1600, 1000), "white")
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default(size=36)
    draw.text((70, 60), "SYNTHETIC TEST - Boletim de negociacao de acoes", font=font, fill="black")
    for x in (70, 600, 950, 1500):
        draw.line((x, 200, x, 650), fill="black", width=4)
    for y in (200, 350, 500, 650):
        draw.line((70, y, 1500, y), fill="black", width=4)
    for y, row in zip(
        (250, 400, 550),
        (
            ("DATA", "ATIVO", "VALOR"),
            ("2024-03-05", "PETR4", "38,80"),
            ("2024-03-06", "PETR4", "39,20"),
        ),
        strict=True,
    ):
        for x, value in zip((100, 650, 1000), row, strict=True):
            draw.text((x, y), value, font=font, fill="black")
    path = tmp_path / f"synthetic-bulletin.{suffix}"
    image.save(path, "PDF" if suffix == "pdf" else "PNG")
    import_and_run(db, store, path, "b3_bulletin")
    assert db.rows("SELECT state FROM assets")[0]["state"] == "processed"
    text = " ".join(row["text"] for row in db.rows("SELECT text FROM passages WHERE method='ocr'"))
    assert "PETR4" in text and "38,80" in text
    candidate = db.rows("SELECT * FROM candidates")[0]
    assert candidate["cells"][1][2] == "38,80"
    assert candidate["evidence"]["cell_boxes"][1][2]
    assert not db.rows("SELECT * FROM observations")


def test_private_ollama_embeddings_validate_revision_and_response(monkeypatch):
    from ofl.research.search import embed

    settings = ResearchSettings(
        embedding_url="http://model.invalid",
        embedding_model="bge:latest",
        embedding_revision="sha256:pinned",
        embedding_dimensions=3,
    )
    result = {"embeddings": [[1.0, 0.0, 0.0]]}

    class Response:
        def __init__(self, data):
            self.data = data

        def raise_for_status(self):
            pass

        def json(self):
            return self.data

    monkeypatch.setattr(
        "requests.get",
        lambda *a, **kw: Response({"models": [{"name": "bge:latest", "digest": "sha256:pinned"}]}),
    )

    def post(*args, **kwargs):
        assert kwargs["json"]["truncate"] is False
        return Response(result)

    monkeypatch.setattr("requests.post", post)
    assert embed(settings, ["mercado de ações"]) == [[1.0, 0.0, 0.0]]
    result["embeddings"] = []
    with pytest.raises(ValueError, match="count"):
        embed(settings, ["ações"])
    result["embeddings"] = [[float("nan"), 0, 0]]
    with pytest.raises(ValueError, match="finite"):
        embed(settings, ["ações"])
    settings.embedding_revision = "changed"
    with pytest.raises(ValueError, match="digest"):
        embed(settings, ["ações"])
