"""Real PostgreSQL contracts for retrieval coverage, evaluation and worker isolation."""

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pytest

pytest.importorskip("psycopg")
pytest.importorskip("fastapi")

from fastapi.testclient import TestClient
from ofl.research.api import create_app
from ofl.research.archive import upsert_item
from ofl.research.retrieval_eval import corpus_manifest, evaluate
from ofl.research.search import index_coverage, search
from ofl.research.worker import run_once
from ofl.research.workspaces import load, rerun, save
from tests.test_research import research  # noqa: F401


def seed(db):
    with db.connect() as con:
        first = upsert_item(
            con,
            "news",
            {
                "source_id": "a",
                "title": "Inflação e juros",
                "collection": "economia",
                "date_start": "2026-01-05",
                "date_end": "2026-01-05",
                "date_precision": "day",
            },
            "a",
        )
        second = upsert_item(
            con,
            "manager_letter",
            {"source_id": "b", "title": "Carta sobre inflação", "collection": "gestora"},
            "b",
        )
        passage = con.execute("SELECT id FROM passages WHERE item_id=%s", (first,)).fetchone()["id"]
        con.execute(
            "INSERT INTO embeddings VALUES (%s,%s,%s,'[1,0,0]'::vector)",
            (passage, db.settings.embedding_model, db.settings.embedding_revision),
        )
    return first, second, passage


def test_scoped_coverage_counts_only_active_passages_and_current_model(research):
    db, _ = research
    first, _, passage = seed(db)
    coverage = search(db, "inflação", mode="hybrid", vector=[1, 0, 0])["index_coverage"]
    assert (coverage["eligible_passages"], coverage["indexed_passages"], coverage["status"]) == (
        2,
        1,
        "partial",
    )
    scoped = search(db, "termo inexistente", source="news", date_from=date(2026, 1, 1))[
        "index_coverage"
    ]
    assert scoped["status"] == "complete" and scoped["eligible_passages"] == 1
    assert search(db, "inflação", date_from=date(2026, 2, 1))["index_coverage"]["status"] == "empty"
    with db.connect() as con:
        con.execute("UPDATE embeddings SET revision='older' WHERE passage_id=%s", (passage,))
    assert index_coverage(db)["indexed_passages"] == 0
    with db.connect() as con:
        con.execute("UPDATE passages SET active=false WHERE item_id=%s", (first,))
    assert index_coverage(db)["eligible_passages"] == 1
    db.settings.embedding_revision = ""
    assert index_coverage(db)["status"] == "disabled"


def test_api_exposes_coverage_and_empty_vector_scope_does_not_invoke_model(research, monkeypatch):
    db, _ = research
    seed(db)

    def never(*_):
        raise AssertionError("an unindexed filtered scope must not invoke embedding")

    monkeypatch.setattr("ofl.research.search.embed", never)
    client = TestClient(create_app(db.settings))
    headers = {"X-OFL-Gateway-Token": "test-gateway", "X-OFL-Identity": "reader"}
    assert (
        client.get("/api/v1/workbench", headers=headers).json()["index_coverage"][
            "indexed_passages"
        ]
        == 1
    )
    result = client.get(
        "/api/v1/search",
        params={"q": "inflação", "mode": "vector", "source": "manager_letter"},
        headers=headers,
    )
    assert result.status_code == 200 and not result.json()["results"]
    assert result.json()["index_coverage"]["pending_passages"] == 1


def test_indexing_progress_does_not_change_keyword_check_content(research):
    db, store = research
    first, _, passage = seed(db)
    check = save(db, "owner", "check", "Inflação", {"type": "search", "q": "inflação"})
    rerun(db, store, "owner", check["id"])
    with db.connect() as con:
        con.execute("DELETE FROM embeddings WHERE passage_id=%s", (passage,))
    rerun(db, store, "owner", check["id"])
    result = load(db, "owner", check["id"])
    assert result["body"]["content"]["comparison"]["changed"] is False
    assert result["body"]["content"]["result"]["index_coverage"]["indexed_passages"] == 0


def test_judged_evaluation_keeps_ineligible_cases_and_counts_errors(
    research, tmp_path, monkeypatch
):
    db, _ = research
    first, second, _ = seed(db)
    receipt = corpus_manifest(db)
    manifest = {
        "version": 1,
        "split": "held_out",
        "judge": "synthetic test fixture; not independent users",
        "model": receipt["model"],
        "model_revision": receipt["model_revision"],
        "corpus_sha256": receipt["sha256"],
        "cases": [
            {"id": "positive", "query": "inflação", "relevant_item_ids": [first]},
            {
                "id": "excluded",
                "query": "inflação",
                "filters": {"source": "news"},
                "relevant_item_ids": [second],
            },
            {
                "id": "empty",
                "query": "termoausente",
                "filters": {"collection": "missing"},
                "relevant_item_ids": [],
                "expected_empty": True,
            },
        ],
    }
    path = tmp_path / "judged.json"
    path.write_text(json.dumps(manifest))
    monkeypatch.setattr("ofl.research.retrieval_eval.embed", lambda *_: [[1, 0, 0]])
    measured = evaluate(db, path)
    assert measured["planned_cases"] == 3 and measured["ineligible_cases"] == 1
    assert measured["summary"]["vector"]["recall_at_10"] == 1
    assert measured["summary"]["keyword"]["expected_empty_correct"] == 1

    def unavailable(*_):
        raise RuntimeError("private backend details")

    monkeypatch.setattr("ofl.research.retrieval_eval.embed", unavailable)
    failed = evaluate(db, path)
    assert failed["summary"]["vector"]["errors"] == 1
    assert failed["summary"]["vector"]["recall_at_10"] == 0
    assert "private backend details" not in json.dumps(failed)
    with db.connect() as con:
        con.execute("UPDATE items SET title='Revised',revision='changed' WHERE id=%s", (first,))
    with pytest.raises(ValueError, match="snapshot"):
        evaluate(db, path)


def test_evaluation_rejects_corpus_changes_during_measurement(research, tmp_path, monkeypatch):
    db, _ = research
    first, _, _ = seed(db)
    receipt = corpus_manifest(db)
    path = tmp_path / "judged.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "split": "development",
                "judge": "fixture",
                "model": receipt["model"],
                "model_revision": receipt["model_revision"],
                "corpus_sha256": receipt["sha256"],
                "cases": [{"id": "one", "query": "inflação", "relevant_item_ids": [first]}],
            }
        )
    )

    def change(*_):
        with db.connect() as con:
            con.execute("UPDATE items SET revision='changed' WHERE id=%s", (first,))
        return [[1, 0, 0]]

    monkeypatch.setattr("ofl.research.retrieval_eval.embed", change)
    with pytest.raises(ValueError, match="during evaluation"):
        evaluate(db, path)


def test_worker_lanes_preserve_serial_imports_but_allow_checks_during_embedding(
    research, monkeypatch
):
    db, store = research
    embedding = db.enqueue("embed", {})
    source = db.enqueue("library_sync", {})
    check = db.enqueue("research_check", {})
    started = threading.Event()
    release = threading.Event()

    def execute(_db, _store, job, _heartbeat):
        if job["id"] == embedding:
            started.set()
            assert release.wait(5)
        return {"ok": True}

    monkeypatch.setattr("ofl.research.worker.execute", execute)
    with ThreadPoolExecutor(max_workers=2) as pool:
        task = pool.submit(run_once, db, store, lane="embedding")
        try:
            assert started.wait(5)
            assert run_once(db, store, lane="embedding") is False
            assert run_once(db, store, lane="all") is False
            assert run_once(db, store, lane="checks") is True
            assert run_once(db, store, lane="default") is True
        finally:
            release.set()
        assert task.result(5) is True
    assert all(
        r["state"] == "done"
        for r in db.rows("SELECT state FROM jobs WHERE id=ANY(%s)", ([embedding, source, check],))
    )


def test_recurring_embedding_enqueue_is_deduplicated_atomically(research):
    db, _ = research
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = list(pool.map(lambda _: db.enqueue_if_idle("embed", {"limit": 64}), range(4)))
    assert len(set(jobs)) == 1
    assert db.rows("SELECT count(*) AS n FROM jobs WHERE kind='embed'")[0]["n"] == 1


def test_public_synthetic_fixture_cleans_only_its_temporary_schema(research, monkeypatch):
    from ofl.research.retrieval_fixture import evaluate_fixture

    db, _ = research
    seed(db)
    initial = corpus_manifest(db)["sha256"]
    monkeypatch.setattr(
        "ofl.research.search.embed", lambda _settings, texts: [[1, 0, 0] for _ in texts]
    )
    monkeypatch.setattr("ofl.research.retrieval_eval.embed", lambda *_: [[1, 0, 0]])
    result = evaluate_fixture(db.settings)
    assert result["fixture"]["documents"] == 20
    assert result["split"] == "development" and result["planned_cases"] == 6
    assert corpus_manifest(db)["sha256"] == initial
    assert not db.rows(
        "SELECT nspname FROM pg_namespace WHERE nspname LIKE %s", ("ofl_retrieval_eval_%",)
    )


def test_remote_embedding_batches_keep_committed_progress_on_retry(research, monkeypatch):
    from ofl.research.search import embed_pending

    db, _ = research
    db.settings.embedding_url = "http://measured-gpu.invalid"
    db.settings.embedding_batch_size = 2
    with db.connect() as con:
        for number in range(5):
            upsert_item(
                con, "news", {"source_id": str(number), "title": f"Documento {number}"}, str(number)
            )
    calls, progress = [], []

    def interrupted(_settings, texts):
        calls.append(len(texts))
        if len(calls) == 2:
            raise TimeoutError("worker lost its inference connection")
        return [[1, 0, 0] for _ in texts]

    monkeypatch.setattr("ofl.research.search.embed", interrupted)
    with pytest.raises(TimeoutError):
        embed_pending(db, 5, progress.append)
    assert index_coverage(db)["indexed_passages"] == 2
    assert progress == [{"embedded": 2, "total": 5}]
    calls.clear()
    monkeypatch.setattr(
        "ofl.research.search.embed",
        lambda _settings, texts: calls.append(len(texts)) or [[1, 0, 0] for _ in texts],
    )
    assert embed_pending(db, 5, progress.append) == 3
    assert calls == [2, 1]
    assert index_coverage(db)["status"] == "complete"


def test_real_candidate_pool_freezes_sources_and_reuses_query_vector(research, monkeypatch):
    from ofl.research.retrieval_review import assess, collect, worksheet

    db, _ = research
    first, second, _ = seed(db)
    calls = []
    monkeypatch.setattr(
        "ofl.research.retrieval_review.embed",
        lambda _settings, texts: calls.append(texts) or [[1, 0, 0]],
    )
    before = corpus_manifest(db)["sha256"]
    pool = collect(db, {"version": 1, "cases": [{"id": "inflation", "query": "inflação"}]})
    assert calls == [["inflação"]]
    assert corpus_manifest(db)["sha256"] == before
    case = pool["cases"][0]
    assert {c["item_id"] for c in case["candidates"]} == {first, second}
    assert all(r["status"] == "ok" for r in case["rankings"].values())
    # Simulate JSON export and later grading on a machine with no database.
    pool = json.loads(json.dumps(pool, default=str))
    reviews = worksheet(pool)
    reviews["reviewer"] = "synthetic fixture"
    for candidate in reviews["cases"][0]["candidates"]:
        candidate.update(verdict="relevant", rationale="Both fixture titles discuss inflation")
    measured = assess(pool, reviews)
    assert measured["summary"]["keyword"]["precision_returned_at_10"]["mean"] == 1


def test_candidate_pool_records_failures_and_isolates_concurrent_changes(research, monkeypatch):
    from ofl.research.retrieval_review import collect

    db, _ = research
    first, _, _ = seed(db)
    specification = {"version": 1, "cases": [{"id": "inflation", "query": "inflação"}]}

    def unavailable(*_):
        raise RuntimeError("private provider details")

    monkeypatch.setattr("ofl.research.retrieval_review.embed", unavailable)
    pool = collect(db, specification)
    assert pool["cases"][0]["rankings"]["keyword"]["status"] == "ok"
    assert pool["cases"][0]["rankings"]["hybrid"]["status"] == "error"
    assert "private provider details" not in json.dumps(pool, default=str)

    def mutate(*_):
        with db.connect() as con:
            con.execute("UPDATE items SET revision='pool-drift' WHERE id=%s", (first,))
        return [[1, 0, 0]]

    monkeypatch.setattr("ofl.research.retrieval_review.embed", mutate)
    before = corpus_manifest(db)["sha256"]
    frozen = collect(db, specification)
    assert frozen["corpus"]["sha256"] == before
    assert corpus_manifest(db)["sha256"] != before
    assert frozen["collection"]["isolation"] == "repeatable_read_read_only"


def test_original_catalog_metadata_is_discoverable_but_not_vector_evidence(research, monkeypatch):
    from ofl.research.archive import index_text
    from ofl.research.search import embed_pending

    db, _ = research
    with db.connect() as con:
        item = upsert_item(
            con,
            "manager_letter",
            {
                "source_id": "catalog-only",
                "title": "Carta sobre juros",
                "content_tier": "private_original",
            },
            "catalog-only",
        )
        metadata = con.execute("SELECT id FROM passages WHERE item_id=%s", (item,)).fetchone()["id"]
        # Previously backfilled metadata vectors remain stored but never enter
        # semantic coverage/rankings. Original captures and embeddings are not deleted.
        con.execute(
            "INSERT INTO embeddings VALUES (%s,%s,%s,'[1,0,0]'::vector)",
            (metadata, db.settings.embedding_model, db.settings.embedding_revision),
        )
    assert search(db, "juros", mode="keyword")["results"][0]["item_id"] == item
    assert not search(db, "juros", mode="vector", vector=[1, 0, 0])["results"]
    assert index_coverage(db)["eligible_passages"] == 0
    with db.connect() as con:
        index_text(
            con,
            item_id=item,
            revision="extracted-body",
            text="A carta interpreta a política de juros como restritiva.",
            method="pdf_text",
        )
    calls = []
    monkeypatch.setattr(
        "ofl.research.search.embed",
        lambda _settings, texts: calls.extend(texts) or [[1, 0, 0] for _ in texts],
    )
    assert embed_pending(db) == 1
    assert calls == ["A carta interpreta a política de juros como restritiva."]
    result = search(db, "juros", mode="vector", vector=[1, 0, 0])
    assert result["results"][0]["method"] == "pdf_text"
    assert result["index_coverage"]["eligible_passages"] == 1
    assert len(db.rows("SELECT * FROM embeddings WHERE passage_id=%s", (metadata,))) == 1


def test_keyword_exclusion_cannot_escape_through_unindexed_simple_dictionary(research):
    """Real vectors: body uses Portuguese; metadata also indexes simple tokens."""
    from ofl.research.archive import index_text

    db, _ = research
    bodies = {
        "excluded": "O Tesouro oferece títulos IPCA para investidores.",
        "included": "O IPCA mede a inflação ao consumidor.",
        "foreign": "A inflação nos EUA subiu neste mês.",
    }
    ids = {}
    with db.connect() as con:
        for name, text in bodies.items():
            ids[name] = upsert_item(con, "news", {"source_id": name, "title": name}, name)
            con.execute("UPDATE passages SET active=false WHERE item_id=%s", (ids[name],))
            index_text(con, item_id=ids[name], revision=name, text=text, method="text")
        ids["metadata"] = upsert_item(
            con, "mub3", {"source_id": "catalog", "title": "IPCA Tesouros BRXYZ123"}, "catalog"
        )
        ids["identifier"] = upsert_item(
            con, "mub3", {"source_id": "identifier", "title": "BRXYZ123"}, "identifier"
        )
    found = search(db, "IPCA -Tesouro", mode="keyword")["results"]
    assert {r["item_id"] for r in found} == {ids["included"]}
    assert {r["item_id"] for r in search(db, "BRXYZ123")["results"]} == {
        ids["metadata"],
        ids["identifier"],
    }
    assert {r["item_id"] for r in search(db, '"inflação ao consumidor"')["results"]} == {
        ids["included"]
    }
    assert {r["item_id"] for r in search(db, "IPCA OR EUA", source="news")["results"]} == {
        ids["included"],
        ids["excluded"],
        ids["foreign"],
    }


def fake_ollama(monkeypatch, db, *, failure=None):
    """Remote endpoint refusing any input marked oversize, as Ollama does with truncate=false."""
    import requests

    db.settings.embedding_url = "http://ollama.invalid"
    db.settings.embedding_batch_size = 32
    calls = []

    def reply(status, body):
        response = requests.Response()
        response.status_code = status
        response._content = body if isinstance(body, bytes) else json.dumps(body).encode()
        return response

    def post(_url, json, timeout):
        assert json["truncate"] is False
        calls.append(len(json["input"]))
        if failure and (status := failure(len(calls))):
            return reply(*status)
        if any("OVERSIZE" in text for text in json["input"]):
            return reply(413, {"error": "the input length exceeds the context length"})
        return reply(200, {"embeddings": [[1, 0, 0] for _ in json["input"]]})

    tags = {"models": [{"name": "test-model", "digest": "test-revision"}]}
    monkeypatch.setattr(requests, "get", lambda *_a, **_k: reply(200, tags))
    monkeypatch.setattr(requests, "post", post)
    return calls


def seed_letters(db, count, oversize=()):
    with db.connect() as con:
        for number in range(count):
            title = f"Carta {number}" + (" OVERSIZE" if number in oversize else "")
            upsert_item(con, "manager_letter", {"source_id": str(number), "title": title}, "x")


def rejections(db):
    return db.rows("SELECT passage_id,model,revision,reason FROM embedding_rejections")


def test_remote_context_overflow_is_isolated_and_the_queue_progresses(research, monkeypatch):
    from ofl.research.search import embed_pending

    db, _ = research
    seed_letters(db, 32, oversize={17})
    calls, progress = fake_ollama(monkeypatch, db), []
    assert embed_pending(db, 100, progress.append) == 32
    assert calls[0] == 32 and len(calls) == 11
    assert progress == [{"embedded": 31, "total": 32, "rejected": 1}]
    [rejected] = rejections(db)
    assert (rejected["model"], rejected["revision"]) == ("test-model", "test-revision")
    assert "context length" in rejected["reason"]
    coverage = index_coverage(db)
    assert (
        coverage["eligible_passages"],
        coverage["indexed_passages"],
        coverage["rejected_passages"],
        coverage["pending_passages"],
        coverage["status"],
    ) == (32, 31, 1, 0, "partial")
    assert "Rejected passages" in coverage["qualification"]
    calls.clear()
    assert embed_pending(db, 100) == 0 and not calls
    db.settings.embedding_revision = "other-revision"
    assert index_coverage(db)["rejected_passages"] == 0


def test_all_oversize_batch_is_rejected_without_failing_the_job(research, monkeypatch):
    from ofl.research.search import embed_pending

    db, _ = research
    seed_letters(db, 3, oversize={0, 1, 2})
    fake_ollama(monkeypatch, db)
    assert embed_pending(db, 100) == 3
    assert len(rejections(db)) == 3
    coverage = index_coverage(db)
    assert (coverage["indexed_passages"], coverage["rejected_passages"]) == (0, 3)
    assert coverage["pending_passages"] == 0 and coverage["fraction"] == 0


PHRASE = "the input length exceeds the context length"


@pytest.mark.parametrize(
    ("failure", "calls_made"),
    [
        (lambda _: (500, {"error": "model runner has unexpectedly stopped"}), [4]),
        (lambda _: (400, {"error": "invalid input"}), [4]),
        (lambda _: (503, f"<html><body>upstream: {PHRASE}</body></html>".encode()), [4]),
        (lambda _: (500, {"error": f"upstream: {PHRASE} upstream"}), [4]),
        (lambda _: (413, PHRASE.encode()), [4]),
        (lambda _: (413, {"error": f"{PHRASE} (proxy)"}), [4]),
        # The first refusal is genuine; an outage during bisection still fails the batch.
        (lambda call: None if call == 1 else (503, b"Service Unavailable"), [4, 2]),
    ],
)
def test_other_remote_errors_still_fail_the_job(research, monkeypatch, failure, calls_made):
    import requests

    from ofl.research.search import embed_pending

    db, _ = research
    seed_letters(db, 4, oversize={1})
    calls = fake_ollama(monkeypatch, db, failure=failure)
    with pytest.raises(requests.HTTPError):
        embed_pending(db, 100)
    assert calls == calls_made and not rejections(db)
    assert not db.rows("SELECT 1 FROM embeddings")
    assert index_coverage(db)["pending_passages"] == 4


def test_changed_passage_content_clears_its_rejection(research, monkeypatch):
    from ofl.research.search import embed_pending

    db, _ = research
    seed_letters(db, 2, oversize={0, 1})
    fake_ollama(monkeypatch, db)
    assert embed_pending(db, 100) == 2
    [first, second] = sorted(row["passage_id"] for row in rejections(db))
    with db.connect() as con:
        con.execute("UPDATE passages SET text='Carta dividida' WHERE id=%s", (first,))
        con.execute("UPDATE passages SET revision='rechunked' WHERE id=%s", (second,))
    coverage = index_coverage(db)
    assert (coverage["rejected_passages"], coverage["pending_passages"]) == (0, 2)
    assert embed_pending(db, 100) == 2
    coverage = index_coverage(db)
    assert (coverage["indexed_passages"], coverage["rejected_passages"]) == (1, 1)
    # The still-oversize passage is rejected again for its new revision.
    assert db.rows(
        "SELECT passage_revision FROM embedding_rejections WHERE passage_id=%s", (second,)
    ) == [{"passage_revision": "rechunked"}]
