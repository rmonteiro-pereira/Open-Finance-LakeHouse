"""Product-foundation contracts: actual snapshots and collector-to-evidence integration."""

import hashlib
import io
import json
from datetime import date
from types import SimpleNamespace

import pytest

pytest.importorskip("psycopg")

from ofl.documents.periods import HEADER_CHARACTERS
from ofl.research.archive import index_text, item_identity
from ofl.research.db import digest
from ofl.research.library import _letter_candidates, date_interval, sync_library
from ofl.research.macro import catalog, export_series, series_query, sync_macro
from ofl.research.search import search
from tests.test_research import research  # noqa: F401


class LibraryFixture:
    news_tiers = {"official-source": "official", "restricted": "headline_only"}

    def __init__(self, blobs):
        self.blobs = blobs
        self.reads = []

    def read(self, key, maximum, *, snapshot=False):
        self.reads.append(key)
        value = self.blobs[key]
        if len(value) > maximum:
            raise ValueError("source too large")
        return value

    def objects(self, prefix):
        return [
            {"key": k, "signature": hashlib.sha256(v).hexdigest(), "modified": "2026-01-01"}
            for k, v in sorted(self.blobs.items())
            if k.startswith(prefix)
        ]


def news(
    *,
    tier="official",
    source="official-source",
    published="2026-01-05",
    fetched="2026-01-06T12:00:00Z",
    title="Inflação no Brasil",
    body="Indicadores documentados e contexto econômico.",
):
    return {
        "id": "n1",
        "source": source,
        "tier": tier,
        "title": title,
        "link": "https://example.org/story",
        "published": published,
        "fetched_at": fetched,
        "author": "Source author",
        "summary": "Resumo da notícia",
        "body": body,
    }


def snapshot(*, snapshot_id="s1", values=None, private=False):
    import pyarrow as pa
    import pyarrow.parquet as pq

    values = values or [
        {"series_id": "ipca", "date": date(2026, 1, 1), "value": 0.56},
        {"series_id": "ipca", "date": date(2026, 2, 1), "value": 0.42},
    ]
    stream = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(values), stream)
    body = stream.getvalue()
    prefix = f"snapshots/{snapshot_id}/"
    manifest = {
        "run_id": snapshot_id,
        "generated_at": "2026-03-01T12:00:00+00:00",
        "catalog": prefix + "catalog.json",
        "observations": {
            "path": prefix + "observations.parquet",
            "sha256": hashlib.sha256(body).hexdigest(),
        },
    }
    cat = {
        "series": [
            {
                "key": "ipca",
                "name": "Inflação IPCA",
                "unit": "%",
                "frequency": "monthly",
                "source": "official",
                "redistribution": "private" if private else "open",
                "rows": 99999,
            }
        ]
    }
    return LibraryFixture(
        {
            "latest.json": json.dumps({"manifest": prefix + "manifest.json"}).encode(),
            prefix + "manifest.json": json.dumps(manifest).encode(),
            prefix + "catalog.json": json.dumps(cat).encode(),
            prefix + "observations.parquet": body,
        }
    )


@pytest.mark.parametrize(
    "raw,precision,start,end",
    [
        ("", "unknown", None, None),
        ("sem data", "unknown", None, None),
        ("2020", "year", date(2020, 1, 1), date(2020, 12, 31)),
        ("2020-02", "month", date(2020, 2, 1), date(2020, 2, 29)),
        ("2026-01-05T18:00:00-03:00", "day", date(2026, 1, 5), date(2026, 1, 5)),
    ],
)
def test_publication_date_precision(raw, precision, start, end):
    actual_start, actual_end, actual_precision, _timestamp = date_interval(raw)
    assert (actual_start, actual_end, actual_precision) == (start, end, precision)


def test_news_sync_is_searchable_citable_and_idempotent(research):
    db, store = research
    key = "news/items/official-source/2026-01-06/120000.jsonl"
    source = LibraryFixture({key: json.dumps(news(), ensure_ascii=False).encode()})
    result = sync_library(db, store, "news", source=source)
    assert result["items"] == 1
    hits = search(db, "inflação", mode="keyword")["results"]
    assert hits[0]["source"] == "news"
    assert hits[0]["epistemic_status"] == "official_publication"
    assert hits[0]["date_precision"] == "day"
    assert hits[0]["asset_id"]
    assert sync_library(db, store, "news", source=source)["unchanged"] == 1
    assert len(db.rows("SELECT id FROM items")) == 1
    assert len(db.rows("SELECT id FROM observations")) == 0


def test_headline_policy_is_reapplied_even_when_body_was_stored(research):
    db, store = research
    key = "news/items/restricted/2026-01-06/120000.jsonl"
    source = LibraryFixture(
        {
            key: json.dumps(
                news(source="restricted", tier="feed_fulltext", body="SECRETCONTENTNOTALLOWED")
            ).encode()
        }
    )
    sync_library(db, store, "news", source=source)
    assert not search(db, "SECRETCONTENTNOTALLOWED", mode="keyword")["results"]
    asset = db.rows("SELECT object_key FROM assets")[0]
    assert "SECRETCONTENTNOTALLOWED" not in store.path(asset["object_key"]).read_text()
    assert (
        db.rows("SELECT metadata->>'content_tier' AS tier FROM items")[0]["tier"] == "headline_only"
    )


def test_older_news_batch_cannot_replace_newer_capture(research):
    db, store = research
    for stamp, title in [
        ("2026-01-06T12:00:00Z", "Versão nova"),
        ("2026-01-05T12:00:00Z", "Versão antiga"),
    ]:
        key = "news/items/official-source/" + stamp[:10] + "/batch.jsonl"
        sync_library(
            db,
            store,
            "news",
            source=LibraryFixture({key: json.dumps(news(fetched=stamp, title=title)).encode()}),
        )
    assert db.rows("SELECT title FROM items")[0]["title"] == "Versão nova"


def test_bad_news_has_no_partial_batch_publication(research):
    db, store = research
    key = "news/items/official-source/2026-01-06/batch.jsonl"
    payload = "\n".join(json.dumps(row) for row in [news(), news(source="different-source")])
    with pytest.raises(ValueError, match="failed inputs"):
        sync_library(db, store, "news", source=LibraryFixture({key: payload.encode()}))
    assert not db.rows("SELECT id FROM items")
    assert not db.rows("SELECT * FROM library_entries")


def test_letter_original_is_verified_and_date_is_not_invented(research):
    db, store = research
    pdf = b"%PDF-1.7\nsynthetic original for bridge contract\n"
    sha = hashlib.sha256(pdf).hexdigest()
    record = {
        "url": "https://example.org/letter",
        "title": "Carta de gestão",
        "published": "",
        "status": "stored",
        "sha256": sha,
        "seen_at": "2026-01-06T12:00:00Z",
    }
    source = LibraryFixture(
        {
            "letters/manifest/test-manager.jsonl": json.dumps(record).encode(),
            f"letters/files/test-manager/{sha}.pdf": pdf,
        }
    )
    assert sync_library(db, store, "letters", source=source)["items"] == 1
    row = db.rows("SELECT * FROM items")[0]
    assert row["date_start"] is None and row["date_precision"] == "unknown"
    assert row["metadata"]["epistemic_status"] == "attributed_opinion"
    assert sync_library(db, store, "letters", source=source)["unchanged"] == 1
    assert len(db.rows("SELECT id FROM jobs WHERE kind='process_asset'")) == 1
    assert store.path(db.rows("SELECT object_key FROM assets")[0]["object_key"]).read_bytes() == pdf


def letter_manifest(pdf, *, url, title, **fields):
    """A one-letter manifest and its original, as the letters collector stores them."""
    sha = hashlib.sha256(pdf).hexdigest()
    record = {
        "url": url,
        "title": title,
        "published": "",
        "status": "stored",
        "sha256": sha,
        "seen_at": "2026-01-06T12:00:00Z",
        **fields,
    }
    return {
        "letters/manifest/test-manager.jsonl": json.dumps(record).encode(),
        f"letters/files/test-manager/{sha}.pdf": pdf,
    }


def test_letter_with_generic_link_text_is_retitled_from_its_file_name(research):
    db, store = research
    pdf = b"%PDF-1.7\nsynthetic letter for title derivation\n"
    url = "https://example.org/files/Carta%20Mensal%20Julho.pdf"
    described = LibraryFixture(letter_manifest(pdf, url=url, title="Carta de gestão"))
    assert sync_library(db, store, "letters", source=described)["items"] == 1
    # A descriptive title keeps the signature it had before titles were derived, so
    # letters that are already correct are not synced again.
    stable = {"title": "Carta de gestão", "published": "", "url": url}
    stable["sha256"] = hashlib.sha256(pdf).hexdigest()
    assert db.rows("SELECT signature FROM library_entries")[0]["signature"] == digest(stable)

    # The same letter, as an older manifest recorded it with the page's button text.
    chrome = LibraryFixture(letter_manifest(pdf, url=url, title="Mais informações"))
    assert sync_library(db, store, "letters", source=chrome)["items"] == 1
    assert [row["title"] for row in db.rows("SELECT title FROM items")] == ["Carta Mensal Julho"]
    headings = db.rows(
        """SELECT h.heading @@ to_tsquery('portuguese_unaccent','julho') AS julho,
        h.heading @@ to_tsquery('portuguese_unaccent','informacoes') AS chrome
        FROM item_headings h"""
    )
    assert headings == [{"julho": True, "chrome": False}]
    metadata = db.rows("SELECT text FROM passages WHERE method='metadata' AND active")
    assert len(metadata) == 1
    assert metadata[0]["text"].startswith("Carta Mensal Julho ")
    assert "informações" not in metadata[0]["text"]
    assert len(db.rows("SELECT id FROM jobs WHERE kind='process_asset'")) == 1
    assert sync_library(db, store, "letters", source=chrome)["unchanged"] == 1


def test_letter_synced_with_a_generic_title_is_retitled_by_the_next_sync(research):
    db, store = research
    pdf = b"%PDF-1.7\nsynthetic letter stored before titles were derived\n"
    url = "https://example.org/files/CartaMensal_202301.pdf"
    source = LibraryFixture(letter_manifest(pdf, url=url, title="PDF"))
    assert sync_library(db, store, "letters", source=source)["items"] == 1
    # Put the catalog back in the state a sync that copied the link text left it in.
    stored = {"title": "PDF", "published": "", "url": url}
    stored["sha256"] = hashlib.sha256(pdf).hexdigest()
    with db.connect() as con:
        con.execute("UPDATE items SET title='PDF'")
        con.execute("UPDATE library_entries SET signature=%s", (digest(stored),))

    assert sync_library(db, store, "letters", source=source)["items"] == 1
    assert [row["title"] for row in db.rows("SELECT title FROM items")] == ["Carta Mensal 202301"]
    headings = db.rows(
        "SELECT heading @@ to_tsquery('portuguese_unaccent','202301') AS period FROM item_headings"
    )
    assert headings == [{"period": True}]
    assert sync_library(db, store, "letters", source=source)["unchanged"] == 1


def _letter_titles_and_dates(db):
    return [
        (row["title"], row["date_start"], row["date_precision"])
        for row in db.rows("SELECT title, date_start, date_precision FROM items")
    ]


def test_bare_month_letter_takes_its_year_and_date_from_the_file_name(research):
    db, store = research
    pdf = b"%PDF-1.7\nsynthetic letter named by month only\n"
    url = "https://example.org/files/Fundo_X-Carta_do_Gestor-Junho_2023.pdf"
    source = LibraryFixture(letter_manifest(pdf, url=url, title="Jun"))
    assert sync_library(db, store, "letters", source=source)["items"] == 1
    assert _letter_titles_and_dates(db) == [("Junho 2023", date(2023, 6, 1), "month")]
    assert sync_library(db, store, "letters", source=source)["unchanged"] == 1


def test_dated_title_synced_before_periods_were_derived_gets_its_date(research):
    db, store = research
    pdf = b"%PDF-1.7\nsynthetic letter titled with month and year\n"
    url = "https://example.org/_files/ugd/0a1b2c_3d4e5f60718293a4b5c6d7e8f9012345.pdf"
    source = LibraryFixture(letter_manifest(pdf, url=url, title="Junho 2023"))
    assert sync_library(db, store, "letters", source=source)["items"] == 1
    # Put the catalog back in the state a sync that did not derive periods left it in:
    # same title, no date, and a signature without the period.
    stored = {"title": "Junho 2023", "published": "", "url": url}
    stored["sha256"] = hashlib.sha256(pdf).hexdigest()
    with db.connect() as con:
        con.execute("UPDATE items SET date_start=NULL, date_end=NULL, date_precision='unknown'")
        con.execute("UPDATE library_entries SET signature=%s", (digest(stored),))

    assert sync_library(db, store, "letters", source=source)["items"] == 1
    assert _letter_titles_and_dates(db) == [("Junho 2023", date(2023, 6, 1), "month")]
    assert sync_library(db, store, "letters", source=source)["unchanged"] == 1


def test_bare_month_letter_takes_its_year_from_the_listing_page(research):
    db, store = research
    pdf = b"%PDF-1.7\nsynthetic letter listed under a year heading\n"
    url = "https://example.org/files/Carta%20Mensal%20Agosto.pdf"
    source = LibraryFixture(
        letter_manifest(pdf, url=url, title="Carta Mensal Agosto", listing_year=2025)
    )
    assert sync_library(db, store, "letters", source=source)["items"] == 1
    assert _letter_titles_and_dates(db) == [("Carta Mensal Agosto 2025", date(2025, 8, 1), "month")]


def test_bare_month_letter_takes_its_year_from_its_first_page_once_processed(research):
    db, store = research
    pdf = b"%PDF-1.7\nsynthetic letter with an opaque file name\n"
    url = "https://example.org/_files/ugd/0a1b2c_3d4e5f60718293a4b5c6d7e8f9012345.pdf"
    source = LibraryFixture(letter_manifest(pdf, url=url, title="Junho"))
    assert sync_library(db, store, "letters", source=source)["items"] == 1
    # Before the original is processed no source states the year, so none is added.
    assert _letter_titles_and_dates(db) == [("Junho", None, "unknown")]
    assert sync_library(db, store, "letters", source=source)["unchanged"] == 1

    asset = db.rows("SELECT id, item_id FROM assets")[0]
    with db.connect() as con:
        index_text(
            con,
            item_id=asset["item_id"],
            asset_id=asset["id"],
            revision="synthetic-page",
            text="Gestora Exemplo Carta Mensal Junho/2023 Em maio os juros caíram",
            method="text",
            page=1,
        )
    assert sync_library(db, store, "letters", source=source)["items"] == 1
    assert _letter_titles_and_dates(db) == [("Junho 2023", date(2023, 6, 1), "month")]
    # Retitling rewrites the catalog entry only; the original is not processed again.
    assert len(db.rows("SELECT id FROM jobs WHERE kind='process_asset'")) == 1
    assert sync_library(db, store, "letters", source=source)["unchanged"] == 1


def fund_letters_manifest(letters, manager="test-manager"):
    """A manager's manifest of several letters and their originals, from (pdf, url, title)."""
    blobs = {}
    records = []
    for pdf, url, title in letters:
        sha = hashlib.sha256(pdf).hexdigest()
        record = letter_manifest(pdf, url=url, title=title)
        records.append(record["letters/manifest/test-manager.jsonl"].decode())
        blobs[f"letters/files/{manager}/{sha}.pdf"] = pdf
    blobs[f"letters/manifest/{manager}.jsonl"] = "\n".join(records).encode()
    return blobs


def _letter_titles(db):
    return sorted(row["title"] for row in db.rows("SELECT title FROM items"))


def test_letters_of_two_funds_for_the_same_month_get_the_fund_in_their_titles(research):
    db, store = research
    alfa = (
        b"%PDF-1.7\nsynthetic letter for the first fund\n",
        "https://example.org/files/Gestora_Alfa_Macro_FIM-Carta-Dezembro_2024.pdf",
        "Dez",
    )
    beta = (
        b"%PDF-1.7\nsynthetic letter for the second fund\n",
        "https://example.org/files/Gestora_Beta_Acoes_FIA-Carta-Dezembro_2024.pdf",
        "Dez",
    )
    # Another manager's letter, named after its only fund, must keep its title.
    single_fund = fund_letters_manifest(
        [
            (
                b"%PDF-1.7\nsynthetic letter of a single-fund manager\n",
                "https://example.org/files/Gama_Renda_FIA-Carta-Dezembro_2024.pdf",
                "Dez",
            )
        ],
        manager="other-manager",
    )
    first_fund_only = LibraryFixture({**fund_letters_manifest([alfa]), **single_fund})
    result = sync_library(db, store, "letters", source=first_fund_only)
    assert (result["items"], result["unchanged"]) == (2, 0)
    # One fund is no reason to change a title.
    assert _letter_titles(db) == ["Dezembro 2024", "Dezembro 2024"]

    both_funds = LibraryFixture({**fund_letters_manifest([alfa, beta]), **single_fund})
    # The letter already synced is retitled once its collection covers a second fund.
    result = sync_library(db, store, "letters", source=both_funds)
    assert (result["items"], result["unchanged"]) == (2, 1)
    assert _letter_titles(db) == [
        "Alfa Macro FIM - Dezembro 2024",
        "Beta Acoes FIA - Dezembro 2024",
        "Dezembro 2024",
    ]
    result = sync_library(db, store, "letters", source=both_funds)
    assert (result["items"], result["unchanged"]) == (0, 3)


class FirstPageReads:
    """A database stand-in that records which letters' first pages are read."""

    settings = SimpleNamespace(bridge_metadata_bytes=1_000_000, bridge_object_limit=1000)

    def __init__(self):
        self.read_items = []

    def rows(self, query, params=()):
        self.read_items.extend(params[-1])
        return []


def test_first_page_is_read_only_for_letters_no_other_source_names_a_fund_for():
    named = (
        b"%PDF-1.7\nsynthetic letter named by its file name\n",
        "https://example.org/files/Alfa_Macro_FIM-Carta-Dezembro_2024.pdf",
        "Dezembro 2024",
    )
    unnamed = (
        b"%PDF-1.7\nsynthetic letter with an opaque file name\n",
        "https://example.org/_files/ugd/0a1b2c_opaque.pdf",
        "Dezembro 2024",
    )
    # Neither the title nor the file name dates this one, so its first page could give
    # both its year and its fund; it is still looked up only once.
    undated = (
        b"%PDF-1.7\nsynthetic letter titled by month only\n",
        "https://example.org/_files/ugd/0a1b2c_undated.pdf",
        "Junho",
    )
    db = FirstPageReads()
    source = LibraryFixture(fund_letters_manifest([named, unnamed, undated]))
    _letter_candidates(db, source, None, latest_year=2026)
    expected = [
        item_identity("manager_letter", digest(["test-manager", letter[1]]))
        for letter in (undated, unnamed)
    ]
    assert sorted(db.read_items) == sorted(expected)


def test_letters_sharing_a_title_take_the_fund_from_their_first_page(research):
    db, store = research
    opaque = "https://example.org/_files/ugd/0a1b2c_{}.pdf"
    letters = [
        (b"%PDF-1.7\nsynthetic first letter\n", opaque.format("first"), "Junho 2024"),
        (b"%PDF-1.7\nsynthetic second letter\n", opaque.format("second"), "Junho 2024"),
    ]
    source = LibraryFixture(fund_letters_manifest(letters))
    assert sync_library(db, store, "letters", source=source)["items"] == len(letters)
    # Until the originals are processed nothing names the fund, so the titles stay.
    assert _letter_titles(db) == ["Junho 2024", "Junho 2024"]

    headers = {
        opaque.format("first"): "CARTA MENSAL ALFA MACRO FIA JUNHO 2024 Em maio",
        opaque.format("second"): "Carta Mensal Beta Ações FIA Junho 2024 Em maio",
    }
    assets = db.rows(
        """SELECT a.id, a.item_id, i.metadata->>'url' AS url
        FROM assets a JOIN items i ON i.id=a.item_id"""
    )
    for asset in assets:
        with db.connect() as con:
            index_text(
                con,
                item_id=asset["item_id"],
                asset_id=asset["id"],
                revision="synthetic-page",
                text=headers[asset["url"]],
                method="text",
                page=1,
            )
    assert sync_library(db, store, "letters", source=source)["items"] == len(letters)
    assert _letter_titles(db) == ["Alfa Macro FIA - Junho 2024", "Beta Ações FIA - Junho 2024"]
    # Retitling rewrites the catalog entries only; the originals are not processed again.
    assert len(db.rows("SELECT id FROM jobs WHERE kind='process_asset'")) == len(letters)
    assert sync_library(db, store, "letters", source=source)["unchanged"] == len(letters)


def test_a_fund_name_cut_by_the_header_limit_is_not_used(research):
    db, store = research
    named = (
        b"%PDF-1.7\nsynthetic letter named by its file name\n",
        "https://example.org/files/Alfa_Macro_FIA-Carta-Junho_2024.pdf",
        "Junho 2024",
    )
    opaque = (
        b"%PDF-1.7\nsynthetic letter named only past its header\n",
        "https://example.org/_files/ugd/0a1b2c_cut.pdf",
        "Junho 2024",
    )
    letters = [named, opaque]
    source = LibraryFixture(fund_letters_manifest(letters))
    assert sync_library(db, store, "letters", source=source)["items"] == len(letters)

    # The header limit falls inside the fund's name, after "FII Beta Renda Log".
    padding = "x" * (HEADER_CHARACTERS - len(" | FII Beta Renda Log"))
    page = padding + " | FII Beta Renda Logística Junho 2024"
    asset = db.rows(
        """SELECT a.id, a.item_id FROM assets a JOIN items i ON i.id=a.item_id
        WHERE i.metadata->>'url'=%s""",
        (opaque[1],),
    )[0]
    with db.connect() as con:
        index_text(
            con,
            item_id=asset["item_id"],
            asset_id=asset["id"],
            revision="synthetic-page",
            text=page,
            method="text",
            page=1,
        )
    # Only one fund is named reliably, so neither title changes.
    assert sync_library(db, store, "letters", source=source)["unchanged"] == len(letters)
    assert _letter_titles(db) == ["Junho 2024", "Junho 2024"]


def test_macro_snapshot_validates_actual_coverage_and_export(research):
    db, store = research
    source = snapshot()
    assert sync_macro(db, store, source=source)["snapshot_id"] == "s1"
    cat = catalog(db)
    assert cat["series"][0]["rows"] == 2
    assert cat["series"][0]["source_declared_rows"] == 99999
    result = series_query(db, store, "ipca", limit=1)
    assert result["rows"][0]["value"] == "0.56"
    assert result["matching_rows"] == 2 and result["has_more"]
    content, exported = export_series(db, store, "ipca", snapshot_id="s1")
    assert "observation_date" in content and "s1" in content
    assert exported["returned_rows"] == 2
    assert sync_macro(db, store, source=source)["snapshot_id"] == "s1"


def test_old_macro_snapshot_remains_reproducible_after_new_pointer(research):
    db, store = research
    sync_macro(db, store, source=snapshot())
    sync_macro(
        db,
        store,
        source=snapshot(
            snapshot_id="s2",
            values=[{"series_id": "ipca", "date": date(2026, 1, 1), "value": 99.0}],
        ),
    )
    assert series_query(db, store, "ipca")["rows"][0]["value"] == "99.0"
    assert series_query(db, store, "ipca", snapshot_id="s1")["rows"][0]["value"] == "0.56"


@pytest.mark.parametrize("failure", ["checksum", "private", "duplicate", "path", "immutable"])
def test_bad_macro_snapshots_never_replace_serving_pointer(research, failure):
    db, store = research
    sync_macro(db, store, source=snapshot())
    source = snapshot(snapshot_id="s2", private=failure == "private")
    if failure == "checksum":
        source.blobs["snapshots/s2/observations.parquet"] = b"corrupt"
    if failure == "duplicate":
        source = snapshot(
            snapshot_id="s2",
            values=[{"series_id": "ipca", "date": date(2026, 1, 1), "value": 1.0}] * 2,
        )
    if failure == "path":
        source.blobs["latest.json"] = json.dumps(
            {"manifest": "snapshots/../../private/manifest.json"}
        ).encode()
    if failure == "immutable":
        source = snapshot(values=[{"series_id": "ipca", "date": date(2026, 1, 1), "value": 42.0}])
    with pytest.raises(ValueError):
        sync_macro(db, store, source=source)
    assert catalog(db)["snapshot_id"] == "s1"


def test_workbench_api_keeps_auth_and_export_version(research):
    from fastapi.testclient import TestClient

    from ofl.research.api import create_app

    db, store = research
    sync_macro(db, store, source=snapshot())
    api = TestClient(create_app(db.settings))
    headers = {"X-OFL-Gateway-Token": "test-gateway", "X-OFL-Identity": "reader"}
    assert api.get("/api/v1/data/catalog").status_code == 401
    assert api.get("/api/v1/workbench", headers=headers).json()["macro"]["series_with_data"] == 1
    assert api.get("/api/v1/data/series/ipca/export", headers=headers).status_code == 422
    result = api.get("/api/v1/data/series/ipca/export?snapshot_id=s1", headers=headers)
    assert result.status_code == 200 and result.headers["x-ofl-snapshot"] == "s1"
    assert (
        api.post("/api/v1/library/sync", headers=headers, json={"kind": "news"}).status_code == 403
    )


def test_policy_downgrade_reindexes_and_closes_old_body_access(research):
    from fastapi.testclient import TestClient

    from ofl.research.api import create_app

    db, store = research
    key = "news/items/official-source/2026-01-06/120000.jsonl"
    source = LibraryFixture({key: json.dumps(news(body="PREVIOUSFULLBODYUNIQUE")).encode()})
    sync_library(db, store, "news", source=source)
    hit = search(db, "PREVIOUSFULLBODYUNIQUE", mode="keyword")["results"][0]
    source.news_tiers = {"official-source": "headline_only"}
    assert sync_library(db, store, "news", source=source)["items"] == 1
    assert not search(db, "PREVIOUSFULLBODYUNIQUE", mode="keyword")["results"]
    api = TestClient(create_app(db.settings))
    headers = {"X-OFL-Gateway-Token": "test-gateway", "X-OFL-Identity": "reader"}
    assert api.get(f"/api/v1/passages/{hit['id']}", headers=headers).status_code == 404
    assert api.get(f"/api/v1/assets/{hit['asset_id']}", headers=headers).status_code == 404


def test_macro_manifest_hash_matches_csv_bytes(research):
    from fastapi.testclient import TestClient

    from ofl.research.api import create_app

    db, store = research
    sync_macro(db, store, source=snapshot())
    api = TestClient(create_app(db.settings))
    headers = {"X-OFL-Gateway-Token": "test-gateway", "X-OFL-Identity": "reader"}
    base = "/api/v1/data/series/ipca/"
    csv = api.get(base + "export?snapshot_id=s1&date_to=2026-01-31", headers=headers)
    manifest = api.get(base + "manifest?snapshot_id=s1&date_to=2026-01-31", headers=headers).json()
    assert manifest["csv_sha256"] == hashlib.sha256(csv.content).hexdigest()
    assert manifest["matching_rows"] == 1 and manifest["scope"]["date_to"] == "2026-01-31"


def test_passage_context_preserves_publisher_and_unknown_publication(research):
    from fastapi.testclient import TestClient

    from ofl.research.api import create_app

    db, store = research
    key = "news/items/official-source/2026-01-06/120000.jsonl"
    source = LibraryFixture({key: json.dumps(news(published=None)).encode()})
    sync_library(db, store, "news", source=source)
    hit = search(db, "Indicadores documentados", mode="keyword")["results"][0]
    api = TestClient(create_app(db.settings))
    headers = {"X-OFL-Gateway-Token": "test-gateway", "X-OFL-Identity": "reader"}
    assert api.get(f"/api/v1/passages/{hit['id']}").status_code == 401
    result = api.get(f"/api/v1/passages/{hit['id']}", headers=headers)
    assert result.status_code == 200
    passage = result.json()["passage"]
    assert passage["collection"] == "official-source"
    assert passage["date_start"] is None and passage["date_precision"] == "unknown"
    assert passage["active"] is True
    assert "Indicadores documentados" in passage["text"]
