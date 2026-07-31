"""M5 — the near-real-time gold mart: silver OHLC Delta → a DuckDB database file.

The batch lane's gold layer is SQL files in ``models/`` plus a thin runner
(:mod:`ofl.transform.gold.runner`). This is the same shape, on the streaming
lane's clock — deliberately, so the two are legible side by side rather than
being two different ideas about what "gold" means.

Three things differ, and each is a decision rather than an accident:

* **The source is read with delta-rs, not with Spark.** Serving must not depend
  on a JVM being up. The mart is refreshed *after* the streaming job has exited,
  by a process whose only inputs are the committed Delta log and DuckDB.
* **The output is a DuckDB file, not a Delta table.** The batch marts write Delta
  because several engines read them. This one exists to be *served*: a dashboard
  opens a single file read-only, with no cluster, no credentials and no catalog.
  That is what makes it near-real-time in any useful sense — the refresh is
  seconds, and so is the read.
* **The whole mart is rebuilt each pass, not appended to.** The bars themselves
  are append-only upstream (that is silver's job, and its idempotence is measured
  in ``tools/streaming_idempotence.py``); here the derived columns — rolling
  windows, returns against the previous bar, freshness against the newest event —
  are all *relative*, so the last few rows change every time a new bar lands.
  Recomputing a table this size is cheaper and far safer than working out which
  derived rows a new bar invalidated.

Usage::

    ofl stream-mart
    duckdb data/streaming/gold/ofl_streaming_nrt.duckdb -c "SELECT * FROM mart_trade_latest_nrt"
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from ofl.platform.logging import get_logger
from ofl.streaming.metrics import read_silver, silver_table_exists
from ofl.streaming.paths import nrt_mart_path

if TYPE_CHECKING:
    import duckdb

log = get_logger(__name__)

MODELS_DIR = Path(__file__).parent / "models"

#: The name the SQL models read the silver bars under.
SOURCE_VIEW = "silver_ohlc_1m"

#: Built in order; both are independent of each other and read only the source view.
MODELS = ["mart_trade_ohlc_1m_nrt", "mart_trade_latest_nrt"]


def model_sql(name: str) -> str:
    return (MODELS_DIR / f"{name}.sql").read_text(encoding="utf-8")


def execute_models(
    con: "duckdb.DuckDBPyConnection",
    *,
    built_at: datetime,
    models: list[str] | None = None,
) -> dict[str, int]:
    """Materialise each model as a table in an already-prepared connection.

    ``built_at`` is passed as a bound parameter rather than read from the clock
    inside the SQL, so that a test can pin it and two models built in the same
    pass agree on when the pass happened.
    """
    results: dict[str, int] = {}
    for name in models or MODELS:
        con.execute(
            f"CREATE OR REPLACE TABLE {name} AS {model_sql(name).rstrip().rstrip(';')}",
            {"built_at": built_at},
        )
        rows = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        results[name] = rows
        log.info("nrt_model", model=name, rows=rows)
    return results


def build_nrt_mart(path: Path | None = None, *, source: Path | None = None) -> dict[str, int]:
    """Refresh the DuckDB mart from the silver Delta table. Returns rows per model.

    Raises:
        FileNotFoundError: if silver has never been written. A mart over a table
            that does not exist would otherwise be an empty file that looks fine.
    """
    import duckdb

    if not silver_table_exists(source):
        raise FileNotFoundError(
            "silver/fact_trade_ohlc_1m does not exist yet — run `ofl stream-silver` first"
        )

    bars, version = read_silver(source)
    target = Path(path or nrt_mart_path())
    target.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(target))
    try:
        # Arrow, not the Delta scan extension: the source is already in memory and
        # this keeps the mart build free of DuckDB extension downloads, which a CI
        # runner with no egress budget would otherwise pay for on every pass.
        con.register(SOURCE_VIEW, bars.to_arrow())
        built_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
        results = execute_models(con, built_at=built_at)
        # Lineage, in the mart itself: which Delta version these numbers came from.
        con.execute("CREATE OR REPLACE TABLE nrt_build (built_at TIMESTAMP, silver_version BIGINT, silver_rows BIGINT)")
        con.execute("INSERT INTO nrt_build VALUES (?, ?, ?)", [built_at, version, bars.height])
    finally:
        con.close()

    log.info(
        "nrt_mart_built",
        path=str(target),
        silver_version=version,
        silver_rows=bars.height,
        **results,
    )
    return results
