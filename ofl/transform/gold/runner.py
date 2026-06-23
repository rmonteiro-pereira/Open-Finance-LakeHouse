"""DuckDB gold runner: read silver Delta from MinIO, run SQL marts, write gold Delta.

DuckDB's ``delta`` extension reads Delta; writes go back through delta-rs
(``deltalake.write_deltalake``) since the DuckDB Delta writer is read-only.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from ofl.config import get_settings
from ofl.platform.io import delta_storage_options, gold_uri, silver_uri
from ofl.platform.logging import get_logger

if TYPE_CHECKING:
    import duckdb

log = get_logger(__name__)

MODELS_DIR = Path(__file__).parent / "models"

# Marts are independent (no inter-mart refs). Most read `fact_observation`;
# `mart_yield_curve` reads `fact_treasury`.
MODELS = [
    "mart_real_interest",
    "mart_inflation_panel",
    "mart_fx",
    "mart_macro_dashboard",
    "mart_yield_curve",
    "mart_equity_daily",
]

# Silver tables each mart depends on — used to skip marts whose inputs aren't ready.
# `mart_equity_daily` reads `fact_security_price`.
_SILVER_TABLES = ["fact_observation", "dim_series", "fact_treasury", "fact_security_price"]


def _model_sql(name: str) -> str:
    return (MODELS_DIR / f"{name}.sql").read_text(encoding="utf-8")


def configure_minio(con: "duckdb.DuckDBPyConnection") -> None:
    s = get_settings()
    # In the cluster image the `httpfs`/`delta` extensions are pre-baked into an
    # offline directory (pods have no egress). Point DuckDB at it so INSTALL is a
    # local no-op and LOAD never touches the network. Falls back to online install
    # when the env var is unset (local dev).
    import os

    ext_dir = os.environ.get("DUCKDB_EXTENSION_DIRECTORY")
    if ext_dir:
        con.execute(f"SET extension_directory='{ext_dir}'")
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL delta; LOAD delta;")
    endpoint = s.minio_endpoint.split("://", 1)[-1]
    use_ssl = "true" if s.minio_endpoint.startswith("https") else "false"
    # The `delta` extension (delta_scan) is backed by delta-kernel-rs / object_store
    # and reads credentials from DuckDB's *secret manager* — it ignores the legacy
    # `SET s3_*` session variables. Without an explicit S3 secret it falls through to
    # the AWS default credential chain and hangs on the EC2 instance-metadata endpoint
    # (169.254.169.254) inside the cluster. Register the secret so delta_scan binds to
    # MinIO with static creds. (`SET s3_*` is kept for plain httpfs read_parquet paths.)
    con.execute(
        f"""
        CREATE OR REPLACE SECRET minio (
            TYPE S3,
            KEY_ID '{s.minio_user}',
            SECRET '{s.minio_password}',
            ENDPOINT '{endpoint}',
            URL_STYLE 'path',
            USE_SSL {use_ssl},
            REGION '{s.aws_region}'
        )
        """
    )
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_endpoint='{endpoint}'")
    con.execute(f"SET s3_access_key_id='{s.minio_user}'")
    con.execute(f"SET s3_secret_access_key='{s.minio_password}'")
    con.execute(f"SET s3_use_ssl={use_ssl}")


def register_silver(con: "duckdb.DuckDBPyConnection") -> None:
    """Expose silver Delta tables as DuckDB views the SQL models reference by name.

    Tables that don't exist yet (e.g. ``fact_treasury`` before any Tesouro load)
    are skipped — marts that need them are skipped downstream, not failed.
    """
    for table in _SILVER_TABLES:
        try:
            con.execute(
                f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM delta_scan('{silver_uri(table)}')"
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("silver_table_unavailable", table=table, error=str(exc))


def execute_models(
    con: "duckdb.DuckDBPyConnection",
    *,
    write: bool = True,
    models: list[str] | None = None,
    skip_on_error: bool = False,
) -> dict[str, int]:
    """Run each mart against an already-prepared connection.

    With ``skip_on_error`` (used by the orchestrated ``run_gold``), a mart whose
    upstream isn't materialized is logged and skipped instead of failing the run.
    """
    results: dict[str, int] = {}
    for name in models or MODELS:
        try:
            arrow = con.execute(_model_sql(name)).to_arrow_table()
        except Exception as exc:  # noqa: BLE001
            if not skip_on_error:
                raise
            log.warning("gold_model_skipped", model=name, error=str(exc))
            continue
        results[name] = arrow.num_rows
        if write:
            from deltalake import write_deltalake

            write_deltalake(
                gold_uri(name), arrow, mode="overwrite", storage_options=delta_storage_options()
            )
        log.info("gold_model", model=name, rows=arrow.num_rows, written=write)
    return results


def run_gold(write: bool = True) -> dict[str, int]:
    import duckdb

    con = duckdb.connect()
    configure_minio(con)
    register_silver(con)
    return execute_models(con, write=write, skip_on_error=True)
