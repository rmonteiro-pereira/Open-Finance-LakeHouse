# syntax=docker/dockerfile:1.7
# OFL slim image — Polars extract (ingest) + DuckDB serve (gold). No Java/Spark.
#
# Cluster pods have no external egress, so DuckDB's `httpfs`/`delta` extensions
# are PRE-BAKED into /opt/duckdb-ext at build time. At runtime the gold runner
# reads DUCKDB_EXTENSION_DIRECTORY and LOADs them offline — no network needed.
# See ofl/transform/gold/runner.configure_minio.
ARG PYTHON_VERSION=3.11

# ----------------------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim AS build
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app

# Project metadata + source (hatchling builds the `ofl` wheel from these).
COPY pyproject.toml README.md ./
COPY ofl ./ofl
COPY sources ./sources

# Isolated venv with core + the pure-Python extras (Yahoo extract, lineage).
RUN uv venv /opt/venv \
 && VIRTUAL_ENV=/opt/venv uv pip install --no-cache ".[yahoo,lineage]"

# Pre-bake the DuckDB extensions used by the gold lane into an offline directory.
RUN /opt/venv/bin/python - <<'PY'
import os
import duckdb

ext_dir = "/opt/duckdb-ext"
os.makedirs(ext_dir, exist_ok=True)
con = duckdb.connect()
con.execute(f"SET extension_directory='{ext_dir}'")
con.execute("INSTALL httpfs; INSTALL delta;")
print("duckdb", duckdb.__version__, "extensions baked:", os.listdir(ext_dir))
PY

# ----------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    OFL_REGISTRY=/app/sources/registry.yml \
    DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb-ext
WORKDIR /app

COPY --from=build /opt/venv /opt/venv
COPY --from=build /opt/duckdb-ext /opt/duckdb-ext
COPY --from=build /app/sources /app/sources

# Run unprivileged.
RUN useradd -u 10001 -m ofl && chown -R ofl:ofl /app /opt/duckdb-ext
USER ofl

# `ofl` console script: ingest | silver | gold | registry.
ENTRYPOINT ["ofl"]
CMD ["registry"]
