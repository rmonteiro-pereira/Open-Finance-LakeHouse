"""Airflow 3 DAGs for the Open-Finance LakeHouse — generated from the registry.

Topology (data-aware, isolated per series, shared layers). The shape is derived
from ``sources/registry.yml`` at parse time, so the counts below are whatever the
registry currently yields — today 10 handlers / 51 active series:

    ofl_ingest_<source>  (x10, one per HANDLER with >=1 active series)
        ├─ one static task per series  --emit--> Asset(bronze/<series>)   (51 assets)
        └─ ingest_done (runs when every series task has ended, pass or fail)
                                       --emit--> Asset(ingest/<source>)   (10 assets)
                                                      |
    ofl_silver  (schedule = ALL ingest/<source> assets)
                                       --emit--> Asset(silver/fact_observation)
                                                      |
    ofl_gold    (schedule = silver asset)      --emit--> Asset(gold/marts)

Plus `ofl_backfill`: a manual full-rebuild button (ingest-all -> silver -> gold).

Granularity (committee decision, see ofl-pipeline-observability memory):
  * DAGs are per-SOURCE/handler — the unit that actually fails together (API
    outage, expired token, rate-limit), not per-domain (which masks the series).
  * Each series is its OWN static task with its OWN bronze Asset and its OWN
    on_failure_callback, so a failing series never blocks/masks its siblings and
    raises one individually-attributable alert. (Static tasks, not dynamic
    mapping: a KubernetesPodOperator can't emit per-map-index Asset events.)
  * silver/gold stay shared singletons, scheduled by Asset events with
    max_active_runs=1 + idempotent MERGE, so partial/bursty bronze is safe.
  * silver runs ONCE per ingest wave: it waits for every source DAG to finish
    (``ingest_done`` fires on ``all_done``, so a failing series still does not
    hold silver back). Until 2026-10 it ran on ANY bronze asset, which on a small
    node meant 3-4 full Spark MERGEs per night competing with the ingests for CPU.
    ``OFL_SILVER_TRIGGER=any`` restores that. After re-running a single source by
    hand, trigger ``ofl_silver`` by hand too.

Node memory is governed by Airflow POOLS, not granularity: all ingest pods share
``ofl_ingest`` (2 slots) and Spark runs alone in ``ofl_spark`` (1 slot). These
pools must exist in Airflow (created via Helm values / `airflow pools set`).
"""

from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from kubernetes.client import models as k8s

try:  # Airflow 3 conditional asset expressions
    from airflow.sdk import Asset, AssetAny
except ImportError:  # pragma: no cover - fallback for shims without AssetAny
    from airflow.sdk import Asset

    AssetAny = None

from ofl.registry import load_registry

from _alerts import ofl_failure_alert

# --- cluster wiring (override via Airflow env) --------------------------------
# Two lane-specific images (see docker/): the slim image runs Polars ingest +
# DuckDB gold (no JVM); the spark image runs the silver conform/MERGE.
_REPO = os.getenv("OFL_IMAGE_REPO", "ghcr.io/rmonteiro-pereira/open-finance-lakehouse")
SLIM_IMAGE = os.getenv("OFL_SLIM_IMAGE", f"{_REPO}:slim")
SPARK_IMAGE = os.getenv("OFL_SPARK_IMAGE", f"{_REPO}:spark")
NAMESPACE = os.getenv("OFL_NAMESPACE", "default")
MINIO_SECRET = os.getenv("OFL_MINIO_SECRET", "minio-creds")
PULL_SECRET = os.getenv("OFL_PULL_SECRET", "ghcr-pull")
ANBIMA_SECRET = os.getenv("OFL_ANBIMA_SECRET", "anbima-creds")

# Concurrency pools — the load-bearing memory guardrail on the single node.
INGEST_POOL = os.getenv("OFL_INGEST_POOL", "ofl_ingest")  # slots=2
SPARK_POOL = os.getenv("OFL_SPARK_POOL", "ofl_spark")  # slots=1, Spark never co-runs with ingest

# "cycle": silver runs once, after every source DAG has finished. "any": silver runs
# on any bronze refresh (the pre-2026-10 behaviour).
SILVER_TRIGGER = os.getenv("OFL_SILVER_TRIGGER", "cycle")

# Name of the Secret holding OFL_PUBLISH_* (the public snapshot's bucket). Unset means the
# deployment publishes nothing and `ofl_gold` has no publish task.
PUBLISH_SECRET = os.getenv("OFL_PUBLISH_SECRET", "")

# Pushgateway for per-series metrics — surfaced to pods so the ingest CLI and the
# failure callback push to the same gateway.
PUSHGATEWAY_URL = os.getenv("OFL_PUSHGATEWAY_URL", "")

_ENV_FROM = [
    k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=MINIO_SECRET)),
    # ANBIMA Feed creds (ANBIMA_CLIENT_ID/SECRET) — optional so pods (and clusters
    # without the secret) still start; only the anbima ingest task needs them.
    k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=ANBIMA_SECRET, optional=True)),
]
_PULL = [k8s.V1LocalObjectReference(name=PULL_SECRET)]
_POD_ENV = [k8s.V1EnvVar(name="OFL_PUSHGATEWAY_URL", value=PUSHGATEWAY_URL)] if PUSHGATEWAY_URL else []

# Per-task pod sizing. Request is tiny (256Mi) because the node sits ~95-99% on
# memory *requests* but only ~66% real RAM; the 3Gi limit lets Spark burst into
# the free RAM while staying schedulable. (Same fix applied to the retired Kedro DAG.)
# The limit was 3Gi until 2026-10: b3_cotahist peaks at ~3.9 GiB of anonymous memory
# (two full annual archives in one frame), so the kernel OOM-killed it on every try.
# 5Gi covers it; the other 50 series stay around 150 MiB, so this only moves the cap.
_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "250m", "memory": "256Mi"},
    limits={"cpu": "1500m", "memory": os.getenv("OFL_INGEST_MEMORY_LIMIT", "5Gi")},
)
# The silver lane runs a Spark JVM that MERGEs each fact's full bronze every run;
# the largest (fact_derivatives_quote) needs a ~4g driver heap (see
# OFL_SPARK_DRIVER_MEMORY), so its pod gets a bigger limit to host that heap +
# overhead. Request stays tiny (node is oversubscribed on requests, not real RAM).
_SPARK_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "250m", "memory": "256Mi"},
    limits={"cpu": "2000m", "memory": "6Gi"},
)

# No task may run forever: a series stuck on a dead endpoint once held its source DAG (and so
# silver) for 47 minutes with nothing to stop it. Ingest and gold get one limit, Spark another.
_TASK_TIMEOUT = pendulum.duration(minutes=int(os.getenv("OFL_TASK_TIMEOUT_MIN", "45")))
_SPARK_TIMEOUT = pendulum.duration(minutes=int(os.getenv("OFL_SPARK_TIMEOUT_MIN", "120")))
_DEFAULTS = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=2),
    "execution_timeout": _TASK_TIMEOUT,
}


def asset_bronze(series_key: str) -> Asset:
    """Per-series bronze asset — a failed series withholds only its own."""
    return Asset(f"lakehouse://bronze/{series_key}")


def asset_ingest_done(handler: str) -> Asset:
    """Per-source marker: every series task of that source has ended (pass or fail)."""
    return Asset(f"lakehouse://ingest/{handler}")


ASSET_SILVER = Asset("lakehouse://silver/fact_observation")
ASSET_GOLD = Asset("lakehouse://gold/marts")


def _pod(
    task_id: str,
    args: list[str],
    *,
    image: str = SLIM_IMAGE,
    pool: str = INGEST_POOL,
    resources: "k8s.V1ResourceRequirements" = _RESOURCES,
    env_from: list | None = None,
    **kwargs,
) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=NAMESPACE,
        image=image,
        cmds=["ofl"],
        arguments=args,
        env_from=_ENV_FROM + (env_from or []),
        env_vars=_POD_ENV,
        image_pull_secrets=_PULL,
        container_resources=resources,
        pool=pool,
        # Don't bind the kubernetes_default connection: on Airflow 3 the KPO masks
        # the connection extra over the task-SDK msgpack channel and crashes with
        # "SerializationIterator are not supported" before the pod is built.
        # in_cluster=True is used directly instead. (Hard-won fix; do not remove.)
        kubernetes_conn_id=None,
        in_cluster=True,
        on_failure_callback=ofl_failure_alert,
        get_logs=True,
        is_delete_operator_pod=True,
        # Provider default is 120 s; a busy node can take longer just to start the pod,
        # and each miss burns a retry.
        startup_timeout_seconds=int(os.getenv("OFL_POD_STARTUP_TIMEOUT", "600")),
        **kwargs,
    )


registry = load_registry()
_ALL_BRONZE = [asset_bronze(s.key) for s in registry.active()]
_ALL_INGEST_DONE = []

# --- one ingestion DAG per SOURCE/handler ------------------------------------
for handler in registry.handlers():
    series = [s for s in registry.by_handler(handler) if s.is_active]
    if not series:
        continue

    dag_id = f"ofl_ingest_{handler}"
    with DAG(
        dag_id=dag_id,
        schedule="@daily",
        start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
        catchup=False,
        default_args=_DEFAULTS,
        max_active_runs=1,
        tags=["ofl", "ingestion", handler],
    ) as dag:
        # One independent task per series — no inter-task deps, so a failure or DQ
        # rejection on one series leaves the others untouched and emits only its
        # own bronze asset. Concurrency is bounded globally by the shared pool.
        tasks = [
            _pod(f"ingest_{s.key}", ["ingest", "--series", s.key], outlets=[asset_bronze(s.key)])
            for s in series
        ]
        # Marks the source as finished for this wave whatever the series outcomes were.
        done_asset = asset_ingest_done(handler)
        _ALL_INGEST_DONE.append(done_asset)
        tasks >> EmptyOperator(task_id="ingest_done", trigger_rule="all_done", outlets=[done_asset])
        # ingest_done always succeeds, so without this second leaf a run with failed
        # series would be reported as a success. No outlets: the scheduler resolves it
        # without starting a worker process.
        tasks >> EmptyOperator(task_id="ingest_status")

    globals()[dag_id] = dag

# --- silver: once per ingest wave (all sources finished), or on ANY bronze refresh ---
if SILVER_TRIGGER == "any":
    _silver_schedule = AssetAny(*_ALL_BRONZE) if AssetAny is not None else _ALL_BRONZE
else:
    _silver_schedule = _ALL_INGEST_DONE  # a list means "all of them"
with DAG(
    dag_id="ofl_silver",
    schedule=_silver_schedule,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    default_args=_DEFAULTS,
    max_active_runs=1,
    tags=["ofl", "silver"],
) as silver_dag:
    _pod(
        "conform_silver",
        ["silver"],
        image=SPARK_IMAGE,
        pool=SPARK_POOL,
        resources=_SPARK_RESOURCES,
        outlets=[ASSET_SILVER],
        execution_timeout=_SPARK_TIMEOUT,
    )

# --- gold: triggered when silver refreshes ------------------------------------
with DAG(
    dag_id="ofl_gold",
    schedule=[ASSET_SILVER],
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    default_args=_DEFAULTS,
    max_active_runs=1,
    tags=["ofl", "gold"],
) as gold_dag:
    _gold = _pod("build_gold_marts", ["gold"], outlets=[ASSET_GOLD])
    if PUBLISH_SECRET:
        # Reads gold, writes only to the public bucket. Its failure does not withhold the
        # gold asset: the marts are built either way and the last snapshot stays published.
        _gold >> _pod(
            "publish_snapshot",
            ["publish"],
            env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=PUBLISH_SECRET))],
        )

# --- manual full-rebuild button (idempotent) ----------------------------------
with DAG(
    dag_id="ofl_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    default_args=_DEFAULTS,
    tags=["ofl", "backfill", "manual"],
) as backfill_dag:
    ingest_all = _pod("ingest_all", ["ingest"], execution_timeout=_SPARK_TIMEOUT)
    silver = _pod(
        "silver",
        ["silver"],
        image=SPARK_IMAGE,
        pool=SPARK_POOL,
        resources=_SPARK_RESOURCES,
        execution_timeout=_SPARK_TIMEOUT,
    )
    gold = _pod("gold", ["gold"])
    ingest_all >> silver >> gold
