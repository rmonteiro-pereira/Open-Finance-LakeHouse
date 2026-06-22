"""Airflow 3 DAGs for the Open-Finance LakeHouse — generated from the registry.

Topology (data-aware, no single mega-DAG):

    ofl_ingest_<domain>  (x6, scheduled)  --emit-->  Asset(bronze/<domain>)
                                                          |
    ofl_silver  (schedule = all bronze assets)  --emit-->  Asset(silver/fact_observation)
                                                          |
    ofl_gold    (schedule = silver asset)       --emit-->  Asset(gold/marts)

Plus `ofl_backfill`: a manual full-rebuild button (ingest-all -> silver -> gold).

Each task is a `KubernetesPodOperator` running the `ofl` CLI in the project image.
The registry is read at parse time, so adding a series adds a task automatically.
"""

from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset
from kubernetes.client import models as k8s

from ofl.registry import load_registry

# --- cluster wiring (override via Airflow env) --------------------------------
# Two lane-specific images (see docker/): the slim image runs Polars ingest +
# DuckDB gold (no JVM); the spark image runs the silver conform/MERGE.
_REPO = os.getenv("OFL_IMAGE_REPO", "ghcr.io/rmonteiro-pereira/open-finance-lakehouse")
SLIM_IMAGE = os.getenv("OFL_SLIM_IMAGE", f"{_REPO}:slim")
SPARK_IMAGE = os.getenv("OFL_SPARK_IMAGE", f"{_REPO}:spark")
NAMESPACE = os.getenv("OFL_NAMESPACE", "default")
MINIO_SECRET = os.getenv("OFL_MINIO_SECRET", "minio-creds")
PULL_SECRET = os.getenv("OFL_PULL_SECRET", "ghcr-pull")

_ENV_FROM = [k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=MINIO_SECRET))]
_PULL = [k8s.V1LocalObjectReference(name=PULL_SECRET)]

# Per-task pod sizing — small data, single node.
_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "250m", "memory": "1Gi"},
    limits={"cpu": "1500m", "memory": "3Gi"},
)

_DEFAULTS = {"retries": 2, "retry_delay": pendulum.duration(minutes=2)}


def asset_bronze(domain: str) -> Asset:
    return Asset(f"lakehouse://bronze/{domain}")


ASSET_SILVER = Asset("lakehouse://silver/fact_observation")
ASSET_GOLD = Asset("lakehouse://gold/marts")


def _pod(task_id: str, args: list[str], image: str = SLIM_IMAGE, **kwargs) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=NAMESPACE,
        image=image,
        cmds=["ofl"],
        arguments=args,
        env_from=_ENV_FROM,
        image_pull_secrets=_PULL,
        container_resources=_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
        **kwargs,
    )


registry = load_registry()

# --- one ingestion DAG per domain --------------------------------------------
for domain in registry.domains():
    series = [s for s in registry.by_domain(domain) if s.is_active]
    if not series:
        continue

    dag_id = f"ofl_ingest_{domain}"
    with DAG(
        dag_id=dag_id,
        schedule="@daily",
        start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
        catchup=False,
        default_args=_DEFAULTS,
        max_active_tasks=3,  # fit the single node
        tags=["ofl", "ingestion", domain],
    ) as dag:
        ingest_tasks = [_pod(f"ingest_{s.key}", ["ingest", "--series", s.key]) for s in series]
        done = EmptyOperator(task_id="domain_ready", outlets=[asset_bronze(domain)])
        ingest_tasks >> done

    globals()[dag_id] = dag

# --- silver: triggered when every bronze domain asset has refreshed -----------
with DAG(
    dag_id="ofl_silver",
    schedule=[asset_bronze(d) for d in registry.domains()],
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    default_args=_DEFAULTS,
    tags=["ofl", "silver"],
) as silver_dag:
    _pod("conform_silver", ["silver"], image=SPARK_IMAGE, outlets=[ASSET_SILVER])

# --- gold: triggered when silver refreshes ------------------------------------
with DAG(
    dag_id="ofl_gold",
    schedule=[ASSET_SILVER],
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    default_args=_DEFAULTS,
    tags=["ofl", "gold"],
) as gold_dag:
    _pod("build_gold_marts", ["gold"], outlets=[ASSET_GOLD])

# --- manual full-rebuild button (idempotent) ----------------------------------
with DAG(
    dag_id="ofl_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    default_args=_DEFAULTS,
    tags=["ofl", "backfill", "manual"],
) as backfill_dag:
    ingest_all = _pod("ingest_all", ["ingest"])
    silver = _pod("silver", ["silver"], image=SPARK_IMAGE)
    gold = _pod("gold", ["gold"])
    ingest_all >> silver >> gold
