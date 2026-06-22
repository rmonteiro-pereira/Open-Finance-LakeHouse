"""
Open-Finance-Lakehouse — cluster backfill DAG.

Runs each Kedro pipeline as its own pod (KubernetesPodOperator) using the
project image, writing Delta to s3a://lakehouse/. Designed for a one-shot
backfill (schedule=None, trigger manually); per-source schedules can be added
later. Concurrency is capped (max_active_tasks) to fit the single node.

Pods run in the `default` namespace, where the `ghcr-pull` and `minio-creds`
secrets already exist; the airflow-worker ServiceAccount is granted pod rights
there via RBAC (see Infra-lakehouse apps/airflow/kpo-rbac.yaml).
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

IMAGE = "ghcr.io/rmonteiro-pereira/open-finance-lakehouse:cluster"
POD_NAMESPACE = "default"

# 24 pipelines (selic included for a complete, idempotent backfill; the
# yahoo_finance umbrella is omitted in favour of the granular yahoo_* ones).
PIPELINES = [
    "selic", "cdi", "over", "selic_meta",
    "ipca", "ipca_15", "inpc",
    "igp_m", "igp_di", "igp_10",
    "tlp", "usd_brl", "eur_brl",
    "divida_pib", "focus_pib", "reservas_internacionais", "ipea_receita",
    "b3", "ibge", "anbima", "tesouro_direto",
    "yahoo_etf", "yahoo_currency", "yahoo_commodity",
]

_env = [
    k8s.V1EnvVar(name="MINIO_ENDPOINT", value="http://minio.minio.svc.cluster.local:9000"),
    k8s.V1EnvVar(
        name="MINIO_USER",
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name="minio-creds", key="MINIO_USER")),
    ),
    k8s.V1EnvVar(
        name="MINIO_PASSWORD",
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name="minio-creds", key="MINIO_PASSWORD")),
    ),
]

_resources = k8s.V1ResourceRequirements(
    requests={"cpu": "250m", "memory": "1Gi"},
    limits={"cpu": "1500m", "memory": "3Gi"},
)

with DAG(
    dag_id="ofl_backfill",
    description="Backfill all Open-Finance-Lakehouse pipelines into MinIO Delta",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_tasks=3,
    tags=["lakehouse", "backfill", "kedro"],
) as dag:
    for pipeline in PIPELINES:
        KubernetesPodOperator(
            task_id=f"run_{pipeline}",
            name=f"ofl-{pipeline}",
            namespace=POD_NAMESPACE,
            image=IMAGE,
            # The :cluster tag is reused across rebuilds, so without Always a node
            # with a cached image never picks up a new build (k8s defaults a
            # non-:latest tag to IfNotPresent). Always = every run uses the latest
            # pushed :cluster image (e.g. the OpenLineage-enabled build).
            image_pull_policy="Always",
            image_pull_secrets=[k8s.V1LocalObjectReference(name="ghcr-pull")],
            cmds=["kedro"],
            arguments=["run", "--env", "cluster", "--pipeline", pipeline],
            env_vars=_env,
            container_resources=_resources,
            in_cluster=True,
            get_logs=True,
            log_events_on_failure=True,
            on_finish_action="delete_succeeded_pod",
            startup_timeout_seconds=300,
            retries=1,
        )
