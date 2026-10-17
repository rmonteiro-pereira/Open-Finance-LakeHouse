"""Optional hourly queue drain, isolated from the batch source-completion barrier.

Use either this DAG or the continuous private worker. The shared PostgreSQL lock
serializes processing even if both are accidentally enabled.
"""
import os

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id="ofl_research_worker",
    schedule="@hourly" if os.getenv("OFL_RESEARCH_AIRFLOW_ENABLED") == "true" else None,
    start_date=pendulum.datetime(2026, 10, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ofl", "research", "private"],
) as dag:
    drain = KubernetesPodOperator(
        task_id="drain_private_queue",
        name="ofl-research-worker",
        namespace=os.getenv("OFL_NAMESPACE", "ofl"),
        image=os.getenv("OFL_RESEARCH_IMAGE", "ghcr.io/rmonteiro-pereira/open-finance-lakehouse:research"),
        cmds=["ofl"],
        arguments=["research", "worker", "--drain"],
        env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="ofl-research"))],
        volumes=[k8s.V1Volume(name="research", persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="ofl-research"))],
        volume_mounts=[k8s.V1VolumeMount(name="research", mount_path="/data")],
        env_vars={"OFL_RESEARCH_ROOT": "/data"},
        pool="ofl_research",
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "512Mi"}, limits={"cpu": "1000m", "memory": "2Gi"}),
        execution_timeout=pendulum.duration(hours=2),
        startup_timeout_seconds=600,
        retries=1,
        on_finish_action="delete_pod",
        get_logs=True,
    )
