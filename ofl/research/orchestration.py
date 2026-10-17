"""Explicit allowlist for Airflow actions; service credentials never reach clients."""

import httpx

ALLOWED_DAGS = {"ofl_research_worker", "ofl_backfill", "ofl_silver", "ofl_gold"}


def trigger(settings, dag, job_id):
    if dag not in ALLOWED_DAGS:
        raise ValueError("DAG not allowlisted")
    if not settings.airflow_url or not settings.airflow_token:
        raise ValueError("Airflow action integration is not configured")
    run_id = f"research__{job_id}"
    with httpx.Client(
        base_url=settings.airflow_url.rstrip("/"),
        headers={"Authorization": f"Bearer {settings.airflow_token}"},
        timeout=30,
    ) as client:
        response = client.post(
            f"/api/v2/dags/{dag}/dagRuns",
            json={"dag_run_id": run_id, "logical_date": None, "conf": {}},
        )
        if response.status_code == 409:
            response = client.get(f"/api/v2/dags/{dag}/dagRuns/{run_id}")
        response.raise_for_status()
        return {"dag": dag, "run_id": run_id, "state": response.json().get("state")}
