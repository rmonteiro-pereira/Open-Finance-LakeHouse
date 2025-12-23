"""
Airflow DAG for BACEN IPCA pipeline.

This DAG follows the complete lakehouse flow for IPCA (Inflation Index) data.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

from utils.lakefs_operators import (
    LakeFSCommitOperator,
    LakeFSCreateBranchOperator,
    LakeFSMergeOperator,
)
from utils.metadata_operators import (
    OpenMetadataLineageOperator,
    OpenMetadataRegisterOperator,
)
from utils.dremio_operators import DremioRefreshViewOperator

default_args = {
    "owner": "rodrigompereira",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bacen_ipca_pipeline",
    default_args=default_args,
    description="BACEN IPCA pipeline with full lakehouse flow",
    schedule_interval="@monthly",  # IPCA is monthly
    catchup=False,
    tags=["bacen", "ipca", "inflation", "lakehouse"],
) as dag:

    # 1. Create/reuse dev branch
    create_branch = LakeFSCreateBranchOperator(
        task_id="create_dev_branch",
        branch_name="dev",
        source_branch="main",
    )

    # 2. Ingest Raw data
    ingest_raw = BashOperator(
        task_id="ingest_raw_ipca",
        bash_command="kedro run --pipeline=ipca --node=ingest_ipca_raw_node",
    )

    # 3. Register Raw in OpenMetadata (IMMEDIATE)
    register_raw = OpenMetadataRegisterOperator(
        task_id="register_raw_ipca",
        layer="raw",
        dataset_name="raw_ipca",
        storage_path="s3a://lakehouse/raw/bacen/ipca_bacen_raw.json",
        source="BACEN",
        format_type="JSON",
    )

    # 4. Commit Raw to LakeFS
    commit_raw = LakeFSCommitOperator(
        task_id="commit_raw_ipca",
        branch_name="dev",
        layer="raw",
        pipeline_name="bacen_ipca",
    )

    # 5. Transform Raw → Bronze
    transform_bronze = BashOperator(
        task_id="transform_to_bronze",
        bash_command="kedro run --pipeline=ipca --node=transform_ipca_raw_to_bronze_node",
    )

    # 6. Register Bronze in OpenMetadata
    register_bronze = OpenMetadataRegisterOperator(
        task_id="register_bronze_ipca",
        layer="bronze",
        dataset_name="bronze_ipca",
        storage_path="s3a://lakehouse/bronze/bacen_ipca/",
        source="BACEN",
    )

    # 7. Commit Bronze to LakeFS
    commit_bronze = LakeFSCommitOperator(
        task_id="commit_bronze_ipca",
        branch_name="dev",
        layer="bronze",
        pipeline_name="bacen_ipca",
    )

    # 8. Transform Bronze → Silver
    transform_silver = BashOperator(
        task_id="transform_to_silver",
        bash_command="kedro run --pipeline=ipca --node=transform_ipca_bronze_to_silver_node",
    )

    # 9. Validate Silver
    validate_silver = BashOperator(
        task_id="validate_silver_ipca",
        bash_command="kedro run --pipeline=ipca --node=validate_ipca_node",
    )

    # 10. Register Silver + validation in OpenMetadata
    register_silver = OpenMetadataRegisterOperator(
        task_id="register_silver_ipca",
        layer="silver",
        dataset_name="silver_ipca",
        storage_path="s3a://lakehouse/silver/bacen_ipca/",
        source="BACEN",
    )

    # 11. Commit Silver to LakeFS
    commit_silver = LakeFSCommitOperator(
        task_id="commit_silver_ipca",
        branch_name="dev",
        layer="silver",
        pipeline_name="bacen_ipca",
    )

    # 12. Check validation result
    def check_validation_result(**context):
        """Check validation result and set merge flag."""
        validation_result = context["ti"].xcom_pull(
            task_ids="validate_silver_ipca", key="return_value"
        )

        if isinstance(validation_result, dict):
            return validation_result.get("success", False)
        elif isinstance(validation_result, bool):
            return validation_result
        else:
            logger.info("No explicit validation result, assuming success")
            return True

    check_validation = PythonOperator(
        task_id="check_validation_result",
        python_callable=check_validation_result,
    )

    # 13. Merge dev → main (if validation passed)
    merge_to_main = LakeFSMergeOperator(
        task_id="merge_to_main",
        source_branch="dev",
        destination_branch="main",
        validation_passed="{{ ti.xcom_pull(task_ids='check_validation_result') }}",
    )

    # 14. Aggregate Silver → Gold
    aggregate_gold = BashOperator(
        task_id="aggregate_to_gold",
        bash_command="kedro run --pipeline=ipca --node=aggregate_ipca_gold_node",
    )

    # 15. Register Gold + lineage in OpenMetadata
    register_gold = OpenMetadataRegisterOperator(
        task_id="register_gold_ipca",
        layer="gold",
        dataset_name="gold_ipca",
        storage_path="s3a://lakehouse/gold/ipca_kpis/",
        source="BACEN",
    )

    # 16. Register complete lineage
    register_lineage = OpenMetadataLineageOperator(
        task_id="register_lineage",
        pipeline_name="bacen_ipca",
        layers=["raw", "bronze", "silver", "gold"],
        dataset_names=["raw_ipca", "bronze_ipca", "silver_ipca", "gold_ipca"],
        source="BACEN",
    )

    # 17. Commit Gold to LakeFS
    commit_gold = LakeFSCommitOperator(
        task_id="commit_gold_ipca",
        branch_name="main",
        layer="gold",
        pipeline_name="bacen_ipca",
    )

    # 18. Refresh Dremio views
    refresh_dremio = DremioRefreshViewOperator(
        task_id="refresh_dremio_views",
        view_path=["lakehouse", "gold", "bacen"],
    )

    # Define task dependencies
    (
        create_branch
        >> ingest_raw
        >> register_raw
        >> commit_raw
        >> transform_bronze
        >> register_bronze
        >> commit_bronze
        >> transform_silver
        >> validate_silver
        >> register_silver
        >> commit_silver
        >> check_validation
        >> merge_to_main
        >> aggregate_gold
        >> register_gold
        >> register_lineage
        >> commit_gold
        >> refresh_dremio
    )

