"""
Airflow DAG for B3 (Brazilian Stock Exchange) pipeline.

This DAG follows the complete lakehouse flow for B3 market data.
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
    dag_id="b3_pipeline",
    default_args=default_args,
    description="B3 Brazilian Stock Exchange pipeline with full lakehouse flow",
    schedule_interval="@daily",
    catchup=False,
    tags=["b3", "stock_exchange", "lakehouse"],
) as dag:

    # 1. Create/reuse dev branch
    create_branch = LakeFSCreateBranchOperator(
        task_id="create_dev_branch",
        branch_name="dev",
        source_branch="main",
    )

    # 2. Ingest Raw data
    ingest_raw = BashOperator(
        task_id="ingest_raw_b3",
        bash_command="kedro run --pipeline=b3 --node=extract_b3_all_series_raw_node",
    )

    # 3. Register Raw in OpenMetadata (IMMEDIATE)
    register_raw = OpenMetadataRegisterOperator(
        task_id="register_raw_b3",
        layer="raw",
        dataset_name="raw_b3",
        storage_path="s3a://lakehouse/raw/b3/b3_raw_data.json",
        source="B3",
        format_type="JSON",
    )

    # 4. Commit Raw to LakeFS
    commit_raw = LakeFSCommitOperator(
        task_id="commit_raw_b3",
        branch_name="dev",
        layer="raw",
        pipeline_name="b3",
    )

    # 5. Transform Raw → Bronze
    transform_bronze = BashOperator(
        task_id="transform_to_bronze",
        bash_command="kedro run --pipeline=b3 --node=process_b3_bronze_layer",
    )

    # 6. Register Bronze in OpenMetadata
    register_bronze = OpenMetadataRegisterOperator(
        task_id="register_bronze_b3",
        layer="bronze",
        dataset_name="bronze_b3",
        storage_path="s3a://lakehouse/bronze/b3/",
        source="B3",
    )

    # 7. Commit Bronze to LakeFS
    commit_bronze = LakeFSCommitOperator(
        task_id="commit_bronze_b3",
        branch_name="dev",
        layer="bronze",
        pipeline_name="b3",
    )

    # 8. Transform Bronze → Silver
    transform_silver = BashOperator(
        task_id="transform_to_silver",
        bash_command="kedro run --pipeline=b3 --node=process_b3_silver_layer",
    )

    # 9. Validate Silver (using catalog entry creation as validation)
    validate_silver = BashOperator(
        task_id="validate_silver_b3",
        bash_command="kedro run --pipeline=b3 --node=create_b3_catalog_entry_node",
    )

    # 10. Register Silver + validation in OpenMetadata
    register_silver = OpenMetadataRegisterOperator(
        task_id="register_silver_b3",
        layer="silver",
        dataset_name="silver_b3",
        storage_path="s3a://lakehouse/silver/b3/",
        source="B3",
    )

    # 11. Commit Silver to LakeFS
    commit_silver = LakeFSCommitOperator(
        task_id="commit_silver_b3",
        branch_name="dev",
        layer="silver",
        pipeline_name="b3",
    )

    # 12. Check validation result
    def check_validation_result(**context):
        """Check validation result and set merge flag."""
        # For B3, assume success if catalog entry was created
        logger.info("B3 validation: catalog entry created successfully")
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

    # 14. Aggregate Silver → Gold (B3 might not have gold layer, skip if not needed)
    from airflow.operators.empty import EmptyOperator
    aggregate_gold = EmptyOperator(task_id="aggregate_to_gold")

    # 15. Register Gold + lineage in OpenMetadata
    register_gold = EmptyOperator(task_id="register_gold_b3")

    # 16. Register complete lineage
    register_lineage = OpenMetadataLineageOperator(
        task_id="register_lineage",
        pipeline_name="b3",
        layers=["raw", "bronze", "silver"],
        dataset_names=["raw_b3", "bronze_b3", "silver_b3"],
        source="B3",
    )

    # 17. Commit Gold to LakeFS (skip if no gold layer)
    commit_gold = EmptyOperator(task_id="commit_gold_b3")

    # 18. Refresh Dremio views
    refresh_dremio = DremioRefreshViewOperator(
        task_id="refresh_dremio_views",
        view_path=["lakehouse", "silver", "b3"],
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

