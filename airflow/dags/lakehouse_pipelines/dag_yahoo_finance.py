"""
Airflow DAG for Yahoo Finance pipeline.

This DAG follows the complete lakehouse flow for Yahoo Finance data (ETFs, commodities).
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
    dag_id="yahoo_finance_pipeline",
    default_args=default_args,
    description="Yahoo Finance pipeline with full lakehouse flow (ETFs, commodities)",
    schedule_interval="@daily",
    catchup=False,
    tags=["yahoo_finance", "etf", "commodities", "lakehouse"],
) as dag:

    # 1. Create/reuse dev branch
    create_branch = LakeFSCreateBranchOperator(
        task_id="create_dev_branch",
        branch_name="dev",
        source_branch="main",
    )

    # 2. Ingest Raw data
    ingest_raw = BashOperator(
        task_id="ingest_raw_yahoo",
        bash_command="kedro run --pipeline=yahoo_finance --node=extract_yahoo_finance_all_series_raw",
    )

    # 3. Register Raw in OpenMetadata (IMMEDIATE)
    register_raw = OpenMetadataRegisterOperator(
        task_id="register_raw_yahoo",
        layer="raw",
        dataset_name="raw_yahoo_finance",
        storage_path="s3a://lakehouse/raw/yahoo_finance/yahoo_finance_raw_data.json",
        source="YahooFinance",
        format_type="JSON",
    )

    # 4. Commit Raw to LakeFS
    commit_raw = LakeFSCommitOperator(
        task_id="commit_raw_yahoo",
        branch_name="dev",
        layer="raw",
        pipeline_name="yahoo_finance",
    )

    # 5. Transform Raw → Bronze
    transform_bronze = BashOperator(
        task_id="transform_to_bronze",
        bash_command="kedro run --pipeline=yahoo_finance --node=process_yahoo_finance_bronze_layer",
    )

    # 6. Register Bronze in OpenMetadata
    register_bronze = OpenMetadataRegisterOperator(
        task_id="register_bronze_yahoo",
        layer="bronze",
        dataset_name="bronze_yahoo_finance",
        storage_path="s3a://lakehouse/bronze/yahoo_finance/",
        source="YahooFinance",
    )

    # 7. Commit Bronze to LakeFS
    commit_bronze = LakeFSCommitOperator(
        task_id="commit_bronze_yahoo",
        branch_name="dev",
        layer="bronze",
        pipeline_name="yahoo_finance",
    )

    # 8. Transform Bronze → Silver
    transform_silver = BashOperator(
        task_id="transform_to_silver",
        bash_command="kedro run --pipeline=yahoo_finance --node=process_yahoo_finance_silver_layer",
    )

    # 9. Validate Silver (using catalog entry creation as validation)
    validate_silver = BashOperator(
        task_id="validate_silver_yahoo",
        bash_command="kedro run --pipeline=yahoo_finance --node=create_yahoo_finance_catalog_entry",
    )

    # 10. Register Silver + validation in OpenMetadata
    register_silver = OpenMetadataRegisterOperator(
        task_id="register_silver_yahoo",
        layer="silver",
        dataset_name="silver_yahoo_finance",
        storage_path="s3a://lakehouse/silver/yahoo_finance/",
        source="YahooFinance",
    )

    # 11. Commit Silver to LakeFS
    commit_silver = LakeFSCommitOperator(
        task_id="commit_silver_yahoo",
        branch_name="dev",
        layer="silver",
        pipeline_name="yahoo_finance",
    )

    # 12. Check validation result
    def check_validation_result(**context):
        """Check validation result and set merge flag."""
        logger.info("Yahoo Finance validation: catalog entry created successfully")
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

    # 14. Aggregate Silver → Gold (Yahoo Finance might not have gold layer)
    from airflow.operators.empty import EmptyOperator
    aggregate_gold = EmptyOperator(task_id="aggregate_to_gold")

    # 15. Register Gold + lineage in OpenMetadata
    register_gold = EmptyOperator(task_id="register_gold_yahoo")

    # 16. Register complete lineage
    register_lineage = OpenMetadataLineageOperator(
        task_id="register_lineage",
        pipeline_name="yahoo_finance",
        layers=["raw", "bronze", "silver"],
        dataset_names=["raw_yahoo_finance", "bronze_yahoo_finance", "silver_yahoo_finance"],
        source="YahooFinance",
    )

    # 17. Commit Gold to LakeFS (skip if no gold layer)
    commit_gold = EmptyOperator(task_id="commit_gold_yahoo")

    # 18. Refresh Dremio views
    refresh_dremio = DremioRefreshViewOperator(
        task_id="refresh_dremio_views",
        view_path=["lakehouse", "silver", "yahoo_finance"],
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

