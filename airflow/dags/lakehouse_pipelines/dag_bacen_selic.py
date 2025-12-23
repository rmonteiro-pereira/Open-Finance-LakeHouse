"""
Airflow DAG for BACEN Selic pipeline.

This DAG follows the complete lakehouse flow:
1. Create/reuse dev branch in LakeFS
2. Ingest Raw data
3. Register Raw in OpenMetadata (IMMEDIATE)
4. Commit Raw to LakeFS
5. Transform Raw → Bronze
6. Register Bronze in OpenMetadata
7. Commit Bronze to LakeFS
8. Transform Bronze → Silver
9. Validate Silver with Great Expectations
10. Register Silver + validation in OpenMetadata
11. Commit Silver to LakeFS
12. Merge dev → main (if validation passed)
13. Aggregate Silver → Gold
14. Register Gold + lineage in OpenMetadata
15. Commit Gold to LakeFS
16. Refresh Dremio views
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

from utils.data_quality_operator import DataQualityOperator
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
    dag_id="bacen_selic_pipeline",
    default_args=default_args,
    description="BACEN Selic pipeline with full lakehouse flow",
    schedule_interval="@daily",
    catchup=False,
    tags=["bacen", "selic", "lakehouse"],
) as dag:

    # 1. Create/reuse dev branch
    create_branch = LakeFSCreateBranchOperator(
        task_id="create_dev_branch",
        branch_name="dev",
        source_branch="main",
    )

    # 2. Ingest Raw data
    ingest_raw = BashOperator(
        task_id="ingest_raw_selic",
        bash_command="kedro run --pipeline=selic --node=ingest_selic_raw_node",
    )

    # 3. Register Raw in OpenMetadata (IMMEDIATE)
    register_raw = OpenMetadataRegisterOperator(
        task_id="register_raw_selic",
        layer="raw",
        dataset_name="raw_selic",
        storage_path="s3a://lakehouse/raw/bacen/selic_bacen_raw.json",
        source="BACEN",
        format_type="JSON",
    )

    # 4. Commit Raw to LakeFS
    commit_raw = LakeFSCommitOperator(
        task_id="commit_raw_selic",
        branch_name="dev",
        layer="raw",
        pipeline_name="bacen_selic",
    )

    # 5. Transform Raw → Bronze
    transform_bronze = BashOperator(
        task_id="transform_to_bronze",
        bash_command="kedro run --pipeline=selic --node=transform_raw_to_bronze_node",
    )

    # 6. Register Bronze in OpenMetadata
    register_bronze = OpenMetadataRegisterOperator(
        task_id="register_bronze_selic",
        layer="bronze",
        dataset_name="bronze_selic",
        storage_path="s3a://lakehouse/bronze/bacen_selic/",
        source="BACEN",
        schema={},  # Schema will be inferred from DataFrame
    )

    # 7. Commit Bronze to LakeFS
    commit_bronze = LakeFSCommitOperator(
        task_id="commit_bronze_selic",
        branch_name="dev",
        layer="bronze",
        pipeline_name="bacen_selic",
    )

    # 8. Transform Bronze → Silver
    transform_silver = BashOperator(
        task_id="transform_to_silver",
        bash_command="kedro run --pipeline=selic --node=transform_bronze_to_silver_node",
    )

    # 9. Validate Silver (supports multiple validation methods)
    # Option 1: Use Kedro validation node (custom validation)
    validate_silver = BashOperator(
        task_id="validate_silver_selic",
        bash_command="kedro run --pipeline=selic --node=validate_selic_node",
    )
    
    # Alternative: Use DataQualityOperator for more control
    # validate_silver = DataQualityOperator(
    #     task_id="validate_silver_selic",
    #     validation_method="custom",  # or "gx" for Great Expectations
    #     # For GX: gx_asset_name="silver_bacen_selic", gx_suite_name="silver_bacen_selic_suite"
    # )

    # 10. Register Silver + validation in OpenMetadata
    register_silver = OpenMetadataRegisterOperator(
        task_id="register_silver_selic",
        layer="silver",
        dataset_name="silver_selic",
        storage_path="s3a://lakehouse/silver/bacen_selic/",
        source="BACEN",
        schema={},  # Schema will be inferred from DataFrame
    )

    # 11. Commit Silver to LakeFS
    commit_silver = LakeFSCommitOperator(
        task_id="commit_silver_selic",
        branch_name="dev",
        layer="silver",
        pipeline_name="bacen_selic",
    )

    # 12. Merge dev → main (if validation passed)
    # Note: validation_passed will be determined by a PythonOperator that checks XCom
    from airflow.operators.python import PythonOperator

    def check_validation_result(**context):
        """Check validation result and set merge flag."""
        # Try to get validation result from XCom
        validation_result = context["ti"].xcom_pull(
            task_ids="validate_silver_selic", key="return_value"
        )
        
        # Handle different validation result formats
        if isinstance(validation_result, dict):
            return validation_result.get("success", False)
        elif isinstance(validation_result, bool):
            return validation_result
        else:
            # If no explicit result, check if task succeeded
            # For now, assume success if task completed
            logger.info("No explicit validation result, assuming success")
            return True

    check_validation = PythonOperator(
        task_id="check_validation_result",
        python_callable=check_validation_result,
    )

    merge_to_main = LakeFSMergeOperator(
        task_id="merge_to_main",
        source_branch="dev",
        destination_branch="main",
        validation_passed="{{ ti.xcom_pull(task_ids='check_validation_result') }}",
    )

    # 13. Aggregate Silver → Gold
    aggregate_gold = BashOperator(
        task_id="aggregate_to_gold",
        bash_command="kedro run --pipeline=selic --node=aggregate_selic_gold_node",
    )

    # 14. Register Gold + lineage in OpenMetadata
    register_gold = OpenMetadataRegisterOperator(
        task_id="register_gold_selic",
        layer="gold",
        dataset_name="gold_selic",
        storage_path="s3a://lakehouse/gold/selic_kpis/",
        source="BACEN",
        schema={},  # Schema will be inferred from DataFrame
    )

    # 15. Register complete lineage
    register_lineage = OpenMetadataLineageOperator(
        task_id="register_lineage",
        pipeline_name="bacen_selic",
        layers=["raw", "bronze", "silver", "gold"],
        dataset_names=["raw_selic", "bronze_selic", "silver_selic", "gold_selic"],
        source="BACEN",
    )

    # 16. Commit Gold to LakeFS
    commit_gold = LakeFSCommitOperator(
        task_id="commit_gold_selic",
        branch_name="main",
        layer="gold",
        pipeline_name="bacen_selic",
    )

    # 17. Refresh Dremio views
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

