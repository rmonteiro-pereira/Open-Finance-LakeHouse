"""
Template DAG for Lakehouse Pipelines.

This template can be used to create new DAGs following the standard flow:
1. Create/reuse dev branch in LakeFS
2. Ingest Raw data
3. Register Raw in OpenMetadata (IMMEDIATE)
4. Commit Raw to LakeFS
5. Transform Raw → Bronze
6. Register Bronze in OpenMetadata
7. Commit Bronze to LakeFS
8. Transform Bronze → Silver
9. Validate Silver
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


def create_lakehouse_dag(
    dag_id: str,
    description: str,
    pipeline_name: str,
    schedule_interval: str = "@daily",
    tags: list = None,
    raw_node: str = None,
    bronze_node: str = None,
    silver_node: str = None,
    gold_node: str = None,
    validation_node: str = None,
    raw_dataset_name: str = None,
    bronze_dataset_name: str = None,
    silver_dataset_name: str = None,
    gold_dataset_name: str = None,
    raw_storage_path: str = None,
    bronze_storage_path: str = None,
    silver_storage_path: str = None,
    gold_storage_path: str = None,
    source: str = "Unknown",
    format_type: str = "JSON",
) -> DAG:
    """
    Create a standard lakehouse DAG.

    Args:
        dag_id: DAG identifier
        description: DAG description
        pipeline_name: Kedro pipeline name
        schedule_interval: Schedule interval (default: @daily)
        tags: DAG tags
        raw_node: Kedro node name for raw ingestion
        bronze_node: Kedro node name for bronze transformation
        silver_node: Kedro node name for silver transformation
        gold_node: Kedro node name for gold aggregation
        validation_node: Kedro node name for validation (optional)
        raw_dataset_name: OpenMetadata dataset name for raw layer
        bronze_dataset_name: OpenMetadata dataset name for bronze layer
        silver_dataset_name: OpenMetadata dataset name for silver layer
        gold_dataset_name: OpenMetadata dataset name for gold layer
        raw_storage_path: Storage path for raw data
        bronze_storage_path: Storage path for bronze data
        silver_storage_path: Storage path for silver data
        gold_storage_path: Storage path for gold data
        source: Data source name
        format_type: Raw data format (JSON, Parquet, CSV)

    Returns:
        Configured DAG
    """
    if tags is None:
        tags = [pipeline_name, "lakehouse"]

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule_interval=schedule_interval,
        catchup=False,
        tags=tags,
    ) as dag:

        # 1. Create/reuse dev branch
        create_branch = LakeFSCreateBranchOperator(
            task_id="create_dev_branch",
            branch_name="dev",
            source_branch="main",
        )

        # 2. Ingest Raw data
        if raw_node:
            ingest_raw = BashOperator(
                task_id="ingest_raw",
                bash_command=f"kedro run --pipeline={pipeline_name} --node={raw_node}",
            )
        else:
            ingest_raw = BashOperator(
                task_id="ingest_raw",
                bash_command=f"kedro run --pipeline={pipeline_name}",
            )

        # 3. Register Raw in OpenMetadata (IMMEDIATE)
        if raw_dataset_name and raw_storage_path:
            register_raw = OpenMetadataRegisterOperator(
                task_id="register_raw",
                layer="raw",
                dataset_name=raw_dataset_name,
                storage_path=raw_storage_path,
                source=source,
                format_type=format_type,
            )
        else:
            # Skip registration if not configured
            from airflow.operators.empty import EmptyOperator
            register_raw = EmptyOperator(task_id="register_raw")

        # 4. Commit Raw to LakeFS
        commit_raw = LakeFSCommitOperator(
            task_id="commit_raw",
            branch_name="dev",
            layer="raw",
            pipeline_name=pipeline_name,
        )

        # 5. Transform Raw → Bronze
        if bronze_node:
            transform_bronze = BashOperator(
                task_id="transform_to_bronze",
                bash_command=f"kedro run --pipeline={pipeline_name} --node={bronze_node}",
            )
        else:
            from airflow.operators.empty import EmptyOperator
            transform_bronze = EmptyOperator(task_id="transform_to_bronze")

        # 6. Register Bronze in OpenMetadata
        if bronze_dataset_name and bronze_storage_path:
            register_bronze = OpenMetadataRegisterOperator(
                task_id="register_bronze",
                layer="bronze",
                dataset_name=bronze_dataset_name,
                storage_path=bronze_storage_path,
                source=source,
            )
        else:
            from airflow.operators.empty import EmptyOperator
            register_bronze = EmptyOperator(task_id="register_bronze")

        # 7. Commit Bronze to LakeFS
        commit_bronze = LakeFSCommitOperator(
            task_id="commit_bronze",
            branch_name="dev",
            layer="bronze",
            pipeline_name=pipeline_name,
        )

        # 8. Transform Bronze → Silver
        if silver_node:
            transform_silver = BashOperator(
                task_id="transform_to_silver",
                bash_command=f"kedro run --pipeline={pipeline_name} --node={silver_node}",
            )
        else:
            from airflow.operators.empty import EmptyOperator
            transform_silver = EmptyOperator(task_id="transform_to_silver")

        # 9. Validate Silver
        if validation_node:
            validate_silver = BashOperator(
                task_id="validate_silver",
                bash_command=f"kedro run --pipeline={pipeline_name} --node={validation_node}",
            )
        else:
            # Default: pass validation
            def check_validation_result(**context):
                return True

            validate_silver = PythonOperator(
                task_id="validate_silver",
                python_callable=check_validation_result,
            )

        # 10. Register Silver + validation in OpenMetadata
        if silver_dataset_name and silver_storage_path:
            register_silver = OpenMetadataRegisterOperator(
                task_id="register_silver",
                layer="silver",
                dataset_name=silver_dataset_name,
                storage_path=silver_storage_path,
                source=source,
            )
        else:
            from airflow.operators.empty import EmptyOperator
            register_silver = EmptyOperator(task_id="register_silver")

        # 11. Commit Silver to LakeFS
        commit_silver = LakeFSCommitOperator(
            task_id="commit_silver",
            branch_name="dev",
            layer="silver",
            pipeline_name=pipeline_name,
        )

        # 12. Check validation result
        def check_validation_result(**context):
            """Check validation result and set merge flag."""
            validation_result = context["ti"].xcom_pull(
                task_ids="validate_silver", key="return_value"
            )

            if isinstance(validation_result, dict):
                return validation_result.get("success", False)
            elif isinstance(validation_result, bool):
                return validation_result
            else:
                # If no explicit result, assume success if task completed
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
        if gold_node:
            aggregate_gold = BashOperator(
                task_id="aggregate_to_gold",
                bash_command=f"kedro run --pipeline={pipeline_name} --node={gold_node}",
            )
        else:
            from airflow.operators.empty import EmptyOperator
            aggregate_gold = EmptyOperator(task_id="aggregate_to_gold")

        # 15. Register Gold + lineage in OpenMetadata
        if gold_dataset_name and gold_storage_path:
            register_gold = OpenMetadataRegisterOperator(
                task_id="register_gold",
                layer="gold",
                dataset_name=gold_dataset_name,
                storage_path=gold_storage_path,
                source=source,
            )
        else:
            from airflow.operators.empty import EmptyOperator
            register_gold = EmptyOperator(task_id="register_gold")

        # 16. Register complete lineage
        dataset_names = []
        if raw_dataset_name:
            dataset_names.append(raw_dataset_name)
        if bronze_dataset_name:
            dataset_names.append(bronze_dataset_name)
        if silver_dataset_name:
            dataset_names.append(silver_dataset_name)
        if gold_dataset_name:
            dataset_names.append(gold_dataset_name)

        if dataset_names:
            register_lineage = OpenMetadataLineageOperator(
                task_id="register_lineage",
                pipeline_name=pipeline_name,
                layers=["raw", "bronze", "silver", "gold"][: len(dataset_names)],
                dataset_names=dataset_names,
                source=source,
            )
        else:
            from airflow.operators.empty import EmptyOperator
            register_lineage = EmptyOperator(task_id="register_lineage")

        # 17. Commit Gold to LakeFS
        commit_gold = LakeFSCommitOperator(
            task_id="commit_gold",
            branch_name="main",
            layer="gold",
            pipeline_name=pipeline_name,
        )

        # 18. Refresh Dremio views
        refresh_dremio = DremioRefreshViewOperator(
            task_id="refresh_dremio_views",
            view_path=["lakehouse", "gold", source.lower()],
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

    return dag

