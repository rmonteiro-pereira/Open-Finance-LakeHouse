from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="lakehouse_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 1},
) as dag:

    bronze = BashOperator(
        task_id="run_bronze_pipeline",
        bash_command="kedro run --pipeline=bronze"
    )

    silver = BashOperator(
        task_id="run_silver_pipeline",
        bash_command="kedro run --pipeline=silver"
    )

    data_quality = BashOperator(
        task_id="run_data_quality_pipeline",
        bash_command="kedro run --pipeline=data_quality"
    )

    gold = BashOperator(
        task_id="run_gold_pipeline",
        bash_command="kedro run --pipeline=gold"
    )

    bronze >> silver >> data_quality >> gold
