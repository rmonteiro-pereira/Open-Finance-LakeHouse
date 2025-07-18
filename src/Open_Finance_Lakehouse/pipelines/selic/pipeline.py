"""
This is a boilerplate pipeline 'selic'
generated using Kedro 0.19.12
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import (
    ingest_selic_raw,
    transform_raw_to_bronze,
    transform_bronze_to_silver,
    validate_selic_data,
    aggregate_selic_to_gold
)

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            # Stage 1: Raw Data Ingestion
            node(
                func=ingest_selic_raw,
                inputs=dict(series_id="params:selic_series_id"),
                outputs="raw_selic",
                name="ingest_selic_raw_node",
            ),
            # Stage 2: Raw to Bronze (Schema + Basic Validation)
            node(
                func=transform_raw_to_bronze,
                inputs="raw_selic",
                outputs="bronze_selic",
                name="transform_raw_to_bronze_node",
            ),
            # Stage 3: Bronze to Silver (Cleaning + Transformation)
            node(
                func=transform_bronze_to_silver,
                inputs="bronze_selic",
                outputs="silver_selic",
                name="transform_bronze_to_silver_node",
            ),
            # Stage 4: Data Validation
            node(
                func=validate_selic_data,
                inputs="silver_selic",
                outputs="selic_validation_results",
                name="validate_selic_node",
            ),
            # Stage 5: Silver to Gold (Aggregations + KPIs)
            node(
                func=aggregate_selic_to_gold,
                inputs="silver_selic",
                outputs="gold_selic",
                name="aggregate_selic_gold_node",
            ),
        ]
    )
