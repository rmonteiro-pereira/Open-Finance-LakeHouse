"""
OVER Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_over_raw,
    transform_over_raw_to_bronze,
    transform_over_bronze_to_silver,
    aggregate_over_to_gold,
    validate_over_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_over_raw,
                inputs=["params:over_series_id"],
                outputs="raw_over",
                name="ingest_over_raw_node",
            ),
            node(
                func=transform_over_raw_to_bronze,
                inputs="raw_over",
                outputs="bronze_over",
                name="transform_over_raw_to_bronze_node",
            ),
            node(
                func=transform_over_bronze_to_silver,
                inputs="bronze_over",
                outputs="silver_over",
                name="transform_over_bronze_to_silver_node",
            ),
            node(
                func=aggregate_over_to_gold,
                inputs="silver_over",
                outputs="gold_over",
                name="aggregate_over_gold_node",
            ),
            node(
                func=validate_over_data,
                inputs="silver_over",
                outputs="over_validation_results",
                name="validate_over_node",
            ),
        ]
    )
