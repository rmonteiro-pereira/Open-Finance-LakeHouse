"""
INPC Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_inpc_raw,
    transform_inpc_raw_to_bronze,
    transform_inpc_bronze_to_silver,
    aggregate_inpc_to_gold,
    validate_inpc_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_inpc_raw,
                inputs=["params:inpc_series_id"],
                outputs="raw_inpc",
                name="ingest_inpc_raw_node",
            ),
            node(
                func=transform_inpc_raw_to_bronze,
                inputs="raw_inpc",
                outputs="bronze_inpc",
                name="transform_inpc_raw_to_bronze_node",
            ),
            node(
                func=transform_inpc_bronze_to_silver,
                inputs="bronze_inpc",
                outputs="silver_inpc",
                name="transform_inpc_bronze_to_silver_node",
            ),
            node(
                func=aggregate_inpc_to_gold,
                inputs="silver_inpc",
                outputs="gold_inpc",
                name="aggregate_inpc_gold_node",
            ),
            node(
                func=validate_inpc_data,
                inputs="silver_inpc",
                outputs="inpc_validation_results",
                name="validate_inpc_node",
            ),
        ]
    )
