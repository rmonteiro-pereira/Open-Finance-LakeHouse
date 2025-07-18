"""
SELIC Meta Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_selic_meta_raw,
    transform_selic_meta_raw_to_bronze,
    transform_selic_meta_bronze_to_silver,
    aggregate_selic_meta_to_gold,
    validate_selic_meta_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_selic_meta_raw,
                inputs=["params:selic_meta_series_id"],
                outputs="raw_selic_meta",
                name="ingest_selic_meta_raw_node",
            ),
            node(
                func=transform_selic_meta_raw_to_bronze,
                inputs="raw_selic_meta",
                outputs="bronze_selic_meta",
                name="transform_selic_meta_raw_to_bronze_node",
            ),
            node(
                func=transform_selic_meta_bronze_to_silver,
                inputs="bronze_selic_meta",
                outputs="silver_selic_meta",
                name="transform_selic_meta_bronze_to_silver_node",
            ),
            node(
                func=aggregate_selic_meta_to_gold,
                inputs="silver_selic_meta",
                outputs="gold_selic_meta",
                name="aggregate_selic_meta_gold_node",
            ),
            node(
                func=validate_selic_meta_data,
                inputs="silver_selic_meta",
                outputs="selic_meta_validation_results",
                name="validate_selic_meta_node",
            ),
        ]
    )
