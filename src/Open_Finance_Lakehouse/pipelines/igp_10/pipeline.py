"""
IGP-10 Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_igp_10_raw,
    transform_igp_10_raw_to_bronze,
    transform_igp_10_bronze_to_silver,
    aggregate_igp_10_to_gold,
    validate_igp_10_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_igp_10_raw,
                inputs=["params:igp_10_series_id"],
                outputs="raw_igp_10",
                name="ingest_igp_10_raw_node",
            ),
            node(
                func=transform_igp_10_raw_to_bronze,
                inputs="raw_igp_10",
                outputs="bronze_igp_10",
                name="transform_igp_10_raw_to_bronze_node",
            ),
            node(
                func=transform_igp_10_bronze_to_silver,
                inputs="bronze_igp_10",
                outputs="silver_igp_10",
                name="transform_igp_10_bronze_to_silver_node",
            ),
            node(
                func=aggregate_igp_10_to_gold,
                inputs="silver_igp_10",
                outputs="gold_igp_10",
                name="aggregate_igp_10_gold_node",
            ),
            node(
                func=validate_igp_10_data,
                inputs="silver_igp_10",
                outputs="igp_10_validation_results",
                name="validate_igp_10_node",
            ),
        ]
    )
