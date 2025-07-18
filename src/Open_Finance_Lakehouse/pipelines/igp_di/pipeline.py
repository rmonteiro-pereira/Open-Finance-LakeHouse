"""
IGP-DI Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_igp_di_raw,
    transform_igp_di_raw_to_bronze,
    transform_igp_di_bronze_to_silver,
    aggregate_igp_di_to_gold,
    validate_igp_di_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_igp_di_raw,
                inputs=["params:igp_di_series_id"],
                outputs="raw_igp_di",
                name="ingest_igp_di_raw_node",
            ),
            node(
                func=transform_igp_di_raw_to_bronze,
                inputs="raw_igp_di",
                outputs="bronze_igp_di",
                name="transform_igp_di_raw_to_bronze_node",
            ),
            node(
                func=transform_igp_di_bronze_to_silver,
                inputs="bronze_igp_di",
                outputs="silver_igp_di",
                name="transform_igp_di_bronze_to_silver_node",
            ),
            node(
                func=aggregate_igp_di_to_gold,
                inputs="silver_igp_di",
                outputs="gold_igp_di",
                name="aggregate_igp_di_gold_node",
            ),
            node(
                func=validate_igp_di_data,
                inputs="silver_igp_di",
                outputs="igp_di_validation_results",
                name="validate_igp_di_node",
            ),
        ]
    )
