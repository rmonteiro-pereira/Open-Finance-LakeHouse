"""
TLP Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_tlp_raw,
    transform_tlp_raw_to_bronze,
    transform_tlp_bronze_to_silver,
    aggregate_tlp_to_gold,
    validate_tlp_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_tlp_raw,
                inputs=["params:tlp_series_id"],
                outputs="raw_tlp",
                name="ingest_tlp_raw_node",
            ),
            node(
                func=transform_tlp_raw_to_bronze,
                inputs="raw_tlp",
                outputs="bronze_tlp",
                name="transform_tlp_raw_to_bronze_node",
            ),
            node(
                func=transform_tlp_bronze_to_silver,
                inputs="bronze_tlp",
                outputs="silver_tlp",
                name="transform_tlp_bronze_to_silver_node",
            ),
            node(
                func=aggregate_tlp_to_gold,
                inputs="silver_tlp",
                outputs="gold_tlp",
                name="aggregate_tlp_gold_node",
            ),
            node(
                func=validate_tlp_data,
                inputs="silver_tlp",
                outputs="tlp_validation_results",
                name="validate_tlp_node",
            ),
        ]
    )
