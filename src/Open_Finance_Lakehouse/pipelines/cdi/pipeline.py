"""
CDI Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_cdi_raw,
    transform_cdi_raw_to_bronze,
    transform_cdi_bronze_to_silver,
    aggregate_cdi_to_gold,
    validate_cdi_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_cdi_raw,
                inputs=["params:cdi_series_id"],
                outputs="raw_cdi",
                name="ingest_cdi_raw_node",
            ),
            node(
                func=transform_cdi_raw_to_bronze,
                inputs="raw_cdi",
                outputs="bronze_cdi",
                name="transform_cdi_raw_to_bronze_node",
            ),
            node(
                func=transform_cdi_bronze_to_silver,
                inputs="bronze_cdi",
                outputs="silver_cdi",
                name="transform_cdi_bronze_to_silver_node",
            ),
            node(
                func=aggregate_cdi_to_gold,
                inputs="silver_cdi",
                outputs="gold_cdi",
                name="aggregate_cdi_gold_node",
            ),
            node(
                func=validate_cdi_data,
                inputs="silver_cdi",
                outputs="cdi_validation_results",
                name="validate_cdi_node",
            ),
        ]
    )
