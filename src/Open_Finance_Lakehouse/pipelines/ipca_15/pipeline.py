"""
IPCA-15 Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_ipca_15_raw,
    transform_ipca_15_raw_to_bronze,
    transform_ipca_15_bronze_to_silver,
    aggregate_ipca_15_to_gold,
    validate_ipca_15_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_ipca_15_raw,
                inputs=["params:ipca_15_series_id"],
                outputs="raw_ipca_15",
                name="ingest_ipca_15_raw_node",
            ),
            node(
                func=transform_ipca_15_raw_to_bronze,
                inputs="raw_ipca_15",
                outputs="bronze_ipca_15",
                name="transform_ipca_15_raw_to_bronze_node",
            ),
            node(
                func=transform_ipca_15_bronze_to_silver,
                inputs="bronze_ipca_15",
                outputs="silver_ipca_15",
                name="transform_ipca_15_bronze_to_silver_node",
            ),
            node(
                func=aggregate_ipca_15_to_gold,
                inputs="silver_ipca_15",
                outputs="gold_ipca_15",
                name="aggregate_ipca_15_gold_node",
            ),
            node(
                func=validate_ipca_15_data,
                inputs="silver_ipca_15",
                outputs="ipca_15_validation_results",
                name="validate_ipca_15_node",
            ),
        ]
    )
