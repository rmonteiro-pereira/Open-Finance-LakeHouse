"""
IPCA Pipeline - Brazilian Inflation Index
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_ipca_raw,
    transform_ipca_raw_to_bronze,
    transform_ipca_bronze_to_silver,
    aggregate_ipca_to_gold,
    validate_ipca_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_ipca_raw,
                inputs=["params:ipca_series_id"],
                outputs="raw_ipca",
                name="ingest_ipca_raw_node",
            ),
            node(
                func=transform_ipca_raw_to_bronze,
                inputs="raw_ipca",
                outputs="bronze_ipca",
                name="transform_ipca_raw_to_bronze_node",
            ),
            node(
                func=transform_ipca_bronze_to_silver,
                inputs="bronze_ipca",
                outputs="silver_ipca",
                name="transform_ipca_bronze_to_silver_node",
            ),
            node(
                func=aggregate_ipca_to_gold,
                inputs="silver_ipca",
                outputs="gold_ipca",
                name="aggregate_ipca_gold_node",
            ),
            node(
                func=validate_ipca_data,
                inputs="silver_ipca",
                outputs="ipca_validation_results",
                name="validate_ipca_node",
            ),
        ]
    )
