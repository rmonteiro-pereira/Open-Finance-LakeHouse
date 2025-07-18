"""
USD/BRL Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_usd_brl_raw,
    transform_usd_brl_raw_to_bronze,
    transform_usd_brl_bronze_to_silver,
    aggregate_usd_brl_to_gold,
    validate_usd_brl_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_usd_brl_raw,
                inputs=["params:usd_brl_series_id"],
                outputs="raw_usd_brl",
                name="ingest_usd_brl_raw_node",
            ),
            node(
                func=transform_usd_brl_raw_to_bronze,
                inputs="raw_usd_brl",
                outputs="bronze_usd_brl",
                name="transform_usd_brl_raw_to_bronze_node",
            ),
            node(
                func=transform_usd_brl_bronze_to_silver,
                inputs="bronze_usd_brl",
                outputs="silver_usd_brl",
                name="transform_usd_brl_bronze_to_silver_node",
            ),
            node(
                func=aggregate_usd_brl_to_gold,
                inputs="silver_usd_brl",
                outputs="gold_usd_brl",
                name="aggregate_usd_brl_gold_node",
            ),
            node(
                func=validate_usd_brl_data,
                inputs="silver_usd_brl",
                outputs="usd_brl_validation_results",
                name="validate_usd_brl_node",
            ),
        ]
    )
