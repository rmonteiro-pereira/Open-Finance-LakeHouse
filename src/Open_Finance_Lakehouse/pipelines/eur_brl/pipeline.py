"""
EUR/BRL Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_eur_brl_raw,
    transform_eur_brl_raw_to_bronze,
    transform_eur_brl_bronze_to_silver,
    aggregate_eur_brl_to_gold,
    validate_eur_brl_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_eur_brl_raw,
                inputs=["params:eur_brl_series_id"],
                outputs="raw_eur_brl",
                name="ingest_eur_brl_raw_node",
            ),
            node(
                func=transform_eur_brl_raw_to_bronze,
                inputs="raw_eur_brl",
                outputs="bronze_eur_brl",
                name="transform_eur_brl_raw_to_bronze_node",
            ),
            node(
                func=transform_eur_brl_bronze_to_silver,
                inputs="bronze_eur_brl",
                outputs="silver_eur_brl",
                name="transform_eur_brl_bronze_to_silver_node",
            ),
            node(
                func=aggregate_eur_brl_to_gold,
                inputs="silver_eur_brl",
                outputs="gold_eur_brl",
                name="aggregate_eur_brl_gold_node",
            ),
            node(
                func=validate_eur_brl_data,
                inputs="silver_eur_brl",
                outputs="eur_brl_validation_results",
                name="validate_eur_brl_node",
            ),
        ]
    )
