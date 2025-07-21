"""
Focus PIB pipeline
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_focus_pib_raw,
    transform_focus_pib_raw_to_bronze,
    transform_focus_pib_bronze_to_silver,
    aggregate_focus_pib_to_gold,
    validate_focus_pib_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=ingest_focus_pib_raw,
            inputs=None,
            outputs="raw_focus_pib",
            name="ingest_focus_pib_raw_node",
        ),
        node(
            func=transform_focus_pib_raw_to_bronze,
            inputs="raw_focus_pib",
            outputs="bronze_focus_pib",
            name="transform_focus_pib_raw_to_bronze_node",
        ),
        node(
            func=transform_focus_pib_bronze_to_silver,
            inputs="bronze_focus_pib",
            outputs="silver_focus_pib",
            name="transform_focus_pib_bronze_to_silver_node",
        ),
        node(
            func=aggregate_focus_pib_to_gold,
            inputs="silver_focus_pib",
            outputs="gold_focus_pib",
            name="aggregate_focus_pib_to_gold_node",
        ),
        node(
            func=validate_focus_pib_data,
            inputs="silver_focus_pib",
            outputs="focus_pib_validation_results",
            name="validate_focus_pib_data_node",
        ),
    ])
