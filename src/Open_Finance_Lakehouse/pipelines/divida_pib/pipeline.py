"""
Divida/PIB pipeline
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_divida_pib_raw,
    transform_divida_pib_raw_to_bronze,
    transform_divida_pib_bronze_to_silver,
    aggregate_divida_pib_to_gold,
    validate_divida_pib_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=ingest_divida_pib_raw,
            inputs=None,
            outputs="raw_divida_pib",
            name="ingest_divida_pib_raw_node",
        ),
        node(
            func=transform_divida_pib_raw_to_bronze,
            inputs="raw_divida_pib",
            outputs="bronze_divida_pib",
            name="transform_divida_pib_raw_to_bronze_node",
        ),
        node(
            func=transform_divida_pib_bronze_to_silver,
            inputs="bronze_divida_pib",
            outputs="silver_divida_pib",
            name="transform_divida_pib_bronze_to_silver_node",
        ),
        node(
            func=aggregate_divida_pib_to_gold,
            inputs="silver_divida_pib",
            outputs="gold_divida_pib",
            name="aggregate_divida_pib_to_gold_node",
        ),
        node(
            func=validate_divida_pib_data,
            inputs="silver_divida_pib",
            outputs="divida_pib_validation_results",
            name="validate_divida_pib_data_node",
        ),
    ])
