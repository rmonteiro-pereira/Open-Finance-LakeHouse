"""
Reservas Internacionais pipeline
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_reservas_internacionais_raw,
    transform_reservas_internacionais_raw_to_bronze,
    transform_reservas_internacionais_bronze_to_silver,
    aggregate_reservas_internacionais_to_gold,
    validate_reservas_internacionais_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=ingest_reservas_internacionais_raw,
            inputs=None,
            outputs="raw_reservas_internacionais",
            name="ingest_reservas_internacionais_raw_node",
        ),
        node(
            func=transform_reservas_internacionais_raw_to_bronze,
            inputs="raw_reservas_internacionais",
            outputs="bronze_reservas_internacionais",
            name="transform_reservas_internacionais_raw_to_bronze_node",
        ),
        node(
            func=transform_reservas_internacionais_bronze_to_silver,
            inputs="bronze_reservas_internacionais",
            outputs="silver_reservas_internacionais",
            name="transform_reservas_internacionais_bronze_to_silver_node",
        ),
        node(
            func=aggregate_reservas_internacionais_to_gold,
            inputs="silver_reservas_internacionais",
            outputs="gold_reservas_internacionais",
            name="aggregate_reservas_internacionais_to_gold_node",
        ),
        node(
            func=validate_reservas_internacionais_data,
            inputs="silver_reservas_internacionais",
            outputs="reservas_internacionais_validation_results",
            name="validate_reservas_internacionais_data_node",
        ),
    ])
