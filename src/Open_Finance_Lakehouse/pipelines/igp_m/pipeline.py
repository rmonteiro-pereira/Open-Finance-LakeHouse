"""
IGP-M Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_igp_m_raw,
    transform_igp_m_raw_to_bronze,
    transform_igp_m_bronze_to_silver,
    aggregate_igp_m_to_gold,
    validate_igp_m_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_igp_m_raw,
                inputs=["params:igp_m_series_id"],
                outputs="raw_igp_m",
                name="ingest_igp_m_raw_node",
            ),
            node(
                func=transform_igp_m_raw_to_bronze,
                inputs="raw_igp_m",
                outputs="bronze_igp_m",
                name="transform_igp_m_raw_to_bronze_node",
            ),
            node(
                func=transform_igp_m_bronze_to_silver,
                inputs="bronze_igp_m",
                outputs="silver_igp_m",
                name="transform_igp_m_bronze_to_silver_node",
            ),
            node(
                func=aggregate_igp_m_to_gold,
                inputs="silver_igp_m",
                outputs="gold_igp_m",
                name="aggregate_igp_m_gold_node",
            ),
            node(
                func=validate_igp_m_data,
                inputs="silver_igp_m",
                outputs="igp_m_validation_results",
                name="validate_igp_m_node",
            ),
        ]
    )
