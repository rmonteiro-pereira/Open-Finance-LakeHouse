"""
This is a boilerplate pipeline 'selic'
generated using Kedro 0.19.12
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import (
    ingest_selic_data,
    transform_selic_to_silver,
    validate_selic_data,
    aggregate_selic_to_gold
)

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            # Stage 2: Ingestão
            node(
                func=ingest_selic_data,
                inputs=dict(series_id="params:selic_series_id"),
                outputs="bronze_selic",
                name="ingest_selic_node",
            ),
            # Stage 3: Transformação Silver (includes pandas to Spark conversion)
            node(
                func=transform_selic_to_silver,
                inputs="bronze_selic",
                outputs="silver_selic",
                name="transform_selic_silver_node",
            ),
            # Stage 4: Validação
            node(
                func=validate_selic_data,
                inputs="silver_selic",
                outputs="selic_validation_results",
                name="validate_selic_node",
            ),
            # Stage 5: Agregação Gold
            node(
                func=aggregate_selic_to_gold,
                inputs="silver_selic",
                outputs="gold_selic",
                name="aggregate_selic_gold_node",
            ),
        ]
    )
