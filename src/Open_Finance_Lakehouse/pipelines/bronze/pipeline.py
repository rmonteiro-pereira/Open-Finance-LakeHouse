# src/open_finance_lakehouse/pipelines/bronze/pipeline.py

from kedro.pipeline import Pipeline, node

from .nodes import fetch_bacen_series, fetch_cvm_fundos, save_as_delta


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        node(
            func=fetch_bacen_series,
            inputs=dict(series_id="params:bacen_selic_id"),
            outputs="bacen_selic_df",
            name="fetch_bacen_selic_node"
        ),
        node(
            func=fetch_cvm_fundos,
            inputs=dict(year="params:cvm_year", month="params:cvm_month"),
            outputs="cvm_fundos_df",
            name="fetch_cvm_fundos_node"
        ),
        node(
            func=save_as_delta,
            inputs=dict(df="bacen_selic_df", output_path="params:bronze_bacen_path"),
            outputs=None,
            name="save_bacen_delta_node"
        ),
        node(
            func=save_as_delta,
            inputs=dict(df="cvm_fundos_df", output_path="params:bronze_cvm_path"),
            outputs=None,
            name="save_cvm_delta_node"
        ),
    ])
