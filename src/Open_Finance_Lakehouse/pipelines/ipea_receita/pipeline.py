"""
IPEA/Receita Federal Pipeline Definition
Unified pipeline for Brazilian economic and fiscal data following Tesouro Direto pattern
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_ipea_receita_all_series_raw,
    process_ipea_receita_bronze_layer,
    process_ipea_receita_silver_layer,
    create_ipea_receita_catalog_entry
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create complete IPEA/Receita Federal pipeline with 4-node structure
    Following the same successful pattern as Tesouro Direto
    """
    return pipeline([
        # Extract all IPEA/Receita series raw data
        node(
            func=extract_ipea_receita_all_series_raw,
            inputs="params:ipea_receita",
            outputs="ipea_receita_raw_data_container",
            name="extract_ipea_receita_all_series_raw_node",
            tags=["ipea_receita", "extraction", "raw"]
        ),
        
        # Process raw data to bronze layer
        node(
            func=process_ipea_receita_bronze_layer,
            inputs=["ipea_receita_raw_data_container", "params:ipea_receita"],
            outputs="ipea_receita_bronze",
            name="process_ipea_receita_bronze_layer", 
            tags=["ipea_receita", "bronze", "processing"]
        ),
        
        # Process bronze data to silver layer
        node(
            func=process_ipea_receita_silver_layer,
            inputs=["ipea_receita_bronze", "params:ipea_receita"],
            outputs="ipea_receita_silver",
            name="process_ipea_receita_silver_layer",
            tags=["ipea_receita", "silver", "processing"]
        ),
        
        # Create catalog entry
        node(
            func=create_ipea_receita_catalog_entry,
            inputs="params:ipea_receita",
            outputs="ipea_receita_catalog_entry",
            name="create_ipea_receita_catalog_entry_node",
            tags=["ipea_receita", "catalog", "metadata"]
        )
    ])
