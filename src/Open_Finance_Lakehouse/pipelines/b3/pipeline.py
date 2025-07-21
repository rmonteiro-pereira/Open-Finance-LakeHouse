"""
B3 Pipeline Definition
Unified pipeline for Brazilian stock exchange data following Tesouro Direto pattern
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_b3_all_series_raw,
    process_b3_bronze_layer,
    process_b3_silver_layer,
    create_b3_catalog_entry
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create complete B3 pipeline with 4-node structure
    Following the same successful pattern as Tesouro Direto
    """
    return pipeline([
        # Extract all B3 series raw data
        node(
            func=extract_b3_all_series_raw,
            inputs="params:b3",
            outputs="b3_raw_data_container",
            name="extract_b3_all_series_raw_node",
            tags=["b3", "extraction", "raw"]
        ),
        
        # Process raw data to bronze layer
        node(
            func=process_b3_bronze_layer,
            inputs=["b3_raw_data_container", "params:b3"],
            outputs="b3_bronze",
            name="process_b3_bronze_layer", 
            tags=["b3", "bronze", "processing"]
        ),
        
        # Process bronze data to silver layer
        node(
            func=process_b3_silver_layer,
            inputs=["b3_bronze", "params:b3"],
            outputs="b3_silver",
            name="process_b3_silver_layer",
            tags=["b3", "silver", "processing"]
        ),
        
        # Create catalog entry
        node(
            func=create_b3_catalog_entry,
            inputs="params:b3",
            outputs="b3_catalog_entry",
            name="create_b3_catalog_entry_node",
            tags=["b3", "catalog", "metadata"]
        )
    ])
