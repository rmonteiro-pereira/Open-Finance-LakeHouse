"""
Tesouro Direto Pipeline
Pipeline definition for Brazilian Treasury bonds data processing
Following the unified bronze/silver layer pattern
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    extract_tesouro_all_series_raw,
    process_tesouro_bronze_layer,
    process_tesouro_silver_layer,
    create_tesouro_catalog_entry
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the Tesouro Direto data pipeline
    
    Returns:
        Kedro Pipeline for Tesouro Direto data processing
    """
    return pipeline([
        # Extract raw data from all configured series
        node(
            func=extract_tesouro_all_series_raw,
            inputs="params:tesouro_direto",
            outputs="tesouro_direto_raw_data_container",
            name="extract_tesouro_all_series_raw",
            tags=["tesouro_direto", "extraction", "raw"]
        ),
        
        # Process raw data to bronze layer
        node(
            func=process_tesouro_bronze_layer,
            inputs=["tesouro_direto_raw_data_container", "params:tesouro_direto"],
            outputs="tesouro_direto_bronze",
            name="process_tesouro_bronze_layer",
            tags=["tesouro_direto", "bronze", "processing"]
        ),
        
        # Process bronze to silver layer
        node(
            func=process_tesouro_silver_layer,
            inputs=["tesouro_direto_bronze", "params:tesouro_direto"],
            outputs="tesouro_direto_silver",
            name="process_tesouro_silver_layer",
            tags=["tesouro_direto", "silver", "processing"]
        ),
        
        # Create catalog metadata
        node(
            func=create_tesouro_catalog_entry,
            inputs="params:tesouro_direto",
            outputs="tesouro_direto_catalog",
            name="create_tesouro_catalog_entry",
            tags=["tesouro_direto", "metadata", "catalog"]
        )
    ])
