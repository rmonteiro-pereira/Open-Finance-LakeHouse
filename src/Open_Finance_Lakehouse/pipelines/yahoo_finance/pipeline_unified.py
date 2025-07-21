"""
Yahoo Finance Unified Pipeline Definition
Follows the same pattern as ANBIMA pipeline for consistency
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes_unified import (
    extract_yahoo_all_series_raw,
    process_yahoo_bronze_layer,
    process_yahoo_silver_layer,
    create_yahoo_catalog_entry
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Create unified Yahoo Finance pipeline
    
    Returns:
        Pipeline configured for Yahoo Finance data processing
    """
    return pipeline([
        # Create catalog entry
        node(
            func=create_yahoo_catalog_entry,
            inputs="params:yahoo_finance",
            outputs="yahoo_finance_catalog",
            name="create_yahoo_finance_catalog_entry",
            tags=["yahoo_finance", "catalog", "metadata"]
        ),
        
        # Extract raw data from all series
        node(
            func=extract_yahoo_all_series_raw,
            inputs="params:yahoo_finance", 
            outputs="yahoo_finance_raw_data_container",
            name="extract_yahoo_finance_all_series_raw",
            tags=["yahoo_finance", "raw", "extraction"]
        ),
        
        # Process raw data to bronze layer
        node(
            func=process_yahoo_bronze_layer,
            inputs=["yahoo_finance_raw_data_container", "params:yahoo_finance"],
            outputs="yahoo_finance_bronze",
            name="process_yahoo_finance_bronze_layer", 
            tags=["yahoo_finance", "bronze", "processing"]
        ),
        
        # Process bronze data to silver layer
        node(
            func=process_yahoo_silver_layer,
            inputs=["yahoo_finance_bronze", "params:yahoo_finance"],
            outputs="yahoo_finance_silver",
            name="process_yahoo_finance_silver_layer",
            tags=["yahoo_finance", "silver", "processing"]
        )
    ])


def create_extraction_pipeline(**kwargs) -> Pipeline:
    """
    Create extraction-only pipeline for Yahoo Finance
    
    Returns:
        Pipeline for Yahoo Finance data extraction
    """
    return pipeline([
        node(
            func=extract_yahoo_all_series_raw,
            inputs="params:yahoo_finance",
            outputs="yahoo_finance_raw_data_container", 
            name="extract_yahoo_finance_all_series_raw",
            tags=["yahoo_finance", "raw", "extraction"]
        )
    ])


def create_processing_pipeline(**kwargs) -> Pipeline:
    """
    Create processing-only pipeline for Yahoo Finance
    
    Returns:
        Pipeline for Yahoo Finance bronze->silver processing
    """
    return pipeline([
        node(
            func=process_yahoo_bronze_layer,
            inputs=["yahoo_finance_raw_data_container", "params:yahoo_finance"],
            outputs="yahoo_finance_bronze",
            name="process_yahoo_finance_bronze_layer",
            tags=["yahoo_finance", "bronze", "processing"]
        ),
        
        node(
            func=process_yahoo_silver_layer,
            inputs=["yahoo_finance_bronze", "params:yahoo_finance"],
            outputs="yahoo_finance_silver",
            name="process_yahoo_finance_silver_layer",
            tags=["yahoo_finance", "silver", "processing"]
        )
    ])
