"""
Yahoo Finance Pipeline Definition
Creates ETL pipelines for Yahoo Finance financial data
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_yahoo_series_raw,
    process_yahoo_series_bronze,
    validate_yahoo_series_bronze,
    process_yahoo_series_silver,
    create_yahoo_catalog_entry
)


def create_yahoo_series_pipeline(series_name: str) -> Pipeline:
    """
    Create a complete ETL pipeline for a specific Yahoo Finance series
    
    Args:
        series_name: Name of the Yahoo Finance series to process
        
    Returns:
        Kedro Pipeline object
    """
    
    return pipeline(
        [
            # Extract raw data from Yahoo Finance API
            node(
                func=extract_yahoo_series_raw,
                inputs=[f"params:series_name_{series_name}", "params:yahoo_finance"],
                outputs=f"yahoo_{series_name}_raw",
                name=f"extract_yahoo_{series_name}_raw",
                tags=["yahoo_finance", "extraction", "raw_layer", series_name]
            ),
            
            # Process to Bronze layer
            node(
                func=process_yahoo_series_bronze,
                inputs=[f"yahoo_{series_name}_raw", f"params:series_name_{series_name}", "params:yahoo_finance"],
                outputs=f"yahoo_{series_name}_bronze",
                name=f"process_yahoo_{series_name}_bronze",
                tags=["yahoo_finance", "bronze_layer", "processing", series_name]
            ),
            
            # Validate Bronze layer data
            node(
                func=validate_yahoo_series_bronze,
                inputs=[f"yahoo_{series_name}_bronze", f"params:series_name_{series_name}", "params:yahoo_finance"],
                outputs=f"yahoo_{series_name}_validation_results",
                name=f"validate_yahoo_{series_name}_bronze",
                tags=["yahoo_finance", "validation", "quality", series_name]
            ),
            
            # Process to Silver layer with features
            node(
                func=process_yahoo_series_silver,
                inputs=[f"yahoo_{series_name}_bronze", f"params:series_name_{series_name}", "params:yahoo_finance"],
                outputs=f"yahoo_{series_name}_silver",
                name=f"process_yahoo_{series_name}_silver",
                tags=["yahoo_finance", "silver_layer", "features", series_name]
            ),
            
            # Create catalog entry
            node(
                func=create_yahoo_catalog_entry,
                inputs=[f"params:series_name_{series_name}", "params:yahoo_finance"],
                outputs=f"yahoo_{series_name}_catalog_entry",
                name=f"create_yahoo_{series_name}_catalog",
                tags=["yahoo_finance", "catalog", "metadata", series_name]
            )
        ],
        tags=["yahoo_finance", series_name]
    )


def create_yahoo_etf_pipeline() -> Pipeline:
    """Create pipeline for all Yahoo Finance ETF series"""
    etf_series = ["bova11", "ivvb11", "smal11", "nasd11", "spxi11", "fixa11"]
    
    etf_pipelines = []
    for series in etf_series:
        etf_pipelines.append(create_yahoo_series_pipeline(series))
    
    return sum(etf_pipelines)


def create_yahoo_commodity_pipeline() -> Pipeline:
    """Create pipeline for all Yahoo Finance commodity series"""
    commodity_series = ["petroleo", "ouro", "soja", "cafe"]
    
    commodity_pipelines = []
    for series in commodity_series:
        commodity_pipelines.append(create_yahoo_series_pipeline(series))
    
    return sum(commodity_pipelines)


def create_yahoo_currency_pipeline() -> Pipeline:
    """Create pipeline for Yahoo Finance currency series"""
    return create_yahoo_series_pipeline("usd_brl")


def create_yahoo_finance_full_pipeline() -> Pipeline:
    """
    Create the complete Yahoo Finance pipeline with all series
    
    Returns:
        Full Yahoo Finance ETL pipeline
    """
    return (
        create_yahoo_etf_pipeline() +
        create_yahoo_commodity_pipeline() +
        create_yahoo_currency_pipeline()
    )
