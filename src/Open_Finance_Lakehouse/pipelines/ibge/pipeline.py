"""
IBGE Pipeline Definition

Este módulo define o pipeline para processamento de dados do IBGE,
incluindo extração, transformação e validação de indicadores econômicos.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_ibge_series_raw,
    process_ibge_series_bronze,
    process_ibge_series_silver,
    validate_ibge_series_bronze,
    create_ibge_catalog_entry
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Cria o pipeline completo do IBGE
    
    Returns:
        Pipeline configurado para todas as séries IBGE
    """
    # Lista das séries IBGE a serem processadas
    ibge_series = [
        "ipca",
        "inpc", 
        "pib_mensal",
        "pib_trimestral",
        "desemprego",
        "renda_media",
        "populacao_economicamente_ativa"
    ]
    
    pipeline_nodes = []
    
    # Criar nós para cada série
    for series in ibge_series:
        # Catalog entry node
        pipeline_nodes.append(
            node(
                func=create_ibge_catalog_entry,
                inputs=[f"params:ibge.series.{series}", "params:ibge"],
                outputs=f"ibge_{series}_catalog_entry",
                name=f"create_ibge_{series}_catalog",
                tags=["ibge", "catalog", series]
            )
        )
        
        # Raw data extraction node
        pipeline_nodes.append(
            node(
                func=extract_ibge_series_raw,
                inputs=[f"params:ibge.series.{series}", "params:ibge"],
                outputs=f"ibge_{series}_raw",
                name=f"extract_ibge_{series}_raw",
                tags=["ibge", "raw", "extraction", series]
            )
        )
        
        # Bronze layer processing node
        pipeline_nodes.append(
            node(
                func=process_ibge_series_bronze,
                inputs=[f"ibge_{series}_raw", f"params:ibge.series.{series}", "params:ibge"],
                outputs=f"ibge_{series}_bronze",
                name=f"process_ibge_{series}_bronze",
                tags=["ibge", "bronze", "processing", series]
            )
        )
        
        # Silver layer processing node
        pipeline_nodes.append(
            node(
                func=process_ibge_series_silver,
                inputs=[f"ibge_{series}_bronze", f"params:ibge.series.{series}.name", "params:ibge"],
                outputs=f"ibge_{series}_silver",
                name=f"process_ibge_{series}_silver",
                tags=["ibge", "silver", "processing", series]
            )
        )
        
        # Validation node
        pipeline_nodes.append(
            node(
                func=validate_ibge_series_bronze,
                inputs=[f"ibge_{series}_bronze", f"params:ibge.series.{series}.name", "params:ibge"],
                outputs=f"ibge_{series}_validation_report",
                name=f"validate_ibge_{series}_bronze",
                tags=["ibge", "validation", "quality", series]
            )
        )
    
    return pipeline(pipeline_nodes)


def create_extraction_pipeline(**kwargs) -> Pipeline:
    """
    Cria pipeline apenas para extração de dados brutos
    
    Returns:
        Pipeline de extração do IBGE
    """
    ibge_series = [
        "ipca", "inpc", "pib_mensal", "pib_trimestral", 
        "desemprego", "renda_media", "populacao_economicamente_ativa"
    ]
    
    nodes = []
    for series in ibge_series:
        nodes.append(
            node(
                func=extract_ibge_series_raw,
                inputs=[f"params:ibge.series.{series}.name", "params:ibge"],
                outputs=f"ibge_{series}_raw",
                name=f"extract_ibge_{series}_raw",
                tags=["ibge", "raw", "extraction", series]
            )
        )
    
    return pipeline(nodes)


def create_processing_pipeline(**kwargs) -> Pipeline:
    """
    Cria pipeline apenas para processamento Bronze->Silver
    
    Returns:
        Pipeline de processamento do IBGE
    """
    ibge_series = [
        "ipca", "inpc", "pib_mensal", "pib_trimestral",
        "desemprego", "renda_media", "populacao_economicamente_ativa"
    ]
    
    nodes = []
    for series in ibge_series:
        # Bronze processing
        nodes.append(
            node(
                func=process_ibge_series_bronze,
                inputs=[f"ibge_{series}_raw", f"params:ibge.series.{series}.name", "params:ibge"],
                outputs=f"ibge_{series}_bronze",
                name=f"process_ibge_{series}_bronze",
                tags=["ibge", "bronze", "processing", series]
            )
        )
        
        # Silver processing
        nodes.append(
            node(
                func=process_ibge_series_silver,
                inputs=[f"ibge_{series}_bronze", f"params:ibge.series.{series}.name", "params:ibge"],
                outputs=f"ibge_{series}_silver",
                name=f"process_ibge_{series}_silver",
                tags=["ibge", "silver", "processing", series]
            )
        )
    
    return pipeline(nodes)
