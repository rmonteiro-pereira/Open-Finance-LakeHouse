"""
IBGE Pipeline - Versão Simplificada
Pipeline para coleta e processamento de dados do IBGE.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes_simple import (
    extract_ibge_ipca_raw,
    extract_ibge_inpc_raw,
    extract_ibge_pib_mensal_raw,
    extract_ibge_pib_trimestral_raw,
    extract_ibge_desemprego_raw,
    extract_ibge_renda_media_raw,
    extract_ibge_populacao_economicamente_ativa_raw,
    process_ibge_bronze,
    process_ibge_silver,
    create_ibge_catalog_entry
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Cria pipeline completo do IBGE
    
    Returns:
        Pipeline do Kedro
    """
    
    pipeline_nodes = []
    
    # IPCA Pipeline
    pipeline_nodes.extend([
        # Raw extraction
        node(
            func=extract_ibge_ipca_raw,
            inputs=["params:ibge"],
            outputs="ibge_ipca_raw",
            name="extract_ibge_ipca_raw",
            tags=["ibge", "raw", "ipca"]
        ),
        # Bronze processing
        node(
            func=process_ibge_bronze,
            inputs=["ibge_ipca_raw", "params:ipca_series_name"],
            outputs="ibge_ipca_bronze",
            name="process_ibge_ipca_bronze",
            tags=["ibge", "bronze", "ipca"]
        ),
        # Silver processing
        node(
            func=process_ibge_silver,
            inputs=["ibge_ipca_bronze", "params:ipca_series_name"],
            outputs="ibge_ipca_silver",
            name="process_ibge_ipca_silver",
            tags=["ibge", "silver", "ipca"]
        ),
        # Catalog entry
        node(
            func=create_ibge_catalog_entry,
            inputs=["params:ipca_series_name"],
            outputs="ibge_ipca_catalog_entry",
            name="create_ibge_ipca_catalog",
            tags=["ibge", "catalog", "ipca"]
        )
    ])
    
    # INPC Pipeline
    pipeline_nodes.extend([
        # Raw extraction
        node(
            func=extract_ibge_inpc_raw,
            inputs=["params:ibge"],
            outputs="ibge_inpc_raw",
            name="extract_ibge_inpc_raw",
            tags=["ibge", "raw", "inpc"]
        ),
        # Bronze processing
        node(
            func=process_ibge_bronze,
            inputs=["ibge_inpc_raw", "params:inpc_series_name"],
            outputs="ibge_inpc_bronze",
            name="process_ibge_inpc_bronze",
            tags=["ibge", "bronze", "inpc"]
        ),
        # Silver processing
        node(
            func=process_ibge_silver,
            inputs=["ibge_inpc_bronze", "params:inpc_series_name"],
            outputs="ibge_inpc_silver",
            name="process_ibge_inpc_silver",
            tags=["ibge", "silver", "inpc"]
        ),
        # Catalog entry
        node(
            func=create_ibge_catalog_entry,
            inputs=["params:inpc_series_name"],
            outputs="ibge_inpc_catalog_entry",
            name="create_ibge_inpc_catalog",
            tags=["ibge", "catalog", "inpc"]
        )
    ])
    
    return pipeline(pipeline_nodes)


def create_extraction_pipeline(**kwargs) -> Pipeline:
    """
    Pipeline apenas para extração de dados brutos
    """
    return pipeline([
        node(
            func=extract_ibge_ipca_raw,
            inputs=["params:ibge"],
            outputs="ibge_ipca_raw",
            name="extract_ibge_ipca_raw",
            tags=["ibge", "raw", "ipca"]
        ),
        node(
            func=extract_ibge_inpc_raw,
            inputs=["params:ibge"],
            outputs="ibge_inpc_raw",
            name="extract_ibge_inpc_raw",
            tags=["ibge", "raw", "inpc"]
        )
    ])


def create_processing_pipeline(**kwargs) -> Pipeline:
    """
    Pipeline apenas para processamento (Bronze + Silver)
    """
    return pipeline([
        # IPCA processing
        node(
            func=process_ibge_bronze,
            inputs=["ibge_ipca_raw", "params:ipca_series_name"],
            outputs="ibge_ipca_bronze",
            name="process_ibge_ipca_bronze",
            tags=["ibge", "bronze", "ipca"]
        ),
        node(
            func=process_ibge_silver,
            inputs=["ibge_ipca_bronze", "params:ipca_series_name"],
            outputs="ibge_ipca_silver",
            name="process_ibge_ipca_silver",
            tags=["ibge", "silver", "ipca"]
        )
    ])
