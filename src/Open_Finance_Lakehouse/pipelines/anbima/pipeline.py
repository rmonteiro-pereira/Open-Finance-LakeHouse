"""
ANBIMA Pipeline Definition
Pipeline for processing Brazilian financial market data from ANBIMA
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    create_anbima_catalog_entry,
    extract_anbima_cdi_raw,
    extract_anbima_curva_di_raw,
    extract_anbima_curva_ipca_raw,
    extract_anbima_curva_pre_raw,
    extract_anbima_ima_b_5_raw,
    extract_anbima_ima_b_raw,
    extract_anbima_ltn_raw,
    extract_anbima_mercado_secundario_raw,
    extract_anbima_ntn_b_raw,
    process_anbima_bronze,
    process_anbima_silver,
    ANBIMARawData,
)


def create_anbima_extraction_pipeline(**kwargs) -> Pipeline:
    """Create ANBIMA data extraction pipeline"""
    return pipeline(
        [
            # Extract raw data from ANBIMA API
            node(
                func=extract_anbima_cdi_raw,
                inputs=None,
                outputs="anbima_cdi_raw",
                name="extract_anbima_cdi_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_ima_b_raw,
                inputs=None,
                outputs="anbima_ima_b_raw",
                name="extract_anbima_ima_b_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_ima_b_5_raw,
                inputs=None,
                outputs="anbima_ima_b_5_raw",
                name="extract_anbima_ima_b_5_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_curva_di_raw,
                inputs=None,
                outputs="anbima_curva_di_raw",
                name="extract_anbima_curva_di_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_curva_pre_raw,
                inputs=None,
                outputs="anbima_curva_pre_raw",
                name="extract_anbima_curva_pre_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_curva_ipca_raw,
                inputs=None,
                outputs="anbima_curva_ipca_raw",
                name="extract_anbima_curva_ipca_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_ntn_b_raw,
                inputs=None,
                outputs="anbima_ntn_b_raw",
                name="extract_anbima_ntn_b_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_ltn_raw,
                inputs=None,
                outputs="anbima_ltn_raw",
                name="extract_anbima_ltn_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_mercado_secundario_raw,
                inputs=None,
                outputs="anbima_mercado_secundario_raw",
                name="extract_anbima_mercado_secundario_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
        ],
        namespace="anbima_extraction",
        tags=["anbima", "extraction"],
    )


def create_anbima_processing_pipeline(**kwargs) -> Pipeline:
    """Create ANBIMA data processing pipeline"""
    return pipeline(
        [
            # Process to bronze layer using lambda to handle multiple inputs
            node(
                func=lambda cdi, ima_b, ima_b_5, curva_di, curva_pre, curva_ipca, ntn_b, ltn, mercado_secundario: process_anbima_bronze(
                    ANBIMARawData(
                        cdi=cdi,
                        ima_b=ima_b,
                        ima_b_5=ima_b_5,
                        curva_di=curva_di,
                        curva_pre=curva_pre,
                        curva_ipca=curva_ipca,
                        ntn_b=ntn_b,
                        ltn=ltn,
                        mercado_secundario=mercado_secundario,
                    )
                ),
                inputs=[
                    "anbima_cdi_raw",
                    "anbima_ima_b_raw",
                    "anbima_ima_b_5_raw",
                    "anbima_curva_di_raw",
                    "anbima_curva_pre_raw",
                    "anbima_curva_ipca_raw",
                    "anbima_ntn_b_raw",
                    "anbima_ltn_raw",
                    "anbima_mercado_secundario_raw",
                ],
                outputs="anbima_bronze",
                name="process_anbima_bronze_node",
                tags=["anbima", "processing", "bronze"],
            ),
            # Process to silver layer
            node(
                func=process_anbima_silver,
                inputs="anbima_bronze",
                outputs="anbima_silver",
                name="process_anbima_silver_node",
                tags=["anbima", "processing", "silver"],
            ),
            # Create catalog entry
            node(
                func=create_anbima_catalog_entry,
                inputs="anbima_silver",
                outputs="anbima_catalog",
                name="create_anbima_catalog_node",
                tags=["anbima", "catalog"],
            ),
        ],
        namespace="anbima_processing",
        tags=["anbima", "processing"],
    )


def create_anbima_pipeline(**kwargs) -> Pipeline:
    """Create complete ANBIMA pipeline without namespaces"""
    return pipeline(
        [
            # Extract raw data from ANBIMA API
            node(
                func=extract_anbima_cdi_raw,
                inputs=None,
                outputs="anbima_cdi_raw",
                name="extract_anbima_cdi_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_ima_b_raw,
                inputs=None,
                outputs="anbima_ima_b_raw",
                name="extract_anbima_ima_b_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_ima_b_5_raw,
                inputs=None,
                outputs="anbima_ima_b_5_raw",
                name="extract_anbima_ima_b_5_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_curva_di_raw,
                inputs=None,
                outputs="anbima_curva_di_raw",
                name="extract_anbima_curva_di_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_curva_pre_raw,
                inputs=None,
                outputs="anbima_curva_pre_raw",
                name="extract_anbima_curva_pre_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_curva_ipca_raw,
                inputs=None,
                outputs="anbima_curva_ipca_raw",
                name="extract_anbima_curva_ipca_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_ntn_b_raw,
                inputs=None,
                outputs="anbima_ntn_b_raw",
                name="extract_anbima_ntn_b_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_ltn_raw,
                inputs=None,
                outputs="anbima_ltn_raw",
                name="extract_anbima_ltn_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            node(
                func=extract_anbima_mercado_secundario_raw,
                inputs=None,
                outputs="anbima_mercado_secundario_raw",
                name="extract_anbima_mercado_secundario_raw_node",
                tags=["anbima", "extraction", "raw"],
            ),
            # Process to bronze layer
            node(
                func=lambda cdi, ima_b, ima_b_5, curva_di, curva_pre, curva_ipca, ntn_b, ltn, mercado_secundario: process_anbima_bronze(
                    ANBIMARawData(
                        cdi=cdi,
                        ima_b=ima_b,
                        ima_b_5=ima_b_5,
                        curva_di=curva_di,
                        curva_pre=curva_pre,
                        curva_ipca=curva_ipca,
                        ntn_b=ntn_b,
                        ltn=ltn,
                        mercado_secundario=mercado_secundario,
                    )
                ),
                inputs=[
                    "anbima_cdi_raw",
                    "anbima_ima_b_raw",
                    "anbima_ima_b_5_raw",
                    "anbima_curva_di_raw",
                    "anbima_curva_pre_raw",
                    "anbima_curva_ipca_raw",
                    "anbima_ntn_b_raw",
                    "anbima_ltn_raw",
                    "anbima_mercado_secundario_raw",
                ],
                outputs="anbima_bronze",
                name="process_anbima_bronze_node",
                tags=["anbima", "processing", "bronze"],
            ),
            # Process to silver layer
            node(
                func=process_anbima_silver,
                inputs="anbima_bronze",
                outputs="anbima_silver",
                name="process_anbima_silver_node",
                tags=["anbima", "processing", "silver"],
            ),
            # Create catalog entry
            node(
                func=create_anbima_catalog_entry,
                inputs="anbima_silver",
                outputs="anbima_catalog",
                name="create_anbima_catalog_node",
                tags=["anbima", "catalog"],
            ),
        ],
        tags=["anbima"],
    )
