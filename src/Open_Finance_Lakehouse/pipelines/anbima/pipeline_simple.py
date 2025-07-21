"""
ANBIMA Pipeline Definition - Simplified
Pipeline simplificado para execução direta no Kedro
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes_simple import (
    create_anbima_catalog_entry_simple,
    extract_anbima_cdi_raw,
    extract_anbima_ima_b_raw,
    extract_anbima_curva_di_raw,
    extract_anbima_ntn_b_raw,
    extract_anbima_ltn_raw,
    process_anbima_bronze_simple,
    process_anbima_silver_simple,
)


def create_anbima_simple_pipeline(**kwargs) -> Pipeline:
    """Create simple ANBIMA pipeline for Kedro execution"""
    return pipeline(
        [
            # Extract raw data from ANBIMA API
            node(
                func=extract_anbima_cdi_raw,
                inputs=None,
                outputs="anbima_cdi_raw_simple",
                name="extract_anbima_cdi_raw_simple",
                tags=["anbima", "extraction"],
            ),
            node(
                func=extract_anbima_ima_b_raw,
                inputs=None,
                outputs="anbima_ima_b_raw_simple",
                name="extract_anbima_ima_b_raw_simple",
                tags=["anbima", "extraction"],
            ),
            node(
                func=extract_anbima_curva_di_raw,
                inputs=None,
                outputs="anbima_curva_di_raw_simple",
                name="extract_anbima_curva_di_raw_simple",
                tags=["anbima", "extraction"],
            ),
            node(
                func=extract_anbima_ntn_b_raw,
                inputs=None,
                outputs="anbima_ntn_b_raw_simple",
                name="extract_anbima_ntn_b_raw_simple",
                tags=["anbima", "extraction"],
            ),
            node(
                func=extract_anbima_ltn_raw,
                inputs=None,
                outputs="anbima_ltn_raw_simple",
                name="extract_anbima_ltn_raw_simple",
                tags=["anbima", "extraction"],
            ),
            # Process to bronze layer
            node(
                func=lambda cdi, ima_b, curva_di, ntn_b, ltn: process_anbima_bronze_simple(
                    cdi_raw=cdi,
                    ima_b_raw=ima_b,
                    ima_b_5_raw="[]",
                    curva_di_raw=curva_di,
                    curva_pre_raw="[]",
                    curva_ipca_raw="[]",
                    ntn_b_raw=ntn_b,
                    ltn_raw=ltn,
                    mercado_secundario_raw="[]"
                ),
                inputs=[
                    "anbima_cdi_raw_simple",
                    "anbima_ima_b_raw_simple",
                    "anbima_curva_di_raw_simple",
                    "anbima_ntn_b_raw_simple",
                    "anbima_ltn_raw_simple",
                ],
                outputs="anbima_bronze_simple",
                name="process_anbima_bronze_simple",
                tags=["anbima", "bronze"],
            ),
            # Process to silver layer
            node(
                func=process_anbima_silver_simple,
                inputs="anbima_bronze_simple",
                outputs="anbima_silver_simple",
                name="process_anbima_silver_simple",
                tags=["anbima", "silver"],
            ),
            # Create catalog entry
            node(
                func=create_anbima_catalog_entry_simple,
                inputs="anbima_silver_simple",
                outputs="anbima_catalog_simple",
                name="create_anbima_catalog_simple",
                tags=["anbima", "catalog"],
            ),
        ],
        tags=["anbima"],
    )


def create_pipeline(**kwargs) -> Pipeline:
    """Default pipeline creation function"""
    return create_anbima_simple_pipeline(**kwargs)
