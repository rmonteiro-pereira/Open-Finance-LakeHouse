"""
ANBIMA Pipeline Nodes - Simplified Version
Versão simplificada para demonstração sem dependências do Spark
"""

import json
import logging
from datetime import datetime
from typing import Any

from open_finance_lakehouse.utils.anbima_api import fetch_anbima_series

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ANBIMA Data Extraction Functions (same as before)
def extract_anbima_cdi_raw() -> str:
    """Extract CDI data from ANBIMA API"""
    logger.info("🏦 Extracting ANBIMA CDI data...")
    return fetch_anbima_series("cdi")


def extract_anbima_ima_b_raw() -> str:
    """Extract IMA-B data from ANBIMA API"""
    logger.info("📊 Extracting ANBIMA IMA-B data...")
    return fetch_anbima_series("ima_b")


def extract_anbima_ima_b_5_raw() -> str:
    """Extract IMA-B 5 years data from ANBIMA API"""
    logger.info("📈 Extracting ANBIMA IMA-B 5 years data...")
    return fetch_anbima_series("ima_b_5")


def extract_anbima_curva_di_raw() -> str:
    """Extract DI yield curve data from ANBIMA API"""
    logger.info("📉 Extracting ANBIMA DI yield curve data...")
    return fetch_anbima_series("curva_di")


def extract_anbima_curva_pre_raw() -> str:
    """Extract Pre-fixed yield curve data from ANBIMA API"""
    logger.info("📊 Extracting ANBIMA Pre-fixed yield curve data...")
    return fetch_anbima_series("curva_pre")


def extract_anbima_curva_ipca_raw() -> str:
    """Extract IPCA+ yield curve data from ANBIMA API"""
    logger.info("💹 Extracting ANBIMA IPCA+ yield curve data...")
    return fetch_anbima_series("curva_ipca")


def extract_anbima_ntn_b_raw() -> str:
    """Extract NTN-B data from ANBIMA API"""
    logger.info("🏛️ Extracting ANBIMA NTN-B data...")
    return fetch_anbima_series("ntn_b")


def extract_anbima_ltn_raw() -> str:
    """Extract LTN data from ANBIMA API"""
    logger.info("💰 Extracting ANBIMA LTN data...")
    return fetch_anbima_series("ltn")


def extract_anbima_mercado_secundario_raw() -> str:
    """Extract secondary market data from ANBIMA API"""
    logger.info("🏪 Extracting ANBIMA secondary market data...")
    return fetch_anbima_series("mercado_secundario")


# Simplified Bronze Layer Processing (no Spark)
def process_anbima_bronze_simple(
    cdi_raw: str,
    ima_b_raw: str,
    ima_b_5_raw: str,
    curva_di_raw: str,
    curva_pre_raw: str,
    curva_ipca_raw: str,
    ntn_b_raw: str,
    ltn_raw: str,
    mercado_secundario_raw: str,
) -> list[dict[str, Any]]:
    """
    Process ANBIMA raw data into bronze layer (simplified without Spark)
    
    Returns:
        List of normalized records
    """
    logger.info("🔄 Processing ANBIMA data to bronze layer (simplified)...")
    
    all_records = []
    
    # Map of raw data inputs
    raw_data_map = {
        "cdi": cdi_raw,
        "ima_b": ima_b_raw,
        "ima_b_5": ima_b_5_raw,
        "curva_di": curva_di_raw,
        "curva_pre": curva_pre_raw,
        "curva_ipca": curva_ipca_raw,
        "ntn_b": ntn_b_raw,
        "ltn": ltn_raw,
        "mercado_secundario": mercado_secundario_raw,
    }
    
    # Process each series
    for series_name, series_raw_data in raw_data_map.items():
        try:
            if series_raw_data and series_raw_data != "[]":
                records = json.loads(series_raw_data)
                for record in records:
                    # Standardize record format
                    standardized_record = {
                        "date": record.get("date"),
                        "rate": float(record.get("rate", 0)),
                        "series": record.get("series", series_name),
                        "source": "ANBIMA",
                        "raw_date": record.get("raw_date"),
                        "ingested_at": record.get("ingested_at"),
                        "index_value": record.get("index_value"),
                        "daily_return": record.get("daily_return"),
                        "vertex": record.get("vertex"),
                        "yield": record.get("yield"),
                        "maturity": record.get("maturity"),
                        "yield_to_maturity": record.get("yield_to_maturity"),
                        "processed_at": datetime.now().isoformat(),
                        "layer": "bronze"
                    }
                    all_records.append(standardized_record)
                    
        except Exception as e:
            logger.error(f"Error processing {series_name}: {e}")
            continue
    
    logger.info(f"✅ Processed {len(all_records)} ANBIMA records to bronze layer")
    return all_records


# Simplified Silver Layer Processing
def process_anbima_silver_simple(bronze_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Process ANBIMA bronze data into silver layer (simplified without Spark)
    """
    logger.info("✨ Processing ANBIMA data to silver layer (simplified)...")
    
    if not bronze_records:
        logger.warning("No bronze data to process to silver")
        return []
    
    silver_records = []
    
    for record in bronze_records:
        # Data quality validations
        if (
            record.get("date") 
            and record.get("rate") is not None 
            and record.get("rate") >= 0
        ):
            # Create silver record with quality indicators
            silver_record = record.copy()
            silver_record["layer"] = "silver"
            silver_record["processed_at"] = datetime.now().isoformat()
            
            # Add data quality flag
            rate = record.get("rate", 0)
            if rate > 50:
                silver_record["data_quality"] = "high_rate"
            elif rate < 0:
                silver_record["data_quality"] = "negative_rate"
            else:
                silver_record["data_quality"] = "normal"
            
            silver_records.append(silver_record)
    
    logger.info(f"✅ Processed {len(silver_records)} ANBIMA records to silver layer")
    return silver_records


# Simplified Catalog Functions
def create_anbima_catalog_entry_simple(silver_records: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Create catalog entry for ANBIMA data (simplified without Spark)
    """
    logger.info("📋 Creating ANBIMA catalog entry (simplified)...")
    
    if not silver_records:
        logger.warning("No silver data for catalog")
        return {"dataset": "anbima_financial_data", "status": "empty"}
    
    # Count series
    series_counts = {}
    dates = []
    
    for record in silver_records:
        series = record.get("series", "unknown")
        series_counts[series] = series_counts.get(series, 0) + 1
        
        if record.get("date"):
            dates.append(record["date"])
    
    catalog_entry = {
        "dataset": "anbima_financial_data",
        "source": "ANBIMA",
        "description": "Brazilian financial market data from ANBIMA",
        "total_records": len(silver_records),
        "series_count": len(series_counts),
        "series_breakdown": series_counts,
        "date_range": {
            "min_date": min(dates) if dates else None,
            "max_date": max(dates) if dates else None,
        },
        "created_at": datetime.now().isoformat(),
        "layer": "silver",
        "data_types": [
            "interest_rates",
            "bond_indices", 
            "yield_curves",
            "government_bonds",
            "secondary_market"
        ],
        "update_frequency": "daily",
        "business_days_only": True,
    }
    
    logger.info(f"✅ Created catalog for ANBIMA with {catalog_entry['total_records']} records")
    return catalog_entry
