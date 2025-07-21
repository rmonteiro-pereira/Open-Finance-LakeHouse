"""
ANBIMA API Integration Module
Associação Brasileira das Entidades dos Mercados Financeiro e de Capitais

Provides functions to fetch financial market data from ANBIMA:
- CDI rates, IMA-B indices, yield curves, government bonds, secondary market
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ANBIMA API Configuration
ANBIMA_BASE_URL = "https://api.anbima.com.br/feed/dados"

# Constants
WEEKEND_START_DAY = 5  # Saturday

# ANBIMA Series Configuration with sample data endpoints
ANBIMA_SERIES_CONFIG = {
    "cdi": {
        "endpoint": "indicadores/cdi",
        "description": "CDI - Certificado de Depósito Interbancário"
    },
    "ima_b": {
        "endpoint": "indices/ima-b",
        "description": "IMA-B - Índice de Mercado ANBIMA Brasil"
    },
    "ima_b_5": {
        "endpoint": "indices/ima-b-5",
        "description": "IMA-B 5 anos - Índice de Mercado ANBIMA"
    },
    "curva_di": {
        "endpoint": "curvas/di",
        "description": "Curva de Juros DI - Depósito Interbancário"
    },
    "curva_pre": {
        "endpoint": "curvas/pre",
        "description": "Curva de Juros Prefixada"
    },
    "curva_ipca": {
        "endpoint": "curvas/ipca",
        "description": "Curva de Juros IPCA+"
    },
    "ntn_b": {
        "endpoint": "titulos/ntn-b",
        "description": "NTN-B - Notas do Tesouro Nacional Série B"
    },
    "ltn": {
        "endpoint": "titulos/ltn",
        "description": "LTN - Letras do Tesouro Nacional"
    },
    "mercado_secundario": {
        "endpoint": "mercado-secundario/precos",
        "description": "Mercado Secundário - Preços e Taxas"
    }
}


def get_sample_rate_for_series(series_name: str, day_index: int) -> float:
    """Calculate sample rate based on series name and day index"""
    rate_configs = {
        "cdi": {"base": 10.75, "increment": 0.01},
        "ima_b": {"base": 3500.0, "increment": 2.5},
        "curva_di": {"base": 11.25, "increment": 0.005},
        "curva_pre": {"base": 12.50, "increment": 0.008},
        "curva_ipca": {"base": 6.75, "increment": 0.003},
        "ntn_b": {"base": 6.25, "increment": 0.002},
        "ltn": {"base": 11.80, "increment": 0.006},
    }
    
    config = rate_configs.get(series_name, {"base": 100.0, "increment": 0.1})
    return config["base"] + (day_index * config["increment"])


def generate_sample_anbima_data(series_name: str) -> list[dict[str, Any]]:
    """
    Generate sample ANBIMA data for demonstration
    
    Args:
        series_name: Name of the ANBIMA series
        
    Returns:
        List of sample data records
    """
    base_date = datetime.now() - timedelta(days=30)
    sample_data = []
    
    for i in range(30):  # 30 days of sample data
        current_date = base_date + timedelta(days=i)
        
        # Skip weekends for financial data
        if current_date.weekday() >= WEEKEND_START_DAY:
            continue
            
        rate = get_sample_rate_for_series(series_name, i)
            
        record = {
            "date": current_date.strftime("%Y-%m-%d"),
            "rate": round(rate, 4),
            "series": series_name,
            "source": "ANBIMA",
            "raw_date": current_date.strftime("%Y%m%d"),
            "ingested_at": datetime.now().isoformat()
        }
        
        # Add series-specific fields
        if series_name.startswith("ima_"):
            record["index_value"] = rate
            record["daily_return"] = round((i * 0.01), 4)
        elif series_name.startswith("curva_"):
            record["vertex"] = "252"  # 1 year vertex
            record["yield"] = rate
        elif series_name in ["ntn_b", "ltn"]:
            record["maturity"] = (current_date + timedelta(days=365)).strftime("%Y-%m-%d")
            record["yield_to_maturity"] = rate
            
        sample_data.append(record)
    
    return sample_data


def fetch_anbima_data(endpoint: str) -> list[dict[str, Any]]:
    """
    Fetch data from ANBIMA API (using sample data for now)
    
    Args:
        endpoint: API endpoint
        
    Returns:
        List of data records
    """
    try:
        # For now, return sample data since ANBIMA API requires authentication
        # In production, this would make actual API calls
        logger.info(f"[ANBIMA] Simulating fetch from endpoint: {endpoint}")
        
        # Extract series name from endpoint
        series_name = endpoint.split("/")[-1].replace("-", "_")
        
        sample_data = generate_sample_anbima_data(series_name)
        
        logger.info(f"[ANBIMA] Generated {len(sample_data)} sample records")
        return sample_data
        
    except Exception as e:
        logger.error(f"[ANBIMA] Error fetching data from {endpoint}: {e}")
        return []


def normalize_anbima_data(raw_data: list[dict[str, Any]], series_name: str) -> list[dict[str, Any]]:
    """
    Normalize ANBIMA data to standard format
    
    Args:
        raw_data: Raw data from ANBIMA API
        series_name: Name of the series for identification
        
    Returns:
        Normalized data records
    """
    normalized_records = []
    
    try:
        for record in raw_data:
            # Basic normalization - data is already in good format from sample generator
            normalized_record = {
                "date": record.get("date"),
                "rate": record.get("rate", record.get("index_value", record.get("yield", 0))),
                "series": series_name,
                "source": "ANBIMA",
                "raw_date": record.get("raw_date"),
                "ingested_at": record.get("ingested_at", datetime.now().isoformat())
            }
            
            # Add additional fields based on series type
            if "index_value" in record:
                normalized_record["index_value"] = record["index_value"]
                normalized_record["daily_return"] = record.get("daily_return", 0)
            
            if "vertex" in record:
                normalized_record["vertex"] = record["vertex"]
                normalized_record["yield"] = record["yield"]
                
            if "maturity" in record:
                normalized_record["maturity"] = record["maturity"]
                normalized_record["yield_to_maturity"] = record["yield_to_maturity"]
                
            normalized_records.append(normalized_record)
            
    except Exception as e:
        logger.error(f"[ANBIMA] Error normalizing data for {series_name}: {e}")
        
    logger.info(f"[ANBIMA] Normalized {len(normalized_records)} records for {series_name}")
    return normalized_records


def fetch_anbima_series(series_name: str) -> str:
    """
    Fetch specific ANBIMA series data
    
    Args:
        series_name: Name of the series to fetch
        
    Returns:
        JSON string with the fetched data
    """
    if series_name not in ANBIMA_SERIES_CONFIG:
        logger.error(f"Unknown ANBIMA series: {series_name}")
        return json.dumps([])
        
    config = ANBIMA_SERIES_CONFIG[series_name]
    
    logger.info(f"[ANBIMA] Starting fetch for series: {series_name}")
    start_time = time.time()
    
    try:
        # Fetch raw data from API
        raw_data = fetch_anbima_data(config["endpoint"])
        
        if not raw_data:
            logger.warning(f"[ANBIMA] No data retrieved for {series_name}")
            return json.dumps([])
            
        # Normalize data
        normalized_data = normalize_anbima_data(raw_data, series_name)
        
        # Convert to JSON
        result_json = json.dumps(normalized_data, ensure_ascii=False, indent=2)
        
        duration = time.time() - start_time
        logger.info(f"[ANBIMA] Successfully fetched {series_name} in {duration:.2f}s")
        
        return result_json
        
    except Exception as e:
        logger.error(f"[ANBIMA] Error fetching {series_name}: {e}")
        return json.dumps([])


def get_available_anbima_series() -> dict[str, str]:
    """
    Get list of available ANBIMA series
    
    Returns:
        Dictionary mapping series names to descriptions
    """
    return {name: config["description"] for name, config in ANBIMA_SERIES_CONFIG.items()}


if __name__ == "__main__":
    # Test ANBIMA API integration
    print("🏦 ANBIMA API Integration Test")
    print("Available series:")
    
    for series, description in get_available_anbima_series().items():
        print(f"  - {series}: {description}")
        
    # Test fetch for CDI
    print("\n📊 Testing CDI fetch...")
    cdi_data = fetch_anbima_series("cdi")
    
    if cdi_data and cdi_data != "[]":
        data = json.loads(cdi_data)
        print(f"✅ Successfully fetched {len(data)} CDI records")
        if data:
            print(f"Latest record: {data[-1]}")
    else:
        print("❌ Failed to fetch CDI data")
