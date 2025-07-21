"""
IBGE API Integration Module - Versão Simplificada
Instituto Brasileiro de Geografia e Estatística
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# IBGE API Configuration
IBGE_BASE_URL = "https://servicodados.ibge.gov.br/api/v1"

# IBGE Series Configuration - Usando API v1 que é mais estável
IBGE_SERIES_CONFIG = {
    "ipca": {
        "endpoint": "indicadores/433",  # IPCA
        "description": "IPCA - Índice Nacional de Preços ao Consumidor Amplo"
    },
    "inpc": {
        "endpoint": "indicadores/188",  # INPC
        "description": "INPC - Índice Nacional de Preços ao Consumidor"
    },
    "pib": {
        "endpoint": "indicadores/1621",  # PIB
        "description": "PIB - Produto Interno Bruto"
    },
    "desemprego": {
        "endpoint": "indicadores/4099",  # Taxa de Desemprego
        "description": "Taxa de Desemprego - PNAD Contínua"
    },
    "pea": {
        "endpoint": "indicadores/4092",  # População Economicamente Ativa
        "description": "População Economicamente Ativa"
    }
}


def fetch_ibge_data(endpoint: str) -> List[Dict]:
    """
    Fetch data from IBGE API v1
    
    Args:
        endpoint: API endpoint (e.g., "indicadores/433")
        
    Returns:
        List of data records
    """
    try:
        url = f"{IBGE_BASE_URL}/{endpoint}"
        
        logger.info(f"[IBGE] Fetching data from {url}")
        
        # Make request with timeout
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        if isinstance(data, list) and len(data) > 0:
            logger.info(f"[IBGE] Retrieved {len(data)} records")
            return data
        else:
            logger.warning(f"[IBGE] No data found for endpoint {endpoint}")
            return []
            
    except requests.RequestException as e:
        logger.error(f"[IBGE] Request error for {endpoint}: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"[IBGE] JSON decode error for {endpoint}: {e}")
        return []
    except Exception as e:
        logger.error(f"[IBGE] Unexpected error for {endpoint}: {e}")
        return []


def normalize_ibge_data(raw_data: List[Dict], series_name: str) -> List[Dict]:
    """
    Normalize IBGE data to standard format
    
    Args:
        raw_data: Raw data from IBGE API
        series_name: Name of the series for identification
        
    Returns:
        Normalized data records
    """
    normalized_records = []
    
    try:
        for record in raw_data:
            if isinstance(record, dict) and "resultados" in record:
                # Extract data from resultados
                for resultado in record["resultados"]:
                    if "series" in resultado:
                        for serie in resultado["series"]:
                            if "serie" in serie:
                                serie_data = serie["serie"]
                                for periodo, valor in serie_data.items():
                                    try:
                                        # Skip invalid values
                                        if valor in [None, "...", "-", ""]:
                                            continue
                                            
                                        # Parse period (format: YYYYMM)
                                        if len(periodo) == 6:
                                            year = int(periodo[:4])
                                            month = int(periodo[4:])
                                            date_str = f"{year:04d}-{month:02d}-01"
                                        else:
                                            continue
                                            
                                        # Convert value to float
                                        rate_value = float(str(valor).replace(",", "."))
                                        
                                        normalized_record = {
                                            "date": date_str,
                                            "rate": rate_value,
                                            "series": series_name,
                                            "source": "IBGE",
                                            "raw_period": periodo,
                                            "ingested_at": datetime.now().isoformat()
                                        }
                                        
                                        normalized_records.append(normalized_record)
                                        
                                    except (ValueError, TypeError) as e:
                                        logger.warning(f"[IBGE] Error processing period {periodo}: {e}")
                                        continue
                                        
    except Exception as e:
        logger.error(f"[IBGE] Error normalizing data for {series_name}: {e}")
        
    logger.info(f"[IBGE] Normalized {len(normalized_records)} records for {series_name}")
    return normalized_records


def fetch_ibge_series(series_name: str) -> str:
    """
    Fetch specific IBGE series data
    
    Args:
        series_name: Name of the series to fetch
        
    Returns:
        JSON string with the fetched data
    """
    if series_name not in IBGE_SERIES_CONFIG:
        logger.error(f"Unknown IBGE series: {series_name}")
        return json.dumps([])
        
    config = IBGE_SERIES_CONFIG[series_name]
    
    logger.info(f"[IBGE] Starting fetch for series: {series_name}")
    start_time = time.time()
    
    try:
        # Fetch raw data from API
        raw_data = fetch_ibge_data(config["endpoint"])
        
        if not raw_data:
            logger.warning(f"[IBGE] No data retrieved for {series_name}")
            return json.dumps([])
            
        # Normalize data
        normalized_data = normalize_ibge_data(raw_data, series_name)
        
        # Convert to JSON
        result_json = json.dumps(normalized_data, ensure_ascii=False, indent=2)
        
        duration = time.time() - start_time
        logger.info(f"[IBGE] Successfully fetched {series_name} in {duration:.2f}s")
        
        return result_json
        
    except Exception as e:
        logger.error(f"[IBGE] Error fetching {series_name}: {e}")
        return json.dumps([])


def get_available_ibge_series() -> Dict[str, str]:
    """
    Get list of available IBGE series
    
    Returns:
        Dictionary mapping series names to descriptions
    """
    return {name: config["description"] for name, config in IBGE_SERIES_CONFIG.items()}


if __name__ == "__main__":
    # Test IBGE API integration
    print("🏛️ IBGE API Integration Test")
    print("Available series:")
    
    for series, description in get_available_ibge_series().items():
        print(f"  - {series}: {description}")
        
    # Test fetch for IPCA
    print("\n📊 Testing IPCA fetch...")
    ipca_data = fetch_ibge_series("ipca")
    
    if ipca_data and ipca_data != "[]":
        data = json.loads(ipca_data)
        print(f"✅ Successfully fetched {len(data)} IPCA records")
        if data:
            print(f"Latest record: {data[-1]}")
    else:
        print("❌ Failed to fetch IPCA data")
