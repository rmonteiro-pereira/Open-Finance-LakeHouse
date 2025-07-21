"""
IBGE API Integration Module
Instituto Brasileiro de Geografia e Estatística

Provides functions to fetch economic data from IBGE's public APIs:
- SIDRA API (Sistema IBGE de Recuperação Automática)
- Economic indicators: IPCA, INPC, PIB, Employment, etc.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# IBGE API Configuration
IBGE_BASE_URL = "https://servicodados.ibge.gov.br/api/v3"
SIDRA_BASE_URL = "https://sidra.ibge.gov.br/api/v3/agregados"

# IBGE Series Configuration
IBGE_SERIES_CONFIG = {
    "ipca": {
        "table": "1737",  # IPCA - Variação mensal, acumulada no ano e em 12 meses
        "variable": "63",  # Variação percentual
        "classification": "315|7169",  # Brasil
        "period": "all",
        "description": "IPCA - Índice Nacional de Preços ao Consumidor Amplo"
    },
    "inpc": {
        "table": "1736",  # INPC - Variação mensal, acumulada no ano e em 12 meses
        "variable": "44",  # Variação percentual
        "classification": "315|7169",  # Brasil
        "period": "all", 
        "description": "INPC - Índice Nacional de Preços ao Consumidor"
    },
    "pib": {
        "table": "1621",  # PIB - Valores correntes e variações volumétricas
        "variable": "584",  # PIB a preços correntes
        "classification": "11255|90707",  # Brasil
        "period": "all",
        "description": "PIB - Produto Interno Bruto"
    },
    "desemprego": {
        "table": "4099",  # PNAD Contínua - Taxa de desocupação
        "variable": "4099",  # Taxa de desocupação
        "classification": "1|1",  # Brasil
        "period": "all",
        "description": "Taxa de Desemprego - PNAD Contínua"
    },
    "pea": {
        "table": "4093",  # PNAD Contínua - População na força de trabalho
        "variable": "1641",  # Pessoas de 14 anos ou mais de idade
        "classification": "1|1",  # Brasil  
        "period": "all",
        "description": "População Economicamente Ativa"
    }
}


def fetch_ibge_sidra_data(table: str, variable: str, classification: str, 
                         period: str = "all", start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> List[Dict]:
    """
    Fetch data from IBGE SIDRA API
    
    Args:
        table: SIDRA table number
        variable: Variable code  
        classification: Classification code
        period: Period specification ("all", specific periods)
        start_date: Start date for filtering (YYYYMM format)
        end_date: End date for filtering (YYYYMM format)
        
    Returns:
        List of data records
    """
    try:
        # Build SIDRA URL
        url = f"{SIDRA_BASE_URL}/{table}/variaveis/{variable}"
        
        params = {
            "localidades": classification,
            "formato": "json"
        }
        
        if period != "all":
            params["periodo"] = period
            
        logger.info(f"[IBGE] Fetching data from table {table}, variable {variable}")
        
        # Make request with timeout
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # SIDRA returns nested structure, extract actual data
        if isinstance(data, list) and len(data) > 1:
            # Skip header row (first element) 
            records = data[1:]
            logger.info(f"[IBGE] Retrieved {len(records)} records")
            return records
        else:
            logger.warning(f"[IBGE] No data found for table {table}")
            return []
            
    except requests.RequestException as e:
        logger.error(f"[IBGE] Request error: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"[IBGE] JSON decode error: {e}")
        return []
    except Exception as e:
        logger.error(f"[IBGE] Unexpected error: {e}")
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
    
    for record in raw_data:
        try:
            # Extract key fields from SIDRA response
            # SIDRA structure: [table, variable, unit, location, period, value]
            if len(record) >= 6:
                period = record[4]  # Period (YYYYMM format)
                value = record[5]   # Value
                
                # Skip invalid records
                if not period or not value or value == "...":
                    continue
                    
                # Parse period to date
                if len(period) == 6:  # YYYYMM format
                    year = int(period[:4])
                    month = int(period[4:])
                    date_str = f"{year:04d}-{month:02d}-01"
                else:
                    continue
                    
                # Convert value to float
                try:
                    rate_value = float(value.replace(",", "."))
                except (ValueError, AttributeError):
                    continue
                    
                normalized_record = {
                    "date": date_str,
                    "rate": rate_value,
                    "series": series_name,
                    "source": "IBGE",
                    "raw_period": period,
                    "ingested_at": datetime.now().isoformat()
                }
                
                normalized_records.append(normalized_record)
                
        except (IndexError, ValueError, TypeError) as e:
            logger.warning(f"[IBGE] Error processing record: {e}")
            continue
            
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
        raise ValueError(f"Unknown IBGE series: {series_name}")
        
    config = IBGE_SERIES_CONFIG[series_name]
    
    logger.info(f"[IBGE] Starting fetch for series: {series_name}")
    start_time = time.time()
    
    try:
        # Fetch raw data from SIDRA
        raw_data = fetch_ibge_sidra_data(
            table=config["table"],
            variable=config["variable"], 
            classification=config["classification"],
            period=config["period"]
        )
        
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
