"""
Tesouro Direto API Integration Module
Provides functions to fetch treasury bond data from Brazilian government APIs

Treasury data includes:
- Tesouro Prefixado (Fixed rate bonds)
- Tesouro IPCA+ (Inflation-linked bonds)
- Tesouro Selic (Floating rate bonds)
"""

import json
import logging
from typing import Any
import requests
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)

# Tesouro Direto API Configuration
TESOURO_BASE_URL = "https://www.tesourotransparente.gov.br/ckan/api/3/action"
TESOURO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Treasury bond types mapping
TREASURY_TYPES = {
    "prefixado": "Tesouro Prefixado",
    "ipca_plus": "Tesouro IPCA+", 
    "selic": "Tesouro Selic"
}


def fetch_tesouro_direto_data(dataset_id: str = "df56aa42-484a-4a59-8184-7676580c81e3") -> list[dict[str, Any]]:
    """
    Fetch treasury bond data from Tesouro Transparente API
    
    Args:
        dataset_id: Dataset ID for treasury bonds (default is prices and rates)
        
    Returns:
        List of dictionaries containing treasury bond data
    """
    logger.info(f"🔄 Fetching Tesouro Direto data from dataset: {dataset_id}")
    
    try:
        # Get dataset metadata first
        url = f"{TESOURO_BASE_URL}/package_show"
        params = {"id": dataset_id}
        
        response = requests.get(url, params=params, headers=TESOURO_HEADERS, timeout=30)
        response.raise_for_status()
        
        dataset_info = response.json()
        
        if not dataset_info.get("success"):
            logger.error(f"❌ Failed to get dataset info: {dataset_info.get('error', 'Unknown error')}")
            return []
        
        # Get the latest resource (CSV file)
        resources = dataset_info["result"]["resources"]
        csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]
        
        if not csv_resources:
            logger.error("❌ No CSV resources found in dataset")
            return []
        
        # Get the most recent CSV file
        latest_resource = max(csv_resources, key=lambda x: x.get("created", ""))
        download_url = latest_resource["url"]
        
        logger.info(f"📥 Downloading data from: {download_url}")
        
        # Download and parse CSV data
        csv_response = requests.get(download_url, headers=TESOURO_HEADERS, timeout=60)
        csv_response.raise_for_status()
        
        # Parse CSV data
        df = pd.read_csv(pd.io.common.StringIO(csv_response.text), sep=";", encoding="utf-8")
        
        # Convert to list of dictionaries
        records = df.to_dict("records")
        
        logger.info(f"✅ Successfully fetched {len(records)} treasury bond records")
        return records
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Network error fetching Tesouro Direto data: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"❌ Error fetching Tesouro Direto data: {str(e)}")
        return []


def fetch_tesouro_series(series_name: str) -> str:
    """
    Fetch specific treasury series data and return as JSON string
    
    Args:
        series_name: Name of the treasury series to fetch
        
    Returns:
        JSON string containing the treasury data
    """
    logger.info(f"🔄 Fetching Tesouro Direto series: {series_name}")
    
    try:
        # Fetch all treasury data
        raw_data = fetch_tesouro_direto_data()
        
        if not raw_data:
            logger.warning("⚠️ No data available for Tesouro Direto")
            return "[]"
        
        # Filter data based on series name
        # Series name should match the title code format
        filtered_data = []
        
        for record in raw_data:
            # Match by title name (case insensitive)
            title = str(record.get("Tipo Titulo", "")).lower()
            vencimento = str(record.get("Data Vencimento", ""))
            
            # Create a matching key based on series name
            if series_name == "tesouro_prefixado_2027" and "prefixado" in title and "2027" in vencimento:
                filtered_data.append(record)
            elif series_name == "tesouro_prefixado_2031" and "prefixado" in title and "2031" in vencimento:
                filtered_data.append(record)
            elif series_name == "tesouro_ipca_2029" and "ipca" in title and "2029" in vencimento:
                filtered_data.append(record)
            elif series_name == "tesouro_ipca_2035" and "ipca" in title and "2035" in vencimento:
                filtered_data.append(record)
            elif series_name == "tesouro_ipca_2045" and "ipca" in title and "2045" in vencimento:
                filtered_data.append(record)
            elif series_name == "tesouro_selic_2027" and "selic" in title and "2027" in vencimento:
                filtered_data.append(record)
            elif series_name == "tesouro_selic_2029" and "selic" in title and "2029" in vencimento:
                filtered_data.append(record)
        
        # Transform data to standardized format
        standardized_data = []
        for record in filtered_data:
            try:
                standardized_record = {
                    "date": record.get("Data Base", ""),
                    "title_type": record.get("Tipo Titulo", ""),
                    "maturity_date": record.get("Data Vencimento", ""),
                    "buy_rate": convert_brazilian_number(record.get("Taxa Compra Manha")),
                    "sell_rate": convert_brazilian_number(record.get("Taxa Venda Manha")),
                    "buy_price": convert_brazilian_number(record.get("PU Compra Manha")),
                    "sell_price": convert_brazilian_number(record.get("PU Venda Manha")),
                    "min_investment": convert_brazilian_number(record.get("PU Base Manha"))
                }
                standardized_data.append(standardized_record)
            except (ValueError, TypeError) as e:
                logger.warning(f"⚠️ Error parsing record: {str(e)}")
                continue
        
        logger.info(f"✅ Successfully processed {len(standardized_data)} records for {series_name}")
        return json.dumps(standardized_data, ensure_ascii=False, default=str)
        
    except Exception as e:
        logger.error(f"❌ Error fetching Tesouro series {series_name}: {str(e)}")
        return "[]"


def get_available_treasury_bonds() -> list[dict[str, Any]]:
    """
    Get list of available treasury bonds with their details
    
    Returns:
        List of dictionaries containing bond information
    """
    logger.info("🔄 Getting available treasury bonds")
    
    try:
        raw_data = fetch_tesouro_direto_data()
        
        if not raw_data:
            return []
        
        # Extract unique bond types
        bonds = {}
        for record in raw_data:
            title = record.get("Tipo Titulo", "")
            maturity = record.get("Data Vencimento", "")
            
            key = f"{title}_{maturity}"
            if key not in bonds:
                bonds[key] = {
                    "title": title,
                    "maturity_date": maturity,
                    "type": "prefixado" if "Prefixado" in title else "ipca_plus" if "IPCA" in title else "selic" if "Selic" in title else "unknown"
                }
        
        return list(bonds.values())
        
    except Exception as e:
        logger.error(f"❌ Error getting available treasury bonds: {str(e)}")
        return []


def convert_brazilian_number(value):
    """
    Convert Brazilian number format (comma as decimal separator) to float.
    
    Args:
        value: Value to convert (string or number)
        
    Returns:
        float or None
    """
    if value is None or value == "":
        return None
    
    try:
        if isinstance(value, str):
            # Remove common formatting and replace comma with dot for decimal separator
            value = value.replace('%', '').strip()
            # For Brazilian format: replace comma with dot for decimal
            value = value.replace(',', '.')
            return float(value) if value else None
        else:
            return float(value)
    except (ValueError, TypeError):
        return None


# Export main functions
__all__ = [
    "fetch_tesouro_direto_data",
    "fetch_tesouro_series", 
    "get_available_treasury_bonds",
    "TREASURY_TYPES"
]
