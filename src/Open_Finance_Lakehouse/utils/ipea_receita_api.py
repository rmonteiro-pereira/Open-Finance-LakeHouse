"""
IPEA/Receita Federal API Utility Functions
Brazilian economic and fiscal data extraction utilities
"""

import requests
import json
import logging
import random
from typing import Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# API Configuration
IPEA_BASE_URL = "http://www.ipeadata.gov.br/api/odata4"
RECEITA_BASE_URL = "https://dadosabertos.rfb.gov.br/api"
DADOS_GOV_BASE_URL = "https://dados.gov.br/api/publico"

# Weekday constants
FRIDAY = 4

# Series configuration mapping
IPEA_RECEITA_SERIES_CONFIG = {
    "deficit_primario": {
        "code": "DEFICIT_PRIM",
        "endpoint": "ipea/fiscal/deficit",
        "category": "fiscal",
        "subcategory": "deficit",
        "unit": "R$ milhões",
        "frequency": "monthly",
        "source": "IPEA",
        "description": "Déficit primário do setor público"
    },
    "arrecadacao": {
        "code": "ARREC_TOTAL",
        "endpoint": "receita/arrecadacao/total",
        "category": "tax",
        "subcategory": "collection",
        "unit": "R$ milhões", 
        "frequency": "monthly",
        "source": "Receita Federal",
        "description": "Arrecadação tributária total"
    },
    "divida_bruta": {
        "code": "DIVIDA_BRUTA",
        "endpoint": "ipea/fiscal/debt",
        "category": "fiscal",
        "subcategory": "debt",
        "unit": "% PIB",
        "frequency": "monthly",
        "source": "IPEA",
        "description": "Dívida bruta do setor público"
    },
    "produtividade": {
        "code": "PROD_TRAB",
        "endpoint": "ipea/economic/productivity",
        "category": "economic",
        "subcategory": "productivity",
        "unit": "Índice",
        "frequency": "quarterly",
        "source": "IPEA",
        "description": "Produtividade do trabalho"
    },
    "investimento_publico": {
        "code": "INV_PUB",
        "endpoint": "ipea/fiscal/investment",
        "category": "fiscal",
        "subcategory": "investment",
        "unit": "R$ milhões",
        "frequency": "monthly",
        "source": "IPEA",
        "description": "Investimento público"
    },
    "carga_tributaria": {
        "code": "CARGA_TRIB",
        "endpoint": "ipea/fiscal/tax_burden",
        "category": "tax",
        "subcategory": "burden",
        "unit": "% PIB",
        "frequency": "annual",
        "source": "IPEA",
        "description": "Carga tributária bruta"
    },
    "irpj": {
        "code": "IRPJ",
        "endpoint": "receita/tributos/irpj",
        "category": "tax",
        "subcategory": "corporate",
        "unit": "R$ milhões",
        "frequency": "monthly",
        "source": "Receita Federal",
        "description": "Imposto de Renda Pessoa Jurídica"
    },
    "csll": {
        "code": "CSLL",
        "endpoint": "receita/tributos/csll",
        "category": "tax",
        "subcategory": "corporate",
        "unit": "R$ milhões",
        "frequency": "monthly",
        "source": "Receita Federal",
        "description": "Contribuição Social sobre Lucro Líquido"
    }
}


def fetch_ipea_receita_series(series_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> str:
    """
    Fetch data from IPEA/Receita Federal APIs for a specific series
    
    Args:
        series_name: Name of the IPEA/Receita series to fetch
        start_date: Start date for data (YYYY-MM-DD format)
        end_date: End date for data (YYYY-MM-DD format)
        
    Returns:
        JSON string containing the fetched data
    """
    logger.info(f"🔄 Fetching IPEA/Receita data for series: {series_name}")
    
    if series_name not in IPEA_RECEITA_SERIES_CONFIG:
        logger.error(f"❌ Unknown IPEA/Receita series: {series_name}")
        return "[]"
    
    series_config = IPEA_RECEITA_SERIES_CONFIG[series_name]
    
    # Set default date range if not provided
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=1095)).strftime("%Y-%m-%d")  # 3 years
    
    try:
        # For now, simulate IPEA/Receita API data since actual APIs may require authentication
        # In production, you would replace this with actual API calls
        mock_data = _generate_mock_ipea_receita_data(series_name, series_config, start_date, end_date)
        
        logger.info(f"✅ Successfully fetched IPEA/Receita data for {series_name}: {len(mock_data)} records")
        return json.dumps(mock_data, ensure_ascii=False)
        
    except Exception as e:
        logger.error(f"❌ Error fetching IPEA/Receita data for {series_name}: {str(e)}")
        return "[]"


def _generate_mock_ipea_receita_data(series_name: str, series_config: dict, start_date: str, end_date: str) -> list:
    """
    Generate mock IPEA/Receita data for testing purposes
    In production, replace with actual API calls
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    mock_data = []
    current_date = start
    
    # Determine frequency interval
    frequency = series_config.get("frequency", "monthly")
    if frequency == "monthly":
        interval = 30
    elif frequency == "quarterly":
        interval = 90
    elif frequency == "annual":
        interval = 365
    else:
        interval = 30
    
    # Base values for different series types
    base_values = {
        "deficit_primario": -50000,  # Negative deficit
        "arrecadacao": 150000,       # Tax collection in millions
        "divida_bruta": 75.0,        # Debt as % of GDP
        "produtividade": 100.0,      # Productivity index
        "investimento_publico": 25000, # Public investment
        "carga_tributaria": 35.0,    # Tax burden as % of GDP
        "irpj": 15000,               # Corporate income tax
        "csll": 8000                 # Social contribution
    }
    
    base_value = base_values.get(series_name, 1000)
    
    while current_date <= end:
        # Generate realistic economic data with seasonal patterns
        if series_config["category"] == "fiscal":
            variation = random.uniform(-0.15, 0.15)  # 15% variation
        elif series_config["category"] == "tax":
            variation = random.uniform(-0.10, 0.20)  # Tax collection can vary more
        else:  # economic
            variation = random.uniform(-0.05, 0.05)  # Economic indicators more stable
        
        value = base_value * (1 + variation)
        
        record = {
            "date": current_date.strftime("%Y-%m-%d"),
            "value": round(value, 2)
        }
        mock_data.append(record)
        
        # Move to next data point based on frequency
        current_date += timedelta(days=interval)
    
    logger.info(f"📊 Generated {len(mock_data)} mock records for {series_name}")
    return mock_data


def get_ipea_receita_series_info(series_name: str) -> dict:
    """
    Get configuration information for an IPEA/Receita series
    
    Args:
        series_name: Name of the IPEA/Receita series
        
    Returns:
        Dictionary with series configuration
    """
    return IPEA_RECEITA_SERIES_CONFIG.get(series_name, {})


def list_available_ipea_receita_series() -> list:
    """
    List all available IPEA/Receita series
    
    Returns:
        List of available series names
    """
    return list(IPEA_RECEITA_SERIES_CONFIG.keys())


def fetch_ipea_data(indicator_code: str) -> str:
    """
    Fetch data directly from IPEA API (future implementation)
    """
    # TODO: Implement actual IPEA API calls
    # url = f"{IPEA_BASE_URL}/Metadados('{indicator_code}')/Valores"
    # response = requests.get(url)
    # return response.json()
    pass


def fetch_receita_data(endpoint: str) -> str:
    """
    Fetch data directly from Receita Federal API (future implementation)
    """
    # TODO: Implement actual Receita Federal API calls
    # url = f"{RECEITA_BASE_URL}/{endpoint}"
    # response = requests.get(url)
    # return response.json()
    pass


# Future implementation notes:
# 1. Replace mock data with actual IPEA and Receita Federal API calls
# 2. Implement authentication if required
# 3. Add rate limiting and retry logic
# 4. Handle different data formats from different agencies
# 5. Add data validation and error handling
# 6. Implement caching mechanism for frequently requested data
# 7. Add support for different date formats and frequencies
