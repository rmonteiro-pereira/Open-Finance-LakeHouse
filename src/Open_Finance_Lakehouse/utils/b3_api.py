"""
B3 API Utility Functions
Brazilian stock exchange data extraction utilities
"""

import requests
import json
import logging
from typing import Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# B3 API Configuration
B3_BASE_URL = "https://www.b3.com.br/api/market-data"
B3_INDICES_URL = "https://www.b3.com.br/api/indices"

# Series configuration mapping
B3_SERIES_CONFIG = {
    "ibov": {
        "symbol": "IBOV",
        "endpoint": "indices/ibovespa",
        "category": "index",
        "index_type": "broad_market",
        "description": "Índice Bovespa - Principal índice da bolsa brasileira"
    },
    "ifix": {
        "symbol": "IFIX",
        "endpoint": "indices/ifix",
        "category": "index", 
        "index_type": "real_estate",
        "description": "Índice de Fundos de Investimentos Imobiliários"
    },
    "imob": {
        "symbol": "IMOB",
        "endpoint": "indices/imob",
        "category": "index",
        "index_type": "real_estate",
        "description": "Índice Imobiliário"
    },
    "icon": {
        "symbol": "ICON",
        "endpoint": "indices/icon",
        "category": "index",
        "index_type": "consumption",
        "description": "Índice de Consumo"
    },
    "iee": {
        "symbol": "IEE",
        "endpoint": "indices/iee",
        "category": "index",
        "index_type": "energy",
        "description": "Índice de Energia Elétrica"
    },
    "volume_negociado": {
        "symbol": "VOL",
        "endpoint": "trading/volume",
        "category": "volume",
        "index_type": "trading",
        "description": "Volume total negociado na B3"
    },
    "etf_bulletins": {
        "symbol": "ETF",
        "endpoint": "etf/bulletins",
        "category": "etf",
        "index_type": "funds",
        "description": "Boletins de ETFs da B3"
    }
}


def fetch_b3_series(series_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> str:
    """
    Fetch data from B3 APIs for a specific series
    
    Args:
        series_name: Name of the B3 series to fetch
        start_date: Start date for data (YYYY-MM-DD format)
        end_date: End date for data (YYYY-MM-DD format)
        
    Returns:
        JSON string containing the fetched data
    """
    logger.info(f"🔄 Fetching B3 data for series: {series_name}")
    
    if series_name not in B3_SERIES_CONFIG:
        logger.error(f"❌ Unknown B3 series: {series_name}")
        return "[]"
    
    series_config = B3_SERIES_CONFIG[series_name]
    
    # Set default date range if not provided
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    
    try:
        # For now, simulate B3 API data since actual B3 APIs may require authentication
        # In production, you would replace this with actual B3 API calls
        mock_data = _generate_mock_b3_data(series_name, series_config, start_date, end_date)
        
        logger.info(f"✅ Successfully fetched B3 data for {series_name}: {len(mock_data)} records")
        return json.dumps(mock_data, ensure_ascii=False)
        
    except Exception as e:
        logger.error(f"❌ Error fetching B3 data for {series_name}: {str(e)}")
        return "[]"


def _generate_mock_b3_data(series_name: str, series_config: dict, start_date: str, end_date: str) -> list:
    """
    Generate mock B3 data for testing purposes
    In production, replace with actual B3 API calls
    """
    import random
    from datetime import datetime, timedelta
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    mock_data = []
    current_date = start
    base_value = 100000  # Base index value
    
    while current_date <= end:
        # Skip weekends for stock market data
        if current_date.weekday() < 5:  # Monday = 0, Friday = 4
            # Generate realistic market data
            change_pct = random.uniform(-3, 3)  # Daily change between -3% and +3%
            value = base_value * (1 + change_pct / 100)
            
            record = {
                "date": current_date.strftime("%Y-%m-%d"),
                "value": round(value, 2),
                "change_pct": round(change_pct, 2),
                "volume": random.randint(1000000, 10000000) if series_config["category"] == "volume" else None,
                "market_cap": random.randint(1000000000, 5000000000) if series_config["category"] == "index" else None
            }
            mock_data.append(record)
            
            # Update base value for next day
            base_value = value
            
        current_date += timedelta(days=1)
    
    logger.info(f"📊 Generated {len(mock_data)} mock records for {series_name}")
    return mock_data


def get_b3_series_info(series_name: str) -> dict:
    """
    Get configuration information for a B3 series
    
    Args:
        series_name: Name of the B3 series
        
    Returns:
        Dictionary with series configuration
    """
    return B3_SERIES_CONFIG.get(series_name, {})


def list_available_b3_series() -> list:
    """
    List all available B3 series
    
    Returns:
        List of available series names
    """
    return list(B3_SERIES_CONFIG.keys())


# Future implementation notes:
# 1. Replace mock data with actual B3 API calls
# 2. Implement authentication if required by B3
# 3. Add rate limiting and retry logic
# 4. Handle different data formats from different B3 endpoints
# 5. Add caching mechanism for frequently requested data
