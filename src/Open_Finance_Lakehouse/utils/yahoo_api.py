"""
Yahoo Finance API Integration Module
Provides functions to fetch financial data from Yahoo Finance

Financial data includes:
- Brazilian ETFs (BOVA11, IVVB11, SMAL11, etc.)
- Commodities (Oil, Gold, Soy, Coffee)
- Currency pairs
"""

import json
import logging
import time
from datetime import datetime, timedelta
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Yahoo Finance Configuration
YAHOO_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Yahoo Finance Series Configuration
YAHOO_SERIES_CONFIG = {
    "bova11": {
        "symbol": "BOVA11.SA",
        "description": "BOVA11 - ETF Bovespa",
        "currency": "BRL"
    },
    "ivvb11": {
        "symbol": "IVVB11.SA",
        "description": "IVVB11 - ETF S&P 500",
        "currency": "BRL"
    },
    "smal11": {
        "symbol": "SMAL11.SA",
        "description": "SMAL11 - ETF Small Caps",
        "currency": "BRL"
    },
    "nasd11": {
        "symbol": "NASD11.SA",
        "description": "NASD11 - ETF Nasdaq",
        "currency": "BRL"
    },
    "spxi11": {
        "symbol": "SPXI11.SA",
        "description": "SPXI11 - ETF S&P",
        "currency": "BRL"
    },
    "fixa11": {
        "symbol": "FIXA11.SA",
        "description": "FIXA11 - ETF Renda Fixa",
        "currency": "BRL"
    },
    "petroleo": {
        "symbol": "CL=F",
        "description": "Crude Oil Futures",
        "currency": "USD"
    },
    "ouro": {
        "symbol": "GC=F",
        "description": "Gold Futures",
        "currency": "USD"
    },
    "soja": {
        "symbol": "ZS=F",
        "description": "Soybean Futures",
        "currency": "USD"
    },
    "cafe": {
        "symbol": "KC=F",
        "description": "Coffee Futures",
        "currency": "USD"
    },
    "usd_brl": {
        "symbol": "USDBRL=X",
        "description": "USD/BRL Exchange Rate",
        "currency": "BRL"
    }
}


def fetch_yahoo_data(symbol: str, period: str = "2y", interval: str = "1d") -> dict:
    """
    Fetch data from Yahoo Finance API
    
    Args:
        symbol: Yahoo Finance symbol
        period: Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
        interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
        
    Returns:
        Raw Yahoo Finance data
    """
    try:
        url = f"{YAHOO_BASE_URL}/{symbol}"
        params = {
            "period1": int((datetime.now() - timedelta(days=730)).timestamp()),  # 2 years ago
            "period2": int(datetime.now().timestamp()),
            "interval": interval,
            "includePrePost": "false",
            "events": "div,splits"
        }
        
        logger.info(f"[YAHOO] Fetching data for symbol: {symbol}")
        
        response = requests.get(url, params=params, headers=YAHOO_HEADERS, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"[YAHOO] Successfully fetched data for {symbol}")
        
        return data
        
    except requests.RequestException as e:
        logger.error(f"[YAHOO] Request error for {symbol}: {e}")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"[YAHOO] JSON decode error for {symbol}: {e}")
        return {}
    except Exception as e:
        logger.error(f"[YAHOO] Unexpected error for {symbol}: {e}")
        return {}


def normalize_yahoo_data(raw_data: dict, series_name: str) -> list[dict]:
    """
    Normalize Yahoo Finance data to standard format
    
    Args:
        raw_data: Raw data from Yahoo Finance API
        series_name: Name of the series for identification
        
    Returns:
        Normalized data records
    """
    normalized_records = []
    
    try:
        # Extract chart data
        chart = raw_data.get("chart", {})
        if not chart.get("result"):
            logger.warning(f"[YAHOO] No chart data found for {series_name}")
            return []
            
        result = chart["result"][0]
        timestamps = result.get("timestamp", [])
        indicators = result.get("indicators", {})
        
        if not timestamps or not indicators.get("quote"):
            logger.warning(f"[YAHOO] No price data found for {series_name}")
            return []
            
        # Extract price data (close prices)
        quote = indicators["quote"][0]
        close_prices = quote.get("close", [])
        
        if len(timestamps) != len(close_prices):
            logger.warning(f"[YAHOO] Timestamp/price length mismatch for {series_name}")
            return []
            
        # Convert to normalized format
        for timestamp, price in zip(timestamps, close_prices):
            if price is not None:  # Skip null prices
                date_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                
                normalized_record = {
                    "date": date_str,
                    "rate": float(price),
                    "series": series_name,
                    "source": "Yahoo Finance",
                    "raw_timestamp": timestamp,
                    "ingested_at": datetime.now().isoformat()
                }
                
                normalized_records.append(normalized_record)
                
        logger.info(f"[YAHOO] Normalized {len(normalized_records)} records for {series_name}")
        return normalized_records
        
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"[YAHOO] Error normalizing data for {series_name}: {e}")
        return []


def fetch_yahoo_series(series_name: str) -> str:
    """
    Fetch specific Yahoo Finance series data
    
    Args:
        series_name: Name of the series to fetch
        
    Returns:
        JSON string with the fetched data
    """
    if series_name not in YAHOO_SERIES_CONFIG:
        raise ValueError(f"Unknown Yahoo Finance series: {series_name}")
        
    config = YAHOO_SERIES_CONFIG[series_name]
    symbol = config["symbol"]
    
    logger.info(f"[YAHOO] Starting fetch for series: {series_name} ({symbol})")
    start_time = time.time()
    
    try:
        # Fetch raw data from Yahoo Finance
        raw_data = fetch_yahoo_data(symbol)
        
        if not raw_data:
            logger.warning(f"[YAHOO] No data retrieved for {series_name}")
            return json.dumps([])
            
        # Normalize data
        normalized_data = normalize_yahoo_data(raw_data, series_name)
        
        # Convert to JSON
        result_json = json.dumps(normalized_data, ensure_ascii=False, indent=2)
        
        duration = time.time() - start_time
        logger.info(f"[YAHOO] Successfully fetched {series_name} in {duration:.2f}s")
        
        return result_json
        
    except Exception as e:
        logger.error(f"[YAHOO] Error fetching {series_name}: {e}")
        return json.dumps([])


def get_available_yahoo_series() -> dict[str, str]:
    """
    Get list of available Yahoo Finance series
    
    Returns:
        Dictionary mapping series names to descriptions
    """
    return {name: config["description"] for name, config in YAHOO_SERIES_CONFIG.items()}


if __name__ == "__main__":
    # Test Yahoo Finance API integration
    print("📊 Yahoo Finance API Integration Test")
    print("Available series:")
    
    for series, description in get_available_yahoo_series().items():
        print(f"  - {series}: {description}")
        
    # Test fetch for BOVA11
    print("\n📈 Testing BOVA11 fetch...")
    bova11_data = fetch_yahoo_series("bova11")
    
    if bova11_data and bova11_data != "[]":
        data = json.loads(bova11_data)
        print(f"✅ Successfully fetched {len(data)} BOVA11 records")
        if data:
            print(f"Latest record: {data[-1]}")
    else:
        print("❌ Failed to fetch BOVA11 data")
        
    # Test fetch for USD/BRL
    print("\n💵 Testing USD/BRL fetch...")
    usd_brl_data = fetch_yahoo_series("usd_brl")
    
    if usd_brl_data and usd_brl_data != "[]":
        data = json.loads(usd_brl_data)
        print(f"✅ Successfully fetched {len(data)} USD/BRL records")
        if data:
            print(f"Latest record: {data[-1]}")
    else:
        print("❌ Failed to fetch USD/BRL data")
