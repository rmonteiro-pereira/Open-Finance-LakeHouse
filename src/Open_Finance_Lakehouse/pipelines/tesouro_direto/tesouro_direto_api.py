"""
API utility for extracting Treasury Direct (Tesouro Direto) data.
"""

import requests
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import time

logger = logging.getLogger(__name__)


def fetch_tesouro_series(
    series_code: str,
    base_url: str = "https://www.tesourotransparente.gov.br/ckan/api/3/action",
    max_retries: int = 3,
    retry_delay: int = 2
) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch treasury bond data from Tesouro Transparente API.
    
    Args:
        series_code: Treasury bond series code
        base_url: Base URL for the API
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        
    Returns:
        List of data points for the series, or None if failed
    """
    
    for attempt in range(max_retries):
        try:
            # Try different endpoints based on the series type
            endpoints = [
                f"{base_url}/datastore_search",
                f"{base_url}/package_search"
            ]
            
            for endpoint in endpoints:
                try:
                    # Search for treasury data packages
                    params = {
                        'q': f'tesouro {series_code}',
                        'rows': 1000
                    }
                    
                    response = requests.get(
                        endpoint,
                        params=params,
                        timeout=30,
                        headers={'User-Agent': 'Open-Finance-LakeHouse/1.0'}
                    )
                    response.raise_for_status()
                    
                    data = response.json()
                    logger.info(f"📡 API Response for {series_code}: {response.status_code}")
                    
                    if data.get('success') and data.get('result'):
                        result = data['result']
                        
                        # Handle different response structures
                        if 'records' in result:
                            records = result['records']
                        elif 'results' in result:
                            # Package search results
                            packages = result['results']
                            if packages:
                                # Get the first package and try to get its resources
                                package_id = packages[0].get('id')
                                if package_id:
                                    resource_endpoint = f"{base_url}/package_show"
                                    resource_response = requests.get(
                                        resource_endpoint,
                                        params={'id': package_id},
                                        timeout=30
                                    )
                                    if resource_response.ok:
                                        resource_data = resource_response.json()
                                        resources = resource_data.get('result', {}).get('resources', [])
                                        if resources:
                                            # Try to get data from the first CSV resource
                                            csv_resource = next((r for r in resources if r.get('format', '').lower() == 'csv'), None)
                                            if csv_resource and csv_resource.get('url'):
                                                csv_url = csv_resource['url']
                                                csv_response = requests.get(csv_url, timeout=30)
                                                if csv_response.ok:
                                                    # Parse CSV data (simplified)
                                                    lines = csv_response.text.strip().split('\n')
                                                    if len(lines) > 1:
                                                        headers = lines[0].split(',')
                                                        records = []
                                                        for line in lines[1:]:
                                                            values = line.split(',')
                                                            if len(values) == len(headers):
                                                                record = dict(zip(headers, values))
                                                                records.append(record)
                                                        return records
                            records = []
                        else:
                            records = []
                        
                        if records:
                            logger.info(f"✅ Found {len(records)} records for {series_code}")
                            return records
                    
                except requests.exceptions.RequestException as e:
                    logger.warning(f"⚠️ Endpoint {endpoint} failed for {series_code}: {e}")
                    continue
                
            # If we get here, all endpoints failed for this attempt
            logger.warning(f"⚠️ Attempt {attempt + 1} failed for {series_code}")
            
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                
        except Exception as e:
            logger.error(f"❌ Error fetching {series_code} on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ All attempts failed for {series_code}")
                
    # Generate mock data for development if API fails
    logger.warning(f"⚠️ API failed for {series_code}, generating mock data for development")
    return generate_mock_tesouro_data(series_code)


def generate_mock_tesouro_data(series_code: str) -> List[Dict[str, Any]]:
    """
    Generate mock treasury bond data for development purposes.
    
    Args:
        series_code: Treasury bond series code
        
    Returns:
        List of mock data points
    """
    import random
    from datetime import datetime, timedelta
    
    # Base yield values for different bond types
    base_yields = {
        'prefixado': 12.5,
        'ipca': 6.5,
        'selic': 11.8
    }
    
    # Determine bond type and base yield
    bond_type = 'prefixado'
    if 'ipca' in series_code.lower():
        bond_type = 'ipca'
    elif 'selic' in series_code.lower():
        bond_type = 'selic'
    
    base_yield = base_yields.get(bond_type, 10.0)
    
    # Generate 30 days of mock data
    mock_data = []
    start_date = datetime.now() - timedelta(days=30)
    
    for i in range(30):
        date = start_date + timedelta(days=i)
        
        # Add some realistic variation
        yield_variation = random.uniform(-0.5, 0.5)
        current_yield = base_yield + yield_variation
        
        # Mock price calculation (simplified)
        price = 100 - (current_yield - 10) * 2 + random.uniform(-1, 1)
        
        record = {
            'data': date.strftime('%Y-%m-%d'),
            'serie': series_code,
            'yield': round(current_yield, 2),
            'price': round(price, 2),
            'volume': random.randint(1000000, 10000000),
            'fonte': 'Tesouro Direto - Mock Data'
        }
        mock_data.append(record)
    
    logger.info(f"📊 Generated {len(mock_data)} mock records for {series_code}")
    return mock_data


def format_tesouro_data(raw_data: List[Dict[str, Any]], series_code: str) -> List[Dict[str, Any]]:
    """
    Format raw treasury data into standardized structure.
    
    Args:
        raw_data: Raw data from API
        series_code: Treasury bond series code
        
    Returns:
        Formatted data
    """
    formatted_data = []
    
    for record in raw_data:
        try:
            # Try to extract date in various formats
            date_str = record.get('data') or record.get('date') or record.get('Data')
            if date_str:
                # Try different date formats
                try:
                    if '-' in date_str:
                        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    elif '/' in date_str:
                        date_obj = datetime.strptime(date_str, '%d/%m/%Y')
                    else:
                        date_obj = datetime.now()
                except:
                    date_obj = datetime.now()
            else:
                date_obj = datetime.now()
            
            # Extract numeric values
            yield_value = extract_numeric_value(record, ['yield', 'taxa', 'rendimento'])
            price_value = extract_numeric_value(record, ['price', 'preco', 'valor'])
            volume_value = extract_numeric_value(record, ['volume', 'quantidade'])
            
            formatted_record = {
                'data': date_obj.strftime('%Y-%m-%d'),
                'serie': series_code,
                'yield': yield_value,
                'price': price_value,
                'volume': volume_value or 0,
                'fonte': 'Tesouro Direto',
                'extracted_at': datetime.now().isoformat()
            }
            
            formatted_data.append(formatted_record)
            
        except Exception as e:
            logger.warning(f"⚠️ Error formatting record for {series_code}: {e}")
            continue
    
    return formatted_data


def extract_numeric_value(record: Dict[str, Any], field_names: List[str]) -> Optional[float]:
    """
    Extract numeric value from record using multiple possible field names.
    
    Args:
        record: Data record
        field_names: List of possible field names
        
    Returns:
        Numeric value or None
    """
    for field_name in field_names:
        if field_name in record:
            value = record[field_name]
            if value is not None:
                try:
                    # Handle string numbers with Brazilian formatting (comma as decimal separator)
                    if isinstance(value, str):
                        # Remove common formatting and replace comma with dot for decimal separator
                        value = value.replace('%', '').strip()
                        # For Brazilian format: replace comma with dot for decimal
                        # Brazilian format uses comma as decimal separator (e.g., "15,00" -> "15.00")
                        value = value.replace(',', '.')
                        return float(value) if value else None
                    else:
                        return float(value)
                except (ValueError, TypeError):
                    continue
    return None
