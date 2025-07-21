"""
IBGE Pipeline Nodes

Este módulo contém os nós de processamento para dados do IBGE (Instituto Brasileiro 
de Geografia e Estatística), incluindo indicadores econômicos e demográficos.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ...utils.ibge_api import fetch_ibge_series, IBGE_SERIES_CONFIG

logger = logging.getLogger(__name__)


def extract_ibge_series_raw(series_config: dict[str, Any], parameters: dict[str, Any]) -> str:
    """
    Extrai dados brutos de uma série do IBGE
    
    Args:
        series_config: Configuração da série (da configuração parameters_ibge.yml)
        parameters: Parâmetros de configuração
        
    Returns:
        JSON string com dados brutos
    """
    # Inferir o nome da série a partir da configuração
    series_name = None
    ibge_series_configs = parameters.get("ibge", {}).get("series", {})
    for name, config in ibge_series_configs.items():
        if config == series_config:
            series_name = name
            break
    
    if not series_name:
        # Fallback: tentar usar o código da série
        series_name = series_config.get("code", "unknown")
    
    logger.info(f"🔄 Starting raw data extraction for IBGE series: {series_name}")
    
    try:
        # Buscar dados da API do IBGE
        raw_data = fetch_ibge_series(series_name)
        
        if raw_data and raw_data != "[]":
            records = json.loads(raw_data)
            logger.info(f"✅ Successfully extracted {len(records)} records for {series_name}")
            return raw_data
        else:
            logger.warning(f"⚠️ No data found for {series_name}")
            return "[]"
            
    except Exception as e:
        logger.error(f"❌ Error extracting {series_name}: {e}")
        return "[]"


def process_ibge_series_bronze(raw_data: str, series_config: dict[str, Any], parameters: dict[str, Any]):
    """
    Processa dados brutos do IBGE para camada Bronze usando Spark
    
    Args:
        raw_data: Dados JSON brutos da API do IBGE
        series_name: Nome da série sendo processada
        parameters: Parâmetros de configuração do IBGE
        
    Returns:
        Spark DataFrame para camada Bronze
    """
    logger.info(f"🔄 Processing {series_name} data to Bronze layer")
    
    try:
        # Parse raw JSON data
        if not raw_data or raw_data == "[]":
            logger.warning(f"⚠️ No raw data to process for {series_name}")
            # Return empty Spark DataFrame
            spark = SparkSession.getActiveSession() or SparkSession.builder.appName("ibge_bronze").getOrCreate()
            return spark.createDataFrame([], schema="")
            
        records = json.loads(raw_data)
        
        if not records:
            logger.warning(f"⚠️ Empty records list for {series_name}")
            spark = SparkSession.getActiveSession() or SparkSession.builder.appName("ibge_bronze").getOrCreate()
            return spark.createDataFrame([], schema="")
        
        # Convert to Pandas DataFrame first for processing
        df = pd.DataFrame(records)
        
        # Add bronze layer metadata
        df["bronze_ingested_at"] = datetime.now().isoformat()
        df["bronze_source"] = "ibge"
        df["bronze_series_name"] = series_name
        
        # Add series configuration info
        if series_name in IBGE_SERIES_CONFIG:
            config = IBGE_SERIES_CONFIG[series_name]
            df["bronze_category"] = config.get("category", "unknown")
            df["bronze_frequency"] = config.get("frequency", "unknown")
            df["bronze_unit"] = config.get("unit", "unknown")
        
        # Ensure consistent column types
        df["date"] = pd.to_datetime(df["date"])
        df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
        
        # Sort by date
        df = df.sort_values("date").reset_index(drop=True)
        
        logger.info(f"✅ Processed {len(df)} records to Bronze layer for {series_name}")
        
        # Convert to Spark DataFrame
        spark = SparkSession.getActiveSession() or SparkSession.builder.appName("ibge_bronze").getOrCreate()
        spark_df = spark.createDataFrame(df)
        
        return spark_df
        
    except Exception as e:
        logger.error(f"❌ Error processing {series_name} to Bronze layer: {e}")
        spark = SparkSession.getActiveSession() or SparkSession.builder.appName("ibge_bronze").getOrCreate()
        return spark.createDataFrame([], schema="")


def process_ibge_series_silver(bronze_data, series_name: str, parameters: dict[str, Any]):
    """
    Processa dados da camada Bronze do IBGE para camada Silver com métricas avançadas
    
    Args:
        bronze_data: DataFrame da camada Bronze (Spark DataFrame)
        series_name: Nome da série sendo processada
        parameters: Parâmetros de configuração do IBGE
        
    Returns:
        Spark DataFrame para camada Silver
    """
    logger.info(f"🔄 Processing {series_name} data to Silver layer")
    
    try:
        # Check if we have data
        if hasattr(bronze_data, 'isEmpty') and bronze_data.isEmpty():
            logger.warning(f"⚠️ Empty Bronze DataFrame for {series_name}")
            spark = SparkSession.getActiveSession() or SparkSession.builder.appName("ibge_silver").getOrCreate()
            return spark.createDataFrame([], schema="")
        
        # Convert Spark DataFrame to Pandas for processing
        if hasattr(bronze_data, 'toPandas'):
            df = bronze_data.toPandas()
        else:
            df = bronze_data.copy()
        
        if df.empty:
            logger.warning(f"⚠️ Empty Bronze DataFrame for {series_name}")
            spark = SparkSession.getActiveSession() or SparkSession.builder.appName("ibge_silver").getOrCreate()
            return spark.createDataFrame([], schema="")
        
        # Remove duplicates (keep latest ingestion)
        df = df.drop_duplicates(subset=["date"], keep="last")
        
        # Clean data
        df = df.dropna(subset=["rate"])
        
        # Add Silver layer metadata
        df["silver_processed_at"] = datetime.now().isoformat()
        df["silver_quality_score"] = 1.0  # High quality as IBGE is official source
        
        # Sort by date for time series calculations
        df = df.sort_values("date")
        
        # Calculate enhanced metrics for economic indicators
        df["rate_change_pct"] = df["rate"].pct_change() * 100
        df["rate_change_abs"] = df["rate"].diff()
        
        # Fill NaN values for first record
        df["rate_change_pct"] = df["rate_change_pct"].fillna(0)
        df["rate_change_abs"] = df["rate_change_abs"].fillna(0)
        
        # Add moving averages (important for economic analysis)
        df["rate_ma_3"] = df["rate"].rolling(window=3, min_periods=1).mean()  # Quarterly average
        df["rate_ma_12"] = df["rate"].rolling(window=12, min_periods=1).mean()  # Annual average
        
        # Calculate year-over-year change (important for inflation data)
        df["rate_yoy_change"] = df["rate"].pct_change(periods=12) * 100
        df["rate_yoy_change"] = df["rate_yoy_change"].fillna(0)
        
        # Add volatility measure (12-month rolling standard deviation)
        df["rate_volatility_12m"] = df["rate"].rolling(window=12, min_periods=1).std()
        df["rate_volatility_12m"] = df["rate_volatility_12m"].fillna(0)
        
        # Calculate trend indicator (simple trend over 6 months)
        if len(df) >= 6:
            df["rate_trend_6m"] = df["rate"].rolling(window=6).apply(
                lambda x: 1 if x.iloc[-1] > x.iloc[0] else -1 if x.iloc[-1] < x.iloc[0] else 0,
                raw=False
            ).fillna(0)
        else:
            df["rate_trend_6m"] = 0
            
        # Add economic cycle indicators for relevant series
        if series_name in ["ipca", "inpc"]:
            # Inflation target analysis (Brazil's target is usually around 3-4%)
            target_rate = 3.5  # Brazilian inflation target center
            df["distance_from_target"] = df["rate"] - target_rate
            df["above_target"] = (df["rate"] > target_rate).astype(int)
            
        elif series_name in ["pib_mensal", "pib_trimestral"]:
            # Economic growth analysis
            df["positive_growth"] = (df["rate"] > 0).astype(int)
            df["recession_indicator"] = (df["rate"] < -1).astype(int)  # Negative growth > 1%
            
        elif series_name == "desemprego":
            # Unemployment analysis
            # High unemployment threshold (Brazil context)
            high_unemployment = 10.0
            df["high_unemployment"] = (df["rate"] > high_unemployment).astype(int)
        
        # Reorganize columns for clarity
        base_columns = [
            "date", "rate", "series_name", "period",
            "rate_change_pct", "rate_change_abs", 
            "rate_ma_3", "rate_ma_12", "rate_yoy_change", "rate_volatility_12m", "rate_trend_6m"
        ]
        
        # Add specific indicators if they exist
        for col in ["distance_from_target", "above_target", "positive_growth", "recession_indicator", "high_unemployment"]:
            if col in df.columns:
                base_columns.append(col)
        
        # Add metadata columns
        metadata_columns = [
            "bronze_ingested_at", "bronze_source", "bronze_series_name",
            "bronze_category", "bronze_frequency", "bronze_unit",
            "silver_processed_at", "silver_quality_score"
        ]
        
        # Keep only existing columns
        all_columns = base_columns + [col for col in metadata_columns if col in df.columns]
        df = df[[col for col in all_columns if col in df.columns]]
        
        logger.info(f"✅ Processed {len(df)} records to Silver layer for {series_name}")
        
        # Convert to Spark DataFrame
        spark = SparkSession.getActiveSession() or SparkSession.builder.appName("ibge_silver").getOrCreate()
        spark_df = spark.createDataFrame(df)
        
        return spark_df
        
    except Exception as e:
        logger.error(f"❌ Error processing {series_name} to Silver layer: {e}")
        spark = SparkSession.getActiveSession() or SparkSession.builder.appName("ibge_silver").getOrCreate()
        return spark.createDataFrame([], schema="")


def validate_ibge_series_bronze(df, series_name: str, parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Valida dados da camada Bronze do IBGE
    
    Args:
        df: DataFrame da camada Bronze (Spark DataFrame)
        series_name: Nome da série sendo validada
        parameters: Parâmetros de configuração do IBGE
        
    Returns:
        Dicionário com resultados da validação
    """
    logger.info(f"🔍 Validating Bronze layer data for {series_name}")
    
    # Convert Spark DataFrame to Pandas for validation
    if hasattr(df, 'toPandas'):
        df_pandas = df.toPandas()
    else:
        df_pandas = df
    
    # Get validation parameters
    validation_config = parameters.get("ibge", {}).get("processing", {}).get("validation_rules", {})
    min_records = validation_config.get("min_records", 24)
    max_null_percentage = validation_config.get("max_null_percentage", 10)
    date_range_days = validation_config.get("date_range_days", 60)

    # Define validation rules
    validation_rules = [
        {
            "name": "minimum_records",
            "description": f"Should have at least {min_records} records",
            "check": len(df_pandas) >= min_records,
            "severity": "error" if len(df_pandas) < min_records else "info",
            "details": f"Found {len(df_pandas)} records, minimum required: {min_records}"
        },
        {
            "name": "no_duplicate_dates",
            "description": "Should not have duplicate dates",
            "check": df_pandas["date"].nunique() == len(df_pandas) if len(df_pandas) > 0 and "date" in df_pandas.columns else True,
            "severity": "warning" if len(df_pandas) > 0 and "date" in df_pandas.columns and df_pandas["date"].nunique() != len(df_pandas) else "info",
            "details": f"Unique dates: {df_pandas['date'].nunique()}, Total records: {len(df_pandas)}" if len(df_pandas) > 0 and "date" in df_pandas.columns else "No data to check"
        },
        {
            "name": "rate_not_null",
            "description": f"Rate values should be less than {max_null_percentage}% null",
            "check": (df_pandas["rate"].isnull().sum() / len(df_pandas) * 100) <= max_null_percentage if len(df_pandas) > 0 and "rate" in df_pandas.columns else True,
            "severity": "error" if len(df_pandas) > 0 and "rate" in df_pandas.columns and (df_pandas["rate"].isnull().sum() / len(df_pandas) * 100) > max_null_percentage else "info",
            "details": f"Null rate percentage: {df_pandas['rate'].isnull().sum() / len(df_pandas) * 100:.2f}%" if len(df_pandas) > 0 and "rate" in df_pandas.columns else "No data to validate"
        },
        {
            "name": "date_range_coverage",
            "description": f"Should cover at least {date_range_days} days",
            "check": (df_pandas["date"].max() - df_pandas["date"].min()).days >= date_range_days if len(df_pandas) > 1 and "date" in df_pandas.columns else True,
            "severity": "warning" if len(df_pandas) > 1 and "date" in df_pandas.columns and (df_pandas["date"].max() - df_pandas["date"].min()).days < date_range_days else "info",
            "details": f"Date range: {(df_pandas['date'].max() - df_pandas['date'].min()).days} days" if len(df_pandas) > 1 and "date" in df_pandas.columns else "Insufficient data for range check"
        }
    ]
    
    # Execute validation rules
    validation_results = []
    for rule in validation_rules:
        result = {
            "rule_name": rule["name"],
            "description": rule["description"],
            "passed": rule["check"],
            "severity": rule["severity"],
            "details": rule["details"]
        }
        validation_results.append(result)
    
    # Generate validation report
    report = generate_quality_report(validation_results, series_name)
    
    logger.info(f"✅ Validation completed for {series_name}: {report['summary']['passed']}/{report['summary']['total']} checks passed")
    
    return report


def generate_quality_report(validation_results: list, series_name: str) -> dict:
    """
    Gera relatório de qualidade dos dados
    
    Args:
        validation_results: Lista de resultados de validação
        series_name: Nome da série
        
    Returns:
        Dicionário com relatório de qualidade
    """
    passed_checks = sum(1 for result in validation_results if result["passed"])
    total_checks = len(validation_results)
    
    # Count by severity
    errors = [r for r in validation_results if r["severity"] == "error" and not r["passed"]]
    warnings = [r for r in validation_results if r["severity"] == "warning" and not r["passed"]]
    
    report = {
        "series_name": series_name,
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total": total_checks,
            "passed": passed_checks,
            "failed": total_checks - passed_checks,
            "success_rate": (passed_checks / total_checks * 100) if total_checks > 0 else 0
        },
        "severity_breakdown": {
            "errors": len(errors),
            "warnings": len(warnings),
            "info": total_checks - len(errors) - len(warnings)
        },
        "validation_results": validation_results,
        "quality_score": (passed_checks / total_checks) if total_checks > 0 else 0,
        "data_quality_status": "PASS" if len(errors) == 0 else "FAIL"
    }
    
    return report


def create_ibge_catalog_entry(series_name: str, parameters: dict[str, Any]) -> dict[str, Any]:
    """
    Cria entrada do catálogo de dados para série IBGE
    
    Args:
        series_name: Nome da série
        parameters: Parâmetros de configuração do IBGE
        
    Returns:
        Dicionário com entrada do catálogo
    """
    series_config = IBGE_SERIES_CONFIG.get(series_name, {})
    
    return {
        "dataset_name": f"ibge_{series_name}",
        "source": "IBGE",
        "description": series_config.get("description", f"IBGE {series_name} series"),
        "code": series_config.get("code", ""),
        "unit": series_config.get("unit", ""),
        "category": series_config.get("category", ""),
        "frequency": series_config.get("frequency", ""),
        "layers": {
            "raw": f"ibge_{series_name}_raw",
            "bronze": f"ibge_{series_name}_bronze",
            "silver": f"ibge_{series_name}_silver"
        },
        "update_frequency": "monthly",
        "data_quality_checks": True,
        "official_source": True,
        "created_at": datetime.now().isoformat(),
        "last_updated": datetime.now().isoformat()
    }
