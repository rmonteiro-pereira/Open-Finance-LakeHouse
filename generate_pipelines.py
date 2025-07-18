#!/usr/bin/env python3
"""
Script to generate all BACEN series pipelines
"""

import os
import sys

# Series configuration
SERIES_CONFIG = {
    "selic_meta": {"id": 432, "name": "SELIC Meta", "max_rate": 100.0},
    "over": {"id": 1178, "name": "OVER", "max_rate": 100.0},
    "cdi": {"id": 12, "name": "CDI", "max_rate": 100.0},
    "tlp": {"id": 26192, "name": "TLP", "max_rate": 100.0},
    "ipca_15": {"id": 1705, "name": "IPCA-15", "max_rate": 50.0},
    "inpc": {"id": 188, "name": "INPC", "max_rate": 50.0},
    "igp_di": {"id": 189, "name": "IGP-DI", "max_rate": 50.0},
    "igp_m": {"id": 190, "name": "IGP-M", "max_rate": 50.0},
    "igp_10": {"id": 7447, "name": "IGP-10", "max_rate": 50.0},
    "usd_brl": {"id": 1, "name": "USD/BRL", "max_rate": 20.0},
    "eur_brl": {"id": 21619, "name": "EUR/BRL", "max_rate": 25.0},
}

def generate_nodes_file(series_key, series_config):
    """Generate nodes.py file for a series"""
    series_id = series_config["id"]
    series_name = series_config["name"]
    max_rate = series_config["max_rate"]
    
    content = f'''"""
{series_name} Pipeline - Brazilian Financial Series
"""
from ..common.nodes import (
    ingest_bacen_raw,
    transform_raw_to_bronze_generic,
    transform_bronze_to_silver_generic,
    validate_bacen_data_generic,
    timing_decorator
)
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, min, max, stddev
import logging

logger = logging.getLogger(__name__)


@timing_decorator
def aggregate_{series_key}_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Create {series_name}-specific gold layer with KPIs
    """
    logger.info("[GOLD] Creating {series_name} gold layer aggregations...")
    
    # Monthly aggregations for {series_name}
    gold_df = silver_df.groupBy("year", "month").agg(
        avg("rate").alias("avg_{series_key}"),
        min("rate").alias("min_{series_key}"),
        max("rate").alias("max_{series_key}"),
        stddev("rate").alias("stddev_{series_key}")
    ).orderBy("year", "month")
    
    # Add series identifier
    gold_df = gold_df.withColumn("series_name", col("avg_{series_key}") * 0 + "{series_name}")
    
    logger.info(f"[GOLD] {series_name} gold aggregation complete: {{gold_df.count()}} monthly aggregations")
    
    return gold_df


def ingest_{series_key}_raw(series_id: int, end_date: str = None) -> str:
    """{series_name} raw data ingestion"""
    return ingest_bacen_raw(series_id, end_date)


def transform_{series_key}_raw_to_bronze(raw_{series_key}: str) -> DataFrame:
    """Transform {series_name} raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_{series_key}, {series_id})


def transform_{series_key}_bronze_to_silver(bronze_{series_key}: DataFrame) -> DataFrame:
    """Transform {series_name} bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_{series_key}, {series_id})


def validate_{series_key}_data(silver_{series_key}: DataFrame) -> dict:
    """Validate {series_name} data with series-specific rules"""
    return validate_bacen_data_generic(silver_{series_key}, {series_id}, max_rate={max_rate})
'''
    return content

def generate_pipeline_file(series_key, series_name):
    """Generate pipeline.py file for a series"""
    content = f'''"""
{series_name} Pipeline - Brazilian Financial Series
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    ingest_{series_key}_raw,
    transform_{series_key}_raw_to_bronze,
    transform_{series_key}_bronze_to_silver,
    aggregate_{series_key}_to_gold,
    validate_{series_key}_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_{series_key}_raw,
                inputs=["params:{series_key}_series_id"],
                outputs="raw_{series_key}",
                name="ingest_{series_key}_raw_node",
            ),
            node(
                func=transform_{series_key}_raw_to_bronze,
                inputs="raw_{series_key}",
                outputs="bronze_{series_key}",
                name="transform_{series_key}_raw_to_bronze_node",
            ),
            node(
                func=transform_{series_key}_bronze_to_silver,
                inputs="bronze_{series_key}",
                outputs="silver_{series_key}",
                name="transform_{series_key}_bronze_to_silver_node",
            ),
            node(
                func=aggregate_{series_key}_to_gold,
                inputs="silver_{series_key}",
                outputs="gold_{series_key}",
                name="aggregate_{series_key}_gold_node",
            ),
            node(
                func=validate_{series_key}_data,
                inputs="silver_{series_key}",
                outputs="{series_key}_validation_results",
                name="validate_{series_key}_node",
            ),
        ]
    )
'''
    return content

def generate_parameters_file(series_key, series_id):
    """Generate parameters file for a series"""
    content = f'''# {series_key.upper()} Pipeline Parameters
{series_key}_series_id: {series_id}
'''
    return content

def generate_catalog_entries(series_key, series_name):
    """Generate catalog entries for a series"""
    content = f'''
# {series_name} Raw data from BACEN API (stored in MinIO as JSON)
raw_{series_key}:
  type: text.TextDataset
  filepath: s3a://lakehouse/raw/{series_key}_bacen_raw.json
  credentials: minio_credentials

# {series_name} Bronze data (structured but raw)
bronze_{series_key}:
  type: spark.SparkDataset
  filepath: s3a://lakehouse/bronze/bacen_{series_key}/
  file_format: delta
  save_args:
    mode: overwrite

# {series_name} Silver layer (cleaned and validated)
silver_{series_key}:
  type: spark.SparkDataset
  filepath: s3a://lakehouse/silver/bacen_{series_key}/
  file_format: delta
  save_args:
    mode: overwrite
    overwriteSchema: true

# {series_name} Gold layer (KPIs and aggregations)
gold_{series_key}:
  type: spark.SparkDataset
  filepath: s3a://lakehouse/gold/{series_key}_kpis/
  file_format: delta
  save_args:
    mode: overwrite

# {series_name} validation results
{series_key}_validation_results:
  type: json.JSONDataset
  filepath: data/08_reporting/{series_key}_validation_results.json
'''
    return content

if __name__ == "__main__":
    print("Generating BACEN series pipeline files...")
    
    # Create pipeline files
    for series_key, config in SERIES_CONFIG.items():
        print(f"Generating {series_key} pipeline files...")
        
        # Create directories
        pipeline_dir = f"src/open_finance_lakehouse/pipelines/{series_key}"
        os.makedirs(pipeline_dir, exist_ok=True)
        
        # Generate __init__.py
        with open(f"{pipeline_dir}/__init__.py", "w") as f:
            f.write(f'"""\\n{config["name"]} pipeline\\n"""\\n')
        
        # Generate nodes.py
        with open(f"{pipeline_dir}/nodes.py", "w") as f:
            f.write(generate_nodes_file(series_key, config))
        
        # Generate pipeline.py
        with open(f"{pipeline_dir}/pipeline.py", "w") as f:
            f.write(generate_pipeline_file(series_key, config["name"]))
        
        # Generate parameters file
        with open(f"conf/base/parameters_{series_key}.yml", "w") as f:
            f.write(generate_parameters_file(series_key, config["id"]))
    
    # Generate catalog entries
    print("Generating catalog entries...")
    catalog_entries = ""
    for series_key, config in SERIES_CONFIG.items():
        catalog_entries += generate_catalog_entries(series_key, config["name"])
    
    # Write catalog entries to a separate file
    with open("catalog_entries.txt", "w") as f:
        f.write(catalog_entries)
    
    print("All pipeline files generated successfully!")
    print("Catalog entries saved to catalog_entries.txt")
