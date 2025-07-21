"""
Focus PIB pipeline nodes
"""

from pyspark.sql import DataFrame
from ..common.nodes import (
    ingest_bacen_raw,
    transform_raw_to_bronze_generic,
    transform_bronze_to_silver_generic,
    validate_bacen_data_generic,
    aggregate_bacen_to_gold_generic
)


def ingest_focus_pib_raw() -> str:
    """Relatorio Focus - Expectativas PIB - % a.a."""
    return ingest_bacen_raw(4389)


def transform_focus_pib_raw_to_bronze(raw_focus_pib: str) -> DataFrame:
    """Transform Focus PIB raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_focus_pib, 4389)


def transform_focus_pib_bronze_to_silver(bronze_focus_pib: DataFrame) -> DataFrame:
    """Transform Focus PIB bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_focus_pib, 4389)


def aggregate_focus_pib_to_gold(silver_focus_pib: DataFrame) -> DataFrame:
    """Aggregate Focus PIB data to gold layer with monthly summaries"""
    return aggregate_bacen_to_gold_generic(silver_focus_pib, "Focus PIB")


def validate_focus_pib_data(silver_focus_pib: DataFrame) -> str:
    """Validate Focus PIB data quality"""
    return validate_bacen_data_generic(silver_focus_pib, 4389)
