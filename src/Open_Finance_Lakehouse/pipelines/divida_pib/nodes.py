"""
Divida/PIB pipeline nodes
"""

from pyspark.sql import DataFrame
from ..common.nodes import (
    ingest_bacen_raw,
    transform_raw_to_bronze_generic,
    transform_bronze_to_silver_generic,
    validate_bacen_data_generic,
    aggregate_bacen_to_gold_generic
)


def ingest_divida_pib_raw() -> str:
    """Divida Liquida do Setor Publico/PIB - %"""
    return ingest_bacen_raw(4513)


def transform_divida_pib_raw_to_bronze(raw_divida_pib: str) -> DataFrame:
    """Transform Divida/PIB raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_divida_pib, 4513)


def transform_divida_pib_bronze_to_silver(bronze_divida_pib: DataFrame) -> DataFrame:
    """Transform Divida/PIB bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_divida_pib, 4513)


def aggregate_divida_pib_to_gold(silver_divida_pib: DataFrame) -> DataFrame:
    """Aggregate Divida/PIB data to gold layer with monthly summaries"""
    return aggregate_bacen_to_gold_generic(silver_divida_pib, "Divida/PIB")


def validate_divida_pib_data(silver_divida_pib: DataFrame) -> str:
    """Validate Divida/PIB data quality"""
    return validate_bacen_data_generic(silver_divida_pib, 4513)
