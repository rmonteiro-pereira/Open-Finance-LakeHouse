"""
Reservas Internacionais pipeline nodes
"""

from pyspark.sql import DataFrame
from ..common.nodes import (
    ingest_bacen_raw,
    transform_raw_to_bronze_generic,
    transform_bronze_to_silver_generic,
    validate_bacen_data_generic,
    aggregate_bacen_to_gold_generic
)


def ingest_reservas_internacionais_raw() -> str:
    """Reservas internacionais do Brasil em milhoes de US$"""
    return ingest_bacen_raw(3545)


def transform_reservas_internacionais_raw_to_bronze(raw_reservas_internacionais: str) -> DataFrame:
    """Transform Reservas Internacionais raw data to bronze"""
    return transform_raw_to_bronze_generic(raw_reservas_internacionais, 3545)


def transform_reservas_internacionais_bronze_to_silver(bronze_reservas_internacionais: DataFrame) -> DataFrame:
    """Transform Reservas Internacionais bronze data to silver"""
    return transform_bronze_to_silver_generic(bronze_reservas_internacionais, 3545)


def aggregate_reservas_internacionais_to_gold(silver_reservas_internacionais: DataFrame) -> DataFrame:
    """Aggregate Reservas Internacionais data to gold layer with monthly summaries"""
    return aggregate_bacen_to_gold_generic(silver_reservas_internacionais, "Reservas Internacionais")


def validate_reservas_internacionais_data(silver_reservas_internacionais: DataFrame) -> str:
    """Validate Reservas Internacionais data quality"""
    return validate_bacen_data_generic(silver_reservas_internacionais, 3545)
