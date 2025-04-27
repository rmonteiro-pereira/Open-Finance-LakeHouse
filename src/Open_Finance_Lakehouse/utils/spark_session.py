from functools import lru_cache

from pyspark.sql import SparkSession


@lru_cache(maxsize=1)
def get_spark_session(app_name: str = "OpenFinanceLakehouse") -> SparkSession:
    """
    Retorna uma SparkSession singleton usando cache interno.
    Não usa variáveis globais.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Configurações adicionais opcionais (ótimas para ambiente local)
        # .config("spark.sql.shuffle.partitions", "8")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )
    return spark
