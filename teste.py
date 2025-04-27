import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, DoubleType
import pandas as pd

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("data", DateType(), True),
    StructField("valor", DoubleType(), True)
])
test_df = pd.DataFrame({"data": [pd.Timestamp("2024-01-01")], "valor": [1.23]})
test_spark_df = spark.createDataFrame(test_df, schema=schema)
test_spark_df.show()
