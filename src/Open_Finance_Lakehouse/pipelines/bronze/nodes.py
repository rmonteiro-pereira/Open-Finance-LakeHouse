"""
This is a boilerplate pipeline 'bronze'
generated using Kedro 0.19.12
"""
# src/open_finance_lakehouse/pipelines/bronze/nodes.py

import pandas as pd
import requests
import zipfile
import io
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# BACEN API Fetch
def fetch_bacen_series(series_id: int) -> pd.DataFrame:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados?formato=json"
    response = requests.get(url)
    df = pd.read_json(io.StringIO(response.text))
    df.columns = ["data", "valor"]
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df

# CVM Download and Read
def fetch_cvm_fundos(spark: SparkSession, year: int, month: int) -> pd.DataFrame:
    base_url = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DI/DADOS/"
    file_name = f"inf_di_fi_{year}{str(month).zfill(2)}.csv"
    url = f"{base_url}{file_name}"
    
    # Read CSV directly (CVM disponibiliza sem zip para anos recentes)
    df = spark.read.csv(url, header=True, sep=";", inferSchema=True, encoding="ISO-8859-1")
    return df

# Save as Delta Table
def save_as_delta(df: pd.DataFrame, output_path: str):
    df.write.format("delta").mode("overwrite").save(output_path)
