# src/open_finance_lakehouse/pipelines/bronze/nodes.py

import io

import pandas as pd
import requests
from pyspark.sql import SparkSession


# BACEN API Fetch
def fetch_bacen_series(series_id: int) -> pd.DataFrame:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados?formato=json"
    response = requests.get(url)
    if not response.ok or not response.text.strip():
        # Return empty DataFrame with expected columns if response is bad or empty
        return pd.DataFrame(columns=["data", "valor"])
    try:
        data = pd.read_json(io.StringIO(response.text))
        if data.empty:
            return pd.DataFrame(columns=["data", "valor"])
        data.columns = ["data", "valor"]
        data["data"] = pd.to_datetime(data["data"], format="%d/%m/%Y")
        data["valor"] = pd.to_numeric(data["valor"], errors="coerce")
        return data
    except Exception:
        # Return empty DataFrame if parsing fails
        return pd.DataFrame(columns=["data", "valor"])

# CVM Download and Read
def fetch_cvm_fundos(year: int, month: int) -> pd.DataFrame:
    spark = SparkSession.builder.appName("LakehouseFinanceiro").getOrCreate()
    base_url = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DI/DADOS/"
    file_name = f"inf_di_fi_{year}{str(month).zfill(2)}.csv"
    url = f"{base_url}{file_name}"

    df = spark.read.csv(url, header=True, sep=";", inferSchema=True, encoding="ISO-8859-1")
    return df

# Save as Delta Table
def save_as_delta(df: pd.DataFrame, output_path: str):
    df.write.format("delta").mode("overwrite").save(output_path)
