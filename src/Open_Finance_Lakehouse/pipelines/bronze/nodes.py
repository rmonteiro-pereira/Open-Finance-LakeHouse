# src/open_finance_lakehouse/pipelines/bronze/nodes.py

import io
import os
import tempfile
from datetime import datetime

import pandas as pd
import requests

from open_finance_lakehouse.utils.spark_session import get_spark_session


# BACEN API Fetch
def fetch_bacen_series(series_id: int, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Fetch BACEN SGS series given a series_id and optional date range.
    Dates must be strings in format 'dd/mm/yyyy'.
    """
    # Se não for passado, pegar últimos 10 anos até hoje
    if not start_date:
        start_date = (datetime.today().replace(year=datetime.today().year - 10)).strftime("%d/%m/%Y")
    if not end_date:
        end_date = datetime.today().strftime("%d/%m/%Y")
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
        f"?formato=json&dataInicial={start_date}&dataFinal={end_date}"
    )

    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Erro ao buscar dados do BACEN: {response.text}")
    df = pd.read_json(io.StringIO(response.text))
    df.columns = ["data", "valor"]
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df

# CVM Download and Read
def fetch_cvm_fundos(year: int, month: int) -> pd.DataFrame:
    spark = get_spark_session()
    base_url = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DI/DADOS/"
    file_name = f"inf_di_fi_{year}{str(month).zfill(2)}.csv"
    url = f"{base_url}{file_name}"

    # Spark cannot read remote URLs directly unless Hadoop is configured for it.
    # Download to a temp file if needed, or ensure the file is local or on HDFS/S3.
    # Here, we use pandas to download and Spark to read from local temp file:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(requests.get(url).content)
        tmp_path = tmp.name

    df = spark.read.csv(
        tmp_path,
        header=True,
        sep=";",
        inferSchema=True,
        encoding="ISO-8859-1"
    )

    # Clean up temp file
    os.unlink(tmp_path)
    return df

# Save as Delta Table
def save_as_delta(df, output_path: str):
    df.write.format("delta").mode("overwrite").save(output_path)
