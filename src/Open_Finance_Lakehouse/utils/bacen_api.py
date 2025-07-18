import io
from datetime import datetime, timedelta

import pandas as pd
import requests


def fetch_bacen_series(series_id: int, end_date: str = None) -> pd.DataFrame:
    """
    Fetch all data from BACEN API for a given series_id, handling the 10-year window limit.
    Abstraction used by all BACEN-related pipelines (SELIC, CDI, IPCA, etc.)
    """
    if end_date is None:
        end_date = datetime.today()
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    all_data = []
    step = 10  # years
    min_year = 1900
    finished = False
    window_count = 0

    while not finished:
        start_date = end_date.replace(year=max(min_year, end_date.year - step + 1))
        print(f"[FETCH] Fetching BACEN {series_id}: {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')} (step={step})")

        url = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
            f"?formato=json&dataInicial={start_date.strftime('%d/%m/%Y')}&dataFinal={end_date.strftime('%d/%m/%Y')}"
        )

        try:
            response = requests.get(url)
            response.raise_for_status()
            df = pd.read_json(io.StringIO(response.text))

            if df.empty:
                print("[WARN] No data returned for window. Reducing step.")
                if step == 1:
                    finished = True
                else:
                    step = max(1, step - 1)
                continue

            print(f"[SUCCESS] Retrieved {len(df)} rows.")
            all_data.append(df)
            window_count += 1

            # Move window back
            end_date = start_date - timedelta(days=1)

        except Exception as e:
            print(f"[ERROR] {e}. Reducing step.")
            if step == 1:
                finished = True
            else:
                step = max(1, step - 1)

    print(f"[COMPLETE] Finished fetching BACEN series {series_id}. Total windows: {window_count}")

    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        result.columns = ["data", "valor"]
        # Keep date as string in DD/MM/YYYY format for Spark compatibility
        result["valor"] = pd.to_numeric(result["valor"], errors="coerce")
        result = result.sort_values("data").reset_index(drop=True)
        print(f"[TOTAL] Total rows fetched: {len(result)}")
        return result
    else:
        print("[WARN] No data fetched from BACEN API.")
        return pd.DataFrame(columns=["data", "valor"])
