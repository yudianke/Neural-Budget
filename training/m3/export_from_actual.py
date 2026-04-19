import requests
import pandas as pd

from ingest_monthly_history import upsert_monthly_history


ACTUAL_API_URL = "http://localhost:5006"  # adjust if needed


def fetch_from_actual():
    # This depends on Actual's internal RPC
    url = f"{ACTUAL_API_URL}/sync"

    payload = {
        "method": "forecast-export-monthly-history",
        "args": [],
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()

    data = response.json()

    if "result" not in data:
        raise ValueError("Invalid response from Actual")

    return data["result"]


def main():
    rows = fetch_from_actual()

    df = pd.DataFrame(rows)

    if df.empty:
        print("No data returned from Actual")
        return

    print(f"Fetched {len(df)} rows from Actual")

    upsert_monthly_history(df)

    print("Inserted into Postgres successfully")


if __name__ == "__main__":
    main()