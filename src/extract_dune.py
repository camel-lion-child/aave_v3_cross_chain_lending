import os
from pathlib import Path
from typing import Optional, Dict

import requests
import pandas as pd
from dotenv import load_dotenv

BASE_URL = "https://api.dune.com/api/v1"

# 1️⃣ Load .env 
load_dotenv(dotenv_path=Path(".env"))

# 2️⃣ API key
DUNE_API_KEY = os.getenv("DUNE_API_KEY")

if DUNE_API_KEY is None:
    raise RuntimeError("DUNE_API_KEY is not set in .env")

def run_dune_query(query_id: int, params: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """Fetch results from a Dune query using the API."""
    headers = {"X-Dune-API-Key": DUNE_API_KEY}
    url = f"{BASE_URL}/query/{query_id}/results"

    response = requests.get(url, headers=headers, params=params or {})
    response.raise_for_status()
    data = response.json()

    rows = data["result"]["rows"]
    return pd.DataFrame(rows)

def main():
    query_id = 5493069

    print("DEBUG KEY IN MAIN:", DUNE_API_KEY)
    print(f"Fetching data from Dune query {query_id}...")

    df = run_dune_query(query_id=query_id)

    out_path = Path("data/raw/aave3_cross_chain_deposits_api.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(f"Saved {len(df)} rows to {out_path}")

if __name__ == "__main__":
    main()


