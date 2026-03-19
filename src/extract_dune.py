"""This script connects to the Dune API, retrieves the results of a query, converts them into a pandas DataFrame, 
and stores the raw output as a CSV file.

Ce script se connecte à l’API Dune, récupère les résultats d’une requête, les convertit en DataFrame pandas, 
puis sauvegarde la sortie brute en fichier CSV."""

import os
from pathlib import Path
from typing import Optional, Dict

import requests
import pandas as pd
from dotenv import load_dotenv

BASE_URL = "https://api.dune.com/api/v1" # Dune API

load_dotenv(dotenv_path=Path(".env")) #load environment variables from local .env file

DUNE_API_KEY = os.getenv("DUNE_API_KEY") #read Dune API key from env

if DUNE_API_KEY is None:
    raise RuntimeError("DUNE_API_KEY is not set in .env") #stop execution if API key is missing

def run_dune_query(query_id: int, params: Optional[Dict[str, str]] = None) -> pd.DataFrame:

    headers = {"X-Dune-API-Key": DUNE_API_KEY} #build authentication header for Dune API
    url = f"{BASE_URL}/query/{query_id}/results" #endpoint to retrieve query results directly

    #send request to Dune API with optional query parameters
    response = requests.get(url, headers=headers, params=params or {})
    response.raise_for_status()
    data = response.json() #parse JSON response

    #extract tabular rows & convert to dataframe
    rows = data["result"]["rows"]
    return pd.DataFrame(rows)

def main():
    query_id = 5493069 #Dune query ID for Aave V3 cross chain deposits

    #debug information to confirm API key is loaded
    print("DEBUG KEY IN MAIN:", DUNE_API_KEY)
    print(f"Fetching data from Dune query {query_id}...")

    df = run_dune_query(query_id=query_id) #run query & load results to dataframe

    #save raw query output to csv
    out_path = Path("data/raw/aave3_cross_chain_deposits_api.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(f"Saved {len(df)} rows to {out_path}")

if __name__ == "__main__":
    main() #entry point: extract raw data from dune and save locally


