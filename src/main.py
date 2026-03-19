"""This script orchestrates the data pipeline by extracting data from Dune, storing it in the raw layer, 
and triggering the transformation step to produce a clean dataset

Ce script orchestre le pipeline de données en extrayant les données depuis Dune, en les stockant dans la couche brute, 
puis en lançant la transformation pour produire un dataset propre."""

from pathlib import Path
from extract_dune import run_dune_query
from transform import transform

PROJECT_ROOT = Path(__file__).resolve().parents[1] #define project root to manage file paths consistency

def main():
    #execute Dune query to extract raw Aave cross-chain deposit data
    query_id = 5493069
    df_raw = run_dune_query(query_id=query_id)
    
    #save raw data to csv
    raw_path = PROJECT_ROOT / "data" / "raw" / "aave3_cross_chain_deposits_api.csv"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    df_raw.to_csv(raw_path, index=False)

    transform() #run transformation pipeline to clean & enriched data

if __name__ == "__main__":
    main() #entry point: orchestrate extraction + transformation workflow
