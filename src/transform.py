"""This script transforms raw Aave cross-chain deposit data by computing rolling volumes and market share per blockchain, 
creating a clean dataset for liquidity analysis.

Ce script transforme des données brutes de dépôts Aave multi-chaînes en calculant des volumes glissants et des parts de marché par blockchain, 
afin de créer un dataset propre pour l’analyse de liquidité."""

import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1] #define project root

RAW_PATH = PROJECT_ROOT / "data" / "raw" / "aave3_cross_chain_deposits_api.csv" #input raw dataset
PROCESSED_PATH = PROJECT_ROOT / "data" / "processed" / "aave3_cross_chain_deposits_clean.csv" #output cleaned dataset


def transform():
    df = pd.read_csv(RAW_PATH)

    df.columns = [c.lower().strip() for c in df.columns] #standardize column names
    df["day"] = pd.to_datetime(df["day"])

    df = df.sort_values(["blockchain", "day"])

    df["volume_7d_blockchain"] = ( #compute 7-day rolling volume per blockchain (liquidity trend)
        df.groupby("blockchain")["volume"]
          .rolling(7, min_periods=1)
          .sum()
          .reset_index(level=0, drop=True)
    )

    daily_totals = ( #compute total daily volume across all chains
        df.groupby("day")["volume"]
          .sum()
          .rename("total_volume_all_chains")
          .reset_index()
    )
    df = df.merge(daily_totals, on="day") #merge total volume back to original dataset

    df["volume_share"] = df["volume"] / df["total_volume_all_chains"] #compute each chain's share of total daily volume

    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True) #ensure output directory exists
    df.to_csv(PROCESSED_PATH, index=False) #save cleaned & enriched dataset
    print(f"[SUCCESS] Saved clean data to {PROCESSED_PATH}")


if __name__ == "__main__":
    transform() #entry point: run transformation pipeline
