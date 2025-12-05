import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_PATH = PROJECT_ROOT / "data" / "raw" / "aave3_cross_chain_deposits_api.csv"
PROCESSED_PATH = PROJECT_ROOT / "data" / "processed" / "aave3_cross_chain_deposits_clean.csv"


def transform():
    df = pd.read_csv(RAW_PATH)

    df.columns = [c.lower().strip() for c in df.columns]
    df["day"] = pd.to_datetime(df["day"])

    df = df.sort_values(["blockchain", "day"])

    df["volume_7d_blockchain"] = (
        df.groupby("blockchain")["volume"]
          .rolling(7, min_periods=1)
          .sum()
          .reset_index(level=0, drop=True)
    )

    daily_totals = (
        df.groupby("day")["volume"]
          .sum()
          .rename("total_volume_all_chains")
          .reset_index()
    )
    df = df.merge(daily_totals, on="day")

    df["volume_share"] = df["volume"] / df["total_volume_all_chains"]

    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED_PATH, index=False)
    print(f"[SUCCESS] Saved clean data to {PROCESSED_PATH}")


if __name__ == "__main__":
    transform()
