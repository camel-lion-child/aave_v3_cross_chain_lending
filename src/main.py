from pathlib import Path
from extract_dune import run_dune_query
from transform import transform

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def main():
    # 1) Extract
    query_id = 5493069
    df_raw = run_dune_query(query_id=query_id)
    raw_path = PROJECT_ROOT / "data" / "raw" / "aave3_cross_chain_deposits_api.csv"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    df_raw.to_csv(raw_path, index=False)

    # 2) Transform
    transform()

if __name__ == "__main__":
    main()
