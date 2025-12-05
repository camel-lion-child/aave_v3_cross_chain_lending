##Aave V3 Cross-Chain Lending Analysis

**Overview
This project analyzes cross-chain lending and deposit activity on Aave V3 using real on-chain data from **Dune Analytics**.

Highlights:

- Query real on-chain lending data using Dune SQL

- Use Dune API from Python

- Build a simple ETL pipeline (extract → transform → analysis)

- Clean data and create rolling, total, and chain-share metrics

- Perform time-series EDA in a DeFi context

---

Tech Stack

- Python: pandas, matplotlib, requests, python-dotenv

- Dune Analytics: SQL + API

- Jupyter Notebook

---

Possible Future Improvements

- Store processed data in DuckDB or Parquet for analytics

- Automate runs using GitHub Actions or a scheduler

- Add liquidation or risk metrics

---

Why Raw Data Is NOT Stored in the Repository

The repository does not include raw or processed data files.
This is intentional and follows standard data engineering practice:

- Data can be recreated at any time by running the ETL pipeline

- GitHub should track code, SQL, notebooks, and documentation, not large datasets

Keeping the repo clean avoids:

- large files

- outdated snapshots

- accidental exposure of environment or API keys

Data folders (data/raw/ and data/processed/) are kept in the repository using small .gitkeep files so that the folder structure remains visible.
