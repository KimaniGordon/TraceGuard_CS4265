# Data Source Identification

Due to the scale of the datasets (50GB+), raw data is not stored in this repository. The following sources are used:

- **Source 1:** Loghub HDFS logs (found at `https://github.com/logpai/loghub`).
- **Source 2:** CSE-CIC-IDS2018 (retrieved from `s3://aec-common-data/unzipped/CSE-CIC-IDS2018/`).
- **Source 3:** AlienVault OTX API feeds.

**Local Directory Structure (for Spark ingestion):**
- `/data/raw/`: Landing zone for ingested logs.
- `/data/normalized/`: Output for Parquet conversion.
