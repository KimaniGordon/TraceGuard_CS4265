# TraceGuard: Distributed Log Correlation Engine

## Project Structure (Milestone 2 Verified)
Below is the validated directory structure for the TraceGuard data pipeline:

├── data/
│   ├── raw/          <-- SUCCESS: threat_sample.csv is here (M2 goal)
│   └── processed/    <-- (Placeholder for M3 Spark output)
├── src/
│   ├── ingest.py     <-- SUCCESS: OTX API script (M2 goal)
│   └── processing/   <-- (Placeholder for M3 PySpark scripts)
├── .env.example      <-- SUCCESS: Security documentation (M2 goal)
├── requirements.txt  <-- SUCCESS: Dependencies documented
└── README.md

## Setup & Execution
1. **API Setup:** Copy \.env.example\ to \.env\ and add your OTX API Key.
2. **Installation:** Run \pip install -r requirements.txt\.
3. **Ingestion:** Execute \python src/ingest.py\ to populate the landing zone.
