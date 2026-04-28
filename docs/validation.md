# TraceGuard: Data Validation Report

This report provides quantitative evidence that the **TraceGuard** pipeline produces correct, meaningful, and consistent output[cite: 25]. It validates the distributed join operations and the system's real-time detection capabilities.

---

## 1. Test Cases Executed
The following scenarios were tested to ensure the pipeline handles both standard flows and specific edge cases:

* **End-to-End Execution**: Verified that running `main.py` triggers all layers (Ingestion, Batch, Speed) successfully.
* **Automated Data Fetching**: Confirmed that the system detects missing logs and retrieves the 66.8 MB sample automatically.
* **Schema Enforcement**: Validated that raw HDFS logs are correctly transformed into structured Parquet with proper data types.
* **Threat Correlation**: Verified that the Speed Layer correctly identifies malicious IPs from the HBase serving layer.

## 2. Data Quality Metrics
[cite_start]The following quantitative measures were recorded during a full execution of the 66.8 MB sample:

| Pipeline Stage | Input Count | Output Count | Conformance | Metric |
| :--- | :--- | :--- | :--- | :--- |
| **Ingestion (OTX)** | 100,000+ API Records | 7,581 (IP Indicators) | 100% | Accuracy |
| **Batch (HDFS)** | 500,000 Log Lines | 500,000 Parquet Rows | 100% | Completeness |
| **Serving (HBase)** | 7,581 Indicators | 7,581 Rows | 100% | Consistency |
| **Speed (Stream)** | 1,000 Sample Records | 1,000 Scanned Rows | 100% | Integrity |

## 3. Sample Validations (Specific Record Trace)
To demonstrate accuracy, a specific malicious record was traced through the entire system:

1. **Source Indicator**: `1.15.33.90` fetched from AlienVault OTX API.
2. **Batch Transformation**: Normalized into Parquet format; successfully indexed as a `Malicious_IP` category.
3. **Serving Layer**: Verified record exists in HBase: 
   ```bash
   get 'threat_intel', '1.15.33.90'
4. **Detection**: Real-time traffic simulation matched this IP and triggered a Batch 0 alert in the Speed Layer.

## 4. Performance Results
Performance characteristics recorded during the validation phase:
	* Batch Runtime: 88 seconds (From raw HDFS log to Parquet normalization).
	* HBase Throughput: ~520 records per second during ingestion.
	* Detection Latency: Sub-second correlation (Batch 0 processing).
	* Resource Usage: Peak cluster memory usage was 4.2 GB.

## 5. Known Limitations and Issues
	* API Limitations: The fetcher lacks an automated retry mechanism if the AlienVault API is down.
	* Scale Constraints: While architected for 50GB+, processing beyond 10GB on a single-node cluster causes JVM memory pressure.
	* Credential Dependency: System requires a valid .env file; otherwise, API ingestion will fail gracefully with a log error.
