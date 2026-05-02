# TraceGuard: Data Validation Report

This report provides quantitative evidence that the **TraceGuard** pipeline produces correct, meaningful, and consistent output[cite: 25]. It validates the distributed join operations and the system's real-time detection capabilities.

---

## 1. Test Cases Executed
The following scenarios were tested to ensure the pipeline handles both standard flows and specific edge cases:

* **Multi-Vector Detection**: Verified that the Speed Layer can simultaneously detect malicious IPs (IPv4) and malicious payloads (SHA256/MD5).
* **Large-Scale HDFS Ingestion**: Confirmed the system handles "Large" telemetry files (1.47 GB) without data loss or buffer overflows.
* **Cross-Layer Investigation**: Validated the ability to pivot from a real-time alert to a Batch Layer forensic scan.
* **Resiliency (Schema Drift)**: Confirmed the Spark engine successfully processes OTX data even when field headers (e.g., pulse_name vs description) deviate from the expected schema.

## 2. Data Quality Metrics
The following quantitative measures were recorded during the final validation run of the 1.47 GB HDFS dataset:

| Pipeline Stage | Input Count | Output Count | Conformance | Metric |
| :--- | :--- | :--- | :--- | :--- |
| **Ingestion (OTX)** | 71,837 Raw Records | 14,151 (Unique Indicators) | 100% | Accuracy |
| **Batch (HDFS)** | 1.48 GB Log Data | 44 Replicated Blocks | 100% | Completeness |
| **Serving (HBase)** | 14,151 Indicators | 14,151 Rows | 100% | Consistency |
| **Speed (Stream)** | AWS Subset Traffic |Real-Time Correlation | 100% | Integrity |

**HDFS Screenshot**: 
<img width="1278" height="577" alt="Screenshot 2026-04-05 114934" src="https://github.com/user-attachments/assets/e5e7a95a-1b03-4866-a59e-098dcfdb0eed" />

**H-Base**:
<img width="1352" height="591" alt="Screenshot 2026-04-29 192809" src="https://github.com/user-attachments/assets/2e5ce567-b609-4511-829e-a1cc68f0c112" />

**Speed Layer Alerts**:
<img width="1342" height="573" alt="Screenshot 2026-04-29 185051" src="https://github.com/user-attachments/assets/22fc5f3d-1ef3-4910-a152-597dc747d5a5" />








## 3. Sample Validations (Specific Record Trace)
To demonstrate accuracy, a specific malicious record was traced through the entire system:

1. **Source Indicator**: `c529217014b732abbe646046c07ce8f0366a42051839d4cb3be5b400285fc728 (SHA-256 Hash)(ClickFix-style phishing site)` fetched from AlienVault OTX API.
2. **Batch Transformation**: Normalized into Parquet format; successfully indexed as a `Malicious_HASH` category.
3. **Serving Layer**: Verified record exists in HBase:
   <img width="1360" height="205" alt="Screenshot 2026-04-29 193609-clickfix" src="https://github.com/user-attachments/assets/f0b98f21-071f-4489-84d6-0a3b0712be34" />


   
5. **Detection**: Real-time traffic simulation matched this IP and triggered a Batch 0 alert in the Speed Layer.
6.  **Batch Layer Correlation**: A forensic scan of the 1.5 GB historical logs for the internal IP 10.250.19.102 yielded 25 historical matches, linking the threat to specific DataNode actions and Block IDs (blk_-1608...).

   <img width="1327" height="390" alt="Screenshot 2026-05-01 163108" src="https://github.com/user-attachments/assets/1c391f27-6cb2-4bb9-bac4-2fe781c4928c" />


## 4. Performance Results
**Performance characteristics recorded during the validation phase**: 

* Batch Throughput: Successfully converted 1.47 GB of raw logs into Parquet format.
* HBase Throughput: ~520 records per second during ingestion.
* Detection Latency: Sub-second correlation (Batch 0 processing).
* Resource Usage: Peak cluster memory usage was 4.2 GB.
* System Reliability: Spark backpressure successfully managed resource saturation. Despite the batch processing time exceeding the trigger interval (126s vs 30s), the system maintained data integrity and did not crash.
* Serving Layer Latency: HBase random-access lookups allowed for sub-second enrichment of the streaming traffic.

## 5. Known Limitations and Issues
	* API Limitations: The fetcher lacks an automated retry mechanism if the AlienVault API is down.
	* Scale Constraints: While architected for 50GB+, processing beyond 10GB on a single-node cluster causes JVM memory pressure.
	* Credential Dependency: System requires a valid .env file; otherwise, API ingestion will fail gracefully with a log error.
