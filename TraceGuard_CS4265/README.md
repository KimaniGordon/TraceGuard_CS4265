# TraceGuard_CS4265
A Distributed Log Correlation Engine for Multi-Layered Threat Detection


# TraceGuard: Distributed Log Correlation Engine

**TraceGuard** is a high-performance, scalable SIEM (Security Information and Event Management) engine built to address the challenges of massive cybersecurity telemetry. It utilizes a distributed stack to ingest, normalize, and correlate logs in parallel, identifying multi-stage threat patterns that exceed the capacity of single-node systems.

##  Project Overview
In modern enterprise environments, log volumes from firewalls, kernels, and applications frequently exceed single-node processing limits. TraceGuard bridges this "visibility gap" by leveraging distributed storage and in-memory parallel processing.

##  Specific Technology Stack
This project avoids "black box" cloud services by implementing a custom, versioned Big Data stack:

| Layer | Technology | Version | Rationale |
| :--- | :--- | :--- | :--- |
| **Storage** | Apache Hadoop HDFS | v3.3 | Fault-tolerant, block-level distribution |
| **Syntax** | Apache Parquet | v1.12 | Columnar storage with Snappy compression |
| **Data Store** | Apache HBase | v2.4 | Millisecond lookups for threat blacklists |
| **Processing** | Apache Spark | v3.5 | In-memory parallel correlation and joins |

##  Data Sources
TraceGuard utilizes the following publicly accessible datasets to simulate a high-volume enterprise environment:

1. **System Logs (Volume):** [Loghub - HDFS Dataset](https://github.com/logpai/loghub)
   - **Type:** Unstructured raw logs.
   - **Scale:** 30GB+ of distributed system telemetry.
   - **Purpose:** Testing the ingestion and normalization pipeline.

2. **Network Traffic (Ground Truth):** [CSE-CIC-IDS2018](https://registry.opendata.aws/cse-cic-ids2018/)
   - **Type:** Structured CSV/PCAP network flows.
   - **Scale:** 450GB+ hosted on AWS Open Data.
   - **Purpose:** Providing labeled attack data for correlation testing.

3. **Threat Intelligence (Enrichment):** [AlienVault OTX API](https://otx.alienvault.com/)
   - **Type:** Semi-structured JSON (REST API).
   - **Purpose:** Populating the HBase blacklist for real-time IP reputation lookups.


##  Repository Structure
```text
├── data/           # Metadata and sample log snippets
├── docs/           # Milestone reports and architecture diagrams
├── src/            # PySpark normalization and correlation scripts
├── .gitignore      # Standard Python/Spark ignore rules
├── requirements.txt # Python library dependencies
└── README.md       # Project documentation
