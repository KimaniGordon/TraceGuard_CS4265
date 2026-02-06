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
TraceGuard utilizes verified, high-volume public datasets:
* **Loghub (HDFS Logs):** 30GB+ of raw system logs for volume testing.
* **CSE-CIC-IDS2018 (AWS):** 450GB+ of labeled network traffic for large-scale analysis.
* **AlienVault OTX:** Real-time threat intelligence feeds via REST API.



##  Repository Structure
```text
├── data/           # Metadata and sample log snippets
├── docs/           # Milestone reports and architecture diagrams
├── src/            # PySpark normalization and correlation scripts
├── .gitignore      # Standard Python/Spark ignore rules
├── requirements.txt # Python library dependencies
└── README.md       # Project documentation
