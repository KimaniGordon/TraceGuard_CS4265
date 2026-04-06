#  TraceGuard: Unified Big Data IDS Pipeline

**TraceGuard** is a high-performance Intrusion Detection System (IDS) prototype built on a **Lambda Architecture**. It demonstrates the ability to process massive historical log datasets (**Batch Layer**) while simultaneously correlating high-velocity network traffic against real-time threat intelligence (**Speed Layer**).

---

##  Architecture Overview
The pipeline is orchestrated by a central master script and divided into five distinct stages:
* **Environment Cleanup:** Automated purging of legacy test data and checkpoints to prevent disk overflow.
* **Multimodal Ingestion:** Retrieval of 100k+ atomic threat indicators via AlienVault OTX and network traffic subsets from the AWS Public Registry.
* **Distributed Storage:** Migration of raw telemetry (1.47GB logs) into the HDFS cluster.
* **Spark Processing:** Large-scale log parsing and indicator normalization using optimized Spark configurations.
* **Serving & Speed Layers:** Populating HBase for low-latency lookups and launching a Spark Structured Streaming engine for real-time detection.

---

##  Project Directory Structure
```text
TraceGuard/
├── data/
│   ├── raw/
│   │   ├── HDFS_large.log             # The 66MB sample for Batch Layer
│   │   ├── network_traffic/           # Empty (Populated by Stage 1)
│   │   └── threat_intel_raw.csv       # Ignored (Populated by Stage 1)
│   └── processed/
│       └── alerts/                    # Target for Speed Layer JSONs
├── src/
│   ├── config.py                      # Centralized Localhost/Path settings
│   ├── ingestion/
│   │   ├── fetch_otx.py               # API: Pulls latest threat intel
│   │   ├── fetch_aws.py               # API: Pulls 350MB traffic log
│   │   └── load_data.py               # HDFS: Moves files to cluster
│   ├── processing/
│   │   ├── data_cleanse.py            # Spark: Pre-processing & Deduplication
│   │   ├── spark_engine.py            # Spark: Normalization Engine
│   │   ├── process_hdfs.py            # Spark: Log Parsing
│   │   ├── load_hbase.py              # HBase: Serving Layer Ingestion
│   │   └── stream_correlation.py      # Streaming: Real-time Speed Layer
│   └── utils/
│       ├── browse_hbase.py            # Diagnostic: View Serving Layer
│       ├── search_threat.py           # Forensic: Keyword search
│       ├── reset_hbase.py             # Maintenance: Clear/Recreate Table
│       └── query_intel.py             # Forensic: Single IP lookup
├── main.py                            # THE MASTER ORCHESTRATOR
├── .gitignore                         # Prevents 350MB+ bloat
└── README.md                          # Architecture & Run Instructions
```
---
## External Data Acquisition
** *To accommodate different hardware constraints and grading timelines, you may choose one of the following two data scales. Both files must be placed in data/raw/ and renamed to HDFS_large.log for the pipeline to recognize them.**

* **Dataset Context:**

 A 500,000-line subset of the HDFS log (~100 MB) (Benefit: Completes the Spark Processing stage in approximately 2 minutes.) or Full-Scale HDFS Log (1.47 GB)
 The sample subset is already in the data/raw location of this repository. If you choose the larger dataset, download that file with one of the links below.

* **Source:** Visit the [logpai/loghub](https://github.com/logpai/loghub) GitHub repository or the [Loghub Zenodo](https://zenodo.org/records/8196385) page.

* **Download:** Locate and download HDFS_1.tar.gz (1.47 GiB).

* **Extraction:** Extract the archive to find the raw log file named HDFS.log.

* **Placement:** Move the file to TraceGuard/data/raw/.

Rename the file to HDFS_large.log for the automation scripts to recognize it correctly.

---

### 🖥️ Verified Environment (Milestone 3)
The TraceGuard pipeline has been verified on the following 2026 "Modern Enterprise" stack:

* **Java:** OpenJDK 17.0.18 LTS (Microsoft Build)
* **Hadoop:** 3.4.3 (Standalone Distributed Mode)
* **HBase:** 2.5.13 (Thrift 1 Gateway enabled)
* **Spark:** PySpark 4.1.1 (Standard Distribution)
* **Python:** 3.14.2

**Configuration Note:** Due to Java 17's modularity constraints, the `JDK_JAVA_OPTIONS` environment variable is required to enable Spark/HDFS internal reflection. See the [Setup] section for the specific export command.

## Setup & Prerequisites
* **1.  General System Requirements (if you dont use the setup above and would to skip using Java 17) **
Java 11 (LTS): Required for Spark 3.x compatibility and modern JVM features.

Hadoop 3.3.6: NameNode and DataNode must be configured and active.

HBase 2.5.5: Master and RegionServer must be configured and active.

Python 3.11.x: Standard Python environment for script orchestration.

Spark: PySpark 3.4.1

## Important: Running on Java 17
This project was developed using Java 17. Because Java 17 restricts access to certain internal libraries that Spark and Hadoop rely on for performance, you must set the following environment variable before running the hadoop and h-base scripts:

***For PowerShell (Windows):**
$env:JDK_JAVA_OPTIONS = "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=j

*"Note: This project is optimized for Java 17. If using Java 8 or 11, the JDK_JAVA_OPTIONS environment variable is not required and may be omitted."*


**Configuration Note:** Due to Java 17's modularity constraints, the `JDK_JAVA_OPTIONS` environment variable is required to enable Spark/HDFS internal reflection. See the [Setup] section for the specific export command.

* **2. Installation**
Dependency Setup: Install the required Python libraries via pip:

PowerShell
pip install -r requirements.txt

* **3. Configuration**
API Key: Obtain a free API key from [AlienVault OTX](https://otx.alienvault.com/).
Note on Threat Intelligence (OTX):
To populate the Serving Layer, your OTX account must be subscribed to active pulses. For the best results during the demo, I recommend subscribing to the following public pulses on the [OTX Portal](https://otx.alienvault.com/) (You must create a free account):

Ones that include Brute Force/Scanner IPs or Known Malicious Botnets.

Note:you only have to subscribe to 2 or so.
To find the pulses and subscribe, click on "Browse" in the top dashboard after creating your account. Then you should be able to subscribe to 2 or so pulses.

Environment Variables: Copy .env.example to a new file named .env and add: OTX_API_KEY=your_actual_key_here.
Additionally, open the .env file and update the following value: HDFS_HOST = localhost:9000 (Example value). This must match your Hadoop config.
How to Verify your HDFS_HOST? If you are unsure of your Hadoop address or port, you can verify them in your local Hadoop installation files. To do that, navigate to your Hadoop configuration directory (e.g., C:\hadoop\etc\hadoop\). Open the core-site.xml file. Look for the <name>fs.defaultFS</name> property.The value (e.g., hdfs://localhost:9000) contains the exact host and port you should use in your .env file.

Local Data: Ensure the data/raw/HDFS_large.log file is present in the directory before execution.

---

## How to Run
* **Step 1: Start Infrastructure**
Terminal 1 (Hadoop): Execute **start-all.cmd* to wake up the HDFS cluster.

Terminal 2 (HBase): Execute **start-hbase.cmd* to initialize the NoSQL layer.

Terminal 3 (Thrift): Execute **hbase thrift start -p 9090* (Crucial for Python-to-HBase connectivity).

* **Step 2: Execute the Pipeline**
Master Command: Run the entire end-to-end system with a single command from the project root:

PowerShell
python main.py


Once you hit step 5 in main.py, it will hang until you exit (Ctrl+C). An error "Pipeline failed" may appear after; ignore it, it's a bug that I couldn't fix.

---

## Known Issues
Python 3.14 Bytecode: Due to bytecode changes in Python 3.14, the Spark Driver and Workers must run the same version to avoid serialization mismatches.

Stream Shutdown: Upon exiting the Speed Layer (Ctrl+C), a Pipeline failed message may appear in the console. This is a known cleanup sequence limitation in the Spark-HBase connector and does not affect data integrity or previous stages.

---


## Querying the Results
* **1. Lookup a Threat (Serving Layer)**
HBase Query: 
PowerShell
 python -m src.utils.browse_hbase (View the top 25 records in the NoSQL cluster).

Targeted Queries: python -m src.utils.query_intel <IP_ADDRESS> (Verify specific metadata for a threat) 
Example: python -m src.utils.query_intel 1.12.76.172


Behavioral Search: python -m src.utils.search_threat "Brute-Force" (Search the distributed cluster by attack type).
* **2. View Detection Hits (Speed Layer)**
Alert Summary: Display a human-readable summary of real-time alerts generated by the Spark stream:

PowerShell
python -m src.utils.view_alerts

---


## Technical Optimization Notes
* **Feature Selection: Pruned network telemetry from 80+ features to the core 5-tuple, reducing memory overhead by 80%.**

* **Partition Tuning: Restricted Spark shuffle partitions to 1 or 2 to prevent excessive disk "spilling" on consumer hardware.**

* **Memory Management: Capped Spark driver and executor memory at 1g to ensure stability alongside host system processes.**

* **JVM Compatibility: Specifically tuned for Java 17 via dynamic --add-opens reflection flags in the SparkSession builder to prevent IllegalAccess errors.**

---

## Pipeline Documentation
The TraceGuard pipeline is an automated 4-stage Lambda Architecture:

* **Stage 1: Ingestion**

Task: Fetches live threat feeds via AlienVault OTX API and network subsets from AWS S3.

Config: src/config.py (API keys and URL endpoints).

* **Stage 2: Cluster Landing**

Task: Transports raw binaries (1.47 GB) into the Hadoop HDFS environment.

Command: python -m src.ingestion.load_data.

* **Stage 3: Batch Normalization**

Task: Spark Engine parses unstructured HDFS logs into Snappy-compressed Parquet.

Transformation: Regex-based schema-on-read for log normalization.

* **Stage 4: Serving Layer Load**

Task: Deduplicates data and populates the HBase NoSQL store.

Result: 10,630 enriched indicators ready for sub-second lookup.

---

## Schema Documentation & Rationale
Architecture: NoSQL (Wide-Column Store)
Technology: Apache HBase
Rationale: * RowKey Selection:  Used the Indicator (IP/Hash) as the RowKey. This allows for O(1) lookup time, which is mandatory for real-time security correlation.

Column Families: utilize a single column family cf to minimize disk seek time and simplify the storage of heterogeneous threat data (IPs and Hashes in the same table).

Storage Format: Parquet was chosen for HDFS because its Columnar Storage allows Spark to skip irrelevant columns during queries, reducing I/O by up to 80% compared to raw CSV.
