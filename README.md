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

## Setup & Prerequisites
* **1. System Requirements**
Java 17: Required for Spark 3.x compatibility and modern JVM features.

Hadoop 3.x: NameNode and DataNode must be configured and active.

HBase 2.x: Master and RegionServer must be configured and active.

Python 3.9+: Standard Python environment for script orchestration.

* **2. Installation**
Dependency Setup: Install the required Python libraries via pip:

PowerShell
pip install -r requirements.txt

* **3. Configuration**
API Key: Obtain a free API key from [AlienVault OTX](https://otx.alienvault.com/).
Note on Threat Intelligence (OTX):
To populate the Serving Layer, your OTX account must be subscribed to active pulses. For the best results during the demo, I recommend subscribing to the following public pulses on the [OTX Portal](https://otx.alienvault.com/) (You must create a free account):

AlienVault Stock Pulse (General malicious IPs)

Brute Force/Scanner IPs

Known Malicious Botnets

Note: If no pulses are subscribed, the pipeline will still function using the "POC Workaround" injected indicators.

Environment Variables: Copy .env.example to a new file named .env and add: OTX_API_KEY=your_actual_key_here.
Additionally, open the .env file and update the following value: HDFS_HOST = localhost:9000 (Example value). This must match your Hadoop config.
How to Verify your HDFS_HOST? If you are unsure of your Hadoop address or port, you can verify them in your local Hadoop installation files. To do that, navigate to your Hadoop configuration directory (e.g., C:\hadoop\etc\hadoop\). Open the core-site.xml file. Look for the <name>fs.defaultFS</name> property.The value (e.g., hdfs://localhost:9000) contains the exact host and port you should use in your .env file.

Local Data: Ensure the data/raw/HDFS_large.log file is present in the directory before execution.

---

## How to Run
* **Step 1: Start Infrastructure**
Terminal 1 (Hadoop): Execute start-all.cmd to wake up the HDFS cluster.

Terminal 2 (HBase): Execute start-hbase.cmd to initialize the NoSQL layer.

Terminal 3 (Thrift): Execute hbase thrift start -p 9090 (Crucial for Python-to-HBase connectivity).

* **Step 2: Execute the Pipeline**
Master Command: Run the entire end-to-end system with a single command from the project root:

PowerShell
python main.py

---

## Querying the Results
* **1. Lookup a Threat (Serving Layer)**
HBase Query: 
PowerShell
 python -m src.utils.browse_hbase (View the top 25 records in the NoSQL cluster).

Targeted Queries: python -m src.utils.query_intel <IP_ADDRESS> (Verify specific metadata for a threat) 
Example: python -m src.utils.query_intel 1.12.76.172


Behavioral Search: python -m src.utils.search_threats "Brute-Force" (Search the distributed cluster by attack type).
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
