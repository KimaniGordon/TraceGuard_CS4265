from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Start the Analytics Engine
spark = SparkSession.builder \
    .appName("TraceGuard_CrossLayer_Correlation") \
    .getOrCreate()

# 2. Paths
HDFS_PROCESSED_LOGS = "hdfs://localhost:9000/traceguard/processed/hdfs_logs_v1"

# 3. The "Caught" IP from our Speed Layer
MALICIOUS_IP = "10.250.19.102"

print(f"\n--- [CROSS-LAYER INVESTIGATION START] ---")
print(f"Target: Searching 1.5 GB of HDFS Logs for traces of {MALICIOUS_IP}...")

# 4. Load the Batch Layer Data
hdfs_history = spark.read.parquet(HDFS_PROCESSED_LOGS)

# 5. Search for the IP inside the log messages
# We use 'contains' because IPs are often buried in strings like "src: /85.11.161.198:54106"
evidence = hdfs_history.filter(F.col("message").contains(MALICIOUS_IP))

count = evidence.count()

if count > 0:
    print(f"\n[!] ALERT: MATCH FOUND! {count} historical interactions detected.")
    print(f"Showing forensic traces of {MALICIOUS_IP}:")
    evidence.select("date", "time", "component", "block_id", "message").show(10, truncate=False)
else:
    print(f"\n[OK] No historical traces of {MALICIOUS_IP} found in the Batch Layer.")
    print("Conclusion: The threat was blocked at the perimeter (Speed Layer) before internal access.")

spark.stop()