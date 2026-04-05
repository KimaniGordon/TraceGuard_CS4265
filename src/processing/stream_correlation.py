import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, lit
# Pulling everything from config.py to ensure I have all the paths and HDFS connection details
from src.config import ( 
    HDFS_HOST,
    HDFS_TRAFFIC_PATH, 
    INTEL_PARQUET_DIR, 
    TRAFFIC_INPUT_DIR,
    ALERTS_OUTPUT_DIR,
    CHECKPOINT_DIR
)

def start_streaming():
    # 1. Initialize the Engine
    spark = SparkSession.builder \
        .appName("TraceGuard-Velocity-Simulation") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # 2. Path Verification for Schema Inference
    #  use the local sample to 'teach' Spark the column names
    SAMPLE_FILE = os.path.join(TRAFFIC_INPUT_DIR, "aws_subset.csv")

    if not os.path.exists(SAMPLE_FILE):
        print(f"[ERROR] Could not find sample file for schema inference: {SAMPLE_FILE}")
        spark.stop()
        sys.exit(1)

    # 3. Load the bad guys (Dimension Table)
    # This is the data that will be broadcasted to the stream
    threat_intel = spark.read.parquet(INTEL_PARQUET_DIR)

    # 4. AUTO-SCHEMA: Learn the column names (Dst Port, Protocol, etc.)
    temp_df = spark.read.option("header", "true").option("inferSchema", "true").csv(SAMPLE_FILE)
    traffic_schema = temp_df.schema

    # 5. Start the Stream
    raw_stream = spark.readStream \
        .option("header", "true") \
        .schema(traffic_schema) \
        .csv(HDFS_TRAFFIC_PATH)

    # Select only the high-value columns to save your RAM/Disk
    network_stream = raw_stream.select(
        "Dst Port", "Protocol", "Timestamp", "Flow Duration", 
        "TotLen Fwd Pkts", "Label"
    )

    # 6. POC WORKAROUND: Mocking the 'Src IP' 
    # Using '111.11.1.1' to test the join against  OTX data
    stream_with_ip = network_stream.withColumn("Src IP", lit("111.11.1.1")) 

    # 7. Perform the Broadcast Join
    correlated_alerts = stream_with_ip.join(
        broadcast(threat_intel), 
        stream_with_ip["Src IP"] == threat_intel.indicator, 
        "inner"
    )

    # 8. Output the Alerts to the local file system
    query = correlated_alerts.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", ALERTS_OUTPUT_DIR) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime='10 seconds') \
        .start()

    print(f" Simulation Active: Watching HDFS at {HDFS_HOST}")
    print(f" Writing hits to: {ALERTS_OUTPUT_DIR}")

    query.awaitTermination()

# Execute streaming if script is run directly
if __name__ == "__main__":
    start_streaming()