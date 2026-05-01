import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
    # --- MODERN JVM CONFIGURATION ---
    # Essential for Spark Structured Streaming on Java 17+.
    # This ensures the 'Checkpointing' and 'State Management' work without JVM crashes.
    spark = SparkSession.builder \
        .appName("TraceGuard_Speed_Layer") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.extraJavaOptions", 
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions", 
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
        .getOrCreate()
    
    # 2. Path Verification for Schema Inference
    # use the local sample to 'teach' Spark the column names
    SAMPLE_FILE = os.path.join(TRAFFIC_INPUT_DIR, "aws_subset.csv")

    if not os.path.exists(SAMPLE_FILE):
        print(f"[ERROR] Could not find sample file for schema inference: {SAMPLE_FILE}")
        spark.stop()
        sys.exit(1)

    # 3. Load the bad guys (Dimension Table)
    # This is the data that will be broadcasted to the stream
    threat_intel = spark.read.parquet(INTEL_PARQUET_DIR)

    # 4. AUTO-SCHEMA: Learn the column names (Dst Port, Protocol, etc.)
    temp_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .csv(SAMPLE_FILE)
    traffic_schema = temp_df.schema

    # 5. Start the Stream
    raw_stream = spark.readStream \
        .option("header", "true") \
        .option("ignoreLeadingWhiteSpace", "true") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .schema(traffic_schema) \
        .csv(HDFS_TRAFFIC_PATH)

    # Select only the high-value columns to save your RAM/Disk
    network_stream = raw_stream.select(
        F.col("src_ip"), 
        F.col("file_hash"), 
        F.col("Dst Port").alias("dst_port"),
        F.col("Protocol"),
        F.col("Timestamp"),
        F.col("Label")
    )
    # 6. POC WORKAROUND: Mocking the 'Src IP' 
    # Using '111.11.1.1' to test the join against OTX data
    #stream_with_ip = network_stream.withColumn("Src IP", lit("111.11.1.1")) 

    # 7. Perform the Broadcast Join
    # UPDATED: Added F. prefix to broadcast and fixed the column reference
    correlated_alerts = network_stream.join(
        F.broadcast(threat_intel), 
        (F.col("src_ip") == F.col("indicator")) | 
        (F.col("file_hash") == F.col("indicator")), 
        "inner"
    )

    # 8. Output the Alerts to the local file system
    query = correlated_alerts.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("path", ALERTS_OUTPUT_DIR) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime='30 seconds') \
        .start()

    print(f" Simulation Active: Watching HDFS at {HDFS_HOST}")
    print(f" Writing hits to: {ALERTS_OUTPUT_DIR}")

    query.awaitTermination()

# Execute streaming if script is run directly
if __name__ == "__main__":
    start_streaming()