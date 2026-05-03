import os
import sys
import happybase  
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config import ( 
    HDFS_HOST,
    HDFS_TRAFFIC_PATH, 
    INTEL_PARQUET_DIR, 
    TRAFFIC_INPUT_DIR,
    CHECKPOINT_DIR
)

def write_to_hbase(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    print(f"--- [BATCH {batch_id}]: Writing {batch_df.count()} hits to HBase ---")
    
    # POC-friendly conversion to Pandas
    records = batch_df.toPandas()
    
    try:
        connection = happybase.Connection('localhost', port=9090)
        if b'alerts' not in connection.tables():
            connection.create_table('alerts', {'cf': dict()})
        
        table = connection.table('alerts')
        
        for _, row in records.iterrows():
            row_key = f"{row['Timestamp']}_{row['src_ip']}"
            table.put(row_key.encode(), {
                b'cf:src_ip': str(row['src_ip']).encode(),
                b'cf:description': str(row['description']).encode(),
                b'cf:timestamp': str(row['Timestamp']).encode(),
                b'cf:label': str(row['Label']).encode()
            })
        connection.close()
    except Exception as e:
        print(f"[ERROR] HBase Write Failed: {e}")

def start_streaming():
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
    
    SAMPLE_FILE = os.path.join(TRAFFIC_INPUT_DIR, "aws_subset.csv")
    if not os.path.exists(SAMPLE_FILE):
        print(f"[ERROR] Could not find sample: {SAMPLE_FILE}")
        spark.stop()
        sys.exit(1)

    threat_intel = spark.read.parquet(INTEL_PARQUET_DIR)

    temp_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(SAMPLE_FILE)
    traffic_schema = temp_df.schema

    raw_stream = spark.readStream \
        .option("header", "true") \
        .schema(traffic_schema) \
        .csv(HDFS_TRAFFIC_PATH)

    network_stream = raw_stream.select(
        F.col("src_ip"), 
        F.col("file_hash"), 
        F.col("Dst Port").alias("dst_port"),
        F.col("Protocol"),
        F.col("Timestamp"),
        F.col("Label")
    )

    correlated_alerts = network_stream.join(
        F.broadcast(threat_intel), 
        (F.col("src_ip") == F.col("indicator")) | 
        (F.col("file_hash") == F.col("indicator")), 
        "inner"
    )

    # Sink 1: HBase
    query = correlated_alerts.writeStream \
        .foreachBatch(write_to_hbase) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime='30 seconds') \
        .start()
    
    # Sink 2: Console (For Screenshots)
    console_query = correlated_alerts.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime='30 seconds') \
        .start()

    #  Wait for any of the active streams to finish
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    start_streaming()