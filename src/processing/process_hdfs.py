from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col
import os

def process_hdfs_logs():
    print("[INFO] Starting Spark Engine for HDFS log parsing...")
    
    # 1. Initialize Spark with Java 17 fixes and tweaks
    #  Added the extraJavaOptions so Spark doesn't crash on my JDK
    spark = SparkSession.builder \
        .appName("TraceGuard_HDFS_Parsing") \
        .config("spark.driver.extraJavaOptions", 
                "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions", 
                "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
        .getOrCreate()
    
    try:
        # 2. Load the 1.5 GB file from HDFS
        # hdfs://localhost:9000 prefix
        input_path = "hdfs://localhost:9000/traceguard/raw/logs/HDFS_large.log"
        print(f"[INFO] Reading data from: {input_path}")
        raw_logs = spark.read.text(input_path)
        
        # 3. This defines the Regex Pattern
        # This matches: [Date] [Time] [PID] [Level] [Component]: [Message with BlockID]
        log_pattern = r'^(\d{6})\s+(\d{6})\s+(\d+)\s+(\w+)\s+([^:]+):\s+(.*(blk_-?\d+).*)$'
        
        # 4. Extract structured columns
        parsed_df = raw_logs.select(
            regexp_extract(col("value"), log_pattern, 1).alias("date"),
            regexp_extract(col("value"), log_pattern, 2).alias("time"),
            regexp_extract(col("value"), log_pattern, 4).alias("level"),
            regexp_extract(col("value"), log_pattern, 5).alias("component"),
            regexp_extract(col("value"), log_pattern, 7).alias("block_id"), # Crucial for joins
            regexp_extract(col("value"), log_pattern, 6).alias("message")
        )
        
        # 5. Save back to HDFS as Parquet (The standard for Big Data)
        # Saving to HDFS ensures the data stays distributed
        output_path = "hdfs://localhost:9000/traceguard/processed/hdfs_logs_v1"
        
        print(f"[INFO] Processing 1.5 GB. Writing to {output_path}...")
        parsed_df.write.mode("overwrite").parquet(output_path)
        
        print(f"[SUCCESS] HDFS logs converted to Parquet in the cluster!")
        parsed_df.show(10, truncate=False)
        
    except Exception as e:
        print(f"[ERROR] HDFS processing failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_hdfs_logs()