from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col

def process_hdfs_logs():
    print("[INFO] Starting Spark Engine for HDFS log parsing...")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TraceGuard_HDFS_Parsing") \
        .getOrCreate()
    
    try:
        # 1. Load the raw text file
        # Path: data/raw/HDFS_2k.log
        raw_logs = spark.read.text("data/raw/HDFS_2k.log")
        
        # 2. Define the Regex Pattern
        # This pattern matches: [Date] [Time] [PID] [Level] [Component]: [Message]
        log_pattern = r'^(\d{6})\s+(\d{6})\s+(\d+)\s+(\w+)\s+([^:]+):\s+(.*)$'
        
        # 3. Apply the transformation
        parsed_df = raw_logs.select(
            regexp_extract(col("value"), log_pattern, 1).alias("date"),
            regexp_extract(col("value"), log_pattern, 2).alias("time"),
            regexp_extract(col("value"), log_pattern, 3).alias("pid"),
            regexp_extract(col("value"), log_pattern, 4).alias("level"),
            regexp_extract(col("value"), log_pattern, 5).alias("component"),
            regexp_extract(col("value"), log_pattern, 6).alias("message")
        )
        
        # 4. Save to Processed folder as Parquet
        output_path = "data/processed/hdfs_logs_processed.parquet"
        parsed_df.write.mode("overwrite").parquet(output_path)
        
        print(f"[INFO] Success! HDFS logs converted to Parquet at {output_path}")
        
        # Show a sample of the structured data
        parsed_df.show(10, truncate=False)
        
    except Exception as e:
        print(f"[ERROR] HDFS processing failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_hdfs_logs()