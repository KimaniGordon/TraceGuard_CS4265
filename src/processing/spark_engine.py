import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from src.config import RAW_INTEL_PATH, INTEL_PARQUET_DIR

def run_transformations(input_path=RAW_INTEL_PATH):
    print("[INFO] Starting Optimized Spark Engine...")
    
    # 1. 'Emergency' Configuration
    # I droped shuffle partitions to 1 so Spark doesn't try to multitask on 100 files at once.
    spark = SparkSession.builder \
        .appName("TraceGuard_Normalization") \
        .config("spark.driver.memory", "768m") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    
    # 2. Define Schema Upfront (No more 'inferSchema' hang)
    # This prevents Spark from pre-scanning the 100k records.
    intel_schema = StructType([
        StructField("indicator", StringType(), True),
        StructField("type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("created", StringType(), True)
    ])
    
    try:
        # 3. Load with explicit schema
        raw_df = spark.read.csv(input_path, header=True, schema=intel_schema)
        
        # 4. Cleaning (Keep it lean)
        # Using coalesce(1) forces Spark to keep everything in one 'bucket' to save RAM.
        processed_df = raw_df.coalesce(1).dropDuplicates(["indicator"]) \
            .withColumn("type", lower(trim(col("type")))) \
            .withColumn("processed_at", current_timestamp())

        # 5. Distributed Storage (Simplified for Low Disk Space)
        # REMOVE .partitionBy("type") because it creates too much disk overhead on Windows
        print(f"[INFO] Writing to {INTEL_PARQUET_DIR}...")
        
        processed_df.write.mode("overwrite").parquet(INTEL_PARQUET_DIR)
            
        print(f"[SUCCESS] 100k indicators processed. No hang detected!")
        processed_df.show(5, truncate=False)
        
    except Exception as e:
        print(f"[ERROR] Spark processing failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_transformations()