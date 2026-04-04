from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, current_timestamp, to_timestamp

def run_transformations(input_path="data/raw/threat_intel_raw.csv"):
    print("[INFO] Starting Spark Engine for 100k records...")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TraceGuard_Normalization") \
        .getOrCreate()
    
    try:
        # 1. Load the data 
        raw_df = spark.read.csv(input_path, header=True, inferSchema=True)
        
        # 2. Cleaning & Normalization
        # - Remove duplicates based on 'indicator' field
        # - Standardize the 'type' to lowercase
        # - Convert string timestamp to a proper Spark Timestamp object
        processed_df = raw_df.dropDuplicates(["indicator"]) \
            .withColumn("type", lower(trim(col("type")))) \
            .withColumn("created_at", to_timestamp(col("created"))) \
            .withColumn("processed_at", current_timestamp())
        
        # 3. Distributed Storage
        # partition by 'type' (e.g., ipv4, filehash) so it's faster to query later
        output_path = "data/processed/threat_indicators.parquet"
        processed_df.write.mode("overwrite") \
            .partitionBy("type") \
            .parquet(output_path)
            
        print(f"[INFO] Success! Unique indicators processed and saved to {output_path}")
        processed_df.show(10, truncate=False)
        
    except Exception as e:
        print(f"[ERROR] Spark processing failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_transformations()