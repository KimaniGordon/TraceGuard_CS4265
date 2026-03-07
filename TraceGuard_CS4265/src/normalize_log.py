from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# 1. Initialize Spark Session (Technology Choice: Spark v3.5)
spark = SparkSession.builder \
    .appName("TraceGuard_Normalization") \
    .getOrCreate()

# 2. Load Raw Data (Data Source: Loghub/HDFS Logs)
# This demonstrates handling "Variety" (JSON to Structured)
raw_logs = spark.read.json("data/sample_logs.json")

# 3. Simple Normalization Logic
# Adding a processing timestamp and standardizing schema
normalized_df = raw_logs.withColumn("ingested_at", current_timestamp())

# 4. Save to Columnar Format (Technology Choice: Parquet)
# This satisfies the "Syntax Layer" requirement
normalized_df.write.mode("overwrite").parquet("data/normalized_logs.parquet")

print("Normalization successful: JSON converted to Parquet.")
