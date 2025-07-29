"""
silver_process_vendor_registry.py

Processes vendor registry files from ADF (Parquet format) into the Silver layer.

- Reads raw ADF files from mounted container
- Adds hashstring for deduplication
- Uses upsert_with_hashstring for incremental updates

Output: /mnt/delta/silver/vendor_registry_clean
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, current_timestamp

# Import reusable upsert logic
from utils.utils_upsert_with_hashstring import upsert_with_hashstring

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Read raw ADF vendor registry Parquet files
df_raw = spark.read.format("parquet").load("dbfs:/mnt/adf-silver/vendor_registry/")

# Add hashstring for deduplication (based on key business fields)
df_hashed = (
    df_raw.withColumn(
        "hashstring",
        sha2(concat_ws("||", *[
            col("vendor_id"),
            col("industry"),
            col("headquarters"),
            col("onwatchlist"),
            col("registrationdate"),
            col("tier")
        ]), 256)
    ).withColumn("ingestion_timestamp", current_timestamp())
)

# Define target path
target_path = "/mnt/delta/silver/vendor_registry_clean"

# Upsert using hash logic
upsert_with_hashstring(
    df=df_hashed,
    path=target_path,
    primary_key="vendor_id",
    hash_col="hashstring"
)
