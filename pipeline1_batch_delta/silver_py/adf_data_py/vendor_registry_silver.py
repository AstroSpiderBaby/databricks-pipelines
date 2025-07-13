"""
vendor_registry_silver.py

Cleans ADF-sourced vendor registry data and writes it to Silver Delta Lake using hash-based upsert.

Input:
- dbfs:/mnt/adf-silver/ (Parquet from ADF)

Output:
- /mnt/delta/silver/vendor_registry_clean
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, current_timestamp
from utils.utils_upsert_with_hashstring import upsert_with_hashstring

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Read raw ADF Parquet
df_raw = spark.read.format("parquet").load("dbfs:/mnt/adf-silver")

# Clean and select relevant columns
df_clean = df_raw.select(
    "vendor_id", "industry", "headquarters", "onwatchlist",
    "registrationdate", "tier"
).dropna(subset=["vendor_id"])

# Add hashstring for upsert logic
df_hashed = (
    df_clean.withColumn(
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

# Define Silver output path
target_path = "/mnt/delta/silver/vendor_registry_clean"

# Upsert to Delta
upsert_with_hashstring(
    df=df_hashed,
    path=target_path,
    primary_key="vendor_id",
    hash_col="hashstring"
)

print("✅ Vendor registry written to Silver with upsert.")
