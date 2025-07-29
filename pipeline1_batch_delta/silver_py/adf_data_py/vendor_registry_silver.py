"""
vendor_registry_silver.py

Cleans ADF-sourced vendor registry data and writes it to Silver Delta Lake using hash-based upsert.

Input:
- Unity Catalog Volume: /Volumes/thebetty/silver/landing_zone/ (Parquet from ADF)

Output:
- Unity Catalog Table: thebetty.silver.vendor_registry_clean
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, current_timestamp
from utils_py.utils_silver_write import write_silver_upsert

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# === STEP 1: Read raw Parquet from Unity Volume ===
input_path = "/Volumes/thebetty/silver/landing_zone"
df_raw = spark.read.format("parquet").load(input_path)

# === STEP 2: Clean and select relevant columns ===
df_clean = df_raw.select(
    "vendor_id", "industry", "headquarters", "onwatchlist",
    "registrationdate", "tier"
).dropna(subset=["vendor_id"])

# === STEP 3: Add hashstring for upsert logic ===
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

# === STEP 4: Define output and write to Unity Catalog ===
output_path = "/Volumes/thebetty/silver/vendor_registry_clean"
full_table_name = "thebetty.silver.vendor_registry_clean"

write_silver_upsert(
    df=df_hashed,
    path=output_path,
    full_table_name=full_table_name,
    primary_key="vendor_id",
    required_columns=[
        "vendor_id", "industry", "headquarters", "onwatchlist",
        "registrationdate", "tier", "hashstring"
    ],
    register_table=True,
    verbose=True
)

print("✅ Vendor registry written to Silver with Unity-compatible upsert.")
