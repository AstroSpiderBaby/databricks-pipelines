"""
vendor_registry_silver.py

Cleans ADF-sourced vendor registry data and writes it to Silver Delta Lake using hash-based upsert.

Input:
- Azure Blob: wasbs://adf-silver@datalakelv426.blob.core.windows.net/ (from ADF)
- Mirrored to Unity Catalog Volume: /Volumes/thebetty/silver/vendor_registry_clean

Output:
- Unity Catalog Table: thebetty.silver.vendor_registry_clean
"""

# Add repo path to Python sys.path so we can import custom utils
import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

# DEBUG: Show current sys.path
print("\n🔍 Python sys.path:")
for p in sys.path:
    print("  -", p)

# DEBUG: Check if utility script exists
import os
print("\n📂 utils_write_silver.py exists:", os.path.exists(
    "/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta/utils_py/utils_write_silver.py"
))

# --- Imports ---
from pyspark.sql import SparkSession
from utils_py.utils_write_silver import write_silver_upsert
from utils_py.utils_mirror_adf_to_volume import mirror_adf_parquet_to_uc_volume

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# === STEP 0: Mirror ADF output from Blob Storage into Unity Volume ===
mirror_adf_parquet_to_uc_volume(
    dbutils=dbutils,
    container="adf-silver",
    relative_path="",  # root directory of container
    unity_volume_path="/Volumes/thebetty/silver/vendor_registry_clean",
    secret_scope="astro_keyvault",
    secret_key="storage_key"
)

# === STEP 1: Load mirrored Delta from Unity Volume ===
input_path = "/Volumes/thebetty/silver/vendor_registry_clean"
df_raw = spark.read.format("delta").load(input_path)

# === STEP 2: Optional field pruning (you can skip this if you want all fields)
df_clean = df_raw.select(
    "vendor_id", "industry", "headquarters", "onwatchlist",
    "registrationdate", "tier", "hashstring", "ingestion_timestamp"
).dropna(subset=["vendor_id"])

# === STEP 3: Upsert to Unity Catalog Silver table ===
output_path = "/Volumes/thebetty/silver/vendor_registry_clean"
full_table_name = "thebetty.silver.vendor_registry_clean"

write_silver_upsert(
    df=df_clean,
    path=output_path,
    full_table_name=full_table_name,
    primary_key="vendor_id",
    required_columns=[
        "vendor_id", "industry", "headquarters", "onwatchlist",
        "registrationdate", "tier", "hashstring", "ingestion_timestamp"
    ],
    register_table=True,
    verbose=True
)

print("✅ Vendor registry written to Silver with Unity-compatible upsert.")
