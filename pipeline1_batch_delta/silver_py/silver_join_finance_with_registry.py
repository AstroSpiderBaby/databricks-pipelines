"""
silver_join_finance_with_registry.py

Joins Silver-level enriched finance + vendor data with Vendor Registry data.
Writes the fully enriched result to Unity Volume and registers as Delta table.
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from utils_py.utils_write_silver import write_silver_upsert

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# === Step 1: Define source Unity Volume paths ===
finance_path = "/Volumes/thebetty/silver/finance_with_vendor_info"
registry_path = "/Volumes/thebetty/silver/vendor_registry_clean"
output_path = "/Volumes/thebetty/silver/finance_with_vendors_enriched"
full_table_name = "thebetty.silver.finance_with_vendors_enriched"

# === Step 2: Load Silver tables ===
df_finance = spark.read.format("delta").load(finance_path)
df_registry = spark.read.format("delta").load(registry_path)

# === Step 3: Perform enrichment join ===
df_enriched = df_finance.join(df_registry, on="vendor_id", how="left")

# === Step 4: Write to Unity Volume and register table ===
write_silver_upsert(
    df=df_enriched,
    path=output_path,
    full_table_name=full_table_name,
    primary_key="invoice_id",
    required_columns=df_enriched.columns,  # keep all columns
    partition_by=["invoice_date"],  # optional, but good for performance if date exists
    register_table=True,
    verbose=True
)

print(f"✅ Successfully wrote and registered: {full_table_name}")
