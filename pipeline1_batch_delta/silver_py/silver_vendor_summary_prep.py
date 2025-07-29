"""
silver_vendor_summary_prep.py

Combines Silver-level finance and vendor compliance data
to create a summary prep table for downstream Gold layer.

- Joins finance and compliance on vendor_id
- Output path: /Volumes/thebetty/silver/final_vendor_summary_prep
- Table: thebetty.silver.final_vendor_summary_prep
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from utils_py.utils_write_silver import write_silver_upsert

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# === Step 1: Load Silver-level finance + compliance data ===
df_finance = spark.table("thebetty.silver.finance_with_vendor_info")
df_compliance = spark.table("thebetty.bronze.vendor_compliance")

# === Step 2: Join on vendor_id ===
df_final = df_finance.join(df_compliance, on="vendor_id", how="left")

# === Step 3: Define write targets ===
output_path = "/Volumes/thebetty/silver/final_vendor_summary_prep"
full_table_name = "thebetty.silver.final_vendor_summary_prep"

# === Step 4: Write using upsert utility ===
write_silver_upsert(
    df=df_final,
    path=output_path,
    full_table_name=full_table_name,
    primary_key="invoice_id",  # or vendor_id if one row per vendor
    required_columns=df_final.columns,
    partition_by=["invoice_date"],  # adjust if needed
    register_table=True,
    verbose=True
)

print("✅ Vendor summary prep written to Silver.")
