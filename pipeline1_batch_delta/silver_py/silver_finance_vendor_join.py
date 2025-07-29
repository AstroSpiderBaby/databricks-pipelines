"""
silver_finance_vendor_join.py

Joins cleaned finance invoice data with cleaned vendor data for enriched silver output.
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, trim, col
from utils_py.utils_write_silver import write_silver_upsert

spark = SparkSession.builder.getOrCreate()

# === Step 1: Load source tables from Unity Volumes ===
df_finance = spark.read.format("delta").load("/Volumes/thebetty/silver/finances_invoices_clean_new")
df_vendors = spark.read.format("delta").load("/Volumes/thebetty/bronze/vendors")

# === Step 2: Standardize join keys ===
df_finance = df_finance.withColumn("vendor_id", upper(trim(col("vendor_id"))))
df_vendors = df_vendors.withColumn("vendor_id", upper(trim(col("vendor_id"))))

# === Step 3: Join on vendor_id ===
df_joined = df_finance.join(df_vendors, on="vendor_id", how="left")

# === Step 4: Select & rename columns ===
df_final = df_joined.select(
    "invoice_id", "vendor_id", "amount_usd", "invoice_date", "due_date",
    "paid_flag",
    col("vendor").alias("vendor_name"),
    "location", "rating"
)

# === Step 5: Write to Silver Output ===
output_path = "/Volumes/thebetty/silver/finance_with_vendor_info"
full_table_name = "thebetty.silver.finance_with_vendor_info"

write_silver_upsert(
    df=df_final,
    path=output_path,
    full_table_name=full_table_name,
    primary_key="invoice_id",
    required_columns=[
        "invoice_id", "vendor_id", "amount_usd", "invoice_date", "due_date",
        "paid_flag", 
        "vendor_name", "location", "rating"
    ],
    register_table=True,
    verbose=True
)

print("✅ Silver finance-vendor join complete and registered in Unity.")
