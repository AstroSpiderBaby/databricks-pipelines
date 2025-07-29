"""
silver_finance_invoices_clean.py

Cleans Bronze finance invoice data and writes it to Silver Delta Lake using hash-based upsert.

Input:
- Bronze Delta: /Volumes/thebetty/bronze/finances_invoices

Output:
- Unity Volume: /Volumes/thebetty/silver/finances_invoices_clean
- Unity Catalog Table: thebetty.silver.finances_invoices_clean
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, upper, trim, when,
    sha2, concat_ws, current_timestamp
)
from pyspark.sql.types import IntegerType
from utils_py.utils_write_silver import write_silver_upsert

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# === STEP 1: Load Bronze data ===
df = spark.read.format("delta").load("/Volumes/thebetty/bronze/finances_invoices")

# === STEP 2: Clean & Transform ===
df_clean = (
    df.withColumn("vendor", upper(trim(col("vendor"))))  # ✅ Standardize vendor
      .withColumn("invoice_date", to_date("invoice_date"))
      .withColumn("due_date", to_date("due_date"))
      .withColumn("paid_flag", when(col("paid") == "Yes", 1).otherwise(0).cast(IntegerType()))
      .drop("paid", "rating", "location")  # ✅ Retain vendor_name
)

# === STEP 3: Vendor Mapping ===
vendor_map = [
    ("WOLFE LLC", "V010"), ("MOORE-BERNARD", "V008"), ("GARCIA-JAMES", "V006"),
    ("ABBOTT-MUNOZ", "V001"), ("BLAIR PLC", "V003"), ("DUDLEY GROUP", "V004"),
    ("ARNOLD LTD", "V002"), ("MCCLURE, WARD AND LEE", "V007"),
    ("WILLIAMS AND SONS", "V009"), ("GALLOWAY-WYATT", "V005")
]
df_map = spark.createDataFrame(vendor_map, ["vendor", "vendor_id"])

# Join on cleaned vendor and keep vendor metadata
df_final = df_clean.join(df_map, on="vendor", how="inner") \
    .select(
        "vendor_id", "vendor",   # ✅ Include vendor_name
        "invoice_id", "amount_usd", "invoice_date", "due_date",
        "paid_flag", "source_file", "ingestion_type"
    )

# === STEP 4: Add hashstring and timestamp ===
df_hashed = df_final.withColumn(
    "hashstring", sha2(concat_ws("||", *[
        col("vendor_id"), col("invoice_id"), col("amount_usd"),
        col("invoice_date"), col("due_date"), col("paid_flag")
    ]), 256)
).withColumn("ingestion_timestamp", current_timestamp())

# === STEP 5: Write with upsert ===
output_path = "/Volumes/thebetty/silver/finances_invoices_clean_new"
full_table_name = "thebetty.silver.finances_invoices_clean_new"

write_silver_upsert(
    df=df_hashed,
    path=output_path,
    full_table_name=full_table_name,
    primary_key="invoice_id",
    required_columns=[
        "vendor_id", "vendor","invoice_id", "amount_usd", "invoice_date", "due_date",
        "paid_flag", "source_file", "ingestion_type", "hashstring", "ingestion_timestamp"
    ],
    partition_by=["invoice_date"],
    register_table=True,
    verbose=True
)

print("✅ Finance invoices written to Silver successfully.")
