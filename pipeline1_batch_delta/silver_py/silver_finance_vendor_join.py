"""
silver_finance_vendor_join.py

Joins cleaned finance invoice data with cleaned vendor data for enriched silver output.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, trim, col

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load inputs
df_finance = spark.read.format("delta").load("/mnt/delta/silver/finance_invoices_v2")
df_vendors = spark.read.format("delta").load("/mnt/delta/silver/vendors_clean")

# Standardize join keys
df_finance = df_finance.withColumn("vendor_id", upper(trim(col("vendor_id"))))
df_vendors = df_vendors.withColumn("vendor_id", upper(trim(col("vendor_id"))))

# Join
df_joined = df_finance.join(df_vendors, on="vendor_id", how="left")

# Select & rename columns
df_joined = df_joined.select(
    "invoice_id", "vendor_id", "amount_usd", "invoice_date", "due_date",
    "paid_flag", "source_file", "ingestion_type",
    col("name").alias("vendor_name"),
    "location", "rating"
)

# Write output
df_joined.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save("/mnt/delta/silver/finance_with_vendor_info")

print("✅ Silver finance-vendor join complete.")
