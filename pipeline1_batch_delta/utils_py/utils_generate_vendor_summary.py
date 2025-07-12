"""
Utility Script: utils_generate_vendor_summary.py

Generates a quick summary of vendor-level invoice data by aggregating invoice counts
and identifying the latest dates per vendor for testing or standalone use.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, max

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load cleaned Silver finance + vendor data
df = spark.read.format("delta").load("/mnt/delta/silver/finance_with_vendor_info")

# Aggregate to create summary
summary_df = df.groupBy("vendor_id", "vendor_name").agg(
    countDistinct("invoice_id").alias("total_invoices"),
    max("due_date").alias("latest_due_date"),
    max("invoice_date").alias("latest_invoice_date")
)

# Show results
summary_df.display()
