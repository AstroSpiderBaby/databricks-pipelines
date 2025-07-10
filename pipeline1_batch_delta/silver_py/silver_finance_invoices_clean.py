"""
silver_finance_invoices_clean.py

Cleans raw finance invoice data from the Bronze Delta layer and prepares it for downstream joins.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Define paths
input_path = "/mnt/delta/bronze/bronze_finance_invoices"
output_path = "/mnt/delta/silver/silver_finance_invoices_clean"

# Read raw Bronze data
df = spark.read.format("delta").load(input_path)

# Clean and rename columns (update logic as needed)
df_cleaned = df.select(
    col("InvoiceID").alias("invoice_id"),
    col("VendorID").alias("vendor_id"),
    col("InvoiceDate").alias("invoice_date"),
    col("DueDate").alias("due_date"),
    col("Amount").alias("amount")
)

# Write to Silver
df_cleaned.write.format("delta").mode("overwrite").save(output_path)
