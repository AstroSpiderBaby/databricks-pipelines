"""
bronze_finances_invoices_ingest.py

Loads raw invoice CSV from Azure Blob Storage (mounted) into Bronze Delta Lake.
"""

from pyspark.sql import SparkSession
from utils.write_utils import write_to_delta

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Paths
input_path = "/mnt/raw-ingest/finance_invoice_data.csv"
output_path = "/mnt/delta/bronze/bronze_finance_invoices"

# Load CSV
df = spark.read.option("header", True).csv(input_path)

# Write to Delta
write_to_delta(
    df=df,
    path=output_path,
    partitionBy=None,
    mode="overwrite"
)
