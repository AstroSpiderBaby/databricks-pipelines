"""
bronze_finances_invoices_ingest.py

Loads raw invoice CSV from Azure Blob Storage (mounted) into Bronze Delta Lake.
"""

from pyspark.sql import SparkSession
from utils_py.utils_write_delta import write_df_to_delta  # <- Updated import path

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Define input and output paths
input_path = "/mnt/raw-ingest/finance_invoice_data.csv"
output_path = "/mnt/delta/bronze/bronze_finance_invoices"

# Load CSV as DataFrame
df = spark.read.option("header", True).csv(input_path)

# Write DataFrame to Delta with enhanced utility function
write_df_to_delta(
    df=df,
    path=output_path,
    partition_by=None,         # Change to e.g., ["invoice_date"] if partitioning is needed
    mode="overwrite",
    merge_schema=True,
    register_table=True,
    dry_run=False,
    verbose=True               # Helpful for debugging/logging
)
