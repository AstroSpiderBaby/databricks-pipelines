"""
bronze_vendor_compliance_ingest.py

Loads raw vendor compliance CSV data from Azure Blob Storage (mounted)
and writes it to the Bronze Delta Lake layer.
"""

from pyspark.sql import SparkSession
from utils.write_utils import write_df_to_delta
from pyspark.sql.functions import input_file_name, lit

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Define input and output paths
input_path = "/mnt/raw-ingest/vendor_compliance.csv"
output_path = "/mnt/delta/bronze/vendor_compliance"

# Load CSV with metadata
df_vendor_compliance = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_type", lit("vendor_compliance"))
)

# Write to Bronze
write_df_to_delta(
    df_vendor_compliance,
    path=output_path,
    partitionBy=None,
    mode="overwrite"
)
