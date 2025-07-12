"""
bronze_shipments_ingest.py

Ingests raw shipment CSV data from Azure Blob Storage (mounted)
and writes it to the Bronze Delta Lake layer.
"""

from pyspark.sql import SparkSession
from utils_py import write_df_to_delta
from pyspark.sql.functions import input_file_name, lit

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define input and output paths
input_path = "/mnt/raw-ingest/shipments_data.csv"
output_path = "/mnt/delta/bronze/shipments"

# Read CSV file
df_shipments = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_type", lit("shipments"))
)

# Write to Bronze
write_df_to_delta(
    df_shipments,
    path=output_path,
    partition_by=None,
    mode="overwrite"
)
