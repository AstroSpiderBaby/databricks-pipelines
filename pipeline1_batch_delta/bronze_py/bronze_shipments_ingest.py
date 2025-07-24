"""
bronze_shipments_ingest.py

Ingests raw shipment CSV data from Unity Catalog Volume
and writes it to the Bronze Delta Lake layer.
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql.functions import lit, col, input_file_name
from pyspark.sql import SparkSession
from utils_py.utils_write_delta import write_to_delta

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define input and output paths (using Unity Catalog Volumes)
input_path = "/Volumes/thebetty/bronze/landing_zone/Shipments.csv"
output_path = "/Volumes/thebetty/bronze/shipments"


# Read CSV file from volume
df_shipments = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_type", lit("shipments"))
)

# Write to Delta format in Unity Volume
write_to_delta(
    df_shipments,
    path=output_path,
    partition_by=None,
    mode="overwrite"
)
