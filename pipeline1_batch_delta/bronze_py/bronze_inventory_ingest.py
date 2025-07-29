"""
bronze_inventory_ingest.py

Ingests raw inventory CSV data from Unity Catalog Volume
and writes it to the Bronze Delta Lake layer.
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from utils_py.utils_write_delta import write_to_delta

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define input and output paths (using Unity Catalog Volumes)
input_path = "/Volumes/thebetty/bronze/landing_zone/Inventory.csv"
output_path = "/Volumes/thebetty/bronze/inventory"
full_table_name = "thebetty.bronze.inventory"  # ✅ Add this

# Read CSV file from volume
df_inventory = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_type", lit("inventory"))
)

# Write to Delta format in Unity Volume
write_to_delta(
    df=df_inventory,
    path=output_path,
    full_table_name=full_table_name,  # ✅ Pass it here
    partition_by=None,
    mode="overwrite",
    merge_schema=True,
    register_table=True,
    dry_run=False,
    verbose=True
)