"""
bronze_vendors_ingest.py

Loads raw vendor CSV data from Azure Blob Storage (mounted)
and writes it to the Bronze Delta Lake layer.
"""
import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from utils_py.utils_write_delta import write_to_delta
from pyspark.sql.functions import input_file_name, lit

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Define input and output paths
input_path = "dbfs:/FileStore/pipeline1_batch_delta/moc_source_c/Vendors.csv"
output_path = "/mnt/delta/bronze/vendors"

# Load CSV with metadata
df_vendors = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_type", lit("vendors"))
)

# Write to Bronze
write_to_delta(
    df_vendors,
    path=output_path,
    partition_by=None,
    mode="overwrite"
)
