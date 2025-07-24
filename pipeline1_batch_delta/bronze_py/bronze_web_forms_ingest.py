"""
bronze_web_forms_ingest.py

Ingests web form submission data from JSON (multiline) stored in Unity Catalog Volume.
Writes raw data to the Bronze Delta Lake layer with source tracking metadata.
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from utils_py.utils_write_delta import write_to_delta
from pyspark.sql.functions import col, lit

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Define input and output paths
input_path = "/Volumes/thebetty/bronze/landing_zone/web_form_submissions.json"
output_path = "/Volumes/thebetty/bronze/web_forms"

# Load JSON data
df_web_forms = (
    spark.read
        .option("multiline", "true")
        .json(input_path)
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_type", lit("web_forms"))
)

# Write to Bronze Delta
write_to_delta(
    df_web_forms,
    path=output_path,
    partition_by=None,
    mode="overwrite"
)
