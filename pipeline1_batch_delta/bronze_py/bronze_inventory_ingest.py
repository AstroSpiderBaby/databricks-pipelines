"""
bronze_inventory_ingest.py

Ingests raw inventory CSV data from Blob Storage into Bronze Delta Lake.
"""
import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from utils_py.utils_write_delta import write_to_delta

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

inventory_path = "dbfs:/FileStore/pipeline1_batch_delta/moc_source_a/Inventory.csv"
output_path = "/mnt/delta/bronze/inventory"

df_inventory = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inventory_path)
)

write_df_to_delta(df_inventory, output_path)
