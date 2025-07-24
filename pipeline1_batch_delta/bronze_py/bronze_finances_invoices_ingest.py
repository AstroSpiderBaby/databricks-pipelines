from pyspark.sql.functions import input_file_name, lit
import sys

# Add pipeline path
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from utils_py.utils_write_delta import write_to_delta

# Define paths
input_path = "/mnt/raw-ingest/finance_invoice_data.csv"

# Unity Catalog target path and table
output_path = "/mnt/adf-silver/thebetty/bronze/bronze_finance_invoices"
full_table_name = "thebetty.bronze.bronze_finance_invoices"


# Load CSV and add metadata columns
df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_type", lit("finance_invoices"))
)

# Write to Unity Catalog Bronze table
write_to_delta(
    df=df,
    path=output_path,
    full_table_name=full_table_name,
    partition_by=None,
    mode="overwrite",
    merge_schema=True,
    register_table=True,
    dry_run=False,
    verbose=True
)
