"""
bronze_vendor_compliance_ingest.py

Pulls Vendor Compliance data directly from SQL Server via JDBC
and writes it to the Bronze Delta Lake layer in Unity Catalog Volumes.
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from utils_py.utils_write_delta import write_to_delta
from utils_py.utils_sql_connector import read_sql_table
from pyspark.sql.functions import lit

# Read table from SQL Server into DataFrame
df_vendor_compliance = (
    read_sql_table("Vendor_Compliance")
    .withColumn("ingestion_type", lit("vendor_compliance"))
)

# Define target Unity Volume path
output_path = "/Volumes/thebetty/bronze/vendor_compliance"

# Write to Delta
write_to_delta(
    df=df_vendor_compliance,
    path=output_path,
    partition_by=None,
    mode="overwrite",
    merge_schema=True,
    register_table=True,
    dry_run=False,
    verbose=True
)
