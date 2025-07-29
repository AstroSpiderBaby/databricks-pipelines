"""
bronze_vendor_compliance_ingest.py

Pulls Vendor Compliance data directly from SQL Server via JDBC
and writes it to the Bronze Delta Lake layer in Unity Catalog Volumes.
"""

# === Setup Path for Local Dev ===
import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

# === Spark + Utilities ===
import sys
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from utils_py.utils_write_delta import write_to_delta
from utils_py.utils_sql_connector import read_sql_table

# === Initialize Spark first (required for dbutils fallback) ===
spark = SparkSession.builder.getOrCreate()

# === Safe dbutils import for .py scripts ===
try:
    dbutils
except NameError:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

# === STEP 1: Read SQL Server table via JDBC (ngrok tunnel) ===
print("📥 Reading from 'Vendor_Compliance' table...")
df_sql = (
    read_sql_table("Vendor_Compliance")
    .withColumn("ingestion_type", lit("vendor_compliance"))
)

# === STEP 2: Write to intermediate Delta (bypasses JDBC re-read)
delta_temp_path = "/tmp/vendor_compliance_staging"
delta_dbfs_path = f"/dbfs{delta_temp_path}"

print(f"🧹 Cleaning up any old Delta staging at {delta_dbfs_path} ...")
if os.path.exists(delta_dbfs_path):
    shutil.rmtree(delta_dbfs_path)

print(f"📤 Writing intermediate Delta to {delta_temp_path} ...")
df_sql.write.format("delta").mode("overwrite").save(delta_temp_path)

# === STEP 3: Read back from Delta to decouple SQL auth ===
print("📥 Reading Delta from intermediate checkpoint...")
df_json = spark.read.format("delta").load(delta_temp_path)

# === STEP 4: Write final Delta output ===
output_path = "/Volumes/thebetty/bronze/vendor_compliance"
full_table_name = "thebetty.bronze.vendor_compliance"

write_to_delta(
    df=df_json,
    path=output_path,
    full_table_name=full_table_name,
    mode="overwrite",
    merge_schema=True,
    register_table=True,
    partition_by=None,
    dry_run=False,
    verbose=True,
    required_columns=[
        "vendor_id",
        "compliance_score",
        "status",
        "last_audit_date"
    ]
)
