"""
gold_vendor_summary.py

Generates the final Gold-level vendor summary by aggregating invoices,
joining compliance and registry info, and writing the result to Unity Delta Volume.

Output:
- Unity Volume: /Volumes/thebetty/gold/final_vendor_summary
- Table: thebetty.gold.final_vendor_summary
- Log Table: thebetty.gold.final_vendor_summary_runs
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, countDistinct, current_timestamp
from utils_py.utils_write_delta import write_to_delta

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# === Step 1: Load Silver inputs from Unity Volumes ===
df_finance = spark.read.format("delta").load("/Volumes/thebetty/silver/final_vendor_summary_prep")
df_registry = spark.read.format("delta").load("/Volumes/thebetty/silver/vendor_registry_clean").alias("registry")
df_compliance = spark.read.format("delta").load("/Volumes/thebetty/bronze/vendor_compliance").alias("compliance")

# === Step 2: Join and Aggregate ===
df_gold = (
    df_finance
    .join(df_registry, on="vendor_id", how="left")
    .join(df_compliance, on="vendor_id", how="left")
    .groupBy("vendor_id", "vendor_name")
    .agg(
        countDistinct("invoice_id").alias("total_invoices"),
        max("due_date").alias("latest_due_date"),
        max("invoice_date").alias("latest_invoice_date"),
        max(col("compliance.last_audit_date")).alias("last_audit_date"),
        max(col("compliance.compliance_score")).alias("compliance_score"),
        max(col("compliance.status")).alias("compliance_status"),
        max(col("registry.industry")).alias("industry"),
        max(col("registry.headquarters")).alias("headquarters"),
        max(col("registry.onwatchlist")).alias("onwatchlist"),
        max(col("registry.registrationdate")).alias("registration_date"),
        max(col("registry.tier")).alias("tier")
    )
    .withColumn("pipeline_run_timestamp", current_timestamp())
)

# === Step 3: Write Gold table ===
target_path = "/Volumes/thebetty/gold/final_vendor_summary"
write_to_delta(
    df=df_gold,
    path=target_path,
    mode="overwrite",
    register_table=True,
    merge_schema=True,
    partition_by=["tier"],
    full_table_name="thebetty.gold.final_vendor_summary",
    verbose=True
)

# === Step 4: Log Pipeline Run ===
df_log = df_gold.select(
    current_timestamp().alias("run_time"),
    countDistinct("vendor_id").alias("vendor_count"),
    countDistinct("vendor_name").alias("vendor_name_count")
)

log_path = "/Volumes/thebetty/gold/logs/final_vendor_summary_runs"
write_to_delta(
    df=df_log,
    path=log_path,
    mode="append",
    register_table=True,
    merge_schema=True,
    full_table_name="thebetty.gold.final_vendor_summary_runs",
    verbose=True
)

print("✅ Final vendor summary written to Unity Gold layer.")
