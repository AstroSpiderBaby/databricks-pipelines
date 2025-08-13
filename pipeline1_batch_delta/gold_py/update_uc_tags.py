
# governance/update_uc_tags.py
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql import Row
import uuid

spark = spark  # noqa

# --- 1) Stamp tags on the Gold table ---
user = spark.sql("SELECT current_user() AS u").first().u
now  = spark.sql("SELECT current_timestamp() AS t").first().t

spark.sql(f"""
ALTER TABLE thebetty.gold.final_vendor_summary
  SET TAGS (
    'last_run_user' = '{user}',
    'last_pipeline_run' = '{now}'
  )
""")

# --- 2) Collect rows_changed from upstream tasks (best effort) ---
# Adjust names if your job task keys differ; these are from your workflow.
task_keys = [
    "silver_web_forms_clean_py",
    "silver_clean_finance_invoices_py",
    "silver_finalize_vendor_summary_py",
    "silver_clean_vendor_compliance_py",
    "silver_join_finance_registry_py",
    "silver_join_inventory_shipments_py",
]
total_changed = 0
for tk in task_keys:
    try:
        v = dbutils.jobs.taskValues.getOrElse(tk, "rows_changed", "0")
        total_changed += int(v)
    except Exception:
        pass  # task may not have set the value; ignore

# --- 3) Ensure the run-log table exists & append a receipt row ---
spark.sql("""
CREATE TABLE IF NOT EXISTS thebetty.gold.final_vendor_summary_runs
(
  run_id        STRING,
  run_time      TIMESTAMP,
  ran_by        STRING,
  job_id        STRING,
  task_name     STRING,
  rows_changed  BIGINT,
  status        STRING,
  note          STRING
) USING DELTA
""")

# Try to capture a job id (best effort)
try:
    job_id = str(dbutils.jobs.taskContext().jobId())
except Exception:
    job_id = None

row = [
    (str(uuid.uuid4()), now, user, job_id, "batch1_py_pipeline",
     int(total_changed), "succeeded",
     "Pipeline executed; tags stamped; demo-friendly receipt")
]

spark.createDataFrame(row, schema="""
    run_id STRING, run_time TIMESTAMP, ran_by STRING, job_id STRING, task_name STRING,
    rows_changed BIGINT, status STRING, note STRING
""").write.mode("append").saveAsTable("thebetty.gold.final_vendor_summary_runs")

print(f"🧾 Run receipt written. rows_changed_total={total_changed}")
print("🏷️ Tags stamped on thebetty.gold.final_vendor_summary.")
