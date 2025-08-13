# Runs the static SQL tags, then stamps dynamic tags (user + time)
from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Resolve path to the .sql file relative to this script
this_dir = Path(__file__).parent
sql_file = this_dir / "lineage_tags.sql"

sql_text = sql_file.read_text(encoding="utf-8")

# Execute each SQL statement (split by semicolon)
for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
    spark.sql(stmt)

# Dynamic tags: who ran it and when
row = spark.sql("SELECT current_user() AS u, current_timestamp() AS t").first()
run_user = row.u
run_time = row.t  # timestamp object; Spark will cast to string

spark.sql(f"""
  ALTER TABLE thebetty.gold.final_vendor_summary
  SET TAGS (
    'last_run_user' = '{run_user}',
    'last_pipeline_run' = '{run_time}'
  )
""")

print("✅ Unity Catalog tags applied and dynamic run info stamped.")
