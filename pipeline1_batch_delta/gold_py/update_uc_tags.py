from pyspark.sql import SparkSession
from pathlib import Path

spark = SparkSession.builder.getOrCreate()

def _find_sql() -> Path:
    # 1) Next to this script (works when __file__ is defined)
    if "__file__" in globals():
        p = Path(__file__).resolve().parent / "lineage_tags.sql"
        if p.exists():
            return p

    # 2) Repo-root candidates (Databricks Git tasks start in repo root)
    cwd = Path.cwd()
    for rel in [
        "pipeline1_batch_delta/lineage_tags/lineage_tags.sql",
        "lineage_tags/lineage_tags.sql",
        "lineage_tags.sql",
    ]:
        p = cwd / rel
        if p.exists():
            return p

    raise FileNotFoundError("Could not locate lineage_tags.sql from script or repo root")

sql_path = _find_sql()
sql_text = sql_path.read_text(encoding="utf-8")

# Execute each statement separated by semicolons
for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
    spark.sql(stmt)

print(f"✅ Applied lineage/data-governance SQL from {sql_path}")
