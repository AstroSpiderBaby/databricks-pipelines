"""
silver_web_forms_clean.py

Cleans Bronze-level web form submissions for reporting and enrichment.

- Selects relevant fields
- Converts timestamp
- Drops incomplete rows
- Writes to Silver Delta Layer
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
from utils_py.utils_write_silver import write_silver_upsert

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# === Step 1: Load Bronze-level web form submissions ===
df_raw = spark.read.format("delta").load("/Volumes/thebetty/bronze/web_forms")

# === Step 2: Clean and transform ===
df_clean = (
    df_raw.select(
        "submission_id",
        "full_name",
        "email",
        "phone",
        "address",
        "comments",
        to_timestamp("submitted_at").alias("submitted_at"),
        "source_file"
    )
    .dropna(subset=["submission_id", "submitted_at"])
)

# === Step 3: Write to Unity Catalog Silver Volume/Table ===
output_path = "/Volumes/thebetty/silver/web_forms_clean"
full_table_name = "thebetty.silver.web_forms_clean"

write_silver_upsert(
    df=df_clean,
    path=output_path,
    full_table_name=full_table_name,
    primary_key="submission_id",
    required_columns=df_clean.columns,
    partition_by=["submitted_at"],
    register_table=True,
    verbose=True
)

print("✅ Web forms cleaned and written to Silver with Unity Catalog support.")
