"""
silver_web_forms_clean.py

Cleans Bronze-level web form submissions for reporting and enrichment.

- Selects relevant fields
- Converts timestamp
- Drops incomplete rows
- Writes to Silver Delta Layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load Bronze table
df_raw = spark.read.format("delta").load("/mnt/delta/bronze/web_form_submissions")

# Clean and transform
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

# Write to Silver
df_clean.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save("/mnt/delta/silver/web_forms_clean")

# Optional SQL registration
spark.sql("DROP TABLE IF EXISTS web_forms_clean")
spark.sql("""
    CREATE TABLE web_forms_clean
    USING DELTA
    LOCATION '/mnt/delta/silver/web_forms_clean'
""")

print("✅ Web forms cleaned and written to Silver successfully.")
