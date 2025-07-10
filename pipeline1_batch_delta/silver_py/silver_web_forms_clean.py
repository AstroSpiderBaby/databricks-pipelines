"""
silver_web_forms_clean.py

Processes raw web form submission data from Bronze and cleans up key fields.

- Normalizes data types
- Extracts relevant form details
- Drops invalid or null submissions
- Output: /mnt/delta/silver/web_forms_clean
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Load Bronze-level web forms
df_raw = spark.read.format("delta").load("/mnt/delta/bronze/web_forms_raw")

# Clean fields
df_clean = (
    df_raw
    .filter(col("form_id").isNotNull())
    .withColumn("submission_date", to_date("submission_date", "yyyy-MM-dd"))
    .dropDuplicates(["form_id"])
)

# Write clean Silver version
df_clean.write.format("delta").mode("overwrite").save("/mnt/delta/silver/web_forms_clean")
