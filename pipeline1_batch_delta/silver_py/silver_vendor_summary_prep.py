"""
silver_vendor_summary_prep.py

Cleans and enriches vendor registry data for downstream joins.
- Drops rows with null `vendor_id`
- Converts `vendor_id` to string
- Removes duplicates
- Writes to: /mnt/delta/silver/final_vendor_summary_prep
"""

from pyspark.sql import SparkSession

# Spark session
spark = SparkSession.builder.getOrCreate()

# Load Vendor Registry Silver
df_registry = spark.read.format("delta").load("/mnt/delta/silver/vendor_registry")

# Clean and prep
df_registry_clean = (
    df_registry
    .filter("vendor_id IS NOT NULL")
    .withColumn("vendor_id", df_registry["vendor_id"].cast("string"))
    .dropDuplicates(["vendor_id"])
)

# Write cleaned prep output
df_registry_clean.write.format("delta").mode("overwrite").save("/mnt/delta/silver/final_vendor_summary_prep")
