"""
silver_vendor_summary_prep.py

Combines Silver-level finance and vendor compliance data
to create a summary prep table for downstream reporting.

- Joins finance and vendor compliance on vendor_id
- Output path: /mnt/delta/silver/final_vendor_summary_prep
"""

from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load Silver tables
df_finance = spark.read.format("delta").load("/mnt/delta/silver/finance_with_vendor_info")
df_compliance = spark.read.format("delta").load("/mnt/delta/silver/vendor_compliance_clean")

# Join on vendor_id (left join to retain all finance records)
df_final = df_finance.join(df_compliance, on="vendor_id", how="left")

# Define output path
output_path = "/mnt/delta/silver/final_vendor_summary_prep"

# Write to Silver
df_final.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(output_path)

print("✅ Vendor summary prep table written successfully.")
