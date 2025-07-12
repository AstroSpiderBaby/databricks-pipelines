"""
silver_finance_vendor_join.py

Joins cleaned finance invoice data with vendor data to prepare for summary rollup.
"""

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load Silver cleaned finance invoice data
df_finance = spark.read.format("delta").load("/mnt/delta/silver/finance_invoices_clean")

# Load Silver cleaned vendor data
df_vendor = spark.read.format("delta").load("/mnt/delta/silver/vendor_compliance_clean")

# Join the datasets on vendor_id
df_joined = df_finance.join(df_vendor, on="vendor_id", how="left")

# Write to next Silver stage
df_joined.write.format("delta").mode("overwrite").save("/mnt/delta/silver/finance_vendor_join")
