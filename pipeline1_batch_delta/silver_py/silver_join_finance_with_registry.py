"""
silver_join_finance_with_registry.py

Joins Silver Finance data with Vendor Registry data
and outputs an enriched dataset to the Silver Delta layer.
"""

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define source paths
finance_path = "/mnt/delta/silver/finance_invoices_clean"
registry_path = "/mnt/delta/silver/adf_data/vendor_registry_silver"

# Read Silver Finance and Vendor Registry Data
df_finance = spark.read.format("delta").load(finance_path)
df_registry = spark.read.format("delta").load(registry_path)

# Perform join on vendor_id
df_joined = df_finance.join(df_registry, on="vendor_id", how="left")

# Define output path
output_path = "/mnt/delta/silver/finance_with_registry_joined"

# Write joined data to Delta
df_joined.write.format("delta").mode("overwrite").save(output_path)

print(f"Successfully wrote joined data to: {output_path}")
