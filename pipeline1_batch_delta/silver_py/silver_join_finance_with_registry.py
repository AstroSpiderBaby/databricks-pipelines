"""
silver_join_finance_with_registry.py

Joins Silver-level enriched finance + vendor data with Vendor Registry data.
Outputs the fully enriched dataset and registers it as a Delta table.
"""

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define source paths
finance_path = "/mnt/delta/silver/finance_with_vendor_info"
registry_path = "/mnt/delta/silver/vendor_registry_clean"
output_path = "/mnt/delta/silver/finance_with_vendors_enriched"

# Load data
df_finance = spark.read.format("delta").load(finance_path)
df_registry = spark.read.format("delta").load(registry_path)

# Perform join
df_enriched = df_finance.join(df_registry, on="vendor_id", how="left")

# Write to Silver (final enrichment before Gold)
df_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(output_path)

# Register as Delta table (optional for query/BI access)
spark.sql("DROP TABLE IF EXISTS finance_with_vendors_enriched")
spark.sql(f"""
    CREATE TABLE finance_with_vendors_enriched
    USING DELTA
    LOCATION '{output_path}'
""")

print(f"✅ Successfully wrote and registered: {output_path}")
