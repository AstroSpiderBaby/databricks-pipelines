"""
vendor_registry_silver.py

Cleans ADF-sourced vendor registry data and writes it to Silver Delta Lake.

Input:
- /mnt/delta/bronze/registry_adf_source (Bronze)

Output:
- /mnt/delta/silver/registry_vendor_silver (Silver)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from utils.utils_write_delta import write_df_to_delta

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Load Bronze table
bronze_df = spark.read.format("delta").load("/mnt/delta/bronze/registry_adf_source")

# Clean and transform
silver_df = (
    bronze_df
    .withColumnRenamed("VendorID", "vendor_id")
    .withColumnRenamed("VendorName", "vendor_name")
    .withColumnRenamed("IsActive", "is_active")
    .withColumn("vendor_name", col("vendor_name").cast("string"))
    .dropna(subset=["vendor_id", "vendor_name"])
)

# Write to Silver
write_df_to_delta(
    df=silver_df,
    path="/mnt/delta/silver/registry_vendor_silver",
    partitionBy=None,
    mode="overwrite"
)
