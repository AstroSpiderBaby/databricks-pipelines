"""
silver_inventory_shipments_join.py

Enriches inventory and shipments data by adding missing fields,
joining with vendor information, and writing cleaned Silver output.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import DateType
from datetime import datetime, timedelta
import random

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Step 1: Load Bronze Data
df_inventory = spark.read.format("delta").load("/mnt/delta/bronze/inventory")
df_shipments = spark.read.format("delta").load("/mnt/delta/bronze/shipments")
df_vendors = spark.read.format("delta").load("/mnt/delta/bronze/vendors")

# Step 2: Add shipment_date if missing
@udf(DateType())
def random_date():
    base = datetime(2025, 6, 1)
    return base + timedelta(days=random.randint(0, 14))

if "shipment_date" not in df_shipments.columns:
    df_shipments = df_shipments.withColumn("shipment_date", random_date())

# Step 3: Write enriched intermediates (optional, skip if pipeline handles all in-memory)
df_inventory = df_inventory.withColumn(
    "vendor_id",
    when(col("item_id") == "ITM001", "V001")
    .when(col("item_id") == "ITM002", "V002")
    .when(col("item_id") == "ITM003", "V003")
    .when(col("item_id") == "ITM004", "V004")
    .when(col("item_id") == "ITM005", "V005")
    .when(col("item_id") == "ITM006", "V006")
    .when(col("item_id") == "ITM007", "V007")
    .when(col("item_id") == "ITM008", "V008")
    .when(col("item_id") == "ITM009", "V009")
    .otherwise("V010")
)

# Step 4: Join inventory + shipments + vendors
df_joined = (
    df_inventory.alias("inv")
    .join(df_shipments.alias("ship"), on="vendor_id", how="inner")
    .join(df_vendors.alias("vend"), on="vendor_id", how="left")
    .select(
        "inv.vendor_id",
        "inv.item_id", "inv.item_name", "inv.quantity_on_hand", "inv.reorder_level", "inv.last_updated",
        "ship.shipment_id", "ship.ship_date", "ship.shipment_date", "ship.destination", "ship.status",
        col("vend.name").alias("vendor_name"),
        "vend.location", "vend.rating"
    )
)

# Step 5: Write output
output_path = "dbfs:/mnt/delta/silver/inventory_shipments_joined_clean"
df_joined.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .partitionBy("shipment_date") \
    .save(output_path)

print(f"✅ Successfully wrote joined inventory-shipments-vendors data to: {output_path}")
