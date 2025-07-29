"""
silver_inventory_shipments_join.py

Enriches inventory and shipments data by adding missing fields,
joining with vendor information, and writing cleaned Silver output
into Unity Catalog with hash-based partitioning.
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import DateType
from datetime import datetime, timedelta
import random

from utils_py.utils_write_silver import write_silver_upsert

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# === Step 1: Load Bronze Tables from Unity Volumes ===
df_inventory = spark.read.format("delta").load("/Volumes/thebetty/bronze/inventory")
df_shipments = spark.read.format("delta").load("/Volumes/thebetty/bronze/shipments")
df_vendors = spark.read.format("delta").load("/Volumes/thebetty/bronze/vendors")

# === Step 2: Add shipment_date if missing ===
@udf(DateType())
def random_date():
    base = datetime(2025, 6, 1)
    return base + timedelta(days=random.randint(0, 14))

if "shipment_date" not in df_shipments.columns:
    df_shipments = df_shipments.withColumn("shipment_date", random_date())

# === Step 3: Assign vendor_id based on item_id (simulated mapping) ===
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

# === Step 4: Join inventory + shipments + vendors ===
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

# === Step 5: Write to Unity Catalog Silver Volume ===
output_path = "/Volumes/thebetty/silver/inventory_shipments_joined_clean"
full_table_name = "thebetty.silver.inventory_shipments_joined_clean"

write_silver_upsert(
    df=df_joined,
    path=output_path,
    full_table_name=full_table_name,
    primary_key="shipment_id",
    required_columns=[
        "vendor_id", "item_id", "item_name", "quantity_on_hand", "reorder_level", "last_updated",
        "shipment_id", "ship_date", "shipment_date", "destination", "status",
        "vendor_name", "location", "rating"
    ],
    partition_by=["shipment_date"],
    register_table=True,
    verbose=True
)

print(f"✅ Successfully wrote and registered inventory-shipments-vendors data to: {full_table_name}")
