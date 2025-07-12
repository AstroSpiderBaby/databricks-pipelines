"""
silver_inventory_shipments_join.py

Joins Silver Inventory data with Shipments data
and writes the enriched dataset to the Silver Delta layer.
"""

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define source paths
inventory_path = "/mnt/delta/silver/inventory_clean"
shipments_path = "/mnt/delta/silver/shipments_clean"

# Read Silver Inventory and Shipments Data
df_inventory = spark.read.format("delta").load(inventory_path)
df_shipments = spark.read.format("delta").load(shipments_path)

# Join on shipment_id
df_joined = df_inventory.join(df_shipments, on="shipment_id", how="inner")

# Define output path
output_path = "/mnt/delta/silver/inventory_shipments_joined"

# Write joined data to Delta
df_joined.write.format("delta").mode("overwrite").save(output_path)

print(f"Successfully wrote joined data to: {output_path}")
