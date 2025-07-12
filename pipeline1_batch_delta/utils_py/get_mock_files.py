"""
get_mock_files.py

Utility script to generate and upload mock files to the Databricks FileStore
or another mount point. Use this for testing pipeline ingestion.
"""

import pandas as pd
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

def create_mock_inventory_data(path: str):
    data = {
        "item_id": [101, 102, 103],
        "item_name": ["Widget A", "Widget B", "Widget C"],
        "quantity": [10, 20, 15],
        "last_updated": [None, None, None]
    }

    df = pd.DataFrame(data)
    df["last_updated"] = pd.Timestamp.now()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Mock inventory CSV created at: {path}")

def write_mock_data_to_delta(csv_path: str, delta_path: str):
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df.write.format("delta").mode("overwrite").save(delta_path)
    print(f"Delta table written at: {delta_path}")

if __name__ == "__main__":
    # Example paths — adjust as needed
    csv_output_path = "/dbfs/FileStore/mock_data/inventory_mock.csv"
    delta_output_path = "/mnt/delta/bronze/inventory_mock"

    create_mock_inventory_data(csv_output_path)
    write_mock_data_to_delta(csv_output_path, delta_output_path)
