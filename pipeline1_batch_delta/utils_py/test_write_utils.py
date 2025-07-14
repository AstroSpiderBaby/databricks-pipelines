"""
Utility Script: test_write_utils.py

Tests the write_utils module to ensure Delta write functionality works correctly,
including overwrite modes, partitioning, and schema merging.
"""

from pyspark.sql import SparkSession
from pipeline1_batch_delta.utils_py.write_utils import write_delta

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Create mock DataFrame
data = [("A", 1), ("B", 2), ("C", 3)]
df = spark.createDataFrame(data, ["category", "value"])

# Define Delta output path (DEV path)
output_path = "/mnt/delta/tmp/test_write_output"

# Run write
write_delta(
    df=df,
    output_path=output_path,
    partition_cols=["category"],
    mode="overwrite",
    merge_schema=True
)

print(f"✅ Data written successfully to: {output_path}")
