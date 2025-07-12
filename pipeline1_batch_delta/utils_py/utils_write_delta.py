"""
Utility Script: utils_write_delta.py

Handles writing Spark DataFrames to Delta Lake with configurable write modes and partitioning.
"""

from pyspark.sql import DataFrame

def write_delta_table(df: DataFrame, output_path: str, mode: str = "overwrite", partition_cols: list = None):
    """
    Writes a Spark DataFrame to Delta format with optional partitioning.

    Args:
        df (DataFrame): The DataFrame to write.
        output_path (str): Destination DBFS path (e.g., /mnt/delta/gold/my_table).
        mode (str): Write mode ("overwrite", "append", "mergeSchema", etc.).
        partition_cols (list[str], optional): Columns to partition by.

    Returns:
        None
    """
    writer = df.write.format("delta").mode(mode)

    if partition_cols:
        writer = writer.partitionBy(partition_cols)

    writer.option("overwriteSchema", "true").save(output_path)
