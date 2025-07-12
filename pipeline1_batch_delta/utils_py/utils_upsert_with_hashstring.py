"""
Utility Script: utils_upsert_with_hashstring.py

Provides a reusable function to perform Delta upserts using a hash-based deduplication strategy.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws

spark = SparkSession.builder.getOrCreate()

def add_hash_column(df, hash_columns, hash_col_name="hashstring"):
    """
    Adds a hash column to the DataFrame for deduplication or upsert matching.

    Args:
        df (DataFrame): Input Spark DataFrame.
        hash_columns (list[str]): Columns to use for hash generation.
        hash_col_name (str): Name of the generated hash column.

    Returns:
        DataFrame: Spark DataFrame with hash column appended.
    """
    return df.withColumn(hash_col_name, sha2(concat_ws("||", *hash_columns), 256))


def upsert_with_hashstring(spark, df_new, target_path, primary_keys, hash_col="hashstring"):
    """
    Upserts a DataFrame to a Delta table using hash-based change detection.

    Args:
        spark (SparkSession): Spark session.
        df_new (DataFrame): Incoming DataFrame with new data (including hashstring).
        target_path (str): DBFS path to the Delta table.
        primary_keys (list[str]): List of primary key column names.
        hash_col (str): Column name used for hash comparison (default: "hashstring").

    Returns:
        None
    """
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)

        update_condition = " AND ".join([
            f"target.{col} = source.{col}" for col in primary_keys
        ])

        delta_table.alias("target").merge(
            df_new.alias("source"),
            update_condition
        ).whenMatchedUpdate(
            condition=f"target.{hash_col} != source.{hash_col}",
            set={col: f"source.{col}" for col in df_new.columns}
        ).whenNotMatchedInsertAll().execute()
    else:
        df_new.write.format("delta").mode("overwrite").save(target_path)
