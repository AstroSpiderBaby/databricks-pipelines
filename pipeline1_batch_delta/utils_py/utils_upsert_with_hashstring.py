"""
Utility Script: utils_upsert_with_hashstring.py

Provides a reusable function to perform Delta upserts using a hash-based deduplication strategy.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws

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


def upsert_with_hashstring(df_new, path, primary_key, hash_col="hashstring"):
    """
    Performs a hash-based upsert (merge) into a Delta table.

    Args:
        df_new (DataFrame): Incoming DataFrame with new data (must include hashstring column).
        path (str): Target Delta table path.
        primary_key (str): Primary key column (string or list).
        hash_col (str): Hash column used for change detection.

    Returns:
        None
    """
    from delta.tables import DeltaTable
    spark = df_new.sparkSession  # Use existing session

    if isinstance(primary_key, str):
        primary_key = [primary_key]

    if DeltaTable.isDeltaTable(spark, path):
        delta_table = DeltaTable.forPath(spark, path)

        condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_key])

        delta_table.alias("target").merge(
            df_new.alias("source"),
            condition
        ).whenMatchedUpdate(
            condition=f"target.{hash_col} != source.{hash_col}",
            set={col: f"source.{col}" for col in df_new.columns}
        ).whenNotMatchedInsertAll().execute()
    else:
        df_new.write.format("delta").mode("overwrite").save(path)
