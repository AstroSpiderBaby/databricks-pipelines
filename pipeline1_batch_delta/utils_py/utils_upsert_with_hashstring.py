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
    Assumes df_new already contains the hash column.
    """
    from delta.tables import DeltaTable
    spark = df_new.sparkSession

    # Normalize PK to list
    if isinstance(primary_key, str):
        primary_key = [primary_key]

    if DeltaTable.isDeltaTable(spark, path):
        delta_table = DeltaTable.forPath(spark, path)

        # Join on PK(s)
        pk_cond = " AND ".join([f"target.{c} = source.{c}" for c in primary_key])

        # Intersect columns; make sure we also write the hash column
        target_cols = set(delta_table.toDF().columns)
        source_cols = set(df_new.columns)
        common_cols = list(target_cols & source_cols)
        if hash_col in source_cols and hash_col not in common_cols:
            common_cols.append(hash_col)

        set_expr = {c: f"source.{c}" for c in common_cols}

        # NULL-safe inequality treats NULL vs value as a change
        change_cond = f"NOT (target.{hash_col} <=> source.{hash_col})"

        (delta_table.alias("target")
            .merge(df_new.alias("source"), pk_cond)
            .whenMatchedUpdate(condition=change_cond, set=set_expr)
            .whenNotMatchedInsert(values=set_expr)
            .execute())
    else:
        # First write creates the table (schema merged)
        (df_new.write
              .format("delta")
              .option("mergeSchema", "true")
              .mode("overwrite")
              .save(path))