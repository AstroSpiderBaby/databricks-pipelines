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


# utils_upsert_with_hashstring.py

def upsert_with_hashstring(df_new, path, primary_key, hash_col="hashstring"):
    """
    Delta upsert with hash-based change detection when possible.
    Falls back to unconditional UPDATE if target doesn't have the hash column.
    Assumes df_new already contains the hash column.
    """
    from delta.tables import DeltaTable
    from pyspark.sql.utils import AnalysisException

    spark = df_new.sparkSession

    # Normalize PK to list
    if isinstance(primary_key, str):
        primary_key = [primary_key]

    if DeltaTable.isDeltaTable(spark, path):
        dt = DeltaTable.forPath(spark, path)

        # Build PK join
        pk_cond = " AND ".join([f"target.{c} = source.{c}" for c in primary_key])

        # Columns to write (intersection)
        tcols = set(dt.toDF().columns)
        scols = set(df_new.columns)
        common = list(tcols & scols)
        set_expr = {c: f"source.{c}" for c in common}

        # Try to ensure target has the hash column (best effort)
        has_hash_t = (hash_col in tcols)
        has_hash_s = (hash_col in scols)
        if has_hash_s and not has_hash_t:
            try:
                spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMN IF NOT EXISTS {hash_col} STRING")
                # refresh tcols/common/set_expr after the ALTER
                tcols = set(dt.toDF().columns)  # might still reflect the old schema; OK if so
                has_hash_t = (hash_col in tcols)
                if has_hash_t and hash_col not in common:
                    common.append(hash_col)
                    set_expr[hash_col] = f"source.{hash_col}"
            except AnalysisException:
                # If ALTER fails (e.g., path-level ALTER not supported), just fall back below.
                has_hash_t = False
            except Exception:
                has_hash_t = False

        # Choose strategy
        builder = dt.alias("target").merge(df_new.alias("source"), pk_cond)

        if has_hash_t and has_hash_s:
            # Hash-based update with NULL-safe inequality
            builder = builder.whenMatchedUpdate(
                condition=f"NOT (target.{hash_col} <=> source.{hash_col})",
                set=set_expr
            )
        else:
            # Fallback: unconditional update to avoid unresolved column errors
            builder = builder.whenMatchedUpdate(set=set_expr)

        builder.whenNotMatchedInsert(values=set_expr).execute()

    else:
        # First write creates the table (schema merged)
        (df_new.write
             .format("delta")
             .option("mergeSchema", "true")
             .mode("overwrite")
             .save(path))