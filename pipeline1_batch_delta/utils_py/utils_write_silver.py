"""
utils_write_silver.py

Silver-level Delta write utility using hash-based upsert.
"""

import os
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from utils_py.utils_upsert_with_hashstring import upsert_with_hashstring  # ✅ FIXED import


def write_silver_upsert(
    df: DataFrame,
    path: str,
    full_table_name: str,
    primary_key: str,
    hash_col: str = "hashstring",
    required_columns: list[str] = None,
    partition_by: list[str] = None,
    register_table: bool = True,
    verbose: bool = False
):
    spark = df.sparkSession

    # ✅ Column validation
    if required_columns:
        actual = set(df.columns)
        missing = set(required_columns) - actual
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    if verbose:
        print(f"\n🧪 Columns: {df.columns}")
        print(f"📝 Writing to: {path}")
        print(f"🔑 Primary key: {primary_key}")
        print(f"📦 Partitioning: {partition_by or 'None'}")

    # ✅ Step 1: Perform upsert
    try:
        upsert_with_hashstring(
            df_new=df,
            path=path,
            primary_key=primary_key,
            hash_col=hash_col
        )
    except Exception as e:
        raise RuntimeError("🔥 Upsert failed inside write_silver_upsert.") from e

    # ✅ Step 2: Register Unity Catalog table dynamically
    if register_table and path.startswith("/Volumes/"):
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {full_table_name}
                AS SELECT * FROM delta.`{path}`
            """)
            if verbose:
                print(f"📚 Registered Unity Catalog table: {full_table_name}")
        except Exception as e:
            print(f"⚠️ Failed to register table {full_table_name}: {e}")


    if verbose:
        print("✅ Silver table write complete.")
