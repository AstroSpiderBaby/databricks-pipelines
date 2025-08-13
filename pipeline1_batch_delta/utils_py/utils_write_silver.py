"""
utils_write_silver.py

Silver-level Delta write utility using hash-based upsert.
"""

import os
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from utils_py.utils_upsert_with_hashstring import upsert_with_hashstring  
from pyspark.sql.functions import sha2, concat_ws, coalesce, col, lit

def write_silver_upsert(
    df: DataFrame,
    path: str,
    full_table_name: str,
    primary_key: str,
    hash_col: str = "hashstring",
    required_columns: list[str] = None,
    partition_by: list[str] = None,   # kept for signature compatibility
    register_table: bool = True,
    verbose: bool = False
):
    spark = df.sparkSession

    # 0) Validate required columns
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

    # 1) Build a stable hash on the incoming df (exclude hash/timestamp fields)
    EXCLUDE_FROM_HASH = {hash_col, "ingestion_timestamp"}
    cols_for_hash = (required_columns or df.columns)
    cols_for_hash = [c for c in cols_for_hash if c not in EXCLUDE_FROM_HASH]

    hash_expr = sha2(
        concat_ws('||', *[coalesce(col(c).cast('string'), lit('')) for c in cols_for_hash]),
        256
    )
    df_hashed = df.withColumn(hash_col, hash_expr)

    # 2) Ensure the path-based Delta table can support the MERGE condition
    #    (safe no-op if table/column already exist)
    try:
        spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMN IF NOT EXISTS {hash_col} STRING")
    except Exception:
        pass

    # 3) Upsert (now that source has the hash)
    try:
        upsert_with_hashstring(
            df_new=df_hashed,
            path=path,
            primary_key=primary_key,
            hash_col=hash_col
        )
    except Exception as e:
        raise RuntimeError("🔥 Upsert failed inside write_silver_upsert.") from e

    # 4) Optional: register UC table (best-effort) pointing at the path
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
