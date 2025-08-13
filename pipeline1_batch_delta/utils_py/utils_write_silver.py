"""
utils_write_silver.py

Silver-level Delta write utility using hash-based upsert.
"""

import os
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from utils_py.utils_upsert_with_hashstring import upsert_with_hashstring  # ✅ FIXED import
from pyspark.sql.functions import sha2, concat_ws, coalesce, col  # ⬅ add this import

def write_silver_upsert(...):
    spark = df.sparkSession

    # ... your existing validation & logging ...

    # --- NEW: build a stable hash on the incoming df
    EXCLUDE_FROM_HASH = {hash_col, "ingestion_timestamp"}
    cols_for_hash = (required_columns or df.columns)
    cols_for_hash = [c for c in cols_for_hash if c not in EXCLUDE_FROM_HASH]

    hash_expr = sha2(
        concat_ws('||', *[coalesce(col(c).cast('string'), '') for c in cols_for_hash]),
        256
    )
    df_hashed = df.withColumn(hash_col, hash_expr)

    # --- NEW: (best-effort) ensure the path-table can support the MERGE condition
    # If the table doesn't exist yet, this ALTER will just fail harmlessly; first write will create it.
    try:
        spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMN IF NOT EXISTS {hash_col} STRING")
    except Exception:
        pass

    # ✅ Step 1: Perform upsert (now pass df_hashed!)
    try:
        upsert_with_hashstring(
            df_new=df_hashed,  # ⬅ changed from df to df_hashed
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
