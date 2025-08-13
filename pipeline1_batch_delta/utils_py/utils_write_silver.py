# utils_write_silver.py
"""
Silver-level Delta write utility using hash-based upsert (demo-friendly).
Adds pre-merge diff counting + logging so every run has observable output,
even when there are no changes.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import sha2, concat_ws, coalesce, col

# Optional: tiny helper to check table existence in UC
def _table_exists(spark, full_table_name: str) -> bool:
    try:
        return bool(spark._jsparkSession.catalog().tableExists(full_table_name))
    except Exception:
        # Fallback for older runtimes
        try:
            spark.table(full_table_name)
            return True
        except Exception:
            return False

def write_silver_upsert(
    df: DataFrame,
    path: str,
    full_table_name: str,
    primary_key: str,
    hash_col: str = "hashstring",
    required_columns: list[str] = None,
    partition_by: list[str] = None,   # (not used here, kept for signature compatibility)
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

    # 1) Ensure incoming df has a stable hash column
    cols_for_hash = required_columns or [c for c in df.columns if c != hash_col]
    hash_expr = sha2(concat_ws('||', *[coalesce(col(c).cast('string'), '') for c in cols_for_hash]), 256)
    df_hashed = df.withColumn(hash_col, hash_expr)

    # 2) Ensure UC table exists and has the hash column
    #    (If your workspace blocks LOCATION on /Volumes, change this to "USING DELTA" without LOCATION.)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA LOCATION '{path}'")
    spark.sql(f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS {hash_col} STRING")

    # 3) Demo-friendly observability: pre-count diffs (rows that would change)
    #    Cheap for your dataset; if this gets large later, you can gate it behind a flag.
    rows_changed = 0
    try:
        if _table_exists(spark, full_table_name):
            tgt = spark.read.format("delta").load(path).alias("t")
            src = df_hashed.alias("s")
            if isinstance(primary_key, str):
                on = f"t.{primary_key} = s.{primary_key}"
            else:
                on = " AND ".join([f"t.{c} = s.{c}" for c in primary_key])
            # NULL-safe change detection
            cond = f"NOT (t.{hash_col} <=> s.{hash_col}) OR t.{hash_col} IS NULL OR s.{hash_col} IS NULL"
            rows_changed = tgt.join(src, on=on, how="inner").where(cond).count()
    except Exception as _:
        # Counting is best-effort; don't block the write.
        rows_changed = 0

    # 4) Expose the number to downstream tasks (for the run log / tags step)
    try:
        dbutils.jobs.taskValues.set(key="rows_changed", value=str(rows_changed))
    except Exception:
        pass

    if verbose:
        msg = f"🔍 Pre-merge diff estimate (rows_changed): {rows_changed}"
        print(msg)

    # 5) Upsert (merge) as usual
    try:
        from utils_py.utils_upsert_with_hashstring import upsert_with_hashstring
        # Optionally add commit metadata so it shows in DESCRIBE HISTORY if a commit happens
        try:
            user = spark.sql("SELECT current_user() AS u").first().u
            spark.conf.set(
                "spark.databricks.delta.commitInfo.userMetadata",
                f"pipeline=batch1_py_pipeline;table={full_table_name};actor={user}"
            )
        except Exception:
            pass

        upsert_with_hashstring(
            df_new=df_hashed,
            path=path,
            primary_key=primary_key,
            hash_col=hash_col
        )
    except Exception as e:
        raise RuntimeError("🔥 Upsert failed inside write_silver_upsert.") from e

    # 6) (Optional) re-register by LOCATION if needed (no-op if already exists)
    if register_table and path.startswith("/Volumes/"):
        try:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA LOCATION '{path}'")
            if verbose:
                print(f"📚 Registered Unity Catalog table: {full_table_name}")
        except Exception as e:
            print(f"⚠️ Failed to register table {full_table_name}: {e}")

    if verbose:
        print("✅ Silver table write complete.")
