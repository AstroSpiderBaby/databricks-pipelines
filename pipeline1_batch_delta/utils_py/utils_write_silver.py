# utils_write_silver.py
"""
Silver-level Delta write utility using hash-based upsert (demo-friendly).
- Pre-merge diff counting + logging (so every run is observable)
- Best-effort UC registration against /Volumes (skips cleanly if unsupported)
- Ensures the underlying Delta table has the hash column even if UC table is not registered
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import sha2, concat_ws, coalesce, col


# ---- Helpers ---------------------------------------------------------------

def _table_exists(spark, full_table_name: str) -> bool:
    """Check if a UC table exists (without throwing on older runtimes)."""
    try:
        return bool(spark._jsparkSession.catalog().tableExists(full_table_name))
    except Exception:
        try:
            spark.table(full_table_name)
            return True
        except Exception:
            return False


def _ensure_hash_col_on_path(spark, path: str, hash_col: str) -> None:
    """
    Make sure the PHYSICAL Delta table (path-based) has the hash column.
    This does not depend on UC registration and works directly on the path.
    """
    try:
        spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMN IF NOT EXISTS {hash_col} STRING")
    except Exception as e:
        # If the table at path doesn't exist yet, this will fail harmlessly;
        # the initial write will create it with the column via the merge helper.
        print(f"ℹ️ Could not ALTER path-table to add {hash_col}: {str(e).splitlines()[0]}")


def _try_register_on_volume(spark, full_table_name: str, path: str, hash_col: str) -> bool:
    """
    Try to register a UC table that points at a Volume path.
    If the workspace doesn't support tables-on-Volumes, skip gracefully.
    Returns True if registration succeeded, False otherwise.
    """
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA LOCATION '{path}'")
        spark.sql(f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS {hash_col} STRING")
        return True
    except Exception as e:
        msg = str(e)
        known = ("Missing cloud file system scheme", "INVALID_PARAMETER_VALUE", "tables on volumes")
        if any(k in msg for k in known):
            print(f"ℹ️ Skipping UC registration with LOCATION for {full_table_name}: {msg.splitlines()[0]}")
            return False
        # Unknown error → surface it
        raise


# ---- Main writer -----------------------------------------------------------

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

    # 1) Stamp a stable hash on the incoming df
    cols_for_hash = required_columns or [c for c in df.columns if c != hash_col]
    hash_expr = sha2(concat_ws('||', *[coalesce(col(c).cast('string'), '') for c in cols_for_hash]), 256)
    df_hashed = df.withColumn(hash_col, hash_expr)

    # 2) Ensure the PHYSICAL Delta table can support the merge condition
    #    (add hash column at the path-level, independent of UC registration)
    _ensure_hash_col_on_path(spark, path, hash_col)

    # 3) Best-effort UC registration pointing at the Volume path (skip if unsupported)
    if register_table and path.startswith("/Volumes/"):
        _ = _try_register_on_volume(spark, full_table_name, path, hash_col)

    # 4) Demo-friendly observability: pre-count diffs (rows that would change)
    rows_changed = 0
    try:
        # If the Delta table exists at 'path', compare hashes for changed rows
        tgt = spark.read.format("delta").load(path).alias("t")
        src = df_hashed.alias("s")
        if isinstance(primary_key, str):
            on = f"t.{primary_key} = s.{primary_key}"
        else:
            on = " AND ".join([f"t.{c} = s.{c}" for c in primary_key])
        cond = f"NOT (t.{hash_col} <=> s.{hash_col}) OR t.{hash_col} IS NULL OR s.{hash_col} IS NULL"
        rows_changed = tgt.join(src, on=on, how="inner").where(cond).count()
    except Exception:
        # Table likely doesn't exist yet at path → treat as "all rows will be inserted" on first load
        try:
            rows_changed = df_hashed.count()
        except Exception:
            rows_changed = 0  # absolute fallback

    # Expose to downstream tasks (run-receipt step)
    try:
        dbutils.jobs.taskValues.set(key="rows_changed", value=str(rows_changed))
    except Exception:
        pass

    if verbose:
        print(f"🔍 Pre-merge diff estimate (rows_changed): {rows_changed}")

    # 5) Upsert (merge) as usual
    try:
        from utils_py.utils_upsert_with_hashstring import upsert_with_hashstring

        # Optional: user metadata for DESCRIBE HISTORY when a commit happens
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

    if verbose:
        print("✅ Silver table write complete.")
