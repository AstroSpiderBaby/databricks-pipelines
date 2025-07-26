"""
utils_write_silver.py

Silver-level Delta write utility using hash-based upsert.
"""

import os
from pyspark.sql import DataFrame
from utils.utils_upsert_with_hashstring import upsert_with_hashstring

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
    """
    Writes data to a Delta Silver table using hash-based upsert (via merge).

    Args:
        df (DataFrame): Cleaned and hashed DataFrame.
        path (str): Unity Catalog or DBFS path to write the Delta table.
        full_table_name (str): Unity Catalog-qualified table name (e.g., catalog.schema.table).
        primary_key (str): Column used as unique identifier (e.g., 'vendor_id').
        hash_col (str): Column used for detecting changes (e.g., 'hashstring').
        required_columns (list[str]): Ensures essential fields exist before write.
        partition_by (list[str]): Optional partitioning fields.
        register_table (bool): If True, registers with catalog.
        verbose (bool): Enable verbose logging.

    Returns:
        None
    """
    spark = df.sparkSession

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

    # Perform the actual upsert
    upsert_with_hashstring(
        df_new=df,
        path=path,
        primary_key=primary_key,
        hash_col=hash_col
    )

    # Optional Unity Catalog table registration
    if register_table:
        if not spark.catalog._jcatalog.tableExists(full_table_name):
            if verbose:
                print(f"📚 Registering table: {full_table_name}")
            df.limit(0).write.saveAsTable(full_table_name)

    if verbose:
        print("✅ Silver table write complete.")
