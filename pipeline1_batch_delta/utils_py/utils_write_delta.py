"""
utils_write_delta.py

Robust utility to write Spark DataFrames to Delta Lake with logging, partitioning, schema merge, and table registration.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Optional, List

def write_df_to_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    merge_schema: bool = True,
    register_table: bool = True,
    partition_by: Optional[List[str]] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> Optional[str]:
    try:
        if verbose:
            print(f"📁 Path: {path}")
            print(f"📝 Mode: {mode}")
            print(f"🔀 Partition by: {partition_by}")
            df.printSchema()

        writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")

        if merge_schema:
            writer = writer.option("mergeSchema", "true")

        if partition_by:
            writer = writer.partitionBy(partition_by)

        if not dry_run:
            writer.save(path)
            if verbose:
                print(f"✅ Data written to {path}")

        table_name = path.rstrip("/").split("/")[-1]

        if register_table:
            spark = SparkSession.builder.getOrCreate()
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"""
                CREATE TABLE {table_name}
                USING DELTA
                LOCATION '{path}'
            """)
            if verbose:
                print(f"📚 Table registered: {table_name}")

            return table_name

        return None

    except Exception as e:
        print(f"❌ Error writing to Delta: {e}")
        raise
