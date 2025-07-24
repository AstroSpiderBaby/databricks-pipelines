from pyspark.sql import DataFrame, SparkSession
from typing import Optional, List

def write_to_delta(
    df: DataFrame,
    path: str,
    full_table_name: Optional[str] = None,
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

        if register_table and full_table_name:
            # Handle /mnt path conversion to abfss path for Unity Catalog
            if path.startswith("/mnt/"):
                container_and_path = path.replace("/mnt/", "")
                container, *subdirs = container_and_path.split("/")
                storage_account_name = "datalakelv426"  # ✅ Your actual storage account
                abfss_path = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/" + "/".join(subdirs)
            else:
                abfss_path = path

            spark = SparkSession.builder.getOrCreate()
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            spark.sql(f"""
                CREATE TABLE {full_table_name}
                USING DELTA
                LOCATION '{abfss_path}'
            """)
            if verbose:
                print(f"📚 Table registered in Unity Catalog: {full_table_name}")
            return full_table_name

        return None

    except Exception as e:
        print(f"❌ Error writing to Delta: {e}")
        raise
