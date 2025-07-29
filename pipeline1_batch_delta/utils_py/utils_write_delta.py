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
    required_columns: Optional[List[str]] = None,
) -> Optional[str]:
    try:
        spark = SparkSession.builder.getOrCreate()

        if verbose:
            print(f"\n🔧 write_to_delta() called")
            print(f"📁 Path: {path}")
            print(f"📝 Mode: {mode}")
            print(f"📚 Table: {full_table_name or 'N/A'}")
            print(f"🔀 Partition by: {partition_by}")
            df.printSchema()

        # ✅ Validate required columns
        if required_columns:
            missing = [col for col in required_columns if col not in df.columns]
            if missing:
                raise ValueError(f"❌ Schema validation failed. Missing columns: {missing}")
            elif verbose:
                print(f"✅ Schema check passed: {required_columns}")

        if dry_run:
            print("🚫 Dry run mode: DataFrame will not be written.")
            return None

        # ✨ Set up writer
        writer = (
            df.write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
        )

        if merge_schema:
            writer = writer.option("mergeSchema", "true")

        if partition_by:
            writer = writer.partitionBy(partition_by)

        # === Unity Volume Smart Handling ===
        if register_table and full_table_name and path.startswith("/Volumes/"):
            # Let Spark handle Unity Catalog registration
            if verbose:
                print(f"💾 Saving table with saveAsTable (Unity Catalog compatible) ...")
            df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(full_table_name)
            print(f"✅ Table registered in Unity Catalog: {full_table_name}")
            return full_table_name

        # === Legacy Path Save ===
        if verbose:
            print(f"💾 Saving to path: {path} ...")
        writer.save(path)

        # Optional table registration (manual CREATE for /mnt only)
        if register_table and full_table_name:
            if path.startswith("/mnt/"):
                container_and_path = path.replace("/mnt/", "")
                container, *subdirs = container_and_path.split("/")
                storage_account_name = "datalakelv426"  # Customize this per use case
                abfss_path = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/" + "/".join(subdirs)
                spark.sql(f"""
                    CREATE TABLE {full_table_name}
                    USING DELTA
                    LOCATION '{abfss_path}'
                """)
                print(f"📚 Table registered using legacy path: {full_table_name}")
            else:
                print(f"⚠️ Skipped manual registration for path: {path}")

        return full_table_name

    except Exception as e:
        print(f"❌ Error writing to Delta: {e}")
        raise
