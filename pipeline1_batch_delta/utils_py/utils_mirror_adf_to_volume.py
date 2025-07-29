# utils_py/utils_mirror_adf_to_volume.py

from pyspark.sql import SparkSession

def mirror_adf_parquet_to_uc_volume(
    dbutils,  # 👈 must be here
    container: str,
    relative_path: str,
    unity_volume_path: str,
    secret_scope: str,
    secret_key: str,
    storage_account: str = "datalakelv426"
):
    """
    Mirrors Parquet data from Azure Blob (ADF output) into a Unity Catalog Volume.

    Parameters:
    - container: Azure Blob container name (e.g., 'adf-silver')
    - relative_path: Folder inside the container (e.g., 'vendor_registry_clean')
    - unity_volume_path: Full path to UC Volume destination (e.g., '/Volumes/thebetty/silver/vendor_registry_clean')
    - secret_scope: Name of your Databricks secret scope
    - secret_key: Key inside the scope that holds the Azure Storage key
    - storage_account: Name of the Azure Blob storage account
    """

    spark = SparkSession.builder.getOrCreate()

    # Build source path
    source_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/{relative_path}"

    # Set Spark config for secure access
    storage_key = dbutils.secrets.get(scope=secret_scope, key=secret_key)
    spark.conf.set(f"fs.azure.account.key.{storage_account}.blob.core.windows.net", storage_key)

    # Read from Azure Blob
    df = spark.read.format("parquet").load(source_path)
    print(f"✅ Read {df.count()} rows from {source_path}")

    # Clean up if a non-Delta structure already exists
    if dbutils.fs.ls(unity_volume_path):
        print(f"⚠️ Removing non-Delta path: {unity_volume_path}")
        dbutils.fs.rm(unity_volume_path, recurse=True)

    from pyspark.sql.functions import sha2, concat_ws, current_timestamp

    hash_columns = ["vendor_id", "industry", "headquarters", "onwatchlist", "registrationdate", "tier"]

    df = df.withColumn(
        "hashstring",
        sha2(concat_ws("||", *hash_columns), 256)
    ).withColumn("ingestion_timestamp", current_timestamp())
    
    # Write to Unity Volume
    df.write.format("delta").mode("overwrite").save(unity_volume_path)
    print(f"✅ Written to Unity Volume: {unity_volume_path}")