from pyspark.sql import SparkSession

# Ensure dbutils is available when running this script outside of notebooks
try:
    dbutils
except NameError:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(SparkSession.builder.getOrCreate())

def copy_file_to_volume(src_path: str, dest_volume_path: str, overwrite: bool = False):
    """
    Copy a file from DBFS/FileStore/mnt to a Unity Catalog volume path.

    Args:
        src_path (str): Source path, e.g., "dbfs:/FileStore/..."
        dest_volume_path (str): Destination Unity path, e.g., "/Volumes/catalog/schema/volume/filename"
        overwrite (bool): Whether to overwrite existing file
    """
    try:
        if overwrite:
            dbutils.fs.rm(dest_volume_path, recurse=True)
        dbutils.fs.cp(src_path, dest_volume_path)
        print(f"✅ Copied: {src_path} → {dest_volume_path}")
    except Exception as e:
        print(f"❌ Failed to copy {src_path} → {dest_volume_path}: {str(e)}")
