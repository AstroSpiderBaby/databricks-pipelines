import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from utils_py.utils_copy_to_volume import copy_file_to_volume

# Copy files
copy_file_to_volume(
    src_path="dbfs:/FileStore/pipeline1_batch_delta/moc_source_a/Inventory.csv",
    dest_volume_path="/Volumes/thebetty/bronze/landing_zone/Inventory.csv",
    overwrite=True
)

copy_file_to_volume(
    src_path="dbfs:/FileStore/pipeline1_batch_delta/moc_source_b/Shipments.csv",
    dest_volume_path="/Volumes/thebetty/bronze/landing_zone/Shipments.csv",
    overwrite=True
)

copy_file_to_volume(
    src_path="dbfs:/FileStore/pipeline1_batch_delta/moc_source_c/Vendors.csv",
    dest_volume_path="/Volumes/thebetty/bronze/landing_zone/Vendors.csv",
    overwrite=True
)
