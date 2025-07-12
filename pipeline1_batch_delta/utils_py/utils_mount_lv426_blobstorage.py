"""
Utility Script: utils_mount_lv426_blobstorage.py

Mounts Azure Blob Storage containers to DBFS using a secure access key stored in a secret scope.
"""

# Define parameters
storage_account_name = "datalakelv426"
containers = ["raw-ingest", "external-ingest"]
mount_base = "/mnt"

# Secret Scope setup (must already exist in Databricks)
secret_scope = "lv426"
secret_key_name = "lv426-storage-key"

# Retrieve the storage key securely
storage_key = dbutils.secrets.get(scope=secret_scope, key=secret_key_name)

# Mount each container
for container in containers:
    mount_point = f"{mount_base}/{container}"
    source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net"

    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        print(f"Already mounted: {mount_point}")
    else:
        dbutils.fs.mount(
            source=source,
            mount_point=mount_point,
            extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_key}
        )
        print(f"Mounted: {mount_point}")
