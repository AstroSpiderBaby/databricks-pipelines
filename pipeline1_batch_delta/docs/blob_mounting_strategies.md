%md
"""
# 📄 Blob Mounting Strategies (Legacy)

This notebook demonstrates two methods for mounting Azure Blob Storage containers in Databricks.

## Purpose:
To showcase alternative credential handling strategies prior to adopting secure secret management via Azure Key Vault.

## Mounting Methods Covered:
1. **Manual Key Injection**  
   - Requires direct entry of the Azure Storage account key.
   - Demonstrates how to build and mount using hardcoded credentials (not recommended for production).

2. **Databricks Secret Scope**  
   - Retrieves the storage account key from a Databricks-managed secret scope.
   - Suitable for development environments with basic secret rotation policies.

## Notes:
- These methods are maintained for historical reference and debugging.
- For production workloads, it is strongly recommended to use Azure Key Vault integration as shown in `azure_key_vault_setup`.

"""



# utils/mount_lv426_blobstorage.py

# Parameters
storage_account_name = "datalakelv426"
containers = ["raw-ingest", "external-ingest"]
mount_base = "/mnt"

# Use hardcoded key (demo only — in prod use Key Vault or Secret Scope)
storage_key = ""

# Loop through containers and mount if not already mounted
for container_name in containers:
    mount_point = f"{mount_base}/{container_name}"
    configs = {
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_key
    }

    if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"✅ Mounted: {mount_point}")
    else:
        print(f"⚠️ Already mounted: {mount_point}")

SECRETE SCOPE VERSION

dbutils.secrets.listScopes()

# utils/mount_lv426_blobstorage.py

# Parameters
storage_account_name = "datalakelv426"
containers = ["raw-ingest", "external-ingest"]
mount_base = "/mnt"

# Secret Scope setup (must already be created in Databricks via UI or CLI)
secret_scope = "lv426"  # This is the name of your Databricks secret scope
secret_key_name = "lv426-storage-key"  # This is the name of the key in the scope

# Get key securely from secret scope
storage_key = dbutils.secrets.get(scope=secret_scope, key=secret_key_name)

# Loop through containers and mount them
for container_name in containers:
    mount_point = f"{mount_base}/{container_name}"
    configs = {
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_key
    }

    if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"✅ Mounted securely: {mount_point}")
    else:
        print(f"⚠️ Already mounted: {mount_point}")



