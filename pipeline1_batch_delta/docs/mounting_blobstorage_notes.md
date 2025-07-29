%md

# Mounting Azure Blob Storage in Databricks
_Last updated: 2025-06-25_

---

## ✅ Current Method: Using Azure Key Vault-backed Secret Scope

**Why this method?**  
Avoids hardcoding secrets. Securely pulls your storage account key from Azure Key Vault via Databricks-backed secret scope.

---

### 🔧 Setup Requirements

1. **Azure Resources**:
   - An existing Azure Blob Storage account (e.g., `datalakelv426`)
   - One or more containers inside the storage account (e.g., `raw-ingest`, `external-ingest`)
   - Azure Key Vault with a secret (e.g., `lv426-storage-key` = your access key)

2. **Databricks Configuration**:
   - A secret scope with backend type `AZURE_KEYVAULT`
   - The secret scope must reference the correct Key Vault DNS and resource ID

3. **Python Script** (used in `mount_lv426_blobstorage.py`):

```python
storage_account_name = "datalakelv426"
containers = ["raw-ingest", "external-ingest"]
mount_base = "/mnt"

secret_scope = "databricks-secrets-lv426"
secret_key_name = "lv426-storage-key"

storage_key = dbutils.secrets.get(scope=secret_scope, key=secret_key_name)

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
```

---

### 🔒 Previous Methods

#### 1. **Manual Key Input (for testing only)**

- We manually pasted the access key directly in the script.
- This method was deprecated due to obvious security risks.

---

### 🧪 Common Issues

| Symptom | Cause | Solution |
|--------|-------|----------|
| `StorageKeyNotFound` | Incorrect secret scope or key name | Double check Azure Key Vault and secret names |
| Already mounted error | Mount point exists | Run `dbutils.fs.unmount("/mnt/raw-ingest")` before re-mounting |
| InvalidKey error | KeyVault key expired or missing | Regenerate storage key and update Key Vault |

---

### 📁 Related Files in Repo

- `utils/mount_lv426_blobstorage.py`
- `docs/azure_key_vault_setup`
- `docs/token_generation`

