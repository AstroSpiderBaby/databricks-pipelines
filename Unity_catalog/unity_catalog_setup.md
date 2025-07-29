%md
# Unity Catalog Setup (Azure Databricks Version)

## 🔧 1. Prerequisites

- ✅ Azure subscription with admin access
- ✅ Azure CLI and Databricks CLI installed
- ✅ Azure Key Vault created (e.g., `databricks-secrets-lv426`)
- ✅ Databricks workspace provisioned and running on Azure
- ✅ Databricks CLI version **v0.205+**
- ✅ Python virtual environment (recommended)

---

## 🪜 2. Local CLI Environment Setup

**Create and activate virtual environment:**
```bash
python -m venv dbx_env
.\dbx_env\Scriptsctivate   # Windows
```

**Install new CLI:**
```bash
winget install --id Databricks.DatabricksCLI
```

**Verify version:**
```bash
databricks --version
```

**Authenticate using Azure profile:**
```bash
databricks auth login --profile astrospider
```
- Select **Azure** as cloud
- Provide workspace URL (e.g., `https://adb-xxxx.azuredatabricks.net`)
- Authenticate via browser

📁 Save credentials to: `C:\Users\Bruce\.databrickscfg`

---

## 🔐 3. Azure Key Vault Creation & Secrets

**Create the Key Vault:**
```bash
az keyvault create \
  --name databricks-secrets-lv426 \
  --resource-group databricks_AstroSpider \
  --location westus
```

**Add a secret:**
```bash
az keyvault secret set \
  --vault-name databricks-secrets-lv426 \
  --name lv426-storage-key \
  --value <your-storage-key>
```

**Grant Databricks Managed Identity access:**
- Go to Azure → Key Vault → Access Policies
- Add access policy:
  - ✅ Secret Get
  - 👤 Principal: Databricks workspace managed identity

---

## 🧰 4. Create Unity-Compatible Secret Scope (Azure Key Vault–Backed)

**❌ Legacy CLI will fail** with:
```
Scope with Azure KeyVault must have userAADToken defined!
```

✅ Use this workaround:

**Step 1: Get Azure access token**
```bash
az account get-access-token --resource=https://management.core.windows.net/ \
  --query accessToken -o tsv > token.txt
```

**Step 2: Create scope with `curl`:**
```bash
curl -X POST https://adb-<workspace>.azuredatabricks.net/api/2.0/secrets/scopes/create \
  -H "Authorization: Bearer $(cat token.txt)" \
  -H "Content-Type: application/json" \
  -d '{
        "scope": "lv426_kv",
        "scope_backend_type": "AZURE_KEYVAULT",
        "backend_azure_keyvault": {
          "resource_id": "/subscriptions/<subscription-id>/resourceGroups/databricks_AstroSpider/providers/Microsoft.KeyVault/vaults/databricks-secrets-lv426",
          "dns_name": "https://databricks-secrets-lv426.vault.azure.net/"
        }
      }'
```

**Confirm scope created:**
```bash
databricks secrets list-scopes --profile astrospider
```

---

## 🗃️ 5. Mount Azure Blob Containers Using Key Vault Scope

Example from `mount_lv426_blobstorage.py`:
```python
storage_account_name = "datalakelv426"
containers = ["raw-ingest", "external-ingest"]
mount_base = "/mnt"

secret_scope = "lv426_kv"
secret_key_name = "lv426-storage-key"

storage_key = dbutils.secrets.get(scope=secret_scope, key=secret_key_name)

for container in containers:
    mount_point = f"{mount_base}/{container}"
    configs = {
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_key
    }

    if not any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source=f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"✅ Mounted: {mount_point}")
    else:
        print(f"⚠️ Already mounted: {mount_point}")
```

---

## 🧱 6. Create Unity Catalog Metastore

📍Log into the **Admin Console** (Account Console):
- Go to **Metastores**
- Create a new metastore (e.g., `astro-metastore`)
- Assign it to your workspace

Then:
1. Set the metastore as default
2. Attach it to your Azure region
3. Assign **adminspider** or appropriate principal as **Metastore Admin**

---

## 📁 7. Create Catalog + Schema + Tables

Once Unity is active:

```sql
-- From a Unity-enabled notebook or SQL Editor
CREATE CATALOG IF NOT EXISTS astro_catalog;
CREATE SCHEMA IF NOT EXISTS astro_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS astro_catalog.silver;
CREATE SCHEMA IF NOT EXISTS astro_catalog.gold;
```

---

## 📂 8. Optional – Create `unity_catalog/` Folder in Repo

Include:
- Bash helper scripts (e.g., curl-based scope creation)
- Terraform or JSON samples
- README for Unity usage patterns
- Setup test notebooks

---

## 🧪 9. Test & Troubleshoot

| Symptom                 | Cause                        | Fix                                      |
|------------------------|------------------------------|------------------------------------------|
| Scope not listed       | curl didn't succeed          | Double-check AAD token and DNS           |
| Mount fails            | Key not found                | Verify secret name in Azure KV           |
| Unity errors           | Catalog not attached         | Revisit admin console > workspace perms  |
