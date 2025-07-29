## 📘 Notebook Purpose

This notebook documents and optionally executes the full setup required to integrate Azure Key Vault with Databricks for secure secret management.

It includes:
- Token generation using Azure CLI
- Manual cURL command to create a secret scope
- Known CLI limitations and workarounds
- Example usage of secrets in Databricks notebooks

> **Note:** Replace placeholder values (`<your-keyvault>`, etc.) before execution.
# Azure Key Vault Setup for Databricks

This guide outlines the detailed process to configure Databricks to use Azure Key Vault-backed secret scopes, along with CLI barriers encountered and how they were resolved.

---

## 🔐 Overview

Azure Key Vault-backed secret scopes allow you to securely store secrets (like storage account keys) in Azure Key Vault and reference them in Databricks without hardcoding.

---

## 🧱 Prerequisites

* Azure Key Vault already created
* Secret added to Azure Key Vault
* You have `Owner` or `Contributor` access to the subscription/resource group
* Azure CLI installed and logged in
* Databricks CLI v0.256+ installed and configured

---

## 🔧 Azure Key Vault Details Used

* **Key Vault Name:** `<your-keyvault>`
* **Secret Name:** `lv426-storage-key`
* **Storage Account Name:** `datalakelv426`

---

## ✅ Step-by-Step Setup

### 1. **Generate AAD Token (used for authorization)**

```bash
az account get-access-token \
  --resource=https://management.core.windows.net/ \
  --query accessToken -o tsv > token.txt
```

You will use this token in the next step.

---

### 2. **Create Secret Scope Using Azure Key Vault (in Git Bash or WSL)**

```bash
curl -X POST https://adb-<your-instance>.azuredatabricks.net/api/2.0/secrets/scopes/create \
  -H "Authorization: Bearer $(cat token.txt)" \
  -H "Content-Type: application/json" \
  -d '{
    "scope": "databricks-secrets-lv426",
    "scope_backend_type": "AZURE_KEYVAULT",
    "backend_azure_keyvault": {
      "resource_id": "/subscriptions/<your-subscription-id>/resourceGroups/<your-rg>/providers/Microsoft.KeyVault/vaults/<your-keyvault>",
      "dns_name": "https://<your-keyvault>.vault.azure.net/"
    }
  }'
```

✅ If successful, your secret scope is now available to Databricks.

---

## ⚠️ Barriers Faced

### ❌ CLI Compatibility

* CLI v0.18 did not support `--aad-token` (needed for legacy)
* Needed to use `curl` with manual token
* v0.256 CLI expects `--json` with token and still failed unless `userAADToken` was passed

### ❌ PATH Conflicts

* Both CLI versions were installed (`v0.18` and `v0.256`)
* Had to ensure `databricks` pointed to correct version or use `curl` directly

### ❌ Syntax Errors in JSON

* Quotes, escaping, and payload formatting were sensitive. Easier to build `curl` one-liner in VS Code then paste into Git Bash.

---

## 🧪 Verification

```bash
databricks secrets list-scopes --profile DEFAULT
```

Example Output:

```
Scope                            Backend Type
------------------------------  -------------
astro_keyvault                  DATABRICKS
lv426                           DATABRICKS
databricks-secrets-lv426        AZURE_KEYVAULT
```

---

## 🧩 Usage in Notebook

```python
secret_scope = "databricks-secrets-lv426"
secret_key_name = "lv426-storage-key"

storage_key = dbutils.secrets.get(scope=secret_scope, key=secret_key_name)
```

---

✅ Done! You can now securely use your Azure Key Vault secrets inside Databricks.
