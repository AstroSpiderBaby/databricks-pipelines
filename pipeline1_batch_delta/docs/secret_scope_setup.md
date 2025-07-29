%md

# Databricks-Backed Secret Scope Setup
_Last updated: 2025-06-25_

---

## 🔐 Overview

This guide explains how to create a Databricks-backed secret scope (not using Azure Key Vault) for storing secrets like storage account keys. This method is less secure than using Azure Key Vault but simpler and still acceptable for lower environments or quick development work.

---

## ✅ Steps to Create a Databricks Secret Scope

### 1. Create the Secret Scope (via CLI)

```bash
databricks secrets create-scope --scope lv426 --profile astrospider
```

You can name the scope anything, but for consistency we use `lv426`.

---

### 2. Add a Secret to the Scope

```bash
databricks secrets put --scope lv426 --key lv426-storage-key --profile astrospider
```

You will be prompted to paste the value (e.g., your storage account key). After entering the key, press Enter and save.

---

### 3. List Secrets (Optional Verification)

```bash
databricks secrets list --scope lv426 --profile astrospider
```

---

## 📌 Notebook Usage

To retrieve the secret inside Databricks:

```python
secret_scope = "lv426"
secret_key_name = "lv426-storage-key"

storage_key = dbutils.secrets.get(scope=secret_scope, key=secret_key_name)
```

---

## ⚠️ Notes & Best Practices

- Secret scopes created using this method are **Databricks-managed** and stored within the workspace.
- Avoid using this method for production secrets unless you're in a secure and isolated environment.
- Rotate credentials periodically.
- Do not print secret values or commit them into notebooks.

---

## 🧪 Confirm Setup

```bash
databricks secrets list-scopes --profile astrospider
```

Should include:
```
lv426  DATABRICKS
```

---

