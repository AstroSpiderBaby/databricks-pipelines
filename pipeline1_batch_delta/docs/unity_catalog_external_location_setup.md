
# Unity Catalog: External Location Setup (lv426)

_Last Updated: 2025-07-24 16:48:48_

## 🔧 Objective

Create a secure external location in Unity Catalog for the `raw-ingest` container in the `datalakelv426` storage account using the new Databricks CLI v0.205+ and JSON-based grant workflow.

---

## ✅ Prerequisites

- Databricks CLI v0.205+ installed
- `adminspider@brucejenkslive.onmicrosoft.com` has metastore admin rights
- Storage container: `raw-ingest` in `datalakelv426`
- Storage credential already created and listed
- Active CLI profile: `adminspider`

---

## 🧱 Step 1: Verify Existing Storage Credential

```bash
databricks storage-credentials list --profile adminspider
```

Look for your storage credential name (in our case):

```
158feb2a-6f27-4b99-a1e2-723e7d82f245-storage-credential-1752875660848
```

---

## 🛡️ Step 2: Grant Access to `adminspider` User

**JSON File (`grant_adminspider.json`):**

```json
{
  "changes": [
    {
      "principal": "adminspider@brucejenkslive.onmicrosoft.com",
      "add": ["ALL_PRIVILEGES"]
    }
  ]
}
```

**Grant Access with CLI:**

```bash
databricks grants update storage-credential 158feb2a-6f27-4b99-a1e2-723e7d82f245-storage-credential-1752875660848 \
  --json @Unity_catalog/grant_adminspider.json \
  --profile adminspider
```

✅ Output should confirm privileges were granted.

---

## 🌐 Step 3: Create External Location

```bash
databricks external-locations create \
  lv426_raw_external_location \
  abfss://raw-ingest@datalakelv426.dfs.core.windows.net/ \
  158feb2a-6f27-4b99-a1e2-723e7d82f245-storage-credential-1752875660848 \
  --comment="Raw ingest zone for lv426 data lakehouse" \
  --read-only \
  --profile adminspider
```

**Successful Output:**
- Confirms creation
- Verifies metastore ID, ownership, and read-only mode

---

## 🔎 Validation

To check it exists:

```bash
databricks external-locations list --profile adminspider
```

You should see `lv426_raw_external_location` listed.

---

## 🧩 Next Step

Move on to volume creation or table definition from this external location.

