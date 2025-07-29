# 🛠️ Utils – Reusable Modules

This folder contains reusable utility scripts supporting Delta writes, SQL ingestion, secret management, and Azure Blob Storage interactions in a **Unity Catalog–ready** Databricks pipeline.

Each script is designed to be **importable across Python-based pipelines** and supports volume-aware logic for Unity execution.

---

## 🧰 Available Modules

| Script                                | Purpose                                                                 |
|---------------------------------------|-------------------------------------------------------------------------|
| `utils_write_delta.py`                | Core write utility with `write_to_delta()` for Delta write logic.      |
| `utils_write_silver.py`               | Unity Catalog–compatible write with optional upsert logic.             |
| `utils_upsert_with_hashstring.py`     | Enables idempotent updates using hash keys (merge/upsert logic).       |
| `utils_sql_connector.py`              | Secure connection to SQL Server via JDBC, secrets stored in Key Vault. |
| `utils_mount_lv426_blobstorage.py`    | Mounts Blob Storage from `datalakelv426` using scoped secrets.         |
| `utils_copy_to_volume.py`             | Copies data from DBFS to Unity Volumes (`/Volumes/`).                  |
| `utils_mirror_adf_to_volume.py`       | Replicates ADF drop zone files into Unity Volumes.                     |
| `utils_generate_vendor_summary.py`    | (Optional) Business logic for vendor-level aggregation.                |
| `get_mock_files.py`                   | Loads and previews sample test files.                                  |
| `test_write_utils.py`                 | Testbed for validating `write_to_delta()` and schema merge logic.      |
| `test_sql_connection.py`              | Validates SQL Server connection parameters using scoped secrets.       |

---

## 🔐 Secret Scopes Required

For any SQL access or mount operations, the following must exist in **Azure Key Vault + Secret Scope**:

| Secret Key Name     | Purpose                     |
|---------------------|-----------------------------|
| `sql-jdbc-url`      | JDBC connection string      |
| `sql-user`          | SQL login username          |
| `sql-password`      | SQL login password          |
| `azure-storage-key` | Azure Blob Storage access   |

Stored in: `databricks-secrets-lv426`

---

## 🧪 Sample: Writing to Delta Table

```python
from utils_py.utils_write_delta import write_to_delta

write_to_delta(
    df=my_df,
    path="/Volumes/thebetty/silver/inventory_shipments_clean",
    mode="overwrite",
    register_table=True,
    merge_schema=True,
    partition_by=["shipment_date"],
    full_table_name="thebetty.silver.inventory_shipments_clean"
)
🛠 Example: SQL Server Pull via Ngrok
python
Copy
Edit
from utils_py.utils_sql_connector import read_sql_table

df_sql = read_sql_table("fury161.dbo.vendor_compliance_raw")
display(df_sql)
💡 Volume Copy for Unity
python
Copy
Edit
from utils_py.utils_copy_to_volume import copy_to_volume

copy_to_volume(
    source_path="/mnt/raw-ingest/inventory.csv",
    dest_volume="/Volumes/thebetty/bronze/landing_zone/inventory.csv"
)
📍All modules are Unity-ready, and designed to support CI/CD, partitioned writing, and secure credential handling.

🔙 Back to Root README