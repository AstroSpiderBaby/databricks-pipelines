%md
# 📘 ADF Ingestion to Silver Pipeline

**Author:** Bruce Jenks  
**Last Updated:** July 7, 2025  

This document outlines the process of using **Azure Data Factory (ADF)** to trigger a data pipeline that ingests files into **Azure Data Lake**, followed by transformation and promotion into the **Silver layer** in Delta Lake via **Azure Databricks**.

---

## 🔁 Overview of the ADF to Databricks Flow

1. **ADF Pipeline Execution**
   - Triggers when a file lands in Blob Storage (e.g., `raw` or `external` containers)
   - Moves or transforms the file and places it into the `adf-silver` container

2. **Databricks Pipeline**
   - Reads the file from `/mnt/adf-silver/<your-folder>`
   - Converts from Parquet (or CSV) to Delta format
   - Writes output to `/mnt/delta/silver/<your_table_name>`
   - Optionally registers the Delta table to Unity Catalog or Hive Metastore

---

## 📥 ADF Setup Notes

- **Linked Services:** Azure Blob Storage & Databricks
- **Dataset Format:** Parquet or CSV
- **Sink Path Example:**  
  `https://datalakelv426.blob.core.windows.net/adf-silver/<your-folder>`
- **Debugging Tip:** Use *Display Output* in ADF to confirm your destination path

✅ **Dynamic Naming Tip:**  
Use `@dataset().path` + `@utcnow()` to generate unique output files during pipeline runs.

---

## 📂 Mount Check (Optional)

Ensure the `adf-silver` container is mounted correctly in Databricks:

```python
display(dbutils.fs.ls("/mnt/adf-silver"))
```

---

## 🔄 Sample Code to Promote to Silver Layer

This example reads the output file from ADF and converts it to Delta format.

```python
from pyspark.sql import SparkSession
from write_utils import write_df_to_delta  # Already in repo

spark = SparkSession.builder.getOrCreate()

input_path = "/mnt/adf-silver/Vendor_Registry_Silver"  # ADF Output
output_path = "/mnt/delta/silver/vendor_registry_silver"  # Silver Delta Target

df = spark.read.format("parquet").load(input_path)

write_df_to_delta(
    df,
    path=output_path,
    mode="overwrite",
    merge_schema=True,
    register_table=True,
    verbose=True
)
```

---

## 🧪 Validation

After writing to the Delta table, validate its contents:

```sql
SELECT * FROM vendor_registry_silver LIMIT 10;
```

Or with PySpark:

```python
spark.sql("SELECT COUNT(*) FROM vendor_registry_silver").show()
```

---

## 📝 Notes and Gotchas

- If the mount isn't working, confirm that `secret_scope_setup` or `azure_key_vault_setup` is complete.
- Confirm that ADF container and folder names **match exactly** in the portal and pipeline.
- If you get a `PATH_NOT_FOUND` error, verify your mount and blob path.
- You can explore mounts manually via:

```python
display(dbutils.fs.ls("/mnt/adf-silver"))
```

---

## 📌 Summary

- ADF drops files into `adf-silver` Blob container
- Databricks promotes the file into Delta format under Silver layer
- Delta files saved to `/mnt/delta/silver/...`
- Table optionally registered for metastore or Unity Catalog access
