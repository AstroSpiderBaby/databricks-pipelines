# 🟫 Bronze Layer

This folder contains ingestion scripts that load **raw source data** into the **Bronze layer** of the Delta Lake architecture.

Each `.py` notebook corresponds to a mock or real-world dataset (e.g., inventory, shipments, vendors, web forms), simulating ingestion patterns for testing and development.

---

## 📥 Key Characteristics

- **Reads From:**
  - Azure Blob Storage (`/mnt/external-ingest/`)
  - Local SQL Server via **Ngrok** tunnel
- **Writes To:**  
  - Delta tables in `/mnt/delta/bronze/`
- **Transformations:**  
  - None — raw fidelity is preserved for traceability and auditability
- **File Formats Supported:**  
  - CSV, JSON, JDBC

---

## 📂 Included Scripts

| Script Name                  | Description                                                                 |
|-----------------------------|-----------------------------------------------------------------------------|
| `bronze_ingest_inventory.py`         | Ingests inventory data from CSV (Azure Blob)                                |
| `bronze_ingest_shipments.py`         | Loads shipment data from CSV (Azure Blob)                                   |
| `bronze_ingest_vendors.py`           | Loads vendor records from CSV                                               |
| `bronze_ingest_web_forms.py`         | Ingests multiline JSON-formatted web form submissions                       |
| `bronze_ingest_finance_invoices.py`  | Loads invoice records from CSV                                              |
| `bronze_ingest_vendor_compliance.py` | Pulls vendor compliance metadata from **fury161** SQL Server via JDBC tunnel |

---

🔙 [Back to Root README](../../README.md)
