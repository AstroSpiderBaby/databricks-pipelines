# 🚀 Databricks Pipelines

This repository contains modular data pipelines built using **Azure Databricks**, **Azure Blob Storage**, **Delta Lake**, and **Workflows**.  
The goal is to explore multiple strategies for handling **batch ingestion and processing** using clean, cost-effective patterns that can scale to streaming with Autoloader in future iterations.

---

## 📑 Table of Contents

- [📦 Project Structure](#-project-structure)
- [🔁 Pipeline Variants (Planned)](#-pipeline-variants-planned)
- [🧰 Technologies](#-technologies)
- [📊 Pipeline Flow](#-pipeline-flow)
- [📂 Pipeline Stage Documentation](#-pipeline-stage-documentation)
- [📈 Gold Layer Output](#-gold-layer-output)
- [🧪 Testing and Mock Data](#-testing-and-mock-data)
- [🔗 SQL Server Integration via Ngrok + Azure Key Vault](#-sql-server-integration-via-ngrok--azure-key-vault)
- [🧠 Project Goals](#-project-goals)
- [🧑‍💻 Local Development (Optional)](#-local-development-optional)
- [🔒 Security Practices](#-security-practices)
- [📚 Getting Started](#-getting-started)
- [🪪 License](#-license)

---

## 📦 Project Structure

```
databricks-pipelines/
├── pipeline1_batch_delta/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── transform/
│   ├── utils/
│   └── docs/
├── common/
├── LICENSE
└── README.md
```

---

## 🔁 Pipeline Variants (Planned)

| Pipeline                      | Features                                                |
|------------------------------|---------------------------------------------------------|
| `pipeline1_batch_delta`      | Batch ingest → Enrich → Aggregate → Workflow-triggered |
| `pipeline2_modular_functions`| Shared function logic, reusable modules                |
| `pipeline3_autoloader_batch` | Uses Autoloader with manual trigger                    |
| `pipeline4_streaming_mode`   | Future: Continuous ingestion with streaming             |

---

## 🧰 Technologies

- Azure Databricks (Runtime 15.4)
- Delta Lake (Bronze, Silver, Gold architecture)
- Azure Blob Storage (mounted via Azure Key Vault + Secret Scope)
- PySpark
- Databricks Workflows
- GitHub (Databricks Repos integration)

---

## 📊 Pipeline Flow

```
Raw CSVs (Bronze)
     │
     ▼
transform_finance_invoices.py   +   silver_vendor_compliance.py
     │                                      │
     ▼                                      ▼
finance_invoices_v2                vendor_compliance_clean
     │
     ▼
silver_enrichment.py  ──► finance_with_vendor_info
     │
     ▼
04e_transform_all_clean.py ──► final_vendor_summary_prep
     │
     ▼
gold_summary.py ──► vendor_summary_clean (Gold)
```

---

## 📂 Pipeline Stage Documentation

- [🔶 Bronze Layer](pipeline1_batch_delta/bronze/README.md)
- [⚪ Silver Layer](pipeline1_batch_delta/silver/README.md)
- [🥇 Gold Layer](pipeline1_batch_delta/gold/README.md)
- [🛠️ Utils](pipeline1_batch_delta/utils/README.md)

---

## 📈 Gold Layer Output

The Gold layer produces a single Delta table:

### `vendor_summary_clean`

| Column Name         | Description                                            |
|---------------------|--------------------------------------------------------|
| `vendor_id`         | Normalized vendor identifier                           |
| `vendor_name`       | Human-readable vendor name (e.g., "Vendor A")          |
| `total_invoices`    | Count of unique invoices per vendor                    |
| `latest_due_date`   | Most recent due date across all invoices               |
| `latest_invoice_date` | Most recent invoice date                             |
| `last_audit_date`   | Most recent compliance audit                           |
| `compliance_score`  | Latest compliance score (0–100 scale)                  |
| `compliance_status` | "Compliant", "At Risk", or "Suspended"                 |

---

## 🧪 Testing and Mock Data

Mock CSV files are stored in mounted paths like `/mnt/raw-ingest/finance_invoice_data.csv`.  
They are processed by the following scripts:

- Ingestion:
  - `mock_finance_invoices.py`
  - `mock_web_forms.py`
- Silver Cleaning:
  - `transform_finance_invoices.py` → `finance_invoices_v2`
  - `silver_vendor_compliance.py` → `vendor_compliance_clean`
  - `silver_enrichment.py` → joins vendor name
  - `04e_transform_all_clean.py` → `final_vendor_summary_prep`
- Final Gold Output:
  - `gold_summary.py` → `vendor_summary_clean`

---

## 🔗 SQL Server Integration via Ngrok + Azure Key Vault

This project connects to a local SQL Server using:

- Azure Key Vault for secrets
- Databricks-backed secret scopes (e.g., `databricks-secrets-lv426`)
- Ngrok to tunnel `localhost:1433`

**Notebook Example (`utils/sql_connector.py`):**

```python
jdbc_url = dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-jdbc-url")
connection_properties = {
    "user": dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-user"),
    "password": dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df = spark.read.jdbc(url=jdbc_url, table="INFORMATION_SCHEMA.TABLES", properties=connection_properties)
```

---

## 🧠 Project Goals

- Practice modular pipeline design
- Compare batch ingestion strategies
- Enforce schema + data quality
- Cost-aware architecture (< $50/month)
- Extendable to Autoloader + streaming

---

## 🧑‍💻 Local Development (Optional)

To run locally:

```bash
# Install CLI
pip install databricks-cli

# Configure CLI
databricks configure --token
```

---

## 🔒 Security Practices

- ✅ No hardcoded secrets in notebooks or repo
- ✅ Key Vault + Secret Scope for secure storage
- ✅ Secrets excluded from GitHub
- ✅ Uses secure mount logic in `mount_lv426_blobstorage.py`

---

## 📚 Getting Started

```bash
git clone https://github.com/AstroSpiderBaby/databricks-pipelines.git
```

Run the notebooks in order (Databricks Repos or VS Code):

1. `mock_finance_invoices.py`
2. `transform_finance_invoices.py`
3. `silver_enrichment.py`
4. `gold_summary.py`

---

## 🪪 License

MIT License  
Maintained by AstroSpiderBaby  
_Last updated: July 2, 2025_
