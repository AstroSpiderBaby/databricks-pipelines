
# 🚀 Databricks Pipelines – Modular Unity Architecture (Notebooks + Python)

This repository contains scalable, modular data pipelines built on **Azure Databricks**, featuring **Unity Catalog**, **Delta Lake**, **Azure Blob Storage**, **SQL Server (via JDBC)**, and **Azure Data Factory (ADF)**.  

It supports **dual execution modes**:
- Notebook-driven orchestration (for exploration and visualization)
- Git-tracked Python scripts (for production-ready, CI/CD-compliant workflows)

💡 All data is now **managed through Unity Catalog**, with **Delta tables registered in `thebetty` catalog** under Bronze, Silver, and Gold schemas.  
Raw source data still flows through Azure Blob containers (e.g., `raw-ingest`, `external-ingest`) before being processed into Unity-managed Volumes.

---

## 🧠 What’s New in This Version?

- ✅ Full transition to **Unity Catalog**: `/Volumes/thebetty/...` + `thebetty.<schema>.<table>`
- ✅ Modular **Python-based pipeline** executed via **Databricks Workflows**
- ✅ Registered Delta tables for BI/SQL access at each layer
- ✅ SQL Server ingestion via secure **JDBC + Ngrok + Key Vault**
- ✅ Structured logging of pipeline runs to Gold

---

## 📍 Unity Catalog Overview

| Layer   | Path Example                                         | Unity Table Example                            |
|---------|------------------------------------------------------|-------------------------------------------------|
| Bronze  | `/Volumes/thebetty/bronze/inventory`                 | `thebetty.bronze.inventory`                    |
| Silver  | `/Volumes/thebetty/silver/finance_with_vendor_info` | `thebetty.silver.finance_with_vendor_info`     |
| Gold    | `/Volumes/thebetty/gold/final_vendor_summary`        | `thebetty.gold.final_vendor_summary`           |
| Logs    | `/Volumes/thebetty/gold/logs/final_vendor_summary_runs` | `thebetty.gold.final_vendor_summary_runs`  |

---

## 📦 Project Structure

```
databricks-pipelines/
├── pipeline1_batch_delta/
│   ├── bronze_py/             # Python-based ingestion scripts (Unity Bronze)
│   ├── silver_py/             # Python-based transformation and joins (Unity Silver)
│   ├── gold_py/               # Python-based aggregations and outputs (Unity Gold)
│   ├── utils_py/              # Reusable utility functions (e.g., upsert, write)
│   ├── tests/                 # Unit tests and mock data validations
│   └── docs/                  # Optional design notes or diagrams
├── common/                    # Shared modules across pipelines (planned)
├── LICENSE
└── README.md
```

---

## 🚀 Dual Execution Modes

You can now run this pipeline in two different ways:

▶️ Option 1: **Notebook Workflow**  
Execute directly in Databricks Repos UI:  
`bronze/ → silver/ → gold/`

▶️ Option 2: **Python Job Workflow**  
Run the `batch1_py_pipeline` job via Databricks Workflows:  
`bronze_py/ → silver_py/ → gold_py/`  
Each script uses utilities in `utils_py/` for modularity and testing.

---

## 🔁 Pipeline Variants (Planned)

| Pipeline                         | Features                                                                 |
|----------------------------------|--------------------------------------------------------------------------|
| `pipeline1_batch_delta`          | Batch ingestion (ADF + Blob + SQL) → Silver normalization → Gold aggregation |
| `FUTURE-pipeline2_modular_functions` | Centralized utilities (upsert, SQL, mount) reusable across pipelines |
| `FUTURE-pipeline3_autoloader_batch` | Ingest batch files via Autoloader (triggered) |
| `FUTURE-pipeline4_streaming_mode`   | Continuous Structured Streaming ingestion |

---

## 🧰 Technologies

### 🔹 Compute & Processing
- **Azure Databricks** (Runtime 15.4+)
- **PySpark**

### 🔹 Ingestion & Integration
- **Azure Data Factory (ADF)** for vendor registry
- **SQL Server** via JDBC tunnel + **Ngrok**
- **Azure Blob Storage**: Raw file drop zone

### 🔹 Data Management
- **Delta Lake** (Bronze, Silver, Gold architecture)
- **Unity Catalog**: Unified governance for all tables
- **Databricks Workflows**: Visual pipeline orchestration

### 🔹 Security & Source Control
- **GitHub** with Databricks Repos
- **Azure Key Vault** + Secret Scope (`databricks-secrets-lv426`)

---

## 📊 Pipeline Flow

```
Azure Blob + ADF + SQL Server
│
▼
🟫 Bronze Layer (Ingestion)
  - Ingest raw CSV, JSON, and SQL data (JDBC)
  - No transformations

⚪ Silver Layer (Cleansing)
  - Normalize, deduplicate, validate
  - Join registry + compliance + finance

🥇 Gold Layer (Aggregation)
  - Join Silver datasets
  - Aggregate by vendor_id
  - Partitioned by `tier`
  - Tracked via pipeline run timestamp
```

---

## 📈 Gold Output

| Column                 | Description                                  |
|------------------------|----------------------------------------------|
| `vendor_id`            | Unique vendor identifier                     |
| `vendor_name`          | Clean, human-readable name                   |
| `total_invoices`       | Distinct invoice count                       |
| `latest_due_date`      | Most recent due date                         |
| `latest_invoice_date`  | Most recent invoice date                     |
| `last_audit_date`      | Most recent compliance audit                 |
| `compliance_score`     | Latest score (0–100 scale)                   |
| `compliance_status`    | Compliant, At Risk, etc.                     |
| `industry`             | Vendor industry from registry                |
| `headquarters`         | City from registry                           |
| `onwatchlist`          | Boolean flag from registry                   |
| `registration_date`    | Original vendor registration date            |
| `tier`                 | Vendor tier from ADF                         |
| `pipeline_run_timestamp` | Ingestion run timestamp                   |

---

## 🔗 SQL Server Integration via Ngrok + Key Vault

- Tunnels from `localhost:1433` to public via **Ngrok**
- Uses **Azure Key Vault** for:
  - JDBC URL
  - SQL Username / Password
```python
jdbc_url = dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-jdbc-url")
df = spark.read.jdbc(url=jdbc_url, table="your_table_name", properties=...)
```

---

## 🔒 Security Highlights

- 🔐 No plaintext credentials
- ✅ GitHub secrets excluded
- 🔐 Secrets managed with Azure Key Vault + Databricks scopes
- ✅ Volumes replace legacy mounts

---

## 🪪 License

MIT License  
Maintained by AstroSpiderBaby  
_Last updated: July 29, 2025_
