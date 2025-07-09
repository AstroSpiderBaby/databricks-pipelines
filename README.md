
# 🚀 Databricks Pipelines

This repository contains modular data pipelines built using **Azure Databricks**, **Azure Blob Storage**, **Delta Lake**, **Workflows**, and **Azure Data Factory (ADF)**.  
The goal is to explore multiple strategies for handling **batch ingestion and processing**, using clean, cost-effective patterns that can scale to **streaming with Autoloader** and **external orchestration with ADF** in future iterations.

**ADF is currently used to ingest external registry data into Azure Blob Storage, where it is picked up and processed through the Delta Lake pipeline layers.**

## 🤝 Contributing
Contributions are welcome! Please review [CONTRIBUTING.md](./CONTRIBUTING.md) before submitting pull requests.

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
│   ├── bronze/                 # Raw ingestion layer
│   ├── silver/                 # Cleaned + enriched data
│   │   └── adf_data/           # Subfolder for ADF-sourced registry inputs
│   ├── gold/                   # Aggregated and summarized outputs
│   ├── transform/              # Optional staging or enrichment logic
│   ├── utils/                  # Shared functions (e.g., upsert, write, SQL, mount)
│   └── docs/                   # Design notes or metadata if needed
├── common/                    # Shared modules across pipelines (planned)
├── LICENSE
└── README.md
```

---

## 🔁 Pipeline Variants (Planned)

| Pipeline                      | Features                                                                 |
|------------------------------|--------------------------------------------------------------------------|
| `pipeline1_batch_delta`      | Batch ingestion from multiple sources (ADF output, Azure Blob, on-prem SQL via JDBC) → Silver enrichment → Gold aggregation with run tracking |
| `FUTURE-pipeline2_modular_functions`| Centralized utility functions (upsert, write, mount, SQL) for reuse across stages |
| `FUTURE-pipeline3_autoloader_batch` | Planned: File-based batch ingestion using Autoloader with manual trigger |
| `FUTURE-pipeline4_streaming_mode`   | Future: Continuous ingestion and transformation using Structured Streaming |

---

## 🧰 Technologies

### 🔹 Compute & Processing
- **Azure Databricks** (Runtime 15.4): Unified analytics platform for Spark-based processing
- **PySpark**: Data transformation and enrichment logic written in Python

### 🔹 Ingestion & Integration
- **Azure Data Factory (ADF)**: Transfers external vendor registry data into Blob storage
- **SQL Server (On-Prem)**: Pulled securely using JDBC + Ngrok tunneling
- **Azure Blob Storage**: Landing zone for raw and ADF files, mounted via Key Vault

### 🔹 Data Management
- **Delta Lake**: Bronze, Silver, and Gold layer architecture with ACID transaction support
- **Databricks Workflows**: Visual pipeline orchestration and dependency tracking

### 🔹 Source Control & Security
- **GitHub**: Integrated via Databricks Repos for version control and code collaboration
- **Azure Key Vault + Databricks Secret Scopes**: Secure handling of secrets

---

## 📊 Pipeline Flow

```
Azure Blob + ADF + SQL Server
│
▼
🟫 Bronze Layer (Ingestion)
  - bronze_ingest_finance_invoices.py ← Azure Blob (CSV)
  - bronze_ingest_web_forms.py ← External ingest (JSON)
  - bronze_ingest_inventory.py
  - bronze_ingest_vendors.py
  - bronze_ingest_shipments.py
  - bronze_ingest_vendor_compliance.py ← SQL Server via JDBC

⚪ Silver Layer (Cleansing & Enrichment)
  - silver_clean_finance_invoices.py → finance_invoices_v2
  - silver_clean_web_forms.py → web_forms_clean
  - silver_join_inventory_shipments.py → inventory_shipments_joined_clean
  - silver_finance_vendor_join.py → finance_with_vendor_info
  - silver_join_finance_registry.py → vendor_registry_clean (from ADF)
  - silver_finalize_vendor_summary.py → final_vendor_summary_prep

🥇 Gold Layer (Aggregation & Output)
  - gold_write_vendor_summary.py → vendor_summary_enriched (partitioned by tier)
```

---

## 🔁 Databricks Workflow Orchestration

This project uses a visual **Databricks Workflow** to orchestrate full pipeline execution in a modular, dependency-driven manner.

```
bronze_ingest_finance_invoices
bronze_ingest_inventory
bronze_ingest_vendors
bronze_ingest_shipments
bronze_ingest_web_forms
    │
silver_clean_finance_invoices
silver_clean_web_forms
silver_clean_vendor_compliance
    │
silver_join_inventory_shipments
silver_join_finance_registry
silver_finalize_vendor_summary
    │
gold_write_vendor_summary
```

📍 Source notebooks are located in:

- `pipeline1_batch_delta/bronze/`
- `pipeline1_batch_delta/silver/`
- `pipeline1_batch_delta/silver/adf_data/`
- `pipeline1_batch_delta/gold/`
- `pipeline1_batch_delta/utils/`

---

## 📂 Pipeline Stage Documentation

- [🔶 Bronze Layer](pipeline1_batch_delta/bronze/README.md)
- [⚪ Silver Layer](pipeline1_batch_delta/silver/README.md)
- [🥇 Gold Layer](pipeline1_batch_delta/gold/README.md)
- [🛠️ Utils](pipeline1_batch_delta/utils/README.md)

---

## 📈 Gold Layer Output

The Gold layer produces a single enriched and partitioned Delta table:

### `vendor_summary_enriched`

| Column Name         | Description                                            |
|---------------------|--------------------------------------------------------|
| `vendor_id`         | Normalized vendor identifier                           |
| `vendor_name`       | Human-readable vendor name                             |
| `total_invoices`    | Count of unique invoices per vendor                    |
| `latest_due_date`   | Most recent due date across all invoices               |
| `latest_invoice_date` | Most recent invoice date                             |
| `last_audit_date`   | Most recent compliance audit                           |
| `compliance_score`  | Latest compliance score (0–100 scale)                  |
| `compliance_status` | Compliance category ("Compliant", "At Risk", etc.)     |
| `industry`          | Vendor industry from registry                          |
| `headquarters`      | Vendor headquarters city                               |
| `onwatchlist`       | Boolean flag for watchlist status                      |
| `registration_date` | Registration year of vendor (for partitioning)         |
| `tier`              | Tier classification from ADF source                    |
| `ingestion_timestamp` | Auto-generated pipeline ingestion timestamp          |

---

## 🧪 Testing and Mock Data

Mock files are stored in `/mnt/raw-ingest/` and `/mnt/lv426-adf-data/` (Parquet).  
Data is processed and cleaned using the following notebooks:

### Ingestion (Bronze Layer)
- `bronze_ingest_finance_invoices.py`
- `bronze_ingest_web_forms.py`
- `bronze_ingest_inventory.py`
- `bronze_ingest_vendors.py`
- `bronze_ingest_shipments.py`

### Silver Cleaning & Joins
- `silver_clean_finance_invoices.py`
- `silver_clean_web_forms.py`
- `silver_clean_vendor_compliance.py`
- `silver_join_inventory_shipments.py`
- `silver_finalize_vendor_summary.py`
- `silver_join_finance_registry.py` ← (includes ADF data)

### Final Gold Output
- `gold_write_vendor_summary.py` → writes `vendor_summary_enriched`

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
_Last updated: {date.today().strftime('%B %d, %Y')}_
