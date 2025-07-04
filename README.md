# 🚀 Databricks Pipelines

This repository contains modular data pipelines built using **Azure Databricks**, **Azure Blob Storage**, **Delta Lake**, and **Workflows**.

The goal is to explore multiple strategies for handling **batch ingestion and processing** using clean, cost-effective patterns that can scale to streaming with Autoloader in future iterations.
---


## 📦 Project Structure


```text
```text
databricks-pipelines/
├── pipeline1_batch_delta/
│   ├── bronze/         # Mock ingestion scripts (inventory, shipments, vendors)
│   ├── silver/         # Enrichment and transformations (joins, cleansing)
│   ├── gold/           # Aggregated business-ready outputs
│   ├── transform/      # Merge logic and shared transformations (planned)
│   ├── utils/          # Mounting, write helpers, reusable scripts
│   └── docs/           # Setup instructions (Git, Key Vault, Blob mounts)
├── common/             # Shared utilities across pipelines
├── LICENSE
└── README.md           # Project overview and structure
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
- GitHub (with Databricks Repos integration)

---
## 📊 Pipeline Flow

```text
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


## 📂 Pipeline Stage Documentation

- [🔶 Bronze Layer](pipeline1_batch_delta/bronze/README.md)
- [⚪ Silver Layer](pipeline1_batch_delta/silver/README.md)
- [🥇 Gold Layer](pipeline1_batch_delta/gold/README.md)
- [🛠️ Utils](pipeline1_batch_delta/utils/README.md)


## 📈 Gold Layer Output

The Gold layer produces a single Delta table:

### `vendor_summary_clean`

This aggregated table summarizes key compliance and financial metrics per vendor:

| Column Name        | Description                                           |
|--------------------|-------------------------------------------------------|
| `vendor_id`        | Normalized vendor identifier                          |
| `vendor_name`      | Human-readable vendor name (e.g., "Vendor A")         |
| `total_invoices`   | Count of unique invoices associated with the vendor   |
| `latest_due_date`  | Most recent due date across all invoices              |
| `latest_invoice_date` | Most recent invoice date                           |
| `last_audit_date`  | Most recent compliance audit                          |
| `compliance_score` | Latest compliance score (0–100 scale)                 |
| `compliance_status`| Status category: "Compliant", "At Risk", or "Suspended"|

This table joins enriched invoice data with vendor compliance metrics and is designed for analytics, reporting, and downstream BI tools.


---

## 🧪 Testing and Mock Data

- Mock CSV files are stored in mounted Blob Storage paths like `/mnt/raw-ingest/finance_invoice_data.csv`
- Ingested using scripts like:
  - `mock_finance_invoices.py`
  - `mock_web_forms.py`
- Cleaned and normalized in Silver using:
  - `transform_finance_invoices.py` → outputs `finance_invoices_v2`
  - `silver_vendor_compliance.py` → outputs `vendor_compliance_clean`
  - `silver_enrichment.py` → joins to vendor name
  - `04e_transform_all_clean.py` → creates `final_vendor_summary_prep`
- Final aggregation happens in `gold_summary.py`


---
🔗 SQL Server Integration via Ngrok + Azure Key Vault
This project includes secure connectivity between Azure Databricks and a local SQL Server instance using:

🔐 Azure Key Vault for storing secrets like JDBC URL, SQL user, and password

🛠️ Databricks Secret Scopes to fetch secrets at runtime

🌐 Ngrok to tunnel localhost:1433 for remote JDBC access

🔑 Required Azure Key Vault Secrets:
Secret Name	Description
sql-jdbc-url	JDBC string using your ngrok TCP forwarding URL
sql-user	SQL Server login username
sql-password	SQL Server login password

These secrets are stored in your Azure Key Vault instance and referenced in your code via a Databricks-backed secret scope (e.g., databricks-secrets-lv426).

🧪 Sample Usage
In utils/sql_connector.py, the following logic is used:

python
Copy
Edit
jdbc_url = dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-jdbc-url")
connection_properties = {
    "user": dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-user"),
    "password": dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df = spark.read.jdbc(url=jdbc_url, table="INFORMATION_SCHEMA.TABLES", properties=connection_properties)
🚪 Example Ngrok Setup (One-Time)
Install ngrok

Authenticate:

ngrok config add-authtoken <your-token>
Start a TCP tunnel:

ngrok tcp --region=us localhost:1433
Use the tcp://<host>:<port> from ngrok to build your JDBC URL:


jdbc:sqlserver://<host>:<port>;databaseName=fury161;
Paste this into Azure Key Vault under sql-jdbc-url.



## 🧠 Project Goals

- Practice modular pipeline design using notebooks and functions
- Compare batch ingestion strategies across sources and formats
- Enforce schema and data quality standards
- Build scalable workflows with cost awareness (< $50/month)
- Enable growth into Autoloader and streaming architecture

---

## 🧑‍💻 Local Development (Optional)

This project is built in **Azure Databricks**, and synced to GitHub.  
To run commands locally via the CLI:

```bash
# Install the Databricks CLI
pip install databricks-cli

# Authenticate using a personal access token
databricks configure --token

# Required:
#   - Your Databricks workspace URL
#   - Your PAT (Personal Access Token)

⚠️ .databrickscfg is excluded via .gitignore for security.
All secrets (e.g., storage keys) are managed through Azure Key Vault and accessed in notebooks via Databricks Secret Scopes.

🔒 Security Practices
✅ No secrets are stored in this repository

✅ Blob Storage access is handled via Azure Key Vault integration

✅ .databrickscfg, .env, and temp artifacts are excluded

✅ Development occurs in Databricks; code is synced to GitHub and then optionally pulled locally

📚 Getting Started
To replicate this pipeline:

Clone the repo:

git clone https://github.com/AstroSpiderBaby/databricks-pipelines.git
Open in Azure Databricks Repos or locally in VS Code.

In Databricks, run the notebooks in order:

mock_finance_invoices.py

transform_finance_invoices.py

silver_enrichment.py

gold_summary.py

Customize by adding files to your mounted Blob Storage containers.

🪪 License
This project is open source under the MIT License.

🔁 Synced from Azure Databricks
🧠 Maintained by AstroSpiderBaby
🕒 Last updated: July 2, 2025
---

