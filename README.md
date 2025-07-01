# 🚀 Databricks Pipelines

This repository contains modular data pipelines built using **Azure Databricks**, **Azure Blob Storage**, **Delta Lake**, and **Workflows**.

The goal is to explore multiple strategies for handling **batch ingestion and processing** using clean, cost-effective patterns that can scale to streaming with Autoloader in future iterations.
---


## 📦 Project Structure


```text
databricks-pipelines/
├── pipeline1_batch_delta/
│   ├── bronze/       # Mock ingestion scripts (inventory, shipments, vendors, web forms)
│   ├── silver/       # Enrichment and transformations (joins, cleansing)
│   ├── gold/         # Aggregated business-ready outputs
│   ├── transform/    # Merge logic and reusable components (planned)
│   ├── utils/        # Mount scripts, write helpers, secure configs
│   └── docs/         # Setup notes for Git, Azure Key Vault, etc.
├── common/           # Shared utilities across pipelines
└── README.md         # Project overview and structure
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

## 📈 Gold Layer Output

The current gold output includes:

- `vendor_summary_clean`: Aggregates total inventory items and shipment activity per vendor, using joined data from inventory and shipment sources.

---

## 🧪 Testing and Mock Data

- Mock data (CSV/JSON) is stored in `/mnt/raw-ingest/` and `/mnt/external-ingest/`
- Ingested via `mock_*.py` scripts (e.g., `mock_finance_invoices.py`, `mock_web_forms.py`)
- Cleaned, transformed, and versioned for reproducibility
- Data quality checks and transformations applied in the Silver layer

---

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
🕒 Last updated: July 1, 2025
---

