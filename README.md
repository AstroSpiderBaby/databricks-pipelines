# 🚀 Databricks Pipelines

This repository contains modular data pipelines built using **Databricks**, **Azure Blob Storage**, **Delta Lake**, and **Workflows**.

The goal is to explore multiple strategies for handling batch ingestion and processing using clean, cost-effective patterns that can scale to streaming with Autoloader in future iterations.

---

## 📦 Project Structure

databricks-pipelines/
├── pipeline1_batch_delta/
│ ├── bronze/ # Mock ingestion scripts (inventory, shipments, vendors)
│ ├── silver/ # Enrichment and transformations (joins, cleansing)
│ ├── gold/ # Aggregated business-ready outputs
│ ├── transform/ # Merge logic and shared transformations (planned)
│ ├── utils/ # Mounting, write helpers, and reusable scripts
│ └── docs/ # Setup instructions (e.g., Git, Key Vault, Blob mounts)
├── common/ # Shared utilities across pipelines


## 🔁 Pipeline Variants (Planned)

| Pipeline                     | Features                                                |
|-----------------------------|---------------------------------------------------------|
| `pipeline1_batch_delta`     | Batch ingest → Enrich → Aggregate → Workflow-triggered |
| `pipeline2_modular_functions` | Shared function logic, reusable modules                |
| `pipeline3_autoloader_batch` | Uses Autoloader with manual trigger                    |
| `pipeline4_streaming_mode`  | Future: Continuous ingestion with streaming             |

## 🧰 Technologies

- Databricks Runtime 15.4
- Delta Lake
- Azure Blob Storage (mounted via Key Vault + Secret Scope)
- PySpark
- Databricks Workflows
- GitHub Integration

## 📈 Gold Layer Output

The current gold output is:

- `vendor_summary_clean`: Aggregates total inventory items and shipment activity per vendor.

## 🧪 Testing

Mock data is stored under `/data/` folders or loaded via mock ingestion scripts (`mock_inventory.py`, etc.) and versioned for ingestion logic and transformation testing.

## 🧠 Goals

- Practice modular pipeline design using functions and workflows
- Compare batch ingestion strategies and scaling options
- Build reusable, testable transformations
- Expand to streaming ingestion with Autoloader
- Maintain low monthly cloud costs (target: <$50)

## 🔒 License

This project is open source under the MIT license.
