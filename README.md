# 🚀 Databricks Pipelines

This repository contains modular data pipelines built using **Databricks**, **Azure Blob Storage**, **Delta Lake**, and **Workflows**.

The goal is to explore multiple ways to handle batch ingestion and processing using clean, cost-effective patterns that can scale to streaming with Autoloader in future iterations.

---

## 📦 Project Structure

databricks-pipelines/
├── pipeline1_batch_delta/
│   ├── bronze/       # Raw ingestion notebooks (source_a, source_b, etc.)
│   ├── silver/       # Transformation and data quality logic
│   ├── gold/         # Aggregated business-ready outputs
│   ├── transform/    # Merge logic and shared transformations
│   ├── utils/        # Mounting, write helpers, and reusable scripts
│   └── docs/         # Setup, Key Vault, Blob mount instructions
├── common/           # Shared utilities across pipelines

## 🔁 Pipeline Variants (Planned)

| Pipeline                     | Features                                     |
|-----------------------------|----------------------------------------------|
| `pipeline1_batch_delta`     | Batch ingest → Delta write → Workflow-based |
| `pipeline2_modular_functions` | Shared function logic, reusable modules     |
| `pipeline3_autoloader_batch` | Uses Autoloader with manual trigger         |
| `pipeline4_streaming_mode`  | Future: Continuous ingestion with streaming |

## 🧰 Technologies

- Databricks Runtime 15.4
- Delta Lake
- Azure Blob Storage
- PySpark
- Databricks Workflows
- GitHub integration

  ## 🧪 Testing

Mock data is stored under `/data/` folders and versioned for testing ingestion logic and transformations.

## 🧠 Goals

- Practice modular pipeline design using functions
- Compare approaches to batch ingestion
- Scale to Autoloader and Streaming
- Keep costs low (target: <$50/month)

## 🔒 License

This project is open source under the MIT license.
