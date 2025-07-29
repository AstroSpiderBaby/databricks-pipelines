%md
🚀 Azure Data Factory Pipeline Guide: Ingest, Enrich & Append Vendor Registry
This guide outlines how to use ADF to ingest a Parquet dataset from Azure Blob Storage, enrich it with metadata, and prepare it for downstream consumption in Databricks.

📂 1. Linked Service Setup
Go to Manage > Linked Services

Create a new linked service:

Type: Azure Blob Storage

Name: is_blob

Auth Method: RBAC with Managed Identity OR Storage Account Key

Storage account: Select your target (e.g., datalakelv426)

📁 2. Datasets Setup
Create 2 datasets:

Source Dataset

Type: Parquet

Linked Service: is_blob

File path: Container/folder (e.g., pipeline1_batch_delta/vendor_registry.parquet)

Name: ds_vendor_registry

Sink Dataset

Type: Parquet (or Delta if using Synapse/SQL sink)

Linked Service: Same as above

File path: Output folder (e.g., delta/silver/vendor_registry_enriched)

Name: ds_vendor_registry_enriched

🔄 3. Create Data Flow
Go to Author > Data Flows > New Data Flow

💡 Steps inside Data Flow:
Source

Use ds_vendor_registry

Ensure schema projection works

Set Projection to Allow schema drift if needed

Select Columns (optional)

Use Select transformation to rename and reorder columns

Example:

Rename reg_date → registration_date

Keep vendor_id, vendor_name, etc.

Derived Columns
Add 3 new metadata columns:

ingested_at: currentUTC()

source_tag: "vendor_registry_adf"

pipeline_run_id: concat(currentUTC(), '-', $pipelineRunId)

Sink

Use ds_vendor_registry_enriched

Mode: Append

Enable Auto Mapping

⚙️ 4. Add Pipeline to Trigger the Flow
Create a Pipeline

Add a Data Flow Activity

Link to your created data flow

Pass $pipelineRunId as a parameter

🧪 5. Validate, Publish, Trigger
Click Validate All

Fix any projection or expression errors

Click Publish All

Click Trigger Now or schedule as needed

