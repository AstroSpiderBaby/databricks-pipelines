# ⚪ Silver Layer – Data Cleaning & Enrichment

This folder contains all **Silver-level transformation scripts** that clean, standardize, enrich, and join data from the Bronze layer into structured Delta tables within **Unity Catalog**.

Each script focuses on producing clean, business-ready tables while maintaining lineage and traceability.

---

## ⚙️ Key Features

- **Reads From:**  
  - Unity Catalog Bronze Volumes (`/Volumes/thebetty/bronze/...`)
- **Writes To:**  
  - Unity Catalog Silver Volumes (`/Volumes/thebetty/silver/...`)
  - Unity Catalog Silver Tables (e.g., `thebetty.silver.*`)
- **Actions Performed:**  
  - Cleansing & validation  
  - Type casting (dates, flags)  
  - Enrichment with lookups  
  - Hashing & deduplication  
  - Pre-aggregation for Gold

---

## 📄 Scripts Overview

| Script Name | Description |
|-------------|-------------|
| `silver_finance_invoices_clean.py` | Cleans finance invoices, maps `vendor_id`, adds date types, and generates hash keys |
| `silver_web_forms_clean.py` | Extracts and normalizes JSON web form submissions |
| `silver_vendor_summary_prep.py` | Joins finance + compliance for Gold prep |
| `silver_inventory_shipments_join.py` | Joins inventory and shipments on `vendor_id` |
| `silver_join_finance_with_registry.py` | Enriches invoices with ADF vendor registry data |
| `silver_finance_vendor_join.py` | Merges Silver invoices with vendor metadata |
| `silver_join_finance_with_registry.py` | Joins finance with vendor registry (for new ADF pipeline) |
| `adf_data_py/vendor_registry_silver.py` | Mirrors and cleans vendor registry from ADF to Silver table |

---

## 📦 Output Tables (Unity Catalog)

| Table Name | Location |
|------------|----------|
| `thebetty.silver.finances_invoices_clean` | `/Volumes/thebetty/silver/finances_invoices_clean` |
| `thebetty.silver.web_forms_clean` | `/Volumes/thebetty/silver/web_forms_clean` |
| `thebetty.silver.vendor_compliance_clean` | `/Volumes/thebetty/silver/vendor_compliance_clean` |
| `thebetty.silver.inventory_shipments_joined_clean` | `/Volumes/thebetty/silver/inventory_shipments_joined_clean` |
| `thebetty.silver.finance_with_vendor_info` | `/Volumes/thebetty/silver/finance_with_vendor_info` |
| `thebetty.silver.vendor_registry_clean` | `/Volumes/thebetty/silver/vendor_registry_clean` |
| `thebetty.silver.final_vendor_summary_prep` | `/Volumes/thebetty/silver/final_vendor_summary_prep` |

---

📂 **ADF-specific transformations are managed in** `adf_data_py/`

🔙 [Back to Root README](../../README.md)
