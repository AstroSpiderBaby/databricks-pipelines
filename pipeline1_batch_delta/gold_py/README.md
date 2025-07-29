# 🥇 Gold Layer – Business Aggregation & Analytics

This folder contains **business-facing curated tables** derived from enriched Silver-level data.  
The Gold layer is fully **denormalized**, optimized for reporting, and **registered in Unity Catalog**.

---

## 🧠 Purpose

The Gold layer delivers summarized, high-value datasets suitable for:

- Reporting and BI dashboards
- Vendor compliance analytics
- Performance trends and audits
- External system consumption (e.g., APIs)

---

## 📍 Storage & Registration

| Type     | Location |
|----------|----------|
| Volumes  | `/Volumes/thebetty/gold/...` |
| Tables   | `thebetty.gold.*` (Unity Catalog) |

---

## 📄 Script Overview

| Script Name              | Description |
|--------------------------|-------------|
| `gold_vendor_summary.py` | Aggregates invoice, registry, and compliance data into a final summary table |
|                          | - Joins Silver tables: `final_vendor_summary_prep`, `vendor_registry_clean`, `vendor_compliance_clean` |
|                          | - Calculates totals, latest dates, and flags |
|                          | - Partitions on `tier` |
|                          | - Logs pipeline run metadata |

---

## 🧮 Output Tables

### ✅ Final Summary Table

| Table Name                              | Path                                           |
|-----------------------------------------|------------------------------------------------|
| `thebetty.gold.final_vendor_summary`    | `/Volumes/thebetty/gold/final_vendor_summary` |

#### Key Columns

| Column                 | Description                                  |
|------------------------|----------------------------------------------|
| `vendor_id`            | Unique vendor identifier                     |
| `vendor_name`          | Vendor name                                  |
| `total_invoices`       | Distinct invoice count                       |
| `latest_due_date`      | Most recent due date                         |
| `latest_invoice_date`  | Most recent invoice date                     |
| `last_audit_date`      | Most recent compliance audit                 |
| `compliance_score`     | Compliance score (0–100)                     |
| `compliance_status`    | Compliant, At Risk, or Suspended             |
| `industry`             | Industry from registry                       |
| `headquarters`         | Headquarters city                            |
| `onwatchlist`          | Boolean from registry                        |
| `registration_date`    | Vendor onboarding date                       |
| `tier`                 | Tier level from ADF                          |
| `pipeline_run_timestamp` | Current run timestamp                     |

---

### 🧾 Run Logging Table

| Table Name                                 | Path                                                      |
|--------------------------------------------|-----------------------------------------------------------|
| `thebetty.gold.final_vendor_summary_runs`  | `/Volumes/thebetty/gold/logs/final_vendor_summary_runs`   |

Tracks pipeline run metadata including `run_time`, `vendor_count`, and `vendor_name_count`.

---

🔙 [Back to Root README](../../README.md)
