# 📚 Data Engineer Training — Lab Notebooks & Data

Public distribution repo για lab notebooks (Databricks `.py` source format) + synthetic data files που χρησιμοποιούνται στο Data Engineer training program.

> ⚠️ **All data is synthetic** — generated με fixed `random.seed(42)`. Δεν περιέχει real personal information ή confidential business data.

---

## 🚀 First time setup

**Read this first**: [`Databricks_Setup_Guide.md`](./Databricks_Setup_Guide.md) — step-by-step για schema/volume/folder structure στο Databricks Free Edition.

---

## 📦 Contents

### Day 2 — Transformations + DQ Cleansing

| File | Size | Description |
|---|---|---|
| `Day2/Lab1_LIVEDEMO_FreeEdition.py` | — | **Lab 1** — End-to-end live demo (6 cells) |
| `Day2/Lab2_EXERCISE2_Payments.py` | — | **Lab 2** — Payments cleansing + enrichment |
| `Day2/Lab3_MYDATA_CLEANUP_Demo.py` | — | **Lab 3** — myDATA invoices DQ (10 issue types) |
| `Day2/declarations.csv` | 53 KB | 300 ΑΑΔΕ tax declarations |
| `Day2/taxpayers.csv` | 2.7 KB | Taxpayer master |
| `Day2/doy.csv` | 0.7 KB | ΔΟΥ master |
| `Day2/employees.csv` | 0.4 KB | Employee master |
| `Day2/payments.csv` | 25 KB | 250 payments (23 DQ issues) |
| `Day2/mydata_invoices_MESSY.csv` | 16 KB | 100 invoices (35 DQ issues) |

#### 🚀 Quick start — Day 2

```
# Lab 1 (LIVE DEMO):
https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2/Lab1_LIVEDEMO_FreeEdition.py

# Lab 2 (Payments):
https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2/Lab2_EXERCISE2_Payments.py

# Lab 3 (myDATA cleanup):
https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2/Lab3_MYDATA_CLEANUP_Demo.py
```

In Databricks: **Workspace → Day2 folder → Right-click → Import → drag-drop → Run All**.

### Day 3 — Data Contracts, Lineage, Optimize & CDF (Tax Declarations)

| File | Size | Description |
|---|---|---|
| `Day3/data_contract_validation_notebook.py` | 18 KB | **Άσκηση 3** — Data Contract Validation (10 code + 12 markdown cells) |
| `Day3/Lineage_History_Notebook.py` | 14 KB | **Άσκηση 4** — Lineage & Time Travel (DESCRIBE HISTORY, RESTORE, UC Lineage) |
| `Day3/Optimize_Vacuum_Notebook.py` | 11 KB | **Άσκηση 5** — OPTIMIZE, Z-ORDER & VACUUM (compaction, data skipping, cleanup) |
| `Day3/CDF_Incremental_Notebook.py` | 13 KB | **Άσκηση 6** — Change Data Feed & Incremental ETL (CDC, partial recompute, MERGE) |
| `Day3/Streaming_Notebook.py` | 13 KB | **Άσκηση 7** — Structured Streaming + Delta (rate source, watermarks, alerts, unified batch+streaming) |
| `Day3/declarations.csv` | 53 KB | 300 synthetic ΑΑΔΕ tax declarations |
| `Day3/doy.csv` | 0.7 KB | ΔΟΥ reference table (8 ΔΟΥs) |
| `Day3/employees.csv` | 0.4 KB | Tax employees reference |
| `Day3/taxpayers.csv` | 2.7 KB | Taxpayer reference |
| `Day3/aade_declarations_data_contract.yaml` | 4.5 KB | Data contract spec (schema + rules + security) |

#### 🚀 Quick start — Day 3

**Άσκηση 3 (Data Contract Validation)**: Download → Import → Run All.

```bash
curl -O https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/data_contract_validation_notebook.py
```

**Άσκηση 4 (Lineage & Time Travel)**: auto-bootstraps silver αν λείπει.

```bash
curl -O https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/Lineage_History_Notebook.py
```

**Άσκηση 5 (OPTIMIZE / Z-ORDER / VACUUM)**: auto-bootstraps silver αν λείπει.

```bash
curl -O https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/Optimize_Vacuum_Notebook.py
```

**Άσκηση 6 (Change Data Feed & Incremental ETL)**: auto-bootstraps silver αν λείπει.

```bash
curl -O https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/CDF_Incremental_Notebook.py
```

**Άσκηση 7 (Structured Streaming + Delta)**: αυτο-περιεχόμενο, synthetic events.

```bash
curl -O https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/Streaming_Notebook.py
```

⚠️ Streaming notebook ξεκινάει continuous queries — πάντα τρέχε το Βήμα 6 (Stop streams) στο τέλος.

In Databricks: **Workspace → Right-click → Import → drag-drop → Run All**. Όλα τα notebooks είναι self-contained (Cell 0 setup + Cell 0.5 bootstrap αν χρειαστεί).

### Day 4 — Feature Engineering, ML Training & DevOps for Data

| File | Description |
|---|---|
| `Day4/Feature_Engineering_Notebook.py` | **Lab 1** — 14 βήματα: imputation, 6 ΑΑΔΕ features, quality checks, Delta save, correlation ranking |
| `Day4/ML_Model_Notebook.py` | **Lab 2** — Train Random Forest για fraud detection (50K records, stratified split, MLflow tracking, feature importance) |
| `Day4/Git_CICD_Example/` | **Lab 3** — Git/CI/CD walkthrough (clean_citizens.py + unit tests + GitHub Actions workflow + README) |
| `Day4/kep_requests.csv` | 10,000 synthetic ΚΕΠ requests (με intentional leakage features) |
| `Day4/taxpayer_features.csv` | 50,000 synthetic taxpayer features με `is_flagged` target — used by `ML_Model_Notebook.py` |

#### 🚀 Quick start — Day 4

**Lab 1: Feature Engineering** (Databricks Free Edition, ~25-30'):
```
https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4/Feature_Engineering_Notebook.py
```

**Lab 2: ML Model Training** (Databricks Free Edition, ~20-25'):
```
https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4/ML_Model_Notebook.py
```
Στο Databricks: **Workspace → Import → drag-drop → Run All**. Αυτο-κατεβάζει το CSV.

**Lab 3: Git/CI/CD Example** (στο τοπικό σας μηχάνημα, ~30'):
```bash
git clone https://github.com/sattip/dataengineer-labs.git
cd dataengineer-labs/Day4/Git_CICD_Example
# Διαβάστε το README.md για step-by-step walkthrough
```

---

## 🚀 Usage σε Databricks notebook

```python
# One-cell setup: download all Day 3 files into Databricks volume
import urllib.request

REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main"
VOL  = "/Volumes/workspace/aade/aade_data"

# (Make sure schema + volume exist first)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

day3_files = [
    "Day3/declarations.csv",
    "Day3/doy.csv",
    "Day3/employees.csv",
    "Day3/taxpayers.csv",
    "Day3/aade_declarations_data_contract.yaml",
]

for fpath in day3_files:
    fname = fpath.split("/")[-1]
    urllib.request.urlretrieve(f"{REPO}/{fpath}", f"{VOL}/{fname}")
    print(f"✅ {fname}")
```

---

## 🔗 Related

- **Curriculum repo (private)**: training materials, walkthroughs, presentations
- **Live class scripts**: instructor teleprompters για Zoom/in-person delivery
