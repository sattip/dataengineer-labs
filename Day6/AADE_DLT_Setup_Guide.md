# 🏗️ Πώς να φτιάξεις το AADE DLT Pipeline στο Databricks

## 📦 Αρχεία

| Αρχείο | Ρόλος |
|---|---|
| `AADE_DLT_Generator.py` | Παράγει mock CSV files στο volume — τρέχει στο **notebook UI** |
| `AADE_DLT_Pipeline.py` | DLT source code με `@dlt.table` — τρέχει από **DLT Pipeline** |

## 🔢 Βήμα-βήμα οδηγίες

### 1️⃣ Import τα 2 notebooks στο workspace

Στο sidebar **Workspace** → δεξί κλικ σε φάκελο → **Import**:
- Από URL:
  ```
  https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/AADE_DLT_Generator.py
  ```
- Από URL:
  ```
  https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/AADE_DLT_Pipeline.py
  ```

### 2️⃣ Run το Generator notebook (μία φορά αρχικά)

Άνοιξε το `AADE_DLT_Generator` notebook → **Run all**.

Αυτό:
- Δημιουργεί schema `workspace.aade` και volume `aade_data`
- Γράφει 4 CSV files (taxis, mydata, kep, efka) στο
  `/Volumes/workspace/aade/aade_data/streaming/raw/<source>/batch_<timestamp>.csv`

Θα δεις output:
```
✓ taxis    → 50 rows
✓ mydata   → 80 rows
✓ kep      → 120 rows
✓ efka     → 60 rows
```

### 3️⃣ Δημιούργησε το DLT Pipeline

Sidebar → **Workflows** → tab **Delta Live Tables** → **Create pipeline**

**General settings:**
| Πεδίο | Τιμή |
|---|---|
| Pipeline name | `aade_streaming_pipeline` |
| Product edition | **Core** (αρκεί για το Free Edition) |
| Pipeline mode | **Triggered** (όχι Continuous) — απαλλάσσει από always-on cluster |

**Source code:**
- Add source code → επίλεξε το notebook **`AADE_DLT_Pipeline`** που import-αρες παραπάνω

**Destination:**
| Πεδίο | Τιμή |
|---|---|
| Storage option | **Unity Catalog** |
| Catalog | `workspace` |
| Target schema | `aade` |

**Compute:**
- Cluster mode: **Serverless** (αν διαθέσιμο) ή **Enhanced autoscaling** με 1-2 workers

**Advanced (προαιρετικά):**
- Channel: **Current** (stable)
- Photon acceleration: **On** (αν διαθέσιμο)

→ **Create**

### 4️⃣ Run το pipeline

Πάτα το μεγάλο μπλε **Start** κουμπί.

Στην οθόνη του pipeline θα δεις:
- **DAG view** (12 nodes): 4 bronze → 4 silver → 3 gold + 1 health table με τις γραμμές dependencies
- **Per-table progress**: Pending → Running → Completed
- **Row counts** σε κάθε node καθώς ολοκληρώνεται

Το pipeline ολοκληρώνεται σε ~3-5 λεπτά.

### 5️⃣ Δες τα αποτελέσματα

**Στο DLT pipeline page:**
- Tab **"Data Quality"** → metrics ανά rule (passed, dropped, warned)
- Tab **"Lineage"** → οπτικός γράφος dependencies
- Click σε οποιοδήποτε node → Sample data + schema

**Στο Catalog Explorer:**
- `workspace.aade.bronze_taxis`, `bronze_mydata`, `bronze_kep`, `bronze_efka`
- `workspace.aade.silver_tax_declarations_clean`, `silver_invoices_clean`, `silver_kep_events_clean`, `silver_efka_contributions_clean`
- `workspace.aade.gold_citizen_360`, `gold_daily_kpis`, `gold_pipeline_health`

**Στο SQL Editor (queries):**
```sql
SELECT * FROM workspace.aade.gold_citizen_360 ORDER BY total_tax_paid DESC LIMIT 10;
SELECT * FROM workspace.aade.gold_daily_kpis ORDER BY day DESC;
SELECT * FROM workspace.aade.gold_pipeline_health;
```

### 6️⃣ Test incremental processing

Για να δεις πώς το DLT πιάνει νέα data:

1. Re-run το `AADE_DLT_Generator` notebook (γράφει νέο batch με νέο timestamp)
2. Στο pipeline page → **Start**
3. Δες ότι το `bronze_*` έχει επιπλέον rows (incremental), αλλά το silver/gold μένει σταθερό αν δεν έχει νέα valid data

## 🎯 Τι θα δεις στα Pipelines (που ήταν empty πριν)

**Workflows → Delta Live Tables**:
- ✅ `aade_streaming_pipeline` με status (Healthy / Failed / Running)
- ✅ Click → πλήρες DAG view
- ✅ Run history με μετρικές

## 📊 Tab-by-tab guide στο DLT page

| Tab | Τι δείχνει |
|---|---|
| **Pipeline graph** | Visual DAG των 12 tables με dependencies |
| **Updates** | Run history (επιτυχίες, αποτυχίες, διάρκεια) |
| **Data Quality** | Per-expectation metrics (π.χ. "valid_afm_format: 47 dropped") |
| **Settings** | Επεξεργασία source code path, target schema, compute |
| **Lineage** | Source → Bronze → Silver → Gold flow |
| **Configuration** | Spark conf, secrets |

## 🔧 Troubleshooting

### "Volume not found"
→ Τρέξε πρώτα το `AADE_DLT_Generator` για να δημιουργήσεις το volume.

### "Bronze table empty"
→ Τα CSV files δεν είναι στο σωστό path. Έλεγξε:
```sql
LIST '/Volumes/workspace/aade/aade_data/streaming/raw/taxis'
```

### "Free Edition: pipeline can't start"
→ Χρειάζεσαι Serverless DLT compute. Στις settings → cluster → επίλεξε Serverless.

### "Permission denied on schema"
→ Τρέξε:
```sql
GRANT CREATE TABLE ON SCHEMA workspace.aade TO `<your_email>`;
```

## 🔄 Συνεχής λειτουργία (production)

Για να τρέχει αυτόματα:
1. Pipeline settings → **Schedule** → Add trigger
2. Cron: `0 0 * * *` (καθημερινά μεσάνυχτα)
3. Email notifications σε failure

Για **24/7 streaming** (όχι triggered):
- Pipeline mode → **Continuous**
- Compute → ζωντανός cluster always-on (πιο ακριβό)
