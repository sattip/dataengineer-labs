# 📋 AADE Daily Pipeline — Databricks Workflow Job

Πλήρες παράδειγμα **multi-task Databricks Job** με DAG dependencies, schedule,
retries, notifications και health monitoring. Εμφανίζεται στο **Jobs UI**
(όχι στο Pipelines UI όπως το DLT).

## 🆚 Job vs DLT Pipeline — πότε τι;

| Aspect | Workflow Job (αυτό) | DLT Pipeline |
|---|---|---|
| **UI location** | Workflows → Jobs | Workflows → Pipelines |
| **Style** | Imperative — γράφεις τα tasks ως notebooks | Declarative — `@dlt.table` decorators |
| **DAG creation** | Manual (`depends_on`) | Auto-derived από `dlt.read()` |
| **Task types** | Notebook, Python, JAR, SQL, dbt, ML pipelines | Tables/views μόνο |
| **Data quality** | Manual (your code) | `@dlt.expect_or_drop` built-in |
| **Lineage** | Auto (μέσω UC) | Auto + visual στο pipeline page |
| **Streaming** | Πιο manual (Auto Loader στο notebook σου) | Native (`spark.readStream` σε `@dlt.table`) |
| **Cost** | Job cluster ή Serverless | Pipeline cluster (Premium SKU συνήθως) |
| **Use case** | Generic ETL, ML jobs, mixed workloads | Pure streaming/batch ETL pipelines |

**Vibes**: το Job είναι σαν cron + Airflow combo, το DLT είναι σαν framework
που σε «αναγκάζει» σε medallion + DQ best practices.

## 📦 Files

| File | Type | Description |
|---|---|---|
| `01_bronze_ingestion.py` | Notebook | Task 1: Auto Loader → 4 Bronze tables |
| `02_silver_quality.py` | Notebook | Task 2: DQ + MERGE → 4 Silver tables (depends_on task 1) |
| `03_gold_aggregations.py` | Notebook | Task 3: GroupBy + JOIN → 3 Gold tables (depends_on task 2) |
| `job_definition.json` | Job spec | Importable Databricks Job JSON |
| `README.md` | Guide | Αυτό το αρχείο |

## 🗺️ DAG Architecture

```
   ┌──────────────────┐
   │  task_1_bronze   │  Auto Loader → 4 Bronze tables
   │  (12 min)        │  - bronze_taxis, bronze_mydata, bronze_kep, bronze_efka
   └────────┬─────────┘
            │ depends_on
            ▼
   ┌──────────────────┐
   │  task_2_silver   │  DQ filter + MERGE → 4 Silver tables
   │  (8 min)         │  - silver_tax_declarations_clean
   └────────┬─────────┘  - silver_invoices_clean
            │ depends_on  - silver_kep_events_clean
            ▼              - silver_efka_contributions_clean
   ┌──────────────────┐
   │   task_3_gold    │  Aggregations → 3 Gold tables
   │   (5 min)        │  - gold_citizen_360
   └──────────────────┘  - gold_daily_kpis
                         - gold_pipeline_health (audit trail)
```

## 🚀 Setup Steps

### 1️⃣ Import τα 3 task notebooks στο workspace

Sidebar → **Workspace** → δεξί κλικ σε φάκελο → **Import** (από URL × 3):

```
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/AADE_Workflow_Job/01_bronze_ingestion.py
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/AADE_Workflow_Job/02_silver_quality.py
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/AADE_Workflow_Job/03_gold_aggregations.py
```

Βάλτε όλα σε φάκελο `/Users/<your_email>/AADE_Workflow_Job/`.

### 2️⃣ Verify ότι υπάρχουν source CSV files

Το Bronze task χρειάζεται CSV files στο `/Volumes/workspace/aade/aade_data/streaming/raw/{taxis,mydata,kep,efka}/`.
Αν δεν υπάρχουν, τρέξε πρώτα τον **DLT Generator**:
```
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/AADE_DLT_Generator.py
```

### 3️⃣ Create the Job

**Option A — UI (πιο εύκολο)**:

1. Sidebar → **Workflows → Jobs** → **Create Job**
2. Δώστε όνομα: `aade_daily_pipeline`
3. Add **Task 1**:
   - Task name: `task_1_bronze`
   - Type: Notebook
   - Path: `/Users/<your_email>/AADE_Workflow_Job/01_bronze_ingestion`
   - Compute: **Serverless** (αν διαθέσιμο) ή Job cluster
4. Add **Task 2**:
   - Task name: `task_2_silver`
   - Notebook: `/Users/<your_email>/AADE_Workflow_Job/02_silver_quality`
   - **Depends on**: `task_1_bronze`
5. Add **Task 3**:
   - Task name: `task_3_gold`
   - Notebook: `/Users/<your_email>/AADE_Workflow_Job/03_gold_aggregations`
   - **Depends on**: `task_2_silver`
6. **Schedule**: Daily at 06:00 Athens timezone (`0 0 6 * * ?`)
7. **Notifications**: email on failure
8. **Save**

**Option B — JSON Import (πιο γρήγορο)**:

1. Κατέβασε το `job_definition.json`
2. Αλλάξε το email στα 4 σημεία αν χρειάζεται
3. Αλλάξε τα `notebook_path` αν τα έβαλες αλλού
4. Στο UI: **Workflows → Jobs → Create Job → ⋯ → Edit as JSON**
5. Επικόλληση του JSON content
6. Save

**Option C — Databricks CLI**:
```bash
databricks jobs create --json-file job_definition.json
```

### 4️⃣ Run-now για test

Click **Run now** στο job page. Παρακολούθησε τα 3 tasks να γίνονται:
- 🔄 Queued → Running → ✅ Succeeded

## 🎨 Τι θα δεις στο Jobs UI

Μετά το first run στο Jobs page θα βλέπεις:

### Workflows → Jobs tab → click job name
- **Runs tab**: Πίνακας με history (start time, duration, status)
- **Tasks tab**: DAG view με 3 task boxes συνδεδεμένα με βέλη
- **Job runs**: Click σε run → δες κάθε task ξεχωριστά
- **Per-task output**: Click σε task → δες notebook output + dbutils.notebook.exit() value

### Per-task page
- Cell output
- Cluster metrics
- Task duration breakdown
- Stderr/stdout logs
- Retry history (αν υπήρξαν retries)

## ⚙️ Job Features που Demonstrate-άρει αυτό το παράδειγμα

| Feature | Πού φαίνεται |
|---|---|
| **Multi-task DAG** | 3 tasks με `depends_on` chain |
| **Schedule** | Cron `0 0 6 * * ?` (daily 06:00 Athens) |
| **Email notifications** | On failure |
| **Retries** | task_1, task_2 με `max_retries: 1` |
| **Timeout** | Per-task 1200s + job-level 3600s |
| **Health rules** | Alert αν διάρκεια > 30 min |
| **Tags** | Cost allocation (`team`, `cost_center`) |
| **Queue** | Αν τρέχει ήδη run, queue το νέο |
| **dbutils.notebook.exit()** | Per-task output στο Job UI |
| **Idempotency** | Επανεκτέλεση δεν σπάει — MERGE pattern |

## 🧪 Common Issues & Fixes

### "Notebook not found"
- Path λάθος. Δες ακριβές path στο Workspace UI και αντίγραψέ το.

### "Compute not available"
- Serverless ίσως δεν είναι enabled. Switch σε **Job cluster** με ένα node.

### Task 1 fails — "Source path empty"
- Δεν υπάρχουν CSV files στο volume. Τρέξε πρώτα τον DLT Generator.

### Task 2 fails — "Table workspace.aade.bronze_taxis not found"
- Task 1 πιθανώς απέτυχε. Δες logs Task 1 πρώτα.

### "MERGE schema mismatch"
- Bronze schema άλλαξε. Either drop Silver tables (will rebuild) ή ενημέρωσε το `whenMatchedUpdateAll()`.

### Job runs αλλά δεν εμφανίζεται στο Jobs UI
- Refresh page. Αν δεν εμφανίζεται μετά από 5 sec, check **Workflows → Job runs** ανά task.

## 🔄 Variations / Extensions

Αν θες να το extend-εις:

| Extension | Πώς |
|---|---|
| **Streaming continuous** | Trigger=`continuous` αντί `availableNow` σε Task 1 |
| **Daily snapshots** | Add `dt` partition column στα Gold tables |
| **DBT integration** | Replace Task 2/3 με DBT task type |
| **Parametrization** | Add Job parameters (e.g., `target_date`) |
| **Multi-environment** | Use Job parameters για catalog (dev vs prod) |
| **Quality gates** | Task 4 που fails αν DQ failure_pct > 5% |

## 🔗 Related

- **DLT version** (declarative same pipeline): [`AADE_DLT_Pipeline.py`](../AADE_DLT_Pipeline.py)
- **Notebook version** (no Job, runs interactively): [`AADE_Streaming_Pipeline.py`](../AADE_Streaming_Pipeline.py)
- **Setup guide για DLT**: [`AADE_DLT_Setup_Guide.md`](../AADE_DLT_Setup_Guide.md)
- **Alerts setup**: [`AADE_Alerts_Setup_Guide.md`](../AADE_Alerts_Setup_Guide.md)

---

> 💡 **Δίδαγμα**: το ίδιο pipeline μπορεί να εκφραστεί ως notebook,
> Workflow Job, ή DLT Pipeline. Διάλεξε βάση team familiarity και
> requirements (DQ, lineage, streaming).
