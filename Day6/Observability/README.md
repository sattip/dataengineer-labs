# 🏥 Pipeline & Job Health Observability

**Question**: Πώς βλέπω health/SLA σε όλα τα jobs και pipelines, με drill-down σε per-run details;

**Short answer**: Built-in UIs έχουν περιορισμούς. **Φτιάχνεις custom Lakeview Dashboard πάνω σε system tables**.

---

## 🆚 Built-in vs Custom Dashboard

| Need | Built-in UI | Custom Dashboard |
|---|---|---|
| «Ποιο job σπάει αυτή τη στιγμή;» | ✅ Workflows → Jobs UI | ✅ Real-time counter |
| «Πόσα fails αυτή την εβδομάδα;» | ❌ Πρέπει να click ένα-ένα | ✅ Aggregated panel |
| «Trend duration τελευταίους 30 μέρες» | ❌ Δεν υπάρχει | ✅ Line chart |
| «SLA miss rate ανά job» | ❌ | ✅ Per-job KPI |
| «Top 5 slowest tasks» | ❌ | ✅ Ranked table |
| «Cost ανά job/pipeline (DBUs)» | ⚠️ Per-job μόνο | ✅ Cross-job total |
| «Drill-down σε specific run** | ✅ Click run → details | ✅ Click → opens Job UI |
| «Cross-team dashboard για managers» | ❌ | ✅ Permissions-managed |

### Bottom line
- **Built-in** για debugging: "γιατί έσπασε σήμερα η εκτέλεση;"
- **Custom** για operations: "πώς πάει η πλατφόρμα συνολικά;"

---

## 📊 System Tables που χρειάζεσαι

Το Databricks expose-άρει automatically maintained audit/metric tables στο `system.*` schema. Free Edition + Premium το υποστηρίζουν:

| Table | Τι περιέχει | Use case |
|---|---|---|
| **`system.workflow.jobs`** | Job metadata (όνομα, owner, tags) | List όλων των jobs |
| **`system.workflow.job_run_timeline`** | Lifecycle events ανά run | Duration, retries, status |
| **`system.workflow.job_task_run_timeline`** | Per-task events μέσα σε run | Drill-down σε ποιο task έσπασε |
| **`system.lakeflow.pipelines`** | DLT pipelines metadata | List DLT pipelines |
| **`system.lakeflow.pipeline_runs`** | DLT pipeline run history | Health των DLT runs |
| **`system.access.audit`** | Κάθε API call / UI action | Compliance audit |
| **`system.billing.usage`** | DBU consumption | Cost analysis |
| **`system.compute.clusters`** | Cluster lifecycle | Compute spend per cluster |
| **`system.compute.warehouse_events`** | SQL warehouse activity | Query performance |

### Enable system tables (αν δεν είναι ήδη)
```sql
-- Account admin command (μία φορά μόνο)
GRANT USAGE ON SCHEMA system.workflow TO `account users`;
GRANT SELECT ON SCHEMA system.workflow TO `account users`;
-- Same για lakeflow, access, billing, compute
```

---

## 📋 Files σε αυτό το folder

| File | Description |
|---|---|
| `README.md` | Αυτό το αρχείο (overview) |
| `dashboard_queries.sql` | 10 ready-to-use SQL queries για κάθε panel |
| `Pipeline_Health.lvdash.json` | Importable Lakeview Dashboard με 8 widgets |
| `Setup_Guide.md` | Step-by-step instructions για να το deploy-άρεις |

## 🚀 Quick Start

```bash
# 1. Download το dashboard
curl -o ~/Downloads/Pipeline_Health.lvdash.json \
  https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/Observability/Pipeline_Health.lvdash.json

# 2. Στο Databricks: Workspace → Import → Select File
# 3. Open dashboard → Refresh
# 4. Δες το cross-job health πίνακα!
```

## 🎯 Τι θα δεις στο dashboard

```
┌─────────────────────────────────────────────────────────┐
│  Pipeline Health — Last 7 Days        🟢 HEALTHY        │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  📊 Jobs Overview                                       │
│  ┌──────────┬──────────┬──────────┬──────────┐         │
│  │ 12 Jobs  │ 1,247    │ 96.3%    │ 2 Failed │         │
│  │ Active   │ Runs (7d)│ Success  │ Today    │         │
│  └──────────┴──────────┴──────────┴──────────┘         │
│                                                          │
│  📈 Success Rate Trend (30d)        ────────╲──╱──     │
│                                                          │
│  ⏱️  Top Slowest Jobs                 Failed Today      │
│  ┌─────────────────────┐              ┌──────────────┐  │
│  │ aade_pipeline 45min │              │ etl_x  09:42 │  │
│  │ etl_x         32min │              │ ml_y   10:15 │  │
│  └─────────────────────┘              └──────────────┘  │
│                                                          │
│  📍 Per-Job Drill-down                                   │
│  Click row → opens Job UI με run details                 │
└─────────────────────────────────────────────────────────┘
```

## 🔗 Connection με υπάρχοντα material

- **`Day4/MLOps_Observability_Dashboard.py`** — application-level observability (drift, AUC, latency)
- **Αυτός ο φάκελος** — platform-level observability (jobs, pipelines, costs)
- Μαζί συνθέτουν **πλήρες observability stack**: app metrics + platform metrics
