# 📘 Pipeline Health Dashboard — Setup Guide

**Δοκιμασμένο και deployed** στο workspace `dbc-d68abe5a-a04b.cloud.databricks.com`
**Live dashboard URL**: `https://dbc-d68abe5a-a04b.cloud.databricks.com/dashboardsv3/01f1504520191b6989a5d7c3cee5b2c9`

---

## 🎯 Τι θα έχεις στο τέλος

Ένα **AI/BI Dashboard (Lakeview)** που δείχνει:
- ✅ **Cross-job health** όλων των jobs & pipelines σε ένα view
- 📈 **Trends** (30 days success rate, daily failures)
- 🐢 **Top slowest jobs** ranked
- 🔴 **Active failures** drill-down
- 🥇 **Per-job health table** με 🟢/🟡/🔴 status
- 🌊 **DLT pipelines** ξεχωριστά health table
- 🔄 Auto-refresh + scheduling capability

---

## 📋 Prerequisites

| Requirement | Πώς ελέγχω |
|---|---|
| Account admin (one-time) | User Settings → Permissions show "Account admin" |
| SQL Warehouse running | Workflows → SQL Warehouses, **Serverless Starter** state RUNNING |
| System tables enabled | `SHOW SCHEMAS IN system` returns `lakeflow`, `billing`, `compute` |
| Pre-existing jobs/runs | At least 1-2 successful job runs ώστε να φαίνονται data |

⚠️ Σε **brand-new workspaces**, μερικές queries μπορεί να επιστρέψουν NULL/empty αν δεν έχει τρέξει κανένα job ακόμα.

---

## 🚀 Setup Options

### Option A — Vu UI Import (συνιστώμενο)

1. **Download** το `.lvdash.json`:
   ```bash
   curl -o ~/Downloads/Pipeline_Health.lvdash.json \
     https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/Observability/Pipeline_Health.lvdash.json
   ```

2. Στο Databricks UI:
   - Sidebar → **Workspace**
   - δεξί κλικ σε φάκελο → **Import**
   - **Select file** → επίλεξε το `.lvdash.json`
   - **Import**

3. Click στο νέο dashboard → **Edit** → **Run** για test
4. **Publish** → επίλεξε SQL warehouse → **Publish**

### Option B — REST API (αυτό που χρησιμοποίησα εγώ)

```bash
TOKEN="<your_databricks_token>"
HOST="https://<your-workspace>.cloud.databricks.com"

# Step 1: Get a SQL warehouse
WH_ID=$(curl -sS -H "Authorization: Bearer $TOKEN" "$HOST/api/2.0/sql/warehouses" | \
  python3 -c "import json,sys; print(json.load(sys.stdin)['warehouses'][0]['id'])")

# Step 2: Read dashboard JSON και build create payload
python3 << PYEOF
import json
with open('Pipeline_Health.lvdash.json') as f:
    content = json.load(f)
payload = {
    "display_name": "Pipeline & Job Health",
    "warehouse_id": "$WH_ID",
    "serialized_dashboard": json.dumps(content),
    "parent_path": "/Users/<your_email@example.com>"
}
with open('/tmp/create.json', 'w') as f:
    json.dump(payload, f)
PYEOF

# Step 3: Create
DASH_ID=$(curl -sS -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/create.json \
  "$HOST/api/2.0/lakeview/dashboards" | python3 -c "import json,sys; print(json.load(sys.stdin)['dashboard_id'])")

echo "Dashboard ID: $DASH_ID"

# Step 4: Publish (απαραίτητο ώστε να τρέξει χωρίς explicit warehouse selection)
curl -sS -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"warehouse_id\": \"$WH_ID\"}" \
  "$HOST/api/2.0/lakeview/dashboards/$DASH_ID/published"

echo "URL: $HOST/dashboardsv3/$DASH_ID"
```

### Option C — Databricks CLI

```bash
# (CLI command για Lakeview είναι σε public preview — δες docs)
databricks lakeview create --json-file Pipeline_Health.lvdash.json
```

---

## 🔍 Τι θα δεις όταν ανοίξεις το dashboard

### Top Row — 4 KPI Counters
```
┌────────────┬────────────┬────────────┬────────────┐
│ Active Jobs│ Runs (7d)  │ Success %  │ Failed Runs│
│     3      │     14     │    78.6%   │     3      │
└────────────┴────────────┴────────────┴────────────┘
```

### Middle — Daily Trend Chart
Bar chart (success/fail counts) + Line chart (success %) σε δύο άξονες.

### Bottom Left — Top 10 Slowest Jobs
Ranking table με run count, avg duration, max duration.

### Bottom Right — Recent Failures (24h)
Drill-down table: job name, started_at, error code, run_id.

### Master — Per-Job Health
Row ανά job με status 🟢 HEALTHY / 🟡 WATCH / 🔴 DEGRADED.

### Footer — DLT Pipelines Health
Ξεχωριστή table για DLT updates.

---

## 🔄 Schedule Auto-Refresh

1. **Open dashboard** → click ⚙️ Settings (top-right)
2. **Schedule** tab → **Add schedule**
3. Set:
   - Cron: `0 0/15 * * * ?` (κάθε 15 λεπτά)
   - Timezone: Europe/Athens
4. **Subscribers** → ποιοι θα παίρνουν email snapshots
5. **Save**

Τώρα κάθε 15 min, το dashboard refresh-άρει αυτόματα.

---

## 🚨 Email Alerts σε threshold breach

Δημιούργησε **SQL Alert** πάνω στα ίδια queries:

1. **SQL Editor** → νέο query:
   ```sql
   SELECT
     ROUND(100.0*SUM(CASE WHEN result_state='SUCCEEDED' THEN 1 ELSE 0 END)/COUNT(*), 2) AS success_pct
   FROM system.lakeflow.job_run_timeline
   WHERE period_start_time >= current_timestamp() - INTERVAL 1 HOUR
   ```

2. **Save** ως `alert_success_rate_1h`

3. Sidebar → **SQL → Alerts** → **Create alert**
   - Query: `alert_success_rate_1h`
   - Trigger condition: `success_pct < 90`
   - Refresh: every 5 min
   - Subscribers: `<your_email>` + Slack webhook

→ Email αν success rate πέσει κάτω από 90% σε 1 ώρα.

---

## 🛠️ Drill-Down Workflow

Τυπικό flow όταν κάτι σπάει:

```
1. 📊 Open dashboard → δες "Failed Runs (7d) = 3"
2. 📈 Click Daily Trend chart → "Spike on May 13"
3. 🔍 Scroll στο Recent Failures table:
   - aade_daily_pipeline | 10:42 | DRIVER_ERROR | run_id=xxx
4. 📋 Per-Job Health row με 🔴 status:
   - Click → οδηγείται στο Workflows → Jobs UI
5. 🔎 Στο Job UI click run_id:
   - Cell output με stderr error
   - Cluster metrics
   - Retry history
6. ✅ Fix + restart
```

Το dashboard δεν αντικαθιστά το Jobs UI — **το συμπληρώνει** δίνοντάς σου το **«where to start»** view.

---

## 🐛 Troubleshooting

### Dashboard widgets είναι κενά
- **Cause 1**: SQL warehouse stopped → Settings → επίλεξε running warehouse
- **Cause 2**: Δεν υπάρχουν runs στο workspace (brand new)
- **Cause 3**: System tables δεν είναι enabled — δες παρακάτω

### Error: `TABLE_OR_VIEW_NOT_FOUND` σε `system.lakeflow.jobs`
```sql
-- Verify
SHOW SCHEMAS IN system;
-- Πρέπει να βλέπεις: lakeflow, billing, compute, access, ...

-- Αν δεν βλέπεις lakeflow, account admin τρέχει:
GRANT SELECT ON SCHEMA system.lakeflow TO `account users`;
GRANT USE SCHEMA ON SCHEMA system.lakeflow TO `account users`;
```

### Error: `Column not found: creator`
- Παλιά versions του schema έχουν `creator`, νέες `creator_user_name`
- Δες την query στο `dashboard_queries.sql` v ή adjust manually

### Error: `system.workflow.jobs not found`
- Σε νέες versions ενοποιήθηκε σε `system.lakeflow.*`
- Replace όλα τα `system.workflow.` με `system.lakeflow.` (συνέβη σε αυτό το repo στο commit `ef3dfed`)

### Dashboard δεν εμφανίζεται στο Dashboards list
- Refresh page
- Δες στο Workspace folder όπου έγινε save (default: `/Users/<your_email>/`)

---

## 📊 Customizations

### Προσθήκη φίλτρου ανά Job Name
1. Edit dashboard
2. **Add filter** (top-right)
3. Type: **Dropdown**
4. Source: `ds_health_table.job_name`
5. Apply σε όλα τα widgets

### Προσθήκη filter ανά Date Range
1. **Add filter** → **Date range**
2. Default: Last 7 days
3. Update queries να χρησιμοποιήσουν `{{ date_filter }}`

### Cost panel (DBU consumption)
Πρόσθεσε νέο widget από Q10 στο `dashboard_queries.sql` — δείχνει DBU usage ανά resource type.

---

## 🔗 Επόμενα βήματα

| Step | Description |
|---|---|
| **Set schedule** | Auto-refresh κάθε 15 min + email snapshots |
| **Add alerts** | SQL Alerts για success_rate < 90%, latency > SLA |
| **Slack integration** | System destinations → Slack webhook |
| **Drill-down dashboards** | Φτιάξε δεύτερο dashboard για task-level details |
| **Cost dashboard** | Ξεχωριστό dashboard για DBU consumption tracking |
| **Permission grants** | Share read-only σε managers/operations team |

---

## 📚 Related Resources

- **`README.md`** — overview + decision matrix built-in vs custom
- **`dashboard_queries.sql`** — όλες οι queries standalone
- **`Pipeline_Health.lvdash.json`** — importable dashboard
- **Day 4 Observability**: `Day4/MLOps_Observability_Dashboard.py` — application-level metrics (drift, AUC)
- **Databricks Docs**: [Lakeview Dashboards API](https://docs.databricks.com/api/workspace/lakeview)
- **System Tables Reference**: [system.lakeflow schema](https://docs.databricks.com/admin/system-tables/jobs.html)

---

> *«Built-in UI = θες να ξέρεις γιατί έσπασε σήμερα.
> Custom dashboard = θες να ξέρεις πώς πάει η πλατφόρμα συνολικά.
> Χρειάζεσαι και τα δύο.»*
