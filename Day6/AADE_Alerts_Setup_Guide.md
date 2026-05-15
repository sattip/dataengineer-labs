# 🚨 Πώς να στήσεις Alerts στο Databricks

Οδηγός για 4 τύπους alerts που χρειάζεσαι σε production για το AADE pipeline.

## 🎯 Τύποι Alerts

| Τύπος | Πότε χρησιμεύει | Πού στήνεται |
|---|---|---|
| **1. DLT Pipeline Alerts** | Pipeline αποτύχει / drift | DLT pipeline settings |
| **2. Job Alerts** | Workflow Job αποτύχει | Workflow → Notifications |
| **3. SQL Alerts** | Metric υπερβαίνει threshold | SQL → Alerts |
| **4. Lakehouse Monitoring** | Data drift / schema change | Catalog → Monitor |

---

# 1️⃣ DLT Pipeline Alerts (failure / completion)

Αυτό είναι **το πιο σημαντικό** για το `aade_streaming_pipeline`.

## Setup

1. Sidebar → **Jobs & Pipelines** → click στο pipeline σου
2. **Settings** (γρανάζι ⚙️ πάνω-δεξιά)
3. Scroll στο **"Notifications"** section
4. **Add notification**

## Configuration

| Πεδίο | Τιμή |
|---|---|
| **Email** | `<your_email@example.com>` (ή team alias) |
| **Trigger** | Επίλεξε από: |
| | ☑️ On start |
| | ☑️ **On success** (συνιστώ ναι) |
| | ☑️ **On failure** (μη το ξεχάσεις!) |
| | ☑️ **On flow failure** (per-table failures) |

## Sample email content
```
Subject: [Databricks Pipeline] aade_streaming_pipeline FAILED
Body:
- Pipeline: aade_streaming_pipeline
- Run ID: 2026-04-30 07:45:12
- Status: FAILED
- Failed table: silver_tax_declarations_clean
- Error: UC_PERMISSION_DENIED
- View logs: https://dbc-...databricks.com/pipelines/...
```

---

# 2️⃣ Workflow Job Alerts

Αν τρέχεις το pipeline μέσω **Workflow Job** (όχι DLT):

## Setup

1. **Workflows → Jobs** → click στο job
2. **Edit job** → tab **"Notifications"**
3. **Add notification**

## Options

```
Trigger: On failure / On start / On success / On duration warning
Recipient:
   - Email: <email@domain.com>
   - System destination (Slack, PagerDuty, Microsoft Teams)
```

### 🔥 PRO TIP: System destinations (Slack/Teams webhook)

1. **Sidebar → Settings → Notifications → System destinations → Add**
2. Επίλεξε **Slack webhook**
3. Δώσε:
   - Display name: `mlops-alerts-channel`
   - Webhook URL: `https://hooks.slack.com/services/T.../...`
4. Save → χρησιμοποίησέ το σε **όλα** τα jobs ως recipient

Τότε τα alerts πάνε στο **Slack channel** της ομάδας, όχι σε email.

### Setup Slack webhook
1. Στο Slack: **Apps → Incoming Webhooks → Add to Slack**
2. Επίλεξε channel (π.χ. `#aade-mlops-alerts`)
3. Copy webhook URL → paste στο Databricks system destination

---

# 3️⃣ SQL Alerts (threshold-based)

Αυτό είναι **το πιο ευέλικτο** — alert όταν μετρική σπάει τιμή.

## Use cases για AADE pipeline

| Alert | SQL query | Threshold |
|---|---|---|
| **Drift detected** | `SELECT MAX(psi) FROM gold_data_quality_summary` | > 0.2 |
| **Pipeline lag** | `SELECT MAX(_silver_at) - current_timestamp() FROM silver_tax_declarations_clean` | > 1 hour |
| **High failure rate** | `SELECT COUNT(*) FROM gold_pipeline_health WHERE row_count = 0` | > 0 |
| **Volume anomaly** | `SELECT COUNT(*) FROM silver_tax_declarations_clean WHERE date(submitted_at) = current_date()` | < expected |
| **DQ failures spike** | `SELECT SUM(failed_count) FROM gold_data_quality_summary WHERE checked_at > current_timestamp() - INTERVAL 1 HOUR` | > 100 |

## Setup step-by-step

### Step 1: Δημιούργησε query
1. **Sidebar → SQL Editor** → **Create query**
2. Paste το SQL (π.χ. drift detection):
   ```sql
   SELECT
       MAX(psi)             AS max_psi,
       COUNT(*) FILTER (WHERE severity = 'retrain') AS critical_features
   FROM workspace.aade.mock_drift_history
   WHERE week_start = (SELECT MAX(week_start) FROM workspace.aade.mock_drift_history)
   ```
3. **Save** ως `alert_drift_status`

### Step 2: Δημιούργησε Alert
1. **Sidebar → SQL → Alerts** → **Create alert**
2. Configuration:
   - **Query**: `alert_drift_status` (από step 1)
   - **Trigger condition**: `max_psi > 0.2`
   - **Refresh schedule**: Every 1 hour
   - **Notification template**:
     ```
     🚨 DRIFT ALERT — AADE pipeline

     Max PSI detected: {{QUERY_RESULT.max_psi}}
     Critical features: {{QUERY_RESULT.critical_features}}

     Action required: investigate drift, plan retraining.
     ```
3. **Subscribers**: emails / Slack destinations

### Step 3: Test
- Click **"Send test notification"**
- Verify email/Slack arrival

## Πιο ισχυρά SQL Alert patterns

### Pattern 1: Day-over-day anomaly
```sql
WITH today AS (
    SELECT COUNT(*) AS cnt FROM silver_tax_declarations_clean
    WHERE date(submitted_at) = current_date()
), yesterday AS (
    SELECT COUNT(*) AS cnt FROM silver_tax_declarations_clean
    WHERE date(submitted_at) = current_date() - 1
)
SELECT
    today.cnt AS today_count,
    yesterday.cnt AS yesterday_count,
    ROUND((today.cnt - yesterday.cnt) * 100.0 / yesterday.cnt, 2) AS pct_change
FROM today, yesterday
```
**Trigger**: `pct_change < -50` → 50% drop = κάτι έσπασε

### Pattern 2: Stale data check
```sql
SELECT
    DATEDIFF(MINUTE, MAX(_ingested_at), current_timestamp()) AS minutes_since_last_ingest
FROM workspace.aade.bronze_taxis
```
**Trigger**: `minutes_since_last_ingest > 60` → pipeline κόλλησε

### Pattern 3: Schema drift (compare expected columns)
```sql
SELECT COUNT(*) AS missing_columns
FROM (VALUES ('afm'), ('tax_amount'), ('status')) AS expected(col)
WHERE col NOT IN (
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'aade'
      AND table_name = 'silver_tax_declarations_clean'
)
```
**Trigger**: `missing_columns > 0`

---

# 4️⃣ Lakehouse Monitoring (auto-generated)

Νέο feature του Databricks που **αυτόματα** monitorάρει tables. Δημιουργεί:
- Profile metrics (mean, stddev, min, max ανά column)
- Drift detection (week-over-week)
- Schema change tracking

## Setup

1. **Catalog Explorer** → πήγαινε στο `workspace.aade.silver_tax_declarations_clean`
2. Tab **"Quality"** → **Get started with monitoring**
3. Configuration:
   - **Profile type**: **TimeSeries** (recommended για streaming)
   - **Timestamp column**: `_silver_at`
   - **Granularity**: 1 day
   - **Output schema**: `workspace.aade_monitoring`
4. **Create monitor**

Μετά από 1-2 runs, το Databricks αυτόματα φτιάχνει:
- `silver_tax_declarations_clean_profile_metrics` table
- `silver_tax_declarations_clean_drift_metrics` table
- **Auto-generated dashboard** με όλες τις μετρικές

## Connect alerts
SQL alerts (Section 3) πάνω σε αυτά τα tables:
```sql
SELECT *
FROM workspace.aade_monitoring.silver_tax_declarations_clean_drift_metrics
WHERE drift_type = 'CONSECUTIVE'
  AND js_distance > 0.1
```

---

# 🎯 Recommended Alert Setup για AADE pipeline

| # | Alert | Type | Threshold | Recipients |
|---|---|---|---|---|
| 1 | Pipeline failure | DLT pipeline | On failure | ml-team email + Slack |
| 2 | Pipeline > 30 min runtime | Job duration | warning at 30m | platform-team |
| 3 | Drift PSI > 0.2 | SQL alert | hourly check | ml-team |
| 4 | DQ failure rate > 5% | SQL alert | hourly | data-team |
| 5 | Data freshness > 1h lag | SQL alert | every 15 min | platform-team |
| 6 | Volume drop > 50% DoD | SQL alert | daily 9am | business-team |
| 7 | Schema drift | Lakehouse Monitor | hourly | ml-team + data-team |

---

# 💡 Best Practices

## 1. **Severity tiers**
Μην στέλνεις τα πάντα στο ίδιο channel.
- **Critical** → PagerDuty / phone (pipeline failure, prod down)
- **Warning** → Slack channel (drift, slow runtime)
- **Info** → Email digest (daily summary)

## 2. **Avoid alert fatigue**
- Μην alertάρεις σε metrics που flap (true→false→true rapidly)
- Πρόσθεσε **dedup window**: same alert μέσα σε 1 ώρα → 1 notification
- Set **escalation**: αν κανείς δεν acknowledge σε 30 min → escalate σε manager

## 3. **Runbook ανά alert**
Κάθε alert template πρέπει να έχει:
- **Τι σημαίνει** (π.χ. "PSI > 0.2 σε feature income")
- **Τι να ελέγξεις** (recent ETL runs, source data changes)
- **Πώς να το διορθώσεις** (rollback model, retrain, increase batch frequency)
- **Link σε runbook page** (Confluence / wiki)

## 4. **Test alerts σε staging**
Πριν production, force-trigger alerts σε staging:
```sql
-- Simulate drift
INSERT INTO mock_drift_history VALUES (current_date(), 'income', 0.35, 0.5, 0.001, 'retrain', '1');
-- Verify Slack/email arrives
```

## 5. **Document escalation paths**
- L1: ML team responds (mlteam@aade.gr)
- L2 (αν >2 hours): MLOps lead (mlops-lead@aade.gr)
- L3 (critical incidents): VP Data (vp-data@aade.gr)

---

# 🔧 Troubleshooting

### "Δεν λαμβάνω email alerts"
- ✓ Spam/junk folder
- ✓ Workspace email settings (admin console)
- ✓ User email verified στο Databricks profile

### "Alert τρέχει αλλά δεν triggerάρει"
- ✓ SQL query επιστρέφει το expected schema
- ✓ Threshold condition matches τύπος δεδομένων (string vs number)
- ✓ Refresh schedule όντως ενεργό (όχι paused)

### "Slack webhook δεν δουλεύει"
- ✓ Webhook URL valid (test με `curl -X POST -d '{"text":"test"}' <url>`)
- ✓ System destination στα **Workspace Admin → Notifications**
- ✓ Permissions: το Databricks workspace μπορεί να κάνει outbound HTTPS

### "Πάρα πολλά alerts (alert fatigue)"
- ✓ Πρόσθεσε snooze για γνωστά noise
- ✓ Συγχώνευσε σχετικά alerts σε 1 incident
- ✓ Tighten thresholds (0.1 → 0.2)
- ✓ Add deduplication window (15 min)

---

# 📊 Quick Setup Cheatsheet

**Για το AADE pipeline σου, ακολούθησε αυτή τη σειρά:**

```
1. DLT Pipeline → Settings → Notifications
   ☑️ On failure → εμαιλ
   ☑️ On flow failure → email
   (~ 30 seconds setup)

2. SQL Editor → Create Query: alert_drift_status
   Paste το drift query από Pattern 1
   Save (~ 1 min)

3. SQL → Alerts → Create alert
   Source: alert_drift_status
   Condition: max_psi > 0.2
   Schedule: Every 1 hour
   Subscribers: your email + #aade-alerts Slack
   (~ 2 min)

4. Catalog → silver_tax_declarations_clean → Quality → Create monitor
   Type: TimeSeries
   Timestamp: _silver_at
   (~ 1 min, auto-generates dashboard σε background)

Total setup: ~ 5 minutes
Result: Coverage σε 4 διαφορετικές failure modes.
```

---

**Καλή παρακολούθηση! 🎯**
