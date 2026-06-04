# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 FINAL EXERCISE — Monitoring & Dashboard (η τελευταία ώρα)
# MAGIC
# MAGIC **Ημέρα 6 · Capstone · ⏱ 60'** — ΑΑΔΕ pipeline observability
# MAGIC
# MAGIC > Έχτισες contracts, ELT job, governance. **Τώρα τα παρακολουθείς.** Θα φτιάξεις το
# MAGIC > monitoring layer + ένα **AI/BI Dashboard** + ένα **SQL Alert** — ό,τι κοιτάει ο on-call
# MAGIC > κάθε πρωί για να ξέρει αν το nightly pipeline πήγε καλά.
# MAGIC
# MAGIC ## Δομή (60')
# MAGIC 1. 📚 Θεωρία — τι παρακολουθούμε & με τι (12')
# MAGIC 2. 🏭 Setup — παράγουμε 14 μέρες ιστορικό (έτοιμο, 3')
# MAGIC 3. ✍️ Monitoring views — **συμπλήρωσε τα `____`** (25')
# MAGIC 4. ✅ Verification (5')
# MAGIC 5. 📈 Φτιάξε το AI/BI Dashboard στο UI (12')
# MAGIC 6. 🔔 SQL Alert (3') + 🎁 Bonus: Lakehouse Monitoring
# MAGIC
# MAGIC > ✍️ Όπου βλέπεις `____`, αντικατέστησέ το. Αν το αφήσεις, το cell θα σκάσει με
# MAGIC > *"cannot resolve `____`"* — αυτό είναι το σινιάλο, όχι bug. Λύσεις στο τέλος (ΜΕΡΟΣ 7).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📚 ΜΕΡΟΣ 1 — ΘΕΩΡΙΑ
# MAGIC
# MAGIC ## 1.1 Τι παρακολουθούμε; (3 πυλώνες)
# MAGIC | Πυλώνας | Ερώτηση | Μετρικές |
# MAGIC |---|---|---|
# MAGIC | **Pipeline health** | «Έτρεξε; Πόσο γρήγορα;» | success rate, duration, last run |
# MAGIC | **Data quality / freshness** | «Είναι σωστά & πρόσφατα;» | invalid %, quarantined rows, freshness |
# MAGIC | **Business KPIs** | «Τι λένε τα δεδομένα;» | σύνολο φόρου ανά περιφέρεια/κατηγορία |
# MAGIC
# MAGIC ## 1.2 SLO / SLA — από «κοιτάω» σε «με ειδοποιεί»
# MAGIC Ορίζουμε **όρια** (Service Level Objectives) και ειδοποιούμαστε όταν σπάνε:
# MAGIC - 🟢 `invalid_pct ≤ 5%` · 🟢 `freshness < 24h` · 🟢 `success_rate_7d ≥ 95%`
# MAGIC - Αν σπάσει → **alert** (email/Slack), όχι «το είδα τυχαία στο dashboard».
# MAGIC
# MAGIC ## 1.3 Τα εργαλεία του Databricks
# MAGIC | Εργαλείο | Σε τι |
# MAGIC |---|---|
# MAGIC | **AI/BI Dashboard** (Lakeview) | visual dashboards πάνω σε SQL queries |
# MAGIC | **SQL Alerts** | τρέχει query σε schedule → ειδοποιεί όταν σπάει όριο |
# MAGIC | **Lakehouse Monitoring** | auto data-quality/drift profiling + dashboard ανά table |
# MAGIC | **System tables** | `system.access.audit`, `billing.usage`, `query.history` |
# MAGIC
# MAGIC > 💡 **Contract = πρόληψη** (μπλοκάρει κακό data). **Monitoring = ανίχνευση** (σε ειδοποιεί
# MAGIC > όταν κάτι ξεφεύγει στον χρόνο). Χρειάζεσαι **και τα δύο**.
# MAGIC
# MAGIC ## 1.4 Από πού έρχονται τα δεδομένα του monitoring;
# MAGIC Σε production: από το **`ops.elt_runs`** του Day-6 ELT job (run log) + τα Gold tables.
# MAGIC Εδώ τα **παράγουμε** (14 μέρες) ώστε το dashboard σου να έχει ιστορικό να δείξει.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🏭 ΜΕΡΟΣ 2 — Setup (έτοιμο · μην το αλλάξεις)
# MAGIC Παράγει 14 μέρες `pipeline_runs` (με 2 αποτυχίες & μεταβλητή ποιότητα) + `daily_tax` KPIs.

# COMMAND ----------

# DBTITLE 1,Setup — 14 μέρες monitoring history
from datetime import date, timedelta
import random

CATALOG, SCHEMA = "workspace", "aade_ops"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

random.seed(7)
today = date.today()
REGIONS = ["Αττική", "Κεντρικής Μακεδονίας", "Θεσσαλίας", "Κρήτης", "Δυτικής Ελλάδας"]
CATS    = ["VAT", "INCOME", "PROPERTY", "PAYROLL"]

runs, kpis = [], []
for i in range(14):
    d = (today - timedelta(days=13 - i)).isoformat()
    failed = i in (4, 10)                                   # 2 «κακές» μέρες
    total  = 200 + random.randint(-10, 25)
    quar   = random.randint(15, 45) if failed else random.randint(2, 9)
    valid  = total - quar
    inv    = round(100.0 * quar / total, 2)
    dur    = round(random.uniform(45, 85) + (50 if failed else 0), 1)
    status = "FAILED" if failed else "SUCCESS"
    runs.append((f"run_{d}", d, status, dur, total, valid, quar, inv,
                 f"{d}T03:00:00", f"{d}T03:0{random.randint(1,5)}:00"))
    for r in REGIONS:
        for c in CATS:
            kpis.append((d, r, c, random.randint(5, 40), round(random.uniform(5000, 80000), 2)))

run_cols = ["run_id","run_date","status","duration_sec","rows_ingested","rows_valid",
            "rows_quarantined","invalid_pct","started_at","finished_at"]
kpi_cols = ["run_date","region","tax_category","declarations","total_amount_eur"]

from pyspark.sql.functions import to_date, col
(spark.createDataFrame(runs, run_cols).withColumn("run_date", to_date(col("run_date")))
     .write.format("delta").mode("overwrite").saveAsTable("pipeline_runs"))
(spark.createDataFrame(kpis, kpi_cols).withColumn("run_date", to_date(col("run_date")))
     .write.format("delta").mode("overwrite").saveAsTable("daily_tax"))

print(f"✅ {CATALOG}.{SCHEMA}.pipeline_runs ({len(runs)} runs) + daily_tax ({len(kpis)} rows)")
display(spark.table("pipeline_runs").orderBy("run_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✍️ ΜΕΡΟΣ 3 — Φτιάξε τα Monitoring Views
# MAGIC
# MAGIC Κάθε view θα γίνει **dataset για ένα tile** του dashboard. Συμπλήρωσε τα `____`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 — `mon_pipeline_health`: ποσοστό επιτυχίας & διάρκεια ανά μέρα
# MAGIC **ΤΙ:** ανά `run_date`, status + αν ήταν επιτυχία (1/0) + διάρκεια.
# MAGIC **ΓΙΑΤΙ:** το βασικό «έτρεξε καλά;» tile.

# COMMAND ----------

# DBTITLE 1,3.1 mon_pipeline_health
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mon_pipeline_health AS
# MAGIC SELECT
# MAGIC   run_date, status, duration_sec, rows_ingested, rows_quarantined,
# MAGIC   -- ✍️ TODO 1: 1 αν status='SUCCESS' αλλιώς 0
# MAGIC   --   HINT: CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END
# MAGIC   ____ AS is_success
# MAGIC FROM pipeline_runs;
# MAGIC SELECT * FROM mon_pipeline_health ORDER BY run_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 — `mon_kpi_today`: τα νούμερα-κουτάκια (counters)
# MAGIC **ΤΙ:** συνολικά KPIs για τα counter tiles — success rate, μέση διάρκεια, σύνολο runs.
# MAGIC **ΓΙΑΤΙ:** τα 3 μεγάλα νούμερα στην κορυφή κάθε ops dashboard.

# COMMAND ----------

# DBTITLE 1,3.2 mon_kpi_summary
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mon_kpi_summary AS
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_runs,
# MAGIC   -- ✍️ TODO 2: success rate % = επιτυχημένα / σύνολο * 100 (1 δεκαδικό)
# MAGIC   --   HINT: ROUND(100.0 * SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 1)
# MAGIC   ____ AS success_rate_pct,
# MAGIC   ROUND(AVG(duration_sec), 1) AS avg_duration_sec,
# MAGIC   ROUND(AVG(invalid_pct), 2)  AS avg_invalid_pct
# MAGIC FROM pipeline_runs;
# MAGIC SELECT * FROM mon_kpi_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 — `mon_quality_trend`: ποιότητα στον χρόνο + 7-day rolling
# MAGIC **ΤΙ:** invalid_pct ανά μέρα + κινούμενος μέσος όρος 7 ημερών.
# MAGIC **ΓΙΑΤΙ:** μια μέρα μπορεί να είναι τυχαία· το **trend** δείχνει αν χειροτερεύει.

# COMMAND ----------

# DBTITLE 1,3.3 mon_quality_trend (window function)
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mon_quality_trend AS
# MAGIC SELECT
# MAGIC   run_date, invalid_pct, rows_quarantined,
# MAGIC   -- ✍️ TODO 3: 7-ήμερος κινούμενος μέσος του invalid_pct
# MAGIC   --   HINT: ROUND(AVG(invalid_pct) OVER (
# MAGIC   --             ORDER BY run_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)
# MAGIC   ____ AS invalid_pct_7d_avg
# MAGIC FROM pipeline_runs;
# MAGIC SELECT * FROM mon_quality_trend ORDER BY run_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 — `mon_slo`: σπάει κάποιο όριο; (η καρδιά του alert)
# MAGIC **ΤΙ:** ένα row με flags — πέρασε το invalid το 5%; υπάρχει failed run; πόσο «φρέσκο» είναι;
# MAGIC **ΓΙΑΤΙ:** αυτό το view τροφοδοτεί το **SQL Alert** (ΜΕΡΟΣ 6).

# COMMAND ----------

# DBTITLE 1,3.4 mon_slo
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mon_slo AS
# MAGIC WITH latest AS (
# MAGIC   SELECT MAX(run_date) AS last_run,
# MAGIC          MAX(CASE WHEN status='SUCCESS' THEN run_date END) AS last_success
# MAGIC   FROM pipeline_runs
# MAGIC )
# MAGIC SELECT
# MAGIC   l.last_run, l.last_success,
# MAGIC   -- ✍️ TODO 4: μέρες από το τελευταίο ΕΠΙΤΥΧΗΜΕΝΟ run (freshness)
# MAGIC   --   HINT: DATEDIFF(current_date(), l.last_success)
# MAGIC   ____ AS days_since_success,
# MAGIC   (SELECT ROUND(AVG(invalid_pct),2) FROM pipeline_runs
# MAGIC      WHERE run_date >= date_add(current_date(), -7)) AS invalid_pct_7d,
# MAGIC   -- ✍️ TODO 5: SLO BREACH flag — TRUE αν invalid_7d > 5 Ή υπάρχει failed τις τελευταίες 7 μέρες
# MAGIC   --   HINT: CASE WHEN (SELECT AVG(invalid_pct) FROM pipeline_runs
# MAGIC   --                      WHERE run_date >= date_add(current_date(),-7)) > 5
# MAGIC   --             OR EXISTS (SELECT 1 FROM pipeline_runs
# MAGIC   --                      WHERE status='FAILED' AND run_date >= date_add(current_date(),-7))
# MAGIC   --         THEN true ELSE false END
# MAGIC   ____ AS slo_breached
# MAGIC FROM latest l;
# MAGIC SELECT * FROM mon_slo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 — `mon_business`: φόρος ανά περιφέρεια (business tile)
# MAGIC **ΤΙ:** σύνολο φόρου & δηλώσεων ανά περιφέρεια (όλες οι μέρες).

# COMMAND ----------

# DBTITLE 1,3.5 mon_business
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW mon_business AS
# MAGIC SELECT
# MAGIC   region,
# MAGIC   -- ✍️ TODO 6: σύνολο φόρου & δηλώσεων ανά περιφέρεια
# MAGIC   --   HINT: ROUND(SUM(total_amount_eur),2) AS total_tax  ·  SUM(declarations) AS declarations
# MAGIC   ____ AS total_tax,
# MAGIC   ____ AS declarations
# MAGIC FROM daily_tax
# MAGIC GROUP BY region
# MAGIC ORDER BY total_tax DESC;
# MAGIC SELECT * FROM mon_business;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✅ ΜΕΡΟΣ 4 — Verification (τρέξε, δεν γράφεις)

# COMMAND ----------

# DBTITLE 1,Auto-verification
checks = []
def chk(name, cond):
    checks.append((name, bool(cond)))

try:
    h = spark.table("mon_pipeline_health")
    chk("mon_pipeline_health: is_success άθροισμα = 12 (2 fails)", h.selectExpr("sum(is_success)").first()[0] == 12)
except Exception: chk("mon_pipeline_health (TODO 1)", False)

try:
    s = spark.table("mon_kpi_summary").first()
    chk(f"mon_kpi_summary: success_rate ≈ 85.7% (είναι {s['success_rate_pct']})", abs(s["success_rate_pct"] - 85.7) < 0.5)
except Exception: chk("mon_kpi_summary (TODO 2)", False)

try:
    chk("mon_quality_trend: υπάρχει invalid_pct_7d_avg", "invalid_pct_7d_avg" in spark.table("mon_quality_trend").columns)
except Exception: chk("mon_quality_trend (TODO 3)", False)

try:
    slo = spark.table("mon_slo").first()
    chk(f"mon_slo: slo_breached = true (2 fails τις 14 μέρες → {slo['slo_breached']})", slo["slo_breached"] == True)
except Exception: chk("mon_slo (TODO 4/5)", False)

try:
    chk("mon_business: 5 περιφέρειες", spark.table("mon_business").count() == 5)
except Exception: chk("mon_business (TODO 6)", False)

print("="*60)
passed = sum(1 for _, ok in checks if ok)
for n, ok in checks: print(f"  {'✅' if ok else '❌'} {n}")
print("="*60); print(f"  {passed}/{len(checks)} passed")
print("🎉 Έτοιμα τα datasets — πάμε στο dashboard!" if passed == len(checks)
      else "⚠️  Δες ποιο TODO λείπει (λύσεις στο ΜΕΡΟΣ 7).")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📈 ΜΕΡΟΣ 5 — Φτιάξε το AI/BI Dashboard (στο UI)
# MAGIC
# MAGIC > Sidebar **Dashboards** → **Create dashboard**. Σε κάθε tile βάζεις ένα από τα views σου.
# MAGIC
# MAGIC ### Βήμα 1 — Datasets (tab **Data**)
# MAGIC Πρόσθεσε 5 datasets (κουμπί **Create from SQL**), ένα ανά view:
# MAGIC ```sql
# MAGIC SELECT * FROM workspace.aade_ops.mon_kpi_summary;     -- ds_kpi
# MAGIC SELECT * FROM workspace.aade_ops.mon_pipeline_health; -- ds_health
# MAGIC SELECT * FROM workspace.aade_ops.mon_quality_trend;   -- ds_quality
# MAGIC SELECT * FROM workspace.aade_ops.mon_business;         -- ds_business
# MAGIC SELECT * FROM workspace.aade_ops.mon_slo;              -- ds_slo
# MAGIC ```
# MAGIC
# MAGIC ### Βήμα 2 — Tiles (tab **Canvas** → **Add visualization**)
# MAGIC | Tile | Dataset | Τύπος | Ρύθμιση |
# MAGIC |---|---|---|---|
# MAGIC | **Success rate** | ds_kpi | **Counter** | Value: `success_rate_pct` (suffix `%`) |
# MAGIC | **Avg duration** | ds_kpi | **Counter** | Value: `avg_duration_sec` |
# MAGIC | **Avg invalid %** | ds_kpi | **Counter** | Value: `avg_invalid_pct` |
# MAGIC | **Quality trend** | ds_quality | **Line** | X: `run_date` · Y: `invalid_pct`, `invalid_pct_7d_avg` |
# MAGIC | **Run duration** | ds_health | **Bar/Line** | X: `run_date` · Y: `duration_sec` · color: `status` |
# MAGIC | **Tax ανά περιφέρεια** | ds_business | **Bar** | X: `region` · Y: `total_tax` |
# MAGIC | **Πρόσφατα runs** | ds_health | **Table** | στήλες: run_date, status, duration_sec, rows_quarantined |
# MAGIC
# MAGIC ### Βήμα 3 — Filter + Publish
# MAGIC - Πρόσθεσε **Date range filter** στο `run_date` (πάνω-πάνω).
# MAGIC - Πάνω δεξιά **Publish** → μοίρασέ το με τους stakeholders (read-only).
# MAGIC - (Optional) **Schedule** → daily refresh π.χ. 06:00 (μετά το nightly job).
# MAGIC
# MAGIC > 🎯 Στόχος: ένας on-call βλέπει σε 5'' — πέρασε το pipeline; χειροτερεύει η ποιότητα; τι λένε τα νούμερα;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🔔 ΜΕΡΟΣ 6 — SQL Alert (να σε ειδοποιεί, να μην κοιτάς)
# MAGIC
# MAGIC 1. **SQL Editor** → νέο query → **Save** ως `aade_slo_check`:
# MAGIC ```sql
# MAGIC SELECT CAST(slo_breached AS INT) AS breach, invalid_pct_7d, days_since_success
# MAGIC FROM workspace.aade_ops.mon_slo;
# MAGIC ```
# MAGIC 2. Sidebar **Alerts** → **Create alert** → διάλεξε το query `aade_slo_check`.
# MAGIC 3. **Condition**: `breach` **column** → **is greater than** → **Threshold** `0`.
# MAGIC 4. **Schedule**: κάθε μέρα 06:15 (μετά το job).
# MAGIC 5. **Notifications**: email/Slack. **Create**.
# MAGIC
# MAGIC > Τώρα δεν «ελπίζεις να το δεις» — όταν `slo_breached = true`, χτυπάει το alert. ✅

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎁 Bonus — Lakehouse Monitoring (auto data-quality dashboard)
# MAGIC Αντί να γράφεις εσύ μετρικές, το **Lakehouse Monitoring** προφιλάρει αυτόματα έναν πίνακα
# MAGIC (null %, distinct, drift) και φτιάχνει **δικό του dashboard**.
# MAGIC
# MAGIC Catalog Explorer → πίνακας (π.χ. `aade_ops.daily_tax`) → tab **Quality** → **Create monitor**
# MAGIC → τύπος **Snapshot** → **Create**. Σε λίγο εμφανίζεται auto-generated dashboard με metrics & drift.
# MAGIC *(Χρειάζεται serverless/SQL warehouse· σε κάποια editions ίσως δεν είναι διαθέσιμο.)*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎓 Συζήτηση
# MAGIC 1. Ποιο SLO θα έβαζες **πιο αυστηρό** για φορολογικά δεδομένα — freshness ή invalid %;
# MAGIC 2. Πού σταματά το **alert fatigue**; (πόσα alerts είναι «πολλά»;)
# MAGIC 3. Πώς συνδέεται αυτό το dashboard με το **contract** (Day 1) και το **ELT job** (Day 6);
# MAGIC
# MAGIC ---
# MAGIC # 🔑 ΜΕΡΟΣ 7 — Λύσεις (μόνο για τον trainer)
# MAGIC | TODO | Απάντηση |
# MAGIC |---|---|
# MAGIC | 1 | `CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END` |
# MAGIC | 2 | `ROUND(100.0 * SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 1)` |
# MAGIC | 3 | `ROUND(AVG(invalid_pct) OVER (ORDER BY run_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)` |
# MAGIC | 4 | `DATEDIFF(current_date(), l.last_success)` |
# MAGIC | 5 | `CASE WHEN (SELECT AVG(invalid_pct) FROM pipeline_runs WHERE run_date >= date_add(current_date(),-7)) > 5 OR EXISTS (SELECT 1 FROM pipeline_runs WHERE status='FAILED' AND run_date >= date_add(current_date(),-7)) THEN true ELSE false END` |
# MAGIC | 6 | `ROUND(SUM(total_amount_eur),2)` · `SUM(declarations)` |
# MAGIC
# MAGIC ## 🧹 Cleanup (optional)
# MAGIC ```sql
# MAGIC -- DROP SCHEMA IF EXISTS workspace.aade_ops CASCADE;
# MAGIC ```
