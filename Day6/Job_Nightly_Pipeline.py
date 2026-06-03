# Databricks notebook source
# MAGIC %md
# MAGIC # 🌙 Job Example — Nightly ΑΑΔΕ Pipeline (Bronze → Silver → Gold)
# MAGIC
# MAGIC **Ημέρα 6 · Orchestration / Databricks Jobs · ⏱ demo 15' + scheduling**
# MAGIC
# MAGIC > Αυτό το notebook είναι φτιαγμένο για να **τρέχει μόνο του ως scheduled Job** — π.χ. **κάθε βράδυ στις 03:00**.
# MAGIC > Δεν περιμένει άνθρωπο: παίρνει parameters, τρέχει το pipeline, **γράφει run-log**, και **σκάει (fail)**
# MAGIC > αν χαλάσει κάτι, ώστε ο Job orchestrator να στείλει alert.
# MAGIC
# MAGIC ## Τι δείχνει (orchestration concepts)
# MAGIC | Concept | Πού | Γιατί έχει σημασία σε scheduled job |
# MAGIC |---|---|---|
# MAGIC | **Parameters** (`dbutils.widgets`) | Cell «Parameters» | Το ίδιο notebook τρέχει για dev/prod & για οποιαδήποτε `run_date` |
# MAGIC | **Dynamic values** (`{{job.start_time...}}`) | Scheduling | Ο Job περνά αυτόματα την ημερομηνία εκτέλεσης |
# MAGIC | **Idempotency** (`replaceWhere`, `MERGE`) | Stages 1,3,4 | Re-run για την ίδια μέρα → ίδιο αποτέλεσμα, όχι διπλά |
# MAGIC | **Data-contract gate** | Stage 2 | Κακό batch → quarantine ή **fail-fast** (alert) |
# MAGIC | **Run log / audit** | Stage 5 | Ιστορικό κάθε εκτέλεσης (compliance + debugging) |
# MAGIC | **Structured exit** (`dbutils.notebook.exit`) | Exit | Το Job UI & τα downstream tasks διαβάζουν το JSON summary |
# MAGIC | **Fail on error** | Orchestration cell | Ο Job μαρκάρει `FAILED` → notifications |
# MAGIC
# MAGIC > 🔗 Συνεχίζει το `Day1/.../Lab_Data_Contracts.py`: εκεί χτίσαμε το contract «με το χέρι»·
# MAGIC > εδώ το βάζουμε μέσα σε ένα **production scheduled pipeline**.
# MAGIC > Πιο προχωρημένο (DLT + 5 tasks) → `Day6/databricks_lab/`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🅿️ Parameters
# MAGIC Τα `widgets` γίνονται **Job parameters**. Στο scheduled run, ο Job τα γεμίζει αυτόματα
# MAGIC (δες "How to schedule" στο τέλος). Interactive run → χρησιμοποιεί τα defaults.

# COMMAND ----------

# DBTITLE 1,Parameters (widgets)
from datetime import date

dbutils.widgets.text("catalog",          "gt_lab",  "1 · Catalog")
dbutils.widgets.text("env",              "prod",    "2 · Environment")
dbutils.widgets.text("run_date",         "",        "3 · Run date (YYYY-MM-DD, κενό = σήμερα)")
dbutils.widgets.text("fail_threshold_pct","5.0",    "4 · Max % invalid πριν fail")
dbutils.widgets.dropdown("fail_on_breach","true",   ["true", "false"], "5 · Fail-fast on breach;")

CATALOG          = dbutils.widgets.get("catalog").strip()
ENV              = dbutils.widgets.get("env").strip()
RUN_DATE         = dbutils.widgets.get("run_date").strip() or date.today().isoformat()
FAIL_THRESHOLD   = float(dbutils.widgets.get("fail_threshold_pct"))
FAIL_ON_BREACH   = dbutils.widgets.get("fail_on_breach") == "true"

print(f"Catalog={CATALOG} · Env={ENV} · RunDate={RUN_DATE}")
print(f"Fail threshold={FAIL_THRESHOLD}% · Fail-on-breach={FAIL_ON_BREACH}")

# COMMAND ----------

# DBTITLE 1,Run context + helpers
import json, time
from pyspark.sql import functions as F

# --- Ποιος Job/Run είμαι; (δουλεύει και interactive: γυρνά "interactive"/"manual") ---
def _job_context():
    try:
        ctx = json.loads(
            dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
        )
        tags = ctx.get("tags", {})
        return tags.get("jobId", "interactive"), tags.get("runId", tags.get("multitaskParentRunId", "manual"))
    except Exception:
        return "interactive", "manual"

JOB_ID, RUN_ID = _job_context()
OPS_TABLE = f"{CATALOG}.ops.pipeline_runs"

def write_run_log(status, metrics, duration_sec, error_msg=None):
    """Γράφει ΜΙΑ γραμμή ιστορικού για αυτή την εκτέλεση (append = κρατάμε όλη την ιστορία)."""
    row = [(
        str(RUN_ID), str(JOB_ID), RUN_DATE, ENV, status,
        int(metrics.get("rows_ingested", 0)), int(metrics.get("rows_valid", 0)),
        int(metrics.get("rows_quarantined", 0)), float(metrics.get("invalid_pct", 0.0)),
        float(duration_sec), error_msg,
    )]
    cols = ["run_id","job_id","run_date","env","status","rows_ingested","rows_valid",
            "rows_quarantined","invalid_pct","duration_sec","error"]
    (spark.createDataFrame(row, cols)
          .withColumn("logged_at", F.current_timestamp())
          .write.format("delta").mode("append").saveAsTable(OPS_TABLE))

print(f"JobId={JOB_ID} · RunId={RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 Stage 0 — Environment (idempotent)
# MAGIC Σε ένα scheduled job, το environment μπορεί να μην υπάρχει ακόμα στο 1ο run → το φτιάχνουμε με `IF NOT EXISTS`.

# COMMAND ----------

# DBTITLE 1,Stage 0 — UC objects
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for s in ["bronze", "silver", "gold", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
print(f"✅ {CATALOG}.{{bronze,silver,gold,ops}} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏭 Stages 1–4 — οι functions του pipeline
# MAGIC Τις ορίζουμε ως **functions** (production style) και τις τρέχουμε ελεγχόμενα στο orchestration cell.
# MAGIC
# MAGIC - **Stage 1 — Ingest → Bronze**: «έφτασε το βραδινό batch». Idempotent με `replaceWhere` ανά `run_date`.
# MAGIC - **Stage 2 — Contract gate**: τρέχει quality rules· κακό batch → quarantine ή **fail-fast**.
# MAGIC - **Stage 3 — Silver**: `MERGE` (upsert) τα valid· quarantine τα invalid.
# MAGIC - **Stage 4 — Gold**: ημερήσια aggregates, overwrite του partition της μέρας.

# COMMAND ----------

# DBTITLE 1,Stage 1 — Ingest → Bronze
import random

BRONZE = f"{CATALOG}.bronze.tax_declarations_raw"
CATS    = ["VAT", "INCOME", "PROPERTY", "PAYROLL"]
STATUSES= ["APPROVED", "PENDING", "REJECTED"]
REGIONS = ["Αττική", "Κεντρικής Μακεδονίας", "Θεσσαλίας", "Κρήτης", "Δυτικής Ελλάδας"]

def _upsert_partition(df, table, run_date):
    """Idempotent write: αντικαθιστά ΜΟΝΟ το partition αυτής της run_date."""
    if spark.catalog.tableExists(table):
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"run_date = '{run_date}'")
           .option("mergeSchema", "true").saveAsTable(table))
    else:
        df.write.format("delta").partitionBy("run_date").saveAsTable(table)

def stage1_ingest(run_date):
    """Προσομοιώνει το ημερήσιο batch. Seed = run_date → ντετερμινιστικό (re-run = ίδια δεδομένα)."""
    seed = int(run_date.replace("-", ""))
    rnd  = random.Random(seed)
    base_id = seed * 10000
    rows = []
    for i in range(1, 201):                      # 200 καθαρά records
        base = round(rnd.uniform(500, 50000), 2)
        rate = rnd.choice([6.0, 13.0, 24.0])
        rows.append((base_id + i, run_date, f"{rnd.randint(100000000, 999999999)}",
                     rnd.choice(CATS), base, rate, round(base * rate / 100, 2),
                     rnd.choice(STATUSES), rnd.choice(REGIONS)))
    # 3 «κακά» records — δείχνουν το quarantine (κάτω από το threshold → ο job περνά)
    rows += [
        (base_id + 9001, run_date, "12345678",  "VAT",    1000.0, 24.0, 240.0, "APPROVED", "Αττική"),   # bad ΑΦΜ
        (base_id + 9002, run_date, "999999999", "INCOME", 5000.0, 15.0, -100.0,"PENDING",  "Αττική"),   # negative
        (base_id + 9003, run_date, "888888888", "PROPERTY",2000.0,10.0, 200.0, "UNKNOWN",  "Κρήτης"),   # bad status
    ]
    cols = ["declaration_id","run_date","afm","tax_category","base_amount","rate_pct",
            "amount_eur","status","region"]
    df = spark.createDataFrame(rows, cols)
    _upsert_partition(df, BRONZE, run_date)
    print(f"🥉 Bronze ← {df.count()} rows (run_date={run_date})")
    return df

# COMMAND ----------

# DBTITLE 1,Stage 2 — Data-contract gate
# Συμπαγές contract (inline). Πλήρες YAML-driven → δες Lab_Data_Contracts.py
RULES = [
    ("DQ001 pk_not_null",       "declaration_id IS NOT NULL",                              "error"),
    ("DQ002 afm_9_digits",      "afm RLIKE '^[0-9]{9}$'",                                  "error"),
    ("DQ003 amount_non_neg",    "amount_eur >= 0",                                         "error"),
    ("DQ004 valid_status",      "status IN ('APPROVED','PENDING','REJECTED')",             "error"),
    ("DQ005 amount_consistent", "abs(amount_eur - base_amount*rate_pct/100) <= 1.0",       "warning"),
]

def stage2_contract_gate(df):
    total = df.count()
    for name, expr, sev in RULES:
        failed = df.filter(f"NOT ({expr})").count()
        icon = "✅" if failed == 0 else ("🚨" if sev == "error" else "⚠️")
        print(f"  {icon} {name:24s} [{sev:7s}] {failed:>3d} failures")
    error_exprs = [expr for _, expr, sev in RULES if sev == "error"]
    fail_cond   = " OR ".join(f"NOT ({e})" for e in error_exprs)
    invalid_df  = df.filter(fail_cond)
    valid_df    = df.filter(f"NOT ({fail_cond})")
    invalid_pct = round(100.0 * invalid_df.count() / total, 2) if total else 0.0
    print(f"🛡️  invalid={invalid_pct}%  (threshold={FAIL_THRESHOLD}%, fail_on_breach={FAIL_ON_BREACH})")

    if invalid_pct > FAIL_THRESHOLD and FAIL_ON_BREACH:
        # Fail-fast: ο Job θα γίνει FAILED → on_failure notification
        raise ValueError(f"❌ CONTRACT BREACH: {invalid_pct}% invalid > {FAIL_THRESHOLD}% — batch rejected")
    return {"valid_df": valid_df, "invalid_df": invalid_df, "invalid_pct": invalid_pct}

# COMMAND ----------

# DBTITLE 1,Stage 3 — Silver (MERGE upsert) + Quarantine
SILVER     = f"{CATALOG}.silver.tax_declarations"
QUARANTINE = f"{CATALOG}.silver.tax_declarations_quarantine"

def stage3_silver(valid_df, invalid_df, run_date):
    if not spark.catalog.tableExists(SILVER):
        valid_df.limit(0).write.format("delta").saveAsTable(SILVER)   # create empty με σωστό schema
    valid_df.createOrReplaceTempView("v_updates")
    spark.sql(f"""
        MERGE INTO {SILVER} t USING v_updates s
          ON  t.declaration_id = s.declaration_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    _upsert_partition(invalid_df, QUARANTINE, run_date)   # quarantine ανά μέρα (idempotent)
    print(f"🥈 Silver MERGE ✓  ·  🚨 Quarantine ← {invalid_df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Stage 4 — Gold (daily aggregates)
GOLD = f"{CATALOG}.gold.tax_daily_summary"

def stage4_gold(valid_df, run_date):
    agg = (valid_df.groupBy("run_date", "tax_category", "region")
                   .agg(F.count("*").alias("declarations"),
                        F.round(F.sum("amount_eur"), 2).alias("total_amount_eur"),
                        F.round(F.avg("amount_eur"), 2).alias("avg_amount_eur")))
    _upsert_partition(agg, GOLD, run_date)
    print(f"🥇 Gold ← {agg.count()} aggregate rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎛️ Orchestration — τρέξε τα stages, γράψε run-log, βγες με JSON
# MAGIC Αυτό είναι το «κυρίως» cell που τρέχει ο Job. Όλα μέσα σε `try/except/finally`:
# MAGIC - **success** → run-log `SUCCESS` + `dbutils.notebook.exit(summary)`
# MAGIC - **failure** → run-log `FAILED` + **re-raise** ώστε ο Job να γίνει FAILED (→ alert)

# COMMAND ----------

# DBTITLE 1,Orchestration cell
t0 = time.time()
metrics = {"rows_ingested": 0, "rows_valid": 0, "rows_quarantined": 0, "invalid_pct": 0.0}
status, error_msg = "SUCCESS", None

try:
    bronze_df = stage1_ingest(RUN_DATE)
    metrics["rows_ingested"] = bronze_df.count()

    gate = stage2_contract_gate(bronze_df)
    metrics["rows_valid"]       = gate["valid_df"].count()
    metrics["rows_quarantined"] = gate["invalid_df"].count()
    metrics["invalid_pct"]      = gate["invalid_pct"]

    stage3_silver(gate["valid_df"], gate["invalid_df"], RUN_DATE)
    stage4_gold(gate["valid_df"], RUN_DATE)

except Exception as e:
    status, error_msg = "FAILED", str(e)

finally:
    duration = round(time.time() - t0, 1)
    write_run_log(status, metrics, duration, error_msg)
    print(f"🧾 Run-log → {OPS_TABLE}  ({status}, {duration}s)")

summary = {"status": status, "run_date": RUN_DATE, "env": ENV,
           "job_id": JOB_ID, "run_id": RUN_ID, "duration_sec": duration, **metrics,
           "error": error_msg}
print("\n📦 SUMMARY:\n" + json.dumps(summary, ensure_ascii=False, indent=2))

if status == "FAILED":
    raise RuntimeError(f"Pipeline FAILED: {error_msg}")   # ← κάνει τον Job FAILED (μετά το run-log)

dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False))

# COMMAND ----------

# MAGIC %md
# MAGIC # ⏰ How to schedule it at **03:00** (Databricks Workflows)
# MAGIC
# MAGIC ## A) Μέσω UI (γρήγορο)
# MAGIC 1. **Workflows** (αριστερό sidebar) → **Create Job**.
# MAGIC 2. **Task**: Name=`nightly_aade`, Type=**Notebook**, Path=αυτό το notebook, Cluster=Serverless ή small job cluster.
# MAGIC 3. **Parameters** (key → value):
# MAGIC    `catalog=gt_lab` · `env=prod` · `fail_threshold_pct=5.0` · `fail_on_breach=true`
# MAGIC    και **δυναμικά**: `run_date = {{job.start_time.iso_date}}` ← ο Job βάζει την ημερομηνία εκτέλεσης.
# MAGIC 4. **Schedule & Triggers** → **Scheduled** → **Advanced / Cron**:
# MAGIC    - Cron: `0 0 3 * * ?`  (κάθε μέρα 03:00:00)
# MAGIC    - Timezone: **Europe/Athens**
# MAGIC 5. **Notifications** → on **failure** (& on **duration warning**) → email/Slack/Teams.
# MAGIC 6. **Retries**: π.χ. **2**, interval 5'. **Timeout**: π.χ. 3600s. **Max concurrent runs**: 1.
# MAGIC 7. **Save** → **Run now** για δοκιμή.
# MAGIC
# MAGIC > 🧠 Cron `0 0 3 * * ?` = sec=0, min=0, **hour=3**, κάθε μέρα. Για 03:30 → `0 30 3 * * ?`.
# MAGIC
# MAGIC ## B) Ως JSON (Jobs API / `databricks` CLI / Asset Bundles)
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "nightly_aade_pipeline",
# MAGIC   "tags": {"env": "prod", "team": "data-platform"},
# MAGIC   "max_concurrent_runs": 1,
# MAGIC   "schedule": {
# MAGIC     "quartz_cron_expression": "0 0 3 * * ?",
# MAGIC     "timezone_id": "Europe/Athens",
# MAGIC     "pause_status": "UNPAUSED"
# MAGIC   },
# MAGIC   "email_notifications": { "on_failure": ["data-oncall@gov.gr"] },
# MAGIC   "parameters": [
# MAGIC     {"name": "catalog",           "default": "gt_lab"},
# MAGIC     {"name": "env",               "default": "prod"},
# MAGIC     {"name": "run_date",          "default": "{{job.start_time.iso_date}}"},
# MAGIC     {"name": "fail_threshold_pct","default": "5.0"},
# MAGIC     {"name": "fail_on_breach",    "default": "true"}
# MAGIC   ],
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "nightly_aade",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Workspace/Repos/.../Day6/Job_Nightly_Pipeline",
# MAGIC         "base_parameters": {
# MAGIC           "catalog":   "{{job.parameters.catalog}}",
# MAGIC           "env":       "{{job.parameters.env}}",
# MAGIC           "run_date":  "{{job.parameters.run_date}}",
# MAGIC           "fail_threshold_pct": "{{job.parameters.fail_threshold_pct}}",
# MAGIC           "fail_on_breach":     "{{job.parameters.fail_on_breach}}"
# MAGIC         }
# MAGIC       },
# MAGIC       "timeout_seconds": 3600,
# MAGIC       "max_retries": 2,
# MAGIC       "min_retry_interval_millis": 300000
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC Deploy: `databricks jobs create --json @job_nightly_pipeline.json`
# MAGIC (ή βάλ' το σε ένα **Databricks Asset Bundle** `databricks.yml` και `databricks bundle deploy`).
# MAGIC
# MAGIC ## 🧩 Θες πραγματικό multi-task orchestration;
# MAGIC Σπάσε τα stages σε ξεχωριστά tasks με `depends_on` (ingest → contract → silver → gold → notify),
# MAGIC ώστε καθένα να έχει δικό του retry/timeout και να ξαναξεκινάς από το σημείο που έσκασε.
# MAGIC Πλήρες παράδειγμα (DLT + 5 tasks): `Day6/databricks_lab/job_definition.json`.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔍 Verification (interactive — δεν τρέχουν στο job μετά το exit)

# COMMAND ----------

# DBTITLE 1,Τι παρήγαγε το τελευταίο run
# MAGIC %sql
# MAGIC SELECT * FROM gt_lab.ops.pipeline_runs ORDER BY logged_at DESC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Medallion row counts
# MAGIC %sql
# MAGIC SELECT 'silver'      AS layer, COUNT(*) AS rows FROM gt_lab.silver.tax_declarations
# MAGIC UNION ALL SELECT 'quarantine', COUNT(*) FROM gt_lab.silver.tax_declarations_quarantine
# MAGIC UNION ALL SELECT 'gold',       COUNT(*) FROM gt_lab.gold.tax_daily_summary;

# COMMAND ----------

# DBTITLE 1,Gold — daily summary
# MAGIC %sql
# MAGIC SELECT * FROM gt_lab.gold.tax_daily_summary ORDER BY run_date DESC, total_amount_eur DESC LIMIT 20;
