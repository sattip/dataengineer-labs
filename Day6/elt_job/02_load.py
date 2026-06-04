# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Load → 🥈 Silver (Task 3/5)
# MAGIC
# MAGIC **Medallion: L**oad — διάβασε το Bronze της μέρας, πέρασέ το από **data-contract gate**,
# MAGIC και φόρτωσε **μόνο τα καθαρά** στο Silver. Τα κακά → quarantine.
# MAGIC
# MAGIC > **Quality gate**: αν το % invalid ξεπερνά το threshold **και** `fail_on_breach=true` → **fail-fast**
# MAGIC > (ο Job γίνεται FAILED → notification). Αλλιώς quarantine & συνέχεια.
# MAGIC > **Idempotent** per `run_date` (delete-by-day + append) — bulletproof σε Free/Serverless.

# COMMAND ----------

# DBTITLE 1,Parameters
from datetime import date

dbutils.widgets.text("catalog",  "workspace", "Catalog")
dbutils.widgets.text("env",      "prod",      "Environment")
dbutils.widgets.text("run_date", "",          "Run date (κενό = σήμερα)")
dbutils.widgets.text("run_id",   "manual",    "Run id")
dbutils.widgets.text("fail_threshold_pct", "5.0", "Max % invalid πριν fail")
dbutils.widgets.dropdown("fail_on_breach", "true", ["true", "false"], "Fail-fast on breach;")

CATALOG  = dbutils.widgets.get("catalog").strip()
ENV      = dbutils.widgets.get("env").strip()
RUN_DATE = dbutils.widgets.get("run_date").strip() or date.today().isoformat()
RUN_ID   = dbutils.widgets.get("run_id").strip()
FAIL_THRESHOLD = float(dbutils.widgets.get("fail_threshold_pct"))
FAIL_ON_BREACH = dbutils.widgets.get("fail_on_breach") == "true"

BRONZE     = f"{CATALOG}.bronze.tax_declarations_raw"
SILVER     = f"{CATALOG}.silver.tax_declarations"
QUARANTINE = f"{CATALOG}.silver.tax_declarations_quarantine"
ELT_RUNS   = f"{CATALOG}.ops.elt_runs"
print(f"RunDate={RUN_DATE} · threshold={FAIL_THRESHOLD}% · fail_on_breach={FAIL_ON_BREACH}")

# COMMAND ----------

# DBTITLE 1,Helpers
from pyspark.sql import functions as F

def log_run(task, status, rows, message=""):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ops")
    (spark.createDataFrame([(RUN_ID, RUN_DATE, task, status, int(rows), message)],
        ["run_id","run_date","task","status","rows_affected","message"])
     .withColumn("run_date", F.to_date("run_date"))
     .withColumn("logged_at", F.current_timestamp())
     .write.format("delta").mode("append").saveAsTable(ELT_RUNS))

def set_task_value(key, value):
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass

def load_by_day(df, table):
    """Idempotent load: σβήσε ό,τι υπάρχει για αυτή τη run_date και ξαναγράψε."""
    if spark.catalog.tableExists(table):
        spark.sql(f"DELETE FROM {table} WHERE run_date = DATE'{RUN_DATE}'")
    df.write.format("delta").mode("append").saveAsTable(table)

# COMMAND ----------

# DBTITLE 1,Read Bronze για τη run_date
bronze_df = spark.table(BRONZE).where(f"run_date = DATE'{RUN_DATE}'")
total = bronze_df.count()
if total == 0:
    raise ValueError(f"❌ Bronze άδειο για {RUN_DATE} — έτρεξε το 01_extract πρώτα;")
print(f"📥 Bronze rows για {RUN_DATE}: {total}")

# COMMAND ----------

# DBTITLE 1,Data-contract gate (5 rules)
RULES = [
    ("DQ001 pk_not_null",       "declaration_id IS NOT NULL",                          "error"),
    ("DQ002 afm_9_digits",      "afm RLIKE '^[0-9]{9}$'",                              "error"),
    ("DQ003 amount_non_neg",    "amount_eur >= 0",                                     "error"),
    ("DQ004 valid_status",      "status IN ('APPROVED','PENDING','REJECTED')",         "error"),
    ("DQ005 amount_consistent", "abs(amount_eur - base_amount*rate_pct/100) <= 1.0",   "warning"),
]
for name, expr, sev in RULES:
    failed = bronze_df.filter(f"NOT ({expr})").count()
    icon = "✅" if failed == 0 else ("🚨" if sev == "error" else "⚠️")
    print(f"  {icon} {name:24s} [{sev:7s}] {failed:>3d} failures")

error_exprs = [e for _, e, sev in RULES if sev == "error"]
fail_cond   = " OR ".join(f"NOT ({e})" for e in error_exprs)
invalid_df  = bronze_df.filter(fail_cond)
valid_df    = bronze_df.filter(f"NOT ({fail_cond})")
invalid_pct = round(100.0 * invalid_df.count() / total, 2)
print(f"🛡️  invalid={invalid_pct}% (valid={valid_df.count()}, quarantine={invalid_df.count()})")

# COMMAND ----------

# DBTITLE 1,Quality gate — fail-fast αν χρειάζεται
if invalid_pct > FAIL_THRESHOLD and FAIL_ON_BREACH:
    msg = f"CONTRACT BREACH: {invalid_pct}% invalid > {FAIL_THRESHOLD}%"
    log_run("02_load", "FAILED", invalid_df.count(), msg)
    raise ValueError(f"❌ {msg} — batch rejected, Silver ΔΕΝ ενημερώθηκε")

# COMMAND ----------

# DBTITLE 1,Load → Silver + Quarantine (idempotent)
load_by_day(valid_df,   SILVER)
load_by_day(invalid_df, QUARANTINE)
s = spark.sql(f"SELECT COUNT(*) c FROM {SILVER} WHERE run_date = DATE'{RUN_DATE}'").collect()[0]["c"]
q = spark.sql(f"SELECT COUNT(*) c FROM {QUARANTINE} WHERE run_date = DATE'{RUN_DATE}'").collect()[0]["c"]
print(f"🥈 Silver ← {s} · 🚨 Quarantine ← {q}")

# COMMAND ----------

# DBTITLE 1,Log + exit
import json
log_run("02_load", "SUCCESS", s, f"quarantined={q}, invalid_pct={invalid_pct}")
set_task_value("silver_rows", int(s))
set_task_value("quarantine_rows", int(q))
set_task_value("invalid_pct", float(invalid_pct))
dbutils.notebook.exit(json.dumps({"task": "02_load", "status": "SUCCESS", "run_date": RUN_DATE,
                                  "silver_rows": int(s), "quarantine_rows": int(q),
                                  "invalid_pct": invalid_pct}))
