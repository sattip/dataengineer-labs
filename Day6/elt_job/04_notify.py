# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Notify / Summary (Task 5/5 · run_if = ALL_DONE)
# MAGIC
# MAGIC Τελευταίο task — τρέχει **ακόμα κι αν κάποιο upstream απέτυχε** (`run_if: ALL_DONE`),
# MAGIC ώστε να ειδοποιήσει και για **failures**.
# MAGIC
# MAGIC > Διαβάζει τα **task values** (orchestration!) από τα προηγούμενα tasks + το run-log,
# MAGIC > χτίζει summary, και (optionally) στέλνει Slack/Teams webhook.

# COMMAND ----------

# DBTITLE 1,Parameters
from datetime import date

dbutils.widgets.text("catalog",  "workspace", "Catalog")
dbutils.widgets.text("env",      "prod",      "Environment")
dbutils.widgets.text("run_date", "",          "Run date (κενό = σήμερα)")
dbutils.widgets.text("run_id",   "manual",    "Run id")
dbutils.widgets.text("webhook_secret_scope", "", "(Optional) secret scope για webhook")
dbutils.widgets.text("webhook_secret_key",   "", "(Optional) secret key για webhook")

CATALOG  = dbutils.widgets.get("catalog").strip()
ENV      = dbutils.widgets.get("env").strip()
RUN_DATE = dbutils.widgets.get("run_date").strip() or date.today().isoformat()
RUN_ID   = dbutils.widgets.get("run_id").strip()
SCOPE    = dbutils.widgets.get("webhook_secret_scope").strip()
KEY      = dbutils.widgets.get("webhook_secret_key").strip()

ELT_RUNS = f"{CATALOG}.ops.elt_runs"

# COMMAND ----------

# DBTITLE 1,Read task values από τα upstream tasks (orchestration)
def get_val(task_key, key, default=0):
    try:
        return dbutils.jobs.taskValues.get(taskKey=task_key, key=key, default=default, debugValue=default)
    except Exception:
        return default

bronze_rows     = get_val("01_extract",   "bronze_rows")
silver_rows     = get_val("02_load",      "silver_rows")
quarantine_rows = get_val("02_load",      "quarantine_rows")
invalid_pct     = get_val("02_load",      "invalid_pct", 0.0)
gold_rows       = get_val("03_transform", "gold_rows")

summary = {
    "run_date": RUN_DATE, "env": ENV, "run_id": RUN_ID,
    "bronze_rows": bronze_rows, "silver_rows": silver_rows,
    "quarantine_rows": quarantine_rows, "invalid_pct": invalid_pct, "gold_rows": gold_rows,
}
import json
print("📦 ELT SUMMARY:\n" + json.dumps(summary, ensure_ascii=False, indent=2))

# COMMAND ----------

# DBTITLE 1,Run-log αυτού του run (από όλα τα tasks)
from pyspark.sql import functions as F
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ops")
if spark.catalog.tableExists(ELT_RUNS):
    display(spark.table(ELT_RUNS).where(F.col("run_id") == RUN_ID).orderBy("logged_at"))
    failed = spark.table(ELT_RUNS).where((F.col("run_id") == RUN_ID) & (F.col("status") == "FAILED")).count()
else:
    failed = 0

overall = "FAILED" if failed else "SUCCESS"
print(f"🧮 Overall run status: {overall}")

# COMMAND ----------

# DBTITLE 1,(Optional) Στείλε webhook — Slack/Teams
def send_webhook(text):
    if not (SCOPE and KEY):
        print("ℹ️  Δεν δόθηκε webhook secret → skip (interactive/demo).")
        return
    import urllib.request
    url = dbutils.secrets.get(scope=SCOPE, key=KEY)
    body = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        print(f"✅ Webhook sent ({r.status})")

emoji = "✅" if overall == "SUCCESS" else "🚨"
send_webhook(f"{emoji} ELT {ENV} {RUN_DATE} · {overall} · "
             f"silver={silver_rows} quarantine={quarantine_rows} ({invalid_pct}%) gold={gold_rows}")

# COMMAND ----------

# DBTITLE 1,Log + exit
def log_run(task, status, rows, message=""):
    (spark.createDataFrame([(RUN_ID, RUN_DATE, task, status, int(rows), message)],
        ["run_id","run_date","task","status","rows_affected","message"])
     .withColumn("run_date", F.to_date("run_date"))
     .withColumn("logged_at", F.current_timestamp())
     .write.format("delta").mode("append").saveAsTable(ELT_RUNS))

log_run("04_notify", overall, gold_rows, f"summary={json.dumps(summary, ensure_ascii=False)}")
dbutils.notebook.exit(json.dumps({"task": "04_notify", "status": overall, **summary}))
