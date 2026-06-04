# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Extract → 🥉 Bronze (Task 2/5)
# MAGIC
# MAGIC **Medallion: E**xtract το ημερήσιο batch και land-άρισέ το **ως-έχει** στο Bronze.
# MAGIC
# MAGIC > Στο Bronze **δεν** καθαρίζουμε τίποτα — κρατάμε raw, immutable, ανά `run_date`.
# MAGIC > **Idempotent**: re-run για την ίδια `run_date` → σβήνει & ξαναγράφει ΜΟΝΟ εκείνη τη μέρα (delete-by-day + append).
# MAGIC > Καμία `replaceWhere`/`partitionBy` εξάρτηση → δουλεύει και σε Free Edition / Serverless.

# COMMAND ----------

# DBTITLE 1,Parameters
from datetime import date

dbutils.widgets.text("catalog",  "workspace", "Catalog")
dbutils.widgets.text("env",      "prod",      "Environment")
dbutils.widgets.text("run_date", "",          "Run date (κενό = σήμερα)")
dbutils.widgets.text("run_id",   "manual",    "Run id (Job περνά {{job.run_id}})")

CATALOG  = dbutils.widgets.get("catalog").strip()
ENV      = dbutils.widgets.get("env").strip()
RUN_DATE = dbutils.widgets.get("run_date").strip() or date.today().isoformat()
RUN_ID   = dbutils.widgets.get("run_id").strip()

BRONZE   = f"{CATALOG}.bronze.tax_declarations_raw"
ELT_RUNS = f"{CATALOG}.ops.elt_runs"
print(f"Catalog={CATALOG} · RunDate={RUN_DATE} · RunId={RUN_ID}")

# COMMAND ----------

# DBTITLE 1,Helpers (run-log + task value)
from pyspark.sql import functions as F

def log_run(task, status, rows, message=""):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ops")
    (spark.createDataFrame(
        [(RUN_ID, RUN_DATE, task, status, int(rows), message)],
        ["run_id", "run_date", "task", "status", "rows_affected", "message"])
     .withColumn("run_date", F.to_date("run_date"))
     .withColumn("logged_at", F.current_timestamp())
     .write.format("delta").mode("append").saveAsTable(ELT_RUNS))

def set_task_value(key, value):
    try:
        dbutils.jobs.taskValues.set(key=key, value=value)   # δουλεύει μόνο μέσα σε Job
    except Exception:
        pass

# COMMAND ----------

# DBTITLE 1,Extract — generate today's batch (deterministic ανά run_date)
import random

CATS     = ["VAT", "INCOME", "PROPERTY", "PAYROLL"]
STATUSES = ["APPROVED", "PENDING", "REJECTED"]
REGIONS  = ["Αττική", "Κεντρικής Μακεδονίας", "Θεσσαλίας", "Κρήτης", "Δυτικής Ελλάδας"]

seed = int(RUN_DATE.replace("-", ""))
rnd  = random.Random(seed)            # ίδιο seed → ίδια δεδομένα → idempotent
base_id = seed * 10000

rows = []
for i in range(1, 201):               # 200 καθαρά
    base = round(rnd.uniform(500, 50000), 2)
    rate = rnd.choice([6.0, 13.0, 24.0])
    rows.append((base_id + i, RUN_DATE, f"{rnd.randint(100000000, 999999999)}",
                 rnd.choice(CATS), base, rate, round(base * rate / 100, 2),
                 rnd.choice(STATUSES), rnd.choice(REGIONS)))
# 3 «κακά» → θα τα πιάσει το contract στο 02_load
rows += [
    (base_id + 9001, RUN_DATE, "12345678",  "VAT",      1000.0, 24.0, 240.0, "APPROVED", "Αττική"),  # bad ΑΦΜ (8 ψηφία)
    (base_id + 9002, RUN_DATE, "999999999", "INCOME",   5000.0, 15.0, -100.0,"PENDING",  "Αττική"),  # αρνητικό ποσό
    (base_id + 9003, RUN_DATE, "888888888", "PROPERTY", 2000.0, 10.0, 200.0, "UNKNOWN",  "Κρήτης"),  # invalid status
]
cols = ["declaration_id","run_date","afm","tax_category","base_amount",
        "rate_pct","amount_eur","status","region"]
df = (spark.createDataFrame(rows, cols)
            .withColumn("run_date", F.to_date("run_date")))
print(f"📦 Extracted {df.count()} records για {RUN_DATE}")

# COMMAND ----------

# DBTITLE 1,Land → Bronze (idempotent: delete-by-day + append)
if spark.catalog.tableExists(BRONZE):
    spark.sql(f"DELETE FROM {BRONZE} WHERE run_date = DATE'{RUN_DATE}'")   # καθάρισε προηγούμενο run ίδιας μέρας
df.write.format("delta").mode("append").saveAsTable(BRONZE)

n = spark.sql(f"SELECT COUNT(*) c FROM {BRONZE} WHERE run_date = DATE'{RUN_DATE}'").collect()[0]["c"]
print(f"🥉 Bronze ← {n} rows ({BRONZE}, run_date={RUN_DATE})")

# COMMAND ----------

# DBTITLE 1,Log + exit
import json
log_run("01_extract", "SUCCESS", n)
set_task_value("bronze_rows", int(n))
dbutils.notebook.exit(json.dumps({"task": "01_extract", "status": "SUCCESS",
                                  "run_date": RUN_DATE, "bronze_rows": int(n)}))
