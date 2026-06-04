# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Transform → 🥇 Gold (Task 4/5)
# MAGIC
# MAGIC **Medallion: T**ransform — από το καθαρό Silver φτιάχνουμε **business-ready aggregates** στο Gold.
# MAGIC
# MAGIC > Εδώ ζει η business logic (sums, averages, KPIs). Το Gold τρώει το BI/Power BI.
# MAGIC > **Idempotent** by design: `CREATE OR REPLACE TABLE ... AS SELECT` → full recompute, πάντα σωστό.

# COMMAND ----------

# DBTITLE 1,Parameters
from datetime import date

dbutils.widgets.text("catalog",  "workspace", "Catalog")
dbutils.widgets.text("env",      "prod",      "Environment")
dbutils.widgets.text("run_date", "",          "Run date (κενό = σήμερα)")
dbutils.widgets.text("run_id",   "manual",    "Run id")

CATALOG  = dbutils.widgets.get("catalog").strip()
RUN_DATE = dbutils.widgets.get("run_date").strip() or date.today().isoformat()
RUN_ID   = dbutils.widgets.get("run_id").strip()

SILVER   = f"{CATALOG}.silver.tax_declarations"
GOLD     = f"{CATALOG}.gold.tax_daily_summary"
ELT_RUNS = f"{CATALOG}.ops.elt_runs"
print(f"Catalog={CATALOG} · RunDate={RUN_DATE}")

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

# COMMAND ----------

# DBTITLE 1,Transform — daily summary ανά κατηγορία & περιφέρεια
if not spark.catalog.tableExists(SILVER):
    raise ValueError(f"❌ Δεν υπάρχει {SILVER} — έτρεξε το 02_load πρώτα;")

spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD} AS
    SELECT
        run_date,
        tax_category,
        region,
        COUNT(*)                         AS declarations,
        ROUND(SUM(amount_eur), 2)        AS total_amount_eur,
        ROUND(AVG(amount_eur), 2)        AS avg_amount_eur,
        ROUND(SUM(base_amount), 2)       AS total_base_eur
    FROM {SILVER}
    GROUP BY run_date, tax_category, region
""")
n = spark.table(GOLD).count()
print(f"🥇 Gold ← {n} aggregate rows ({GOLD})")
display(spark.table(GOLD).where(f"run_date = DATE'{RUN_DATE}'").orderBy(F.desc("total_amount_eur")).limit(10))

# COMMAND ----------

# DBTITLE 1,Log + exit
import json
log_run("03_transform", "SUCCESS", n)
set_task_value("gold_rows", int(n))
dbutils.notebook.exit(json.dumps({"task": "03_transform", "status": "SUCCESS",
                                  "run_date": RUN_DATE, "gold_rows": int(n)}))
