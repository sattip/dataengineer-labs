# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — ELT environment (Task 1/5)
# MAGIC
# MAGIC **Multi-notebook ELT Job** · Ημέρα 6 · Orchestration
# MAGIC
# MAGIC > Πρώτο task του Job. Φτιάχνει catalog/schemas (idempotent) ώστε τα επόμενα tasks να έχουν πού να γράψουν.
# MAGIC > Τρέχει και standalone (interactive) και ως Job task.
# MAGIC
# MAGIC ## Το ELT DAG
# MAGIC ```
# MAGIC 00_setup ─► 01_extract ─► 02_load ─► 03_transform ─► 04_notify (run_if ALL_DONE)
# MAGIC  (env)      (→Bronze)     (→Silver)   (→Gold)         (summary/alert)
# MAGIC ```
# MAGIC **E**xtract → land raw στο Bronze · **L**oad → καθαρό/validated στο Silver · **T**ransform → aggregates στο Gold.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "workspace", "Catalog")   # workspace = πάντα υπάρχει σε Free/Serverless
dbutils.widgets.text("env",     "prod",      "Environment")

CATALOG = dbutils.widgets.get("catalog").strip()
ENV     = dbutils.widgets.get("env").strip()
print(f"Catalog={CATALOG} · Env={ENV}")

# COMMAND ----------

# DBTITLE 1,Create catalog (tolerant) + schemas
# Σε Free Edition ο user συνήθως ΔΕΝ έχει CREATE CATALOG → αν αποτύχει, υποθέτουμε ότι υπάρχει.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    print(f"✅ Catalog ready: {CATALOG}")
except Exception as e:
    print(f"ℹ️  Χρησιμοποιώ υπάρχον catalog '{CATALOG}' (χωρίς CREATE CATALOG priv).")

for layer, desc in [
    ("bronze", "Raw landed data, ανά run_date"),
    ("silver", "Cleaned + contract-validated"),
    ("gold",   "Business-ready aggregates"),
    ("ops",    "Pipeline run log / audit"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{layer} COMMENT '{desc}'")
    print(f"✅ Schema ready: {CATALOG}.{layer}")

# COMMAND ----------

# DBTITLE 1,Done
import json
dbutils.notebook.exit(json.dumps({"task": "00_setup", "status": "SUCCESS", "catalog": CATALOG}))
