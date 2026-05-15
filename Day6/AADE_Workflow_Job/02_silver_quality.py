# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Task 2: Silver Quality & MERGE
# MAGIC
# MAGIC **Workflow Job Task** — depends_on task_1_bronze
# MAGIC
# MAGIC - **Source**: Bronze Delta tables
# MAGIC - **Target**: 4 Silver Delta tables (clean + validated)
# MAGIC - **Pattern**: Data quality filtering + MERGE upserts
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day6/AADE_Workflow_Job/02_silver_quality.py
# MAGIC > ```

# COMMAND ----------

import logging
from datetime import datetime
import pandas as pd
from pyspark.sql.functions import col, upper, trim, length, to_timestamp, current_timestamp, when
from delta.tables import DeltaTable

for n in ("pyspark.sql.connect.client.core", "py4j", "grpc"):
    logging.getLogger(n).setLevel(logging.CRITICAL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: MERGE-or-INSERT pattern

# COMMAND ----------

def upsert_silver(bronze_df, target_table: str, key_col: str):
    """MERGE bronze rows into Silver target. Creates table αν δεν υπάρχει."""
    if spark.catalog.tableExists(target_table):
        delta = DeltaTable.forName(spark, target_table)
        (delta.alias("t")
            .merge(bronze_df.alias("s"), f"t.{key_col} = s.{key_col}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        bronze_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ Metrics tracker

# COMMAND ----------

dq_records = []

def log_dq(source: str, rule: str, failed: int, total: int):
    dq_records.append({
        "run_id": dbutils.widgets.get("run_id") if "run_id" in [w.name for w in dbutils.widgets.getAll()] else "manual",
        "source": source,
        "rule": rule,
        "failed_count": failed,
        "total_count": total,
        "failure_pct": round(failed / max(total, 1) * 100, 2),
        "checked_at": datetime.utcnow(),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver TAXIS

# COMMAND ----------

bronze_taxis = spark.table("workspace.aade.bronze_taxis")
total_taxis = bronze_taxis.count()

log_dq("taxis", "afm_not_null", bronze_taxis.filter(col("afm").isNull()).count(), total_taxis)
log_dq("taxis", "afm_9_digits", bronze_taxis.filter(length(col("afm")) != 9).count(), total_taxis)
log_dq("taxis", "tax_non_negative", bronze_taxis.filter(col("tax_amount") < 0).count(), total_taxis)

silver_taxis = (bronze_taxis
    .filter(col("afm").isNotNull() & (length(col("afm")) == 9))
    .filter(col("tax_amount") >= 0)
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("submitted_at", to_timestamp(col("submitted_at")))
    .withColumn("_silver_at", current_timestamp())
    .dropDuplicates(["statement_id"])
    .drop("_ingested_at", "_source_file"))

upsert_silver(silver_taxis, "workspace.aade.silver_tax_declarations_clean", "statement_id")
print(f"✓ Silver TAXIS: {silver_taxis.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver myDATA

# COMMAND ----------

bronze_mydata = spark.table("workspace.aade.bronze_mydata")
total_md = bronze_mydata.count()

log_dq("mydata", "no_self_invoice", bronze_mydata.filter(col("issuer_afm") == col("receiver_afm")).count(), total_md)

silver_mydata = (bronze_mydata
    .filter(col("issuer_afm").isNotNull() & col("receiver_afm").isNotNull())
    .filter(col("issuer_afm") != col("receiver_afm"))
    .filter(col("total_amount") >= 0)
    .withColumn("transmission_status", upper(trim(col("transmission_status"))))
    .withColumn("_silver_at", current_timestamp())
    .dropDuplicates(["invoice_id"])
    .drop("_ingested_at", "_source_file"))

upsert_silver(silver_mydata, "workspace.aade.silver_invoices_clean", "invoice_id")
print(f"✓ Silver myDATA: {silver_mydata.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver ΚΕΠ & ΕΦΚΑ

# COMMAND ----------

bronze_kep = spark.table("workspace.aade.bronze_kep")
silver_kep = (bronze_kep
    .filter(col("afm").isNotNull())
    .filter(col("duration_seconds").between(30, 7200))
    .withColumn("event_ts", to_timestamp(col("event_ts")))
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("_silver_at", current_timestamp())
    .dropDuplicates(["event_id"])
    .drop("_ingested_at", "_source_file"))
upsert_silver(silver_kep, "workspace.aade.silver_kep_events_clean", "event_id")
print(f"✓ Silver ΚΕΠ: {silver_kep.count()} rows")

bronze_efka = spark.table("workspace.aade.bronze_efka")
silver_efka = (bronze_efka
    .filter(col("afm").isNotNull() & (col("gross_income") > 0))
    .withColumn("category", upper(trim(col("category"))))
    .withColumn("payment_status", upper(trim(col("payment_status"))))
    .withColumn("_silver_at", current_timestamp())
    .dropDuplicates(["contribution_id"])
    .drop("_ingested_at", "_source_file"))
upsert_silver(silver_efka, "workspace.aade.silver_efka_contributions_clean", "contribution_id")
print(f"✓ Silver ΕΦΚΑ: {silver_efka.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save DQ summary

# COMMAND ----------

if dq_records:
    dq_df = spark.createDataFrame(pd.DataFrame(dq_records))
    dq_df.write.format("delta").mode("append").saveAsTable("workspace.aade.dq_summary")
    print(f"\n✓ {len(dq_records)} DQ rules logged")

# Job-level summary
summary = {
    "task": "silver_quality",
    "silver_tax_count":      spark.table("workspace.aade.silver_tax_declarations_clean").count(),
    "silver_invoices_count": spark.table("workspace.aade.silver_invoices_clean").count(),
    "silver_kep_count":      spark.table("workspace.aade.silver_kep_events_clean").count(),
    "silver_efka_count":     spark.table("workspace.aade.silver_efka_contributions_clean").count(),
    "dq_rules_evaluated":    len(dq_records),
}
print(f"\n=== Silver task complete ===\n{summary}")

dbutils.notebook.exit(f"OK: silver tables ready ({summary})")
