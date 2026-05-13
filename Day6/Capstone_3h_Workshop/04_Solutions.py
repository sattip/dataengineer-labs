# Databricks notebook source
# MAGIC %md
# MAGIC # 🔑 Day 6 — Solutions
# MAGIC
# MAGIC **ΓΙΑ TRAINER USE / POST-WORKSHOP REFERENCE**
# MAGIC
# MAGIC Πλήρεις λύσεις για όλες τις 8 exercises + Capstone.
# MAGIC Μην ανοίγετε αυτό το notebook πριν τελειώσετε εσείς τα exercises.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧰 Setup (κοινό για όλα)

# COMMAND ----------

import os, logging, time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, to_timestamp, upper, trim, length,
    row_number, lag, sum as spark_sum, avg, count, desc, broadcast
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

for n in ("pyspark.sql.connect.client.core", "pyspark.sql.connect",
          "pyspark", "py4j", "grpc"):
    logging.getLogger(n).setLevel(logging.CRITICAL)

YOUR_NAME = "george"
SCHEMA = f"workspace.day6_{YOUR_NAME}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 Exercise 1 Solution — Ingest CSV → Bronze

# COMMAND ----------

csv_path = f"/Volumes/workspace/day6_{YOUR_NAME}/raw/tax_declarations.csv"

spark.sql(f"""
    CREATE OR REPLACE TABLE {SCHEMA}.bronze_tax_declarations
    USING DELTA
    COMMENT 'Bronze layer — raw tax declarations από TAXIS source'
    TBLPROPERTIES (
        'layer' = 'bronze',
        'data_owner' = 'aade',
        'pii_present' = 'true'
    )
    AS
    SELECT *,
        current_timestamp() AS _ingested_at,
        '{csv_path}' AS _source_file
    FROM read_files('{csv_path}', format => 'csv', header => true, inferSchema => true)
""")

print(f"✓ Bronze created: {SCHEMA}.bronze_tax_declarations")
spark.table(f"{SCHEMA}.bronze_tax_declarations").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 Exercise 2 Solution — Quarantine Pattern

# COMMAND ----------

bronze = spark.table(f"{SCHEMA}.bronze_tax_declarations")

# Valid: όλα τα criteria πληρούνται
silver_clean = (bronze
    .filter(col("afm").isNotNull())
    .filter(length(col("afm")) == 9)
    .filter(col("tax_amount") >= 0)
    .filter(col("region") != "INVALID")
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("submitted_at", to_timestamp(col("submitted_at"))))

silver_clean.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.silver_tax_clean")

# Invalid: opposite criteria με reason
quarantine = (bronze
    .withColumn("_reason",
        when(col("afm").isNull(), "NULL_AFM")
        .when(length(col("afm")) != 9, "INVALID_AFM_FORMAT")
        .when(col("tax_amount") < 0, "NEGATIVE_AMOUNT")
        .when(col("region") == "INVALID", "INVALID_REGION")
        .otherwise(None))
    .filter(col("_reason").isNotNull()))

quarantine.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.quarantine_tax")

print(f"Silver clean: {silver_clean.count()}")
print(f"Quarantined:  {quarantine.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 Exercise 3 Solution — MERGE με condition

# COMMAND ----------

incremental_data = [
    ("TX00001", "900000001", 2025, "ΑΤΤΙΚΗ", 9999.99, "APPROVED", datetime.utcnow().isoformat()),
    ("TX00002", "900000002", 2025, "ΑΤΤΙΚΗ", 5500.00, "APPROVED", datetime.utcnow().isoformat()),
    ("TX99001", "900099001", 2025, "ΚΡΗΤΗ", 3200.50, "SUBMITTED", datetime.utcnow().isoformat()),
    ("TX99002", "900099002", 2025, "ΠΑΤΡΑ", 7800.00, "APPROVED", datetime.utcnow().isoformat()),
    ("TX99003", "900099003", 2025, "ΑΤΤΙΚΗ", 12000.00, "APPROVED", datetime.utcnow().isoformat()),
    ("TX00010", "900000010", 2025, "ΑΤΤΙΚΗ", 100.00, "REJECTED", "2020-01-01T00:00:00"),
]
incremental_df = spark.createDataFrame(
    incremental_data,
    "statement_id STRING, afm STRING, fiscal_year INT, region STRING, "
    "tax_amount DOUBLE, status STRING, submitted_at STRING"
).withColumn("submitted_at", to_timestamp("submitted_at"))

target = DeltaTable.forName(spark, f"{SCHEMA}.silver_tax_clean")
(target.alias("t")
    .merge(incremental_df.alias("s"), "t.statement_id = s.statement_id")
    .whenMatchedUpdate(
        condition="s.submitted_at > t.submitted_at",  # ⬅️ key: μόνο newer
        set={
            "tax_amount":    "s.tax_amount",
            "status":        "s.status",
            "submitted_at":  "s.submitted_at"
        })
    .whenNotMatchedInsertAll()
    .execute())

print("✓ MERGE complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 Exercise 4 Solution — Gold με Window

# COMMAND ----------

silver = spark.table(f"{SCHEMA}.silver_tax_clean")

# Step 1: aggregate ανά ΑΦΜ (κρατάμε region για το window)
per_afm = (silver
    .groupBy("afm", "region")
    .agg(
        count("*").alias("total_declarations"),
        spark_sum("tax_amount").alias("total_tax"),
        spark_sum(when(col("status") == "APPROVED", 1).otherwise(0)).alias("approved_count"),
        spark_sum(when(col("status") == "REJECTED", 1).otherwise(0)).alias("rejected_count"),
    )
    .withColumn("rejection_rate",
                col("rejected_count") / col("total_declarations")))

# Step 2: window function για region average
w = Window.partitionBy("region")
gold = (per_afm
    .withColumn("region_avg_tax", avg("total_tax").over(w))
    .withColumn("tax_vs_region_avg", col("total_tax") / col("region_avg_tax"))
    .withColumn("is_above_region_avg", col("tax_vs_region_avg") > 1.0)
    .withColumn("computed_at", current_timestamp())
    .drop("rejected_count"))

gold.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.gold_taxpayer_metrics")
print(f"✓ Gold: {spark.table(f'{SCHEMA}.gold_taxpayer_metrics').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 Exercise 5 Solution — Auto Loader Streaming

# COMMAND ----------

streaming_src = f"/Volumes/workspace/day6_{YOUR_NAME}/raw/streaming_src"
checkpoint_dir = f"/Volumes/workspace/day6_{YOUR_NAME}/raw/checkpoints"

bronze_query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint_dir}/bronze_stream/_schema")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(streaming_src)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_dir}/bronze_stream")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{SCHEMA}.bronze_stream_tax"))

bronze_query.awaitTermination()
print(f"✓ Bronze stream complete: {spark.table(f'{SCHEMA}.bronze_stream_tax').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 Exercise 6 Solution — foreachBatch + MERGE

# COMMAND ----------

def merge_to_silver(microbatch_df, batch_id):
    """Streaming MERGE function — dedupe + upsert."""
    # Deduplicate σε microbatch level (latest submitted_at wins)
    w = Window.partitionBy("statement_id").orderBy(col("_ingested_at").desc())
    dedup = (microbatch_df
        .withColumn("_rn", row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("_merged_at", current_timestamp()))

    # MERGE
    target = DeltaTable.forName(microbatch_df.sparkSession, f"{SCHEMA}.silver_stream_tax")
    (target.alias("t")
        .merge(dedup.alias("s"), "t.statement_id = s.statement_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

(spark.readStream
    .table(f"{SCHEMA}.bronze_stream_tax")
    .writeStream
    .foreachBatch(merge_to_silver)
    .option("checkpointLocation", f"{checkpoint_dir}/silver_merge")
    .trigger(availableNow=True)
    .start()
    .awaitTermination())

print(f"✓ Silver: {spark.table(f'{SCHEMA}.silver_stream_tax').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 Exercise 7 Solution — Schema Evolution

# COMMAND ----------

# Re-run streaming με schemaEvolutionMode
# Σημαντικό: μπορεί να σπάσει την πρώτη φορά, retry δουλεύει
for attempt in range(2):
    try:
        q = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{checkpoint_dir}/bronze_stream/_schema")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(streaming_src)
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
            .writeStream
            .format("delta")
            .option("checkpointLocation", f"{checkpoint_dir}/bronze_stream")
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(f"{SCHEMA}.bronze_stream_tax"))
        q.awaitTermination()
        print(f"✓ Attempt {attempt+1} succeeded")
        break
    except Exception as e:
        if "UnknownFieldException" in str(e) or "schema" in str(e).lower():
            print(f"Schema mismatch detected, retrying...")
            continue
        else:
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🟢 Exercise 8 Solution — Time Travel + RESTORE

# COMMAND ----------

# Sub-task 1: history
history = spark.sql(f"DESCRIBE HISTORY {SCHEMA}.silver_stream_tax")
history.select("version", "timestamp", "operation").show(truncate=False)

# Find το pre-UPDATE version (το UPDATE είναι το latest)
update_version = history.first()["version"]
pre_update_version = update_version - 1

# Sub-task 2: verify previous version
print(f"\nState στο version {pre_update_version}:")
spark.sql(f"""
    SELECT statement_id, tax_amount
    FROM {SCHEMA}.silver_stream_tax VERSION AS OF {pre_update_version}
    LIMIT 5
""").show()

# Sub-task 3: RESTORE
spark.sql(f"RESTORE TABLE {SCHEMA}.silver_stream_tax TO VERSION AS OF {pre_update_version}")
print(f"✓ Restored to version {pre_update_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 Capstone Solution

# COMMAND ----------

CSCHEMA = f"workspace.capstone_{YOUR_NAME}"
CVOLUME = f"/Volumes/workspace/capstone_{YOUR_NAME}/raw"

# === Step 1: Bronze streams (run και τα 2) ===

def bronze_stream(source_name, table_name):
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{CVOLUME}/checkpoints/{source_name}/_schema")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{CVOLUME}/{source_name}")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{CVOLUME}/checkpoints/{source_name}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CSCHEMA}.bronze_{source_name}"))

q1 = bronze_stream("taxis", "bronze_taxis")
q2 = bronze_stream("mydata", "bronze_mydata")
q1.awaitTermination()
q2.awaitTermination()
print("✓ Both Bronze streams complete")

# COMMAND ----------

# === Step 2: Silver με DQ ===

bronze_taxis = spark.table(f"{CSCHEMA}.bronze_taxis")
bronze_mydata = spark.table(f"{CSCHEMA}.bronze_mydata")

# DQ metrics tracker
dq_records = []

def log_dq(source, rule, failed, total):
    dq_records.append({
        "source": source, "rule": rule,
        "failed_count": failed, "total_count": total,
        "failure_pct": round(failed / max(total, 1) * 100, 2),
        "checked_at": datetime.utcnow(),
    })

# Silver TAXIS
total_t = bronze_taxis.count()
log_dq("taxis", "afm_not_null", bronze_taxis.filter(col("afm").isNull()).count(), total_t)
log_dq("taxis", "afm_9_digits", bronze_taxis.filter(length(col("afm")) != 9).count(), total_t)
log_dq("taxis", "positive_income", bronze_taxis.filter(col("declared_income") <= 0).count(), total_t)

silver_taxis = (bronze_taxis
    .filter(col("afm").isNotNull())
    .filter(length(col("afm")) == 9)
    .filter(col("declared_income") > 0)
    .filter(col("tax_paid") >= 0)
    .filter((col("tax_paid") / col("declared_income")).between(0, 0.5))
    .dropDuplicates(["statement_id"]))

silver_taxis.write.format("delta").mode("overwrite").saveAsTable(f"{CSCHEMA}.silver_taxis_clean")

# Silver myDATA
total_m = bronze_mydata.count()
log_dq("mydata", "issuer_not_null", bronze_mydata.filter(col("issuer_afm").isNull()).count(), total_m)
log_dq("mydata", "no_self_invoice", bronze_mydata.filter(col("issuer_afm") == col("receiver_afm")).count(), total_m)

silver_mydata = (bronze_mydata
    .filter(col("issuer_afm").isNotNull() & col("receiver_afm").isNotNull())
    .filter(length(col("issuer_afm")) == 9)
    .filter(length(col("receiver_afm")) == 9)
    .filter(col("issuer_afm") != col("receiver_afm"))
    .filter(col("total_amount") > 0)
    .filter(col("status") == "Accepted")
    .dropDuplicates(["invoice_id"]))

silver_mydata.write.format("delta").mode("overwrite").saveAsTable(f"{CSCHEMA}.silver_mydata_clean")

# Save DQ summary
spark.createDataFrame(pd.DataFrame(dq_records)).write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CSCHEMA}.dq_summary")

print(f"✓ Silver TAXIS: {silver_taxis.count()}")
print(f"✓ Silver myDATA: {silver_mydata.count()}")
print(f"✓ DQ summary: {len(dq_records)} rules logged")

# COMMAND ----------

# === Step 3: Gold audit_priority_top100 ===

mydata_agg = (silver_mydata
    .groupBy(col("issuer_afm").alias("afm"))
    .agg(
        count("*").alias("total_invoices_count"),
        spark_sum("total_amount").alias("total_invoiced_amount"),
    ))

taxis_agg = (silver_taxis
    .groupBy("afm", "region")
    .agg(spark_sum("declared_income").alias("declared_income")))

# Join + score + rank
joined = (taxis_agg
    .join(mydata_agg, "afm", "left")
    .fillna({"total_invoices_count": 0, "total_invoiced_amount": 0.0})
    .withColumn("suspect_score",
        F.when(col("declared_income") > 0,
               col("total_invoiced_amount") / col("declared_income"))
         .otherwise(lit(0))))

w = Window.orderBy(col("suspect_score").desc())
gold = (joined
    .withColumn("audit_rank", row_number().over(w))
    .filter(col("audit_rank") <= 100)
    .withColumn("computed_at", current_timestamp())
    .select("afm", "region", "declared_income",
            "total_invoices_count", "total_invoiced_amount",
            "suspect_score", "audit_rank", "computed_at"))

gold.write.format("delta").mode("overwrite").saveAsTable(f"{CSCHEMA}.audit_priority_top100")

print(f"✓ Gold audit_priority_top100: {gold.count()} rows")
print("\nTop 10 suspects:")
spark.table(f"{CSCHEMA}.audit_priority_top100").orderBy("audit_rank").show(10, truncate=False)
