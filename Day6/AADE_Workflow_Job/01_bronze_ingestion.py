# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Task 1: Bronze Ingestion
# MAGIC
# MAGIC **Workflow Job Task** που τρέχει αυτόματα από schedule.
# MAGIC
# MAGIC - **Source**: CSV files στο volume `/Volumes/workspace/aade/aade_data/streaming/raw/`
# MAGIC - **Target**: 4 Bronze Delta tables
# MAGIC - **Pattern**: Auto Loader incremental ingestion
# MAGIC - **Trigger**: `availableNow=True` (run-and-exit)
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day6/AADE_Workflow_Job/01_bronze_ingestion.py
# MAGIC > ```

# COMMAND ----------

import os
import logging
from pyspark.sql.functions import col, current_timestamp, lit

# Suppress noisy logs
for n in ("pyspark.sql.connect.client.core", "py4j", "grpc"):
    logging.getLogger(n).setLevel(logging.CRITICAL)

# Setup (idempotent)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

VOLUME_ROOT = "/Volumes/workspace/aade/aade_data/streaming"
CHECKPOINT_ROOT = f"{VOLUME_ROOT}/checkpoints"

sources = ["taxis", "mydata", "kep", "efka"]
for src in sources:
    os.makedirs(f"{VOLUME_ROOT}/raw/{src}", exist_ok=True)
    os.makedirs(f"{CHECKPOINT_ROOT}/{src}", exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze streaming function

# COMMAND ----------

def stream_bronze(source_name: str):
    """Auto Loader → Bronze Delta table για ένα source."""
    src = f"{VOLUME_ROOT}/raw/{source_name}"
    chk = f"{CHECKPOINT_ROOT}/{source_name}"

    query = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{chk}/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(src)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source", lit(source_name))
        .writeStream
        .format("delta")
        .option("checkpointLocation", chk)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"workspace.aade.bronze_{source_name}"))
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute all 4 Bronze streams

# COMMAND ----------

print(f"=== Bronze Ingestion — Run {current_timestamp()} ===\n")

queries = []
for src in sources:
    print(f"Starting stream: bronze_{src}...")
    queries.append(stream_bronze(src))

# Wait for all
for q in queries:
    q.awaitTermination()

# Report row counts (μετά την ολοκλήρωση)
print("\n=== Bronze tables — row counts ===")
total = 0
for src in sources:
    cnt = spark.table(f"workspace.aade.bronze_{src}").count()
    total += cnt
    print(f"  bronze_{src:8s}: {cnt:6d} rows")
print(f"  {'TOTAL':10s}: {total:6d} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task Output (για Job UI)
# MAGIC
# MAGIC Το `dbutils.notebook.exit()` περνάει value στον Job DAG που μπορεί να
# MAGIC χρησιμοποιηθεί από downstream tasks (μέσω `{{tasks.task_key.values.key}}`).

# COMMAND ----------

# Exit με metadata για το Job UI
dbutils.notebook.exit(f"OK: bronze ingested {total} rows across {len(sources)} sources")
