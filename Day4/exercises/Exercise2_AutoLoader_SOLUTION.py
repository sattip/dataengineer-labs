# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 2/4: Auto Loader (Incremental File Ingestion)

# COMMAND ----------

import urllib.request, os, pandas as pd

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

LANDING    = f"{VOLUME}/kep_landing"
SCHEMA_LOC = f"{VOLUME}/_schemas/kep_autoloader"
CKPT       = f"{VOLUME}/_checkpoints/kep_autoloader"
BRONZE     = "workspace.aade.kep_bronze_autoloader"

for p in [LANDING, SCHEMA_LOC, CKPT]:
    dbutils.fs.rm(p, recurse=True)
os.makedirs(LANDING, exist_ok=True)
spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")

pdf = pd.read_csv(f"{VOLUME}/kep_requests.csv")
pdf.iloc[0:4000].to_csv(f"{LANDING}/kep_batch1a.csv", index=False)
pdf.iloc[4000:6000].to_csv(f"{LANDING}/kep_batch1b.csv", index=False)
print("✓ batch 1 landed")

# COMMAND ----------

# TODO 1 + 2 — Auto Loader read + write (batch 1)
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(LANDING)
)
q = (df_stream.writeStream.format("delta")
     .option("checkpointLocation", CKPT)
     .trigger(availableNow=True)
     .toTable(BRONZE))
q.awaitTermination()
count_after_b1 = spark.table(BRONZE).count()
print(f"after batch1: {count_after_b1}")   # 6000

# COMMAND ----------

# TODO 3 — land batch 2 + rerun (incremental)
pdf.iloc[6000:8000].to_csv(f"{LANDING}/kep_batch2a.csv", index=False)
pdf.iloc[8000:10000].to_csv(f"{LANDING}/kep_batch2b.csv", index=False)

q2 = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", SCHEMA_LOC)
      .option("cloudFiles.inferColumnTypes", "true")
      .option("header", "true")
      .load(LANDING)
      .writeStream.format("delta")
      .option("checkpointLocation", CKPT)
      .trigger(availableNow=True)
      .toTable(BRONZE))
q2.awaitTermination()
count_after_b2 = spark.table(BRONZE).count()
print(f"after batch2: {count_after_b2} (+{count_after_b2-count_after_b1})")   # 10000, +4000

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 2 ολοκληρώθηκε")
