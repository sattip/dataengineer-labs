# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 2/4: Auto Loader (Incremental File Ingestion)

# COMMAND ----------

import urllib.request, os, pandas as pd
from pyspark.sql.functions import col, current_timestamp, count, avg, round as spark_round

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

LANDING, SCHEMA_LOC, CKPT = f"{VOLUME}/kep_landing", f"{VOLUME}/_schemas/kep_autoloader", f"{VOLUME}/_checkpoints/kep_autoloader"
BRONZE, SILVER = "workspace.aade.kep_bronze_autoloader", "workspace.aade.kep_silver_by_service"

for p in [LANDING, SCHEMA_LOC, CKPT]:
    dbutils.fs.rm(p, recurse=True)
os.makedirs(LANDING, exist_ok=True)
spark.sql(f"DROP TABLE IF EXISTS {BRONZE}"); spark.sql(f"DROP TABLE IF EXISTS {SILVER}")

pdf = pd.read_csv(f"{VOLUME}/kep_requests.csv")
pdf.iloc[0:4000].to_csv(f"{LANDING}/kep_batch1a.csv", index=False)
pdf.iloc[4000:6000].to_csv(f"{LANDING}/kep_batch1b.csv", index=False)
print("✓ batch1 landed")

# COMMAND ----------

# TODO 1 — reader (+audit)
def build_reader():
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_LOC)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .load(LANDING)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp()))

# COMMAND ----------

# TODO 2 — write batch 1
def run_autoloader():
    q = (build_reader().writeStream.format("delta")
         .option("checkpointLocation", CKPT)
         .option("mergeSchema", "true")
         .trigger(availableNow=True)
         .toTable(BRONZE))
    q.awaitTermination()

run_autoloader()
count_b1 = spark.table(BRONZE).count(); print(f"b1: {count_b1}")   # 6000

# COMMAND ----------

# TODO 3 — batch 2 incremental
pdf.iloc[6000:8000].to_csv(f"{LANDING}/kep_batch2a.csv", index=False)
pdf.iloc[8000:10000].to_csv(f"{LANDING}/kep_batch2b.csv", index=False)
run_autoloader()
count_b2 = spark.table(BRONZE).count(); print(f"b2: {count_b2} (+{count_b2-count_b1})")   # 10000

# COMMAND ----------

# TODO 4 — batch 3 schema drift
batch3 = pdf.iloc[0:100].copy()
batch3["request_id"] = range(10001, 10101)
batch3["priority"] = "HIGH"
batch3.to_csv(f"{LANDING}/kep_batch3_newcol.csv", index=False)
run_autoloader()
count_b3 = spark.table(BRONZE).count()
rescued = spark.table(BRONZE).filter(col("_rescued_data").isNotNull()).count()
print(f"b3: {count_b3} · rescued: {rescued}")
spark.table(BRONZE).filter(col("_rescued_data").isNotNull()).select("request_id","_rescued_data").show(3, truncate=False)

# COMMAND ----------

# TODO 5 — Silver aggregation
silver = (spark.table(BRONZE).filter(col("request_id") <= 10000)
    .groupBy("service_type")
    .agg(count("*").alias("total_requests"), spark_round(avg("wait_time_minutes"),1).alias("avg_wait_min"))
    .orderBy(col("total_requests").desc()))
silver.write.format("delta").mode("overwrite").saveAsTable(SILVER)
silver.show(truncate=False)

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 2 ολοκληρώθηκε")
