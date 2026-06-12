# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 3/4: Structured Streaming + foreachBatch MERGE

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import (
    col, lit, to_timestamp, row_number, count, avg, round as spark_round, current_timestamp
)
from pyspark.sql.window import Window

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

SRC, SILVER, GOLD, BATCHLOG = ("workspace.aade.kep_stream_src","workspace.aade.kep_silver_stream",
    "workspace.aade.kep_gold_service_live","workspace.aade.kep_stream_batchlog")
CKPT = f"{VOLUME}/_checkpoints/kep_silver_stream"

dbutils.fs.rm(CKPT, recurse=True)
for t in [SILVER, SRC, GOLD, BATCHLOG]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
(spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/kep_requests.csv")
 .withColumn("request_timestamp", to_timestamp("request_timestamp"))
 .withColumn("wait_time_minutes", col("wait_time_minutes").cast("int"))
 .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(SRC))
spark.sql(f"CREATE TABLE {SILVER} AS SELECT * FROM {SRC} WHERE 1=0")
spark.sql(f"CREATE TABLE IF NOT EXISTS {BATCHLOG} (batch_id BIGINT, rows_in BIGINT, rows_deduped BIGINT, logged_at TIMESTAMP) USING delta")

# COMMAND ----------

# TODO 1
stream_df = spark.readStream.format("delta").table(SRC)

# COMMAND ----------

# TODO 2 — foreachBatch fn
def process_batch(batch_df, batch_id):
    spark_b = batch_df.sparkSession
    rows_in = batch_df.count()
    w = Window.partitionBy("request_id").orderBy(col("request_timestamp").desc())
    deduped = batch_df.withColumn("_rn", row_number().over(w)).filter(col("_rn")==1).drop("_rn")
    deduped.createOrReplaceTempView("batch_updates")
    rows_deduped = deduped.count()
    spark_b.sql(f"""
        MERGE INTO {SILVER} t USING batch_updates s ON t.request_id = s.request_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    (spark_b.table(SILVER).groupBy("service_type")
        .agg(count("*").alias("total_requests"), spark_round(avg("wait_time_minutes"),1).alias("avg_wait_min"))
        .write.format("delta").mode("overwrite").saveAsTable(GOLD))
    (spark_b.createDataFrame([(batch_id, rows_in, rows_deduped)], ["batch_id","rows_in","rows_deduped"])
        .withColumn("logged_at", current_timestamp())
        .write.format("delta").mode("append").saveAsTable(BATCHLOG))
    print(f"  ▶ batch {batch_id}: in={rows_in} deduped={rows_deduped}")

# COMMAND ----------

# TODO 3 — run 1
(stream_df.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CKPT).trigger(availableNow=True).start()).awaitTermination()
run1 = spark.table(SILVER).count(); print(f"run1: {run1}")

# COMMAND ----------

# append new data
tmpl = spark.table(SRC).filter("request_id = 1").limit(1)
upd5 = spark.table(SRC).filter("request_id = 5").limit(1).withColumn("audit_outcome", lit("rejected"))
new_old = tmpl.withColumn("request_id", lit(10001)).withColumn("audit_outcome", lit("passed")).withColumn("request_timestamp", to_timestamp(lit("2024-01-01 00:00:00")))
new_new = tmpl.withColumn("request_id", lit(10001)).withColumn("audit_outcome", lit("rejected")).withColumn("request_timestamp", to_timestamp(lit("2024-12-31 00:00:00")))
upd5.unionByName(new_old).unionByName(new_new).write.format("delta").mode("append").saveAsTable(SRC)

# COMMAND ----------

# TODO 4 — run 2 + run 3 (idempotent)
(spark.readStream.format("delta").table(SRC).writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CKPT).trigger(availableNow=True).start()).awaitTermination()
run2 = spark.table(SILVER).count(); print(f"run2: {run2}")
(spark.readStream.format("delta").table(SRC).writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CKPT).trigger(availableNow=True).start()).awaitTermination()
run3 = spark.table(SILVER).count(); print(f"run3: {run3}")
spark.sql(f"SELECT request_id, audit_outcome FROM {SILVER} WHERE request_id IN (5,10001) ORDER BY request_id").show()

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 3 ολοκληρώθηκε")
