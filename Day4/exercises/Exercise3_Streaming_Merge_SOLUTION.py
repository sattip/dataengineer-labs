# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 3/4: Structured Streaming + foreachBatch MERGE

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col, lit

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

SRC    = "workspace.aade.kep_stream_src"
SILVER = "workspace.aade.kep_silver_stream"
CKPT   = f"{VOLUME}/_checkpoints/kep_silver_stream"

dbutils.fs.rm(CKPT, recurse=True)
spark.sql(f"DROP TABLE IF EXISTS {SILVER}")
spark.sql(f"DROP TABLE IF EXISTS {SRC}")
(spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/kep_requests.csv")
 .write.format("delta").mode("overwrite").saveAsTable(SRC))
spark.sql(f"CREATE TABLE {SILVER} AS SELECT * FROM {SRC} WHERE 1=0")
print(f"✓ Source={spark.table(SRC).count()} Silver=0")

# COMMAND ----------

# TODO 1 — streaming read
stream_df = spark.readStream.format("delta").table(SRC)

# COMMAND ----------

# TODO 2 — upsert function
def upsert_to_silver(batch_df, batch_id):
    batch_df.createOrReplaceTempView("kep_updates")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {SILVER} t
        USING kep_updates s
        ON t.request_id = s.request_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# TODO 3 — run 1
q = (stream_df.writeStream
     .foreachBatch(upsert_to_silver)
     .option("checkpointLocation", CKPT)
     .trigger(availableNow=True)
     .start())
q.awaitTermination()
run1 = spark.table(SILVER).count()
print(f"run1: {run1}")   # 10000

# COMMAND ----------

# append new data + rerun
upd = spark.table(SRC).filter("request_id = 5").limit(1).withColumn("audit_outcome", lit("rejected"))
new = (spark.table(SRC).filter("request_id = 1").limit(1)
       .withColumn("request_id", lit(10001)).withColumn("audit_outcome", lit("passed")))
upd.unionByName(new).write.format("delta").mode("append").saveAsTable(SRC)

q2 = (spark.readStream.format("delta").table(SRC).writeStream
      .foreachBatch(upsert_to_silver)
      .option("checkpointLocation", CKPT)
      .trigger(availableNow=True)
      .start())
q2.awaitTermination()
run2 = spark.table(SILVER).count()
print(f"run2: {run2}")   # 10001
spark.sql(f"SELECT request_id, audit_outcome FROM {SILVER} WHERE request_id IN (5,10001) ORDER BY request_id").show()

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 3 ολοκληρώθηκε")
