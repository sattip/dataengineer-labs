# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 1/4: Full Load vs Incremental Load

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col, max as spark_max, lit

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

SRC, BRONZE_FULL, BRONZE_INCR, WATERMARK = (
    "workspace.aade.kep_requests_src", "workspace.aade.kep_bronze_full",
    "workspace.aade.kep_bronze_incr", "workspace.aade.kep_watermark")

spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/kep_requests.csv") \
     .write.format("delta").mode("overwrite").saveAsTable(SRC)
print(f"✓ Source: {spark.table(SRC).count()}")

# COMMAND ----------

# TODO 1 — full load
spark.table(SRC).write.format("delta").mode("overwrite").saveAsTable(BRONZE_FULL)
full_processed = spark.table(SRC).count()
print(f"FULL: {full_processed}")

# COMMAND ----------

# initial incremental state
spark.table(SRC).filter(col("request_id") <= 8000).write.format("delta").mode("overwrite").saveAsTable(BRONZE_INCR)
spark.createDataFrame([("kep", 8000)], ["table_name","last_id"]).write.format("delta").mode("overwrite").saveAsTable(WATERMARK)

# COMMAND ----------

# TODO 2 — incremental watermark append
last_id = spark.table(WATERMARK).filter(col("table_name")=="kep").select("last_id").collect()[0]["last_id"]
new_batch = spark.table(SRC).filter(col("request_id") > last_id)
incr_processed = new_batch.count()
new_batch.write.format("delta").mode("append").saveAsTable(BRONZE_INCR)
new_max = spark.table(SRC).select(spark_max("request_id")).collect()[0][0]
spark.createDataFrame([("kep", new_max)], ["table_name","last_id"]).write.format("delta").mode("overwrite").saveAsTable(WATERMARK)
print(f"INCREMENTAL processed {incr_processed} · watermark={new_max} · Bronze_incr={spark.table(BRONZE_INCR).count()}")

# COMMAND ----------

# TODO 3 — incremental upsert (MERGE)
daily_changes = spark.createDataFrame([
    (5, "rejected"), (10001, "passed"), (10002, "flagged"),
], ["request_id", "audit_outcome"])
daily_changes.createOrReplaceTempView("kep_daily_changes")
spark.sql(f"""
    MERGE INTO {BRONZE_INCR} t
    USING kep_daily_changes s
    ON t.request_id = s.request_id
    WHEN MATCHED THEN UPDATE SET t.audit_outcome = s.audit_outcome
    WHEN NOT MATCHED THEN INSERT (request_id, audit_outcome) VALUES (s.request_id, s.audit_outcome)
""")
spark.sql(f"SELECT request_id, audit_outcome FROM {BRONZE_INCR} WHERE request_id IN (5,10001,10002) ORDER BY request_id").show()

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 1 ολοκληρώθηκε")
