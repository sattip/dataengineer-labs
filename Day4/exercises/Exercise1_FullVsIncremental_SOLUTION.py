# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 1/4: Full Load vs Incremental Load
# MAGIC Πλήρης, σχολιασμένη λύση (production patterns).

# COMMAND ----------

import urllib.request, os, uuid
from pyspark.sql.functions import (
    col, max as spark_max, sum as spark_sum, lit, current_timestamp,
    to_timestamp, to_date, when, count
)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

SRC, BRONZE_FULL, BRONZE_INCR, WATERMARK, AUDIT_LOG = (
    "workspace.aade.kep_requests_src", "workspace.aade.kep_bronze_full",
    "workspace.aade.kep_bronze_incr", "workspace.aade.kep_watermark",
    "workspace.aade.etl_audit_log")

# Typed source
raw = spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/kep_requests.csv")
src = (raw
    .withColumn("request_timestamp", to_timestamp("request_timestamp"))
    .withColumn("request_date", to_date("request_timestamp"))
    .withColumn("wait_time_minutes", col("wait_time_minutes").cast("int"))
    .withColumn("final_decision_amount", col("final_decision_amount").cast("double")))
src.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(SRC)
print(f"✓ Source {spark.table(SRC).count()}")

# COMMAND ----------

# TODO 1 — audit log + helper
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_LOG} (
        run_id STRING, load_type STRING, target_table STRING,
        rows_processed BIGINT, rows_in_target BIGINT, logged_at TIMESTAMP
    ) USING delta
""")

def log_load(run_id, load_type, target, rows_processed):
    rows_in_target = spark.table(target).count()
    (spark.createDataFrame(
        [(run_id, load_type, target, int(rows_processed), int(rows_in_target))],
        ["run_id","load_type","target_table","rows_processed","rows_in_target"])
     .withColumn("logged_at", current_timestamp())
     .write.format("delta").mode("append").saveAsTable(AUDIT_LOG))
    print(f"  📊 [{load_type}] processed={rows_processed} target={rows_in_target}")

# COMMAND ----------

# TODO 2 — full load
run_full = uuid.uuid4().hex[:8]
audited = (spark.table(SRC)
    .withColumn("_run_id", lit(run_full))
    .withColumn("_load_type", lit("FULL"))
    .withColumn("_loaded_at", current_timestamp()))
audited.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(BRONZE_FULL)
full_processed = spark.table(SRC).count()
log_load(run_full, "FULL", BRONZE_FULL, full_processed)
assert spark.table(BRONZE_FULL).count() == full_processed
print(f"✅ FULL {full_processed}")

# COMMAND ----------

# seed «χθεσινής» κατάστασης
(spark.table(SRC).filter(col("request_id") <= 8000)
    .withColumn("_run_id", lit("seed")).withColumn("_load_type", lit("SEED"))
    .withColumn("_loaded_at", current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(BRONZE_INCR))
spark.sql(f"CREATE TABLE IF NOT EXISTS {WATERMARK} (table_name STRING, last_id BIGINT) USING delta")
spark.sql(f"DELETE FROM {WATERMARK} WHERE table_name='kep'")
spark.createDataFrame([("kep",8000)],["table_name","last_id"]).write.mode("append").saveAsTable(WATERMARK)

# COMMAND ----------

# TODO 3 — incremental watermark append
run_incr = uuid.uuid4().hex[:8]
last_id = spark.table(WATERMARK).filter(col("table_name")=="kep").select("last_id").collect()[0][0]
new_batch = (spark.table(SRC).filter(col("request_id") > last_id)
    .withColumn("_run_id", lit(run_incr)).withColumn("_load_type", lit("INCREMENTAL"))
    .withColumn("_loaded_at", current_timestamp()))
incr_processed = new_batch.count()
new_batch.write.format("delta").mode("append").saveAsTable(BRONZE_INCR)
new_max = spark.table(SRC).select(spark_max("request_id")).collect()[0][0]
spark.sql(f"DELETE FROM {WATERMARK} WHERE table_name='kep'")
spark.createDataFrame([("kep",int(new_max))],["table_name","last_id"]).write.mode("append").saveAsTable(WATERMARK)
log_load(run_incr, "INCREMENTAL", BRONZE_INCR, incr_processed)
print(f"✅ INCREMENTAL {incr_processed} · watermark={new_max}")

# COMMAND ----------

# TODO 4 — incremental upsert (multi-column MERGE)
run_merge = uuid.uuid4().hex[:8]
daily_changes = spark.createDataFrame([
    (5,"rejected",200),(42,"flagged",150),(10001,"passed",30),(10002,"flagged",45),
], ["request_id","audit_outcome","wait_time_minutes"])
daily_changes.createOrReplaceTempView("kep_daily_changes")
spark.sql(f"""
    MERGE INTO {BRONZE_INCR} t
    USING kep_daily_changes s
    ON t.request_id = s.request_id
    WHEN MATCHED THEN UPDATE SET
        t.audit_outcome = s.audit_outcome,
        t.wait_time_minutes = s.wait_time_minutes,
        t._load_type = 'CORRECTION',
        t._loaded_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT
        (request_id, audit_outcome, wait_time_minutes, _run_id, _load_type, _loaded_at)
        VALUES (s.request_id, s.audit_outcome, s.wait_time_minutes,
                '{run_merge}', 'INCREMENTAL', current_timestamp())
""")
log_load(run_merge, "UPSERT", BRONZE_INCR, daily_changes.count())
spark.sql(f"SELECT request_id, audit_outcome, wait_time_minutes, _load_type FROM {BRONZE_INCR} WHERE request_id IN (5,42,10001,10002) ORDER BY request_id").show()

# COMMAND ----------

# TODO 5 — reconciliation + cost report
full_distinct = spark.table(BRONZE_FULL).filter("request_id<=10000").select("request_id").distinct().count()
incr_distinct = spark.table(BRONZE_INCR).filter("request_id<=10000").select("request_id").distinct().count()
print(f"reconcile: full={full_distinct} incr={incr_distinct}")
(spark.table(AUDIT_LOG).groupBy("load_type")
    .agg(count("*").alias("runs"), spark_sum("rows_processed").alias("total_rows"))
    .orderBy("load_type").show())

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 1 ολοκληρώθηκε")
