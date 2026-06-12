# Databricks notebook source
# MAGIC %md
# MAGIC # 🌊 Άσκηση Ημέρα 4 — Μέρος 3/4: Structured Streaming + foreachBatch MERGE
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~70' · **Δυσκολία:** ⭐⭐⭐ Hard · **~14 TODOs**
# MAGIC > Self-contained.
# MAGIC
# MAGIC ## 📖 Πού είμαστε
# MAGIC
# MAGIC Auto Loader = incremental αρχεία. Τώρα: **Structured Streaming** + το production pattern
# MAGIC **`foreachBatch` + `MERGE`** = streaming **upsert**. Κάθε micro-batch θα: (1) κάνει **dedup**
# MAGIC (κρατά την τελευταία version ανά request_id), (2) **MERGE** στο Silver, (3) ανανεώνει ένα
# MAGIC **Gold aggregate**, (4) γράφει **batch metrics**. Με **exactly-once** μέσω checkpoint.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - `readStream` από Delta (incremental = μόνο νέα commits).
# MAGIC - **In-batch deduplication** με Window (`row_number`).
# MAGIC - **`foreachBatch`** που τρέχει batch λογική (MERGE + agg + metrics).
# MAGIC - Streaming **upsert** + **exactly-once / idempotency** (re-run δεν διπλοεγγράφει).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + typed source + empty targets (έτοιμο)

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import (
    col, lit, to_timestamp, to_date, row_number, count, avg, round as spark_round, current_timestamp
)
from pyspark.sql.window import Window

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

SRC    = "workspace.aade.kep_stream_src"
SILVER = "workspace.aade.kep_silver_stream"
GOLD   = "workspace.aade.kep_gold_service_live"
BATCHLOG = "workspace.aade.kep_stream_batchlog"
CKPT   = f"{VOLUME}/_checkpoints/kep_silver_stream"

dbutils.fs.rm(CKPT, recurse=True)
for t in [SILVER, SRC, GOLD, BATCHLOG]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

(spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/kep_requests.csv")
 .withColumn("request_timestamp", to_timestamp("request_timestamp"))
 .withColumn("wait_time_minutes", col("wait_time_minutes").cast("int"))
 .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(SRC))
spark.sql(f"CREATE TABLE {SILVER} AS SELECT * FROM {SRC} WHERE 1=0")
spark.sql(f"CREATE TABLE IF NOT EXISTS {BATCHLOG} (batch_id BIGINT, rows_in BIGINT, rows_deduped BIGINT, logged_at TIMESTAMP) USING delta")
print(f"✓ Source={spark.table(SRC).count()} · Silver=0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Streaming reader από Delta

# COMMAND ----------

stream_df = spark.readStream.format("______").table(SRC)   # TODO 1: delta
print("isStreaming =", stream_df.isStreaming)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — foreachBatch: dedup → MERGE → Gold → metrics
# MAGIC
# MAGIC Το `foreachBatch(fn)` καλεί τη `fn(batch_df, batch_id)` ανά micro-batch με κανονικό DataFrame.
# MAGIC Εκεί κάνουμε: **dedup** (Window: κράτα την τελευταία version ανά request_id), **MERGE** upsert,
# MAGIC **ανανέωση Gold**, **batch metric**. Όλα idempotent (το checkpoint εγγυάται exactly-once).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Η συνάρτηση foreachBatch

# COMMAND ----------

def process_batch(batch_df, batch_id):
    spark_b = batch_df.sparkSession
    rows_in = batch_df.count()

    # 2a. Dedup μέσα στο batch: κράτα την ΤΕΛΕΥΤΑΙΑ version ανά request_id
    w = Window.partitionBy("request_id").orderBy(col("request_timestamp").______())   # TODO 2a: desc
    deduped = (batch_df.withColumn("_rn", row_number().over(w))
               .filter(col("_rn") == ___)                                             # TODO 2b: 1
               .drop("_rn"))
    deduped.createOrReplaceTempView("batch_updates")
    rows_deduped = deduped.count()

    # 2c. MERGE upsert στο Silver
    spark_b.sql(f"""
        MERGE INTO {SILVER} t
        USING batch_updates s
        ON t.request_id = s.request_id
        WHEN ________ THEN UPDATE SET *                  -- TODO 2c: MATCHED
        WHEN ________________ THEN INSERT *              -- TODO 2d: NOT MATCHED
    """)

    # 2d. Ανανέωσε το Gold aggregate (recompute από Silver)
    (spark_b.table(SILVER).groupBy("service_type")
        .agg(count("*").alias("total_requests"),
             spark_round(avg("wait_time_minutes"), 1).alias("avg_wait_min"))
        .write.format("delta").mode("__________").saveAsTable(GOLD))   # TODO 2e: overwrite

    # 2e. Batch metric
    (spark_b.createDataFrame([(batch_id, rows_in, rows_deduped)],
        ["batch_id","rows_in","rows_deduped"])
        .withColumn("logged_at", current_timestamp())
        .write.format("delta").mode("append").saveAsTable(BATCHLOG))
    print(f"  ▶ batch {batch_id}: in={rows_in} deduped={rows_deduped}")

print("✓ process_batch ορίστηκε")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Τρέξτε το streaming upsert (run 1)

# COMMAND ----------

q = (stream_df.writeStream
     .foreachBatch(________________)                   # TODO 3a: process_batch
     .option("checkpointLocation", CKPT)
     .trigger(__________=True)                          # TODO 3b: availableNow
     .start())
q.awaitTermination()
run1 = spark.table(SILVER).count()
print(f"✓ Run 1: Silver = {run1}")   # 10000

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Incremental + dedup + exactly-once
# MAGIC
# MAGIC Append στο source: id=5 update + **δύο** versions του id=10001 (παλιά+νέα). Το dedup μέσα στο
# MAGIC batch κρατά τη **νέα** του 10001. Re-run χωρίς νέα → **τίποτα** (idempotent).

# COMMAND ----------

# DBTITLE 1,Append νέα δεδομένα (έτοιμο)
tmpl = spark.table(SRC).filter("request_id = 1").limit(1)
upd5     = spark.table(SRC).filter("request_id = 5").limit(1).withColumn("audit_outcome", lit("rejected"))
new_old  = (tmpl.withColumn("request_id", lit(10001)).withColumn("audit_outcome", lit("passed"))
            .withColumn("request_timestamp", to_timestamp(lit("2024-01-01 00:00:00"))))
new_new  = (tmpl.withColumn("request_id", lit(10001)).withColumn("audit_outcome", lit("rejected"))
            .withColumn("request_timestamp", to_timestamp(lit("2024-12-31 00:00:00"))))
upd5.unionByName(new_old).unionByName(new_new).write.format("delta").mode("append").saveAsTable(SRC)
print("✓ Appended 3 rows (1 update + 2 versions του 10001)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Run 2 (incremental) + Run 3 (idempotency)

# COMMAND ----------

# 4a. Run 2 — διαβάζει μόνο τα 3 νέα → dedup → 2 → MERGE
(spark.readStream.format("delta").table(SRC).writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CKPT)
    .trigger(availableNow=True)
    .start()).awaitTermination()
run2 = spark.table(SILVER).count()
print(f"✓ Run 2: Silver = {run2} (+1 νέο)")

# 4b. Run 3 — ΧΩΡΙΣ νέα δεδομένα → πρέπει να μην αλλάξει τίποτα (exactly-once)
(spark.readStream.format("delta").table(SRC).writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", ________)            # TODO 4: CKPT (ίδιο → idempotent)
    .trigger(availableNow=True)
    .start()).awaitTermination()
run3 = spark.table(SILVER).count()
print(f"✓ Run 3 (no new data): Silver = {run3} (ίδιο με run 2)")

spark.sql(f"SELECT request_id, audit_outcome FROM {SILVER} WHERE request_id IN (5,10001) ORDER BY request_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 3

# COMMAND ----------

results = {
    "Run 1: Silver = 10000":            run1 == 10000,
    "Run 2: Silver = 10001 (+1)":       run2 == 10001,
    "Run 3 idempotent (= run 2)":       run3 == run2,
    "Dedup κράτησε τη νέα του 10001":   spark.sql(f"SELECT audit_outcome o FROM {SILVER} WHERE request_id=10001").collect()[0]["o"] == "rejected",
    "id=5 έγινε update → rejected":      spark.sql(f"SELECT audit_outcome o FROM {SILVER} WHERE request_id=5").collect()[0]["o"] == "rejected",
    "Καμία διπλοεγγραφή request_id":    spark.sql(f"SELECT count(*) c FROM (SELECT request_id FROM {SILVER} GROUP BY request_id HAVING count(*)>1)").collect()[0]["c"] == 0,
    "Gold = 5 service types":           spark.table(GOLD).count() == 5,
    "Batch log έχει ≥ 2 batches":       spark.table(BATCHLOG).count() >= 2,
}
print("=" * 55)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 55)
print("🎉 Τέλος Μέρους 3!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise4_SCD2_Bonus_STARTER`
# MAGIC
# MAGIC Ο upsert κρατά μόνο την τρέχουσα κατάσταση. Στο Μέρος 4 (bonus): **SCD Type 2** — incremental
# MAGIC MERGE που κρατάει **ιστορικό** (valid_from/valid_to/is_current).
