# Databricks notebook source
# MAGIC %md
# MAGIC # 🌊 Άσκηση Ημέρα 4 — Μέρος 3/4: Structured Streaming + foreachBatch
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~75' · **Δυσκολία:** ⭐⭐⭐ Hard · **~17 TODOs**
# MAGIC > Self-contained.
# MAGIC
# MAGIC ## 📖 Πού είμαστε
# MAGIC
# MAGIC Το production streaming pattern: **`foreachBatch`** — μέσα του τρέχεις κανονική **batch** λογική
# MAGIC σε κάθε micro-batch. Θα γράψεις **εσύ** όλο τον επεξεργαστή batch, που κάνει **4 πράγματα**:
# MAGIC 1. **Data Quality**: ξεχωρίζει «κακές» γραμμές → **quarantine** (δεν μολύνουν το Silver).
# MAGIC 2. **Dedup**: κρατά την τελευταία version ανά `request_id` (Window).
# MAGIC 3. **MERGE upsert** στο Silver.
# MAGIC 4. **Gold** KPIs (με conditional counts) + **batch metrics**.
# MAGIC Όλα **exactly-once** μέσω checkpoint.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - `readStream` από Delta (incremental = μόνο νέα commits).
# MAGIC - **foreachBatch** = batch λογική μέσα σε stream (DQ + dedup + MERGE + Gold).
# MAGIC - **Quarantine pattern** σε streaming (τίποτα δεν χάνεται σιωπηλά).
# MAGIC - In-batch **dedup** (Window) · streaming **upsert** · **exactly-once** (3 runs).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + typed source + empty targets (έτοιμο)

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import (
    col, lit, to_timestamp, to_date, row_number, count, avg, when,
    sum as spark_sum, round as spark_round, current_timestamp
)
from pyspark.sql.window import Window

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

SRC        = "workspace.aade.kep_stream_src"
SILVER     = "workspace.aade.kep_silver_stream"
QUARANTINE = "workspace.aade.kep_stream_quarantine"
GOLD       = "workspace.aade.kep_gold_service_live"
BATCHLOG   = "workspace.aade.kep_stream_batchlog"
CKPT       = f"{VOLUME}/_checkpoints/kep_silver_stream"

VALID_OUTCOMES = ["passed", "flagged", "rejected"]

dbutils.fs.rm(CKPT, recurse=True)
for t in [SILVER, SRC, GOLD, BATCHLOG, QUARANTINE]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

(spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/kep_requests.csv")
 .withColumn("request_timestamp", to_timestamp("request_timestamp"))
 .withColumn("wait_time_minutes", col("wait_time_minutes").cast("int"))
 .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(SRC))
spark.sql(f"CREATE TABLE {SILVER} AS SELECT * FROM {SRC} WHERE 1=0")
spark.sql(f"CREATE TABLE IF NOT EXISTS {BATCHLOG} (batch_id BIGINT, rows_in BIGINT, rows_good BIGINT, rows_quarantined BIGINT, logged_at TIMESTAMP) USING delta")
print(f"✓ Source={spark.table(SRC).count()} · Silver=0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Streaming reader από Delta
# MAGIC
# MAGIC Ένα Delta table μπορεί να είναι streaming source — διαβάζει μόνο νέα commits.

# COMMAND ----------

stream_df = spark.readStream.format("______").table(SRC)   # TODO 1: delta
print("isStreaming =", stream_df.isStreaming)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Ο επεξεργαστής micro-batch (foreachBatch)
# MAGIC
# MAGIC Θα γράψεις τη `process_batch(batch_df, batch_id)`. Σε κάθε micro-batch:
# MAGIC - **DQ split**: «κακή» γραμμή = `request_id` NULL **ή** `audit_outcome` εκτός των έγκυρων τιμών →
# MAGIC   πάει σε **quarantine**. Οι «καλές» συνεχίζουν.
# MAGIC - **Dedup**: Window `partitionBy(request_id) orderBy(request_timestamp desc)` → κράτα `row_number()==1`.
# MAGIC - **MERGE** upsert στο Silver.
# MAGIC - **Gold** KPIs (conditional counts) + **batch metric**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Γράψε τη συνάρτηση foreachBatch (το «κρέας»)

# COMMAND ----------

def process_batch(batch_df, batch_id):
    spark_b = batch_df.sparkSession
    rows_in = batch_df.count()

    # --- 2a. Data Quality flag: «κακή» γραμμή; ---
    flagged = batch_df.withColumn(
        "_bad",
        when(col("request_id").isNull() |
             (~col("audit_outcome").______(VALID_OUTCOMES)),   # TODO 2a: isin (έγκυρες τιμές)
             lit(True)).otherwise(lit(False))
    )
    good = flagged.filter(col("_bad") == False).drop("_bad")
    bad  = flagged.filter(col("_bad") == True).drop("_bad")
    rows_good, rows_quar = good.count(), bad.count()

    # --- 2b. Quarantine: γράψε τις κακές (append) ---
    if rows_quar > 0:
        bad.withColumn("_quarantined_at", current_timestamp()) \
           .write.format("delta").mode("________").option("mergeSchema","true").saveAsTable(QUARANTINE)  # TODO 2b: append

    # --- 2c. Dedup: κράτα την τελευταία version ανά request_id ---
    w = Window.partitionBy("__________").orderBy(col("request_timestamp").______())   # TODO 2c: request_id · TODO 2d: desc
    deduped = (good.withColumn("_rn", row_number().over(w))
               .filter(col("_rn") == ___)                                             # TODO 2e: 1
               .drop("_rn"))
    deduped.createOrReplaceTempView("batch_updates")

    # --- 2f. MERGE upsert στο Silver ---
    spark_b.sql(f"""
        MERGE INTO {SILVER} t
        USING batch_updates s
        ON t.request_id = s.request_id
        WHEN ________ THEN UPDATE SET *                  -- TODO 2f: MATCHED
        WHEN ________________ THEN INSERT *              -- TODO 2g: NOT MATCHED
    """)

    # --- 2h. Gold KPIs ανά service_type (με conditional counts) ---
    (spark_b.table(SILVER).groupBy("____________")                                    # TODO 2h: service_type
        .agg(
            count("*").alias("total_requests"),
            spark_round(avg("wait_time_minutes"), 1).alias("avg_wait_min"),
            spark_sum(when(col("audit_outcome") == "________", 1).otherwise(0)).alias("flagged"),    # TODO 2i: "flagged"
            spark_sum(when(col("audit_outcome") == "rejected", 1).otherwise(0)).alias("rejected"),
        )
        .withColumn("pct_flagged", spark_round(col("flagged") / col("total_requests") * 100, 1))
        .write.format("delta").mode("__________").option("overwriteSchema","true").saveAsTable(GOLD))  # TODO 2j: overwrite

    # --- 2k. Batch metric ---
    (spark_b.createDataFrame([(batch_id, rows_in, rows_good, rows_quar)],
        ["batch_id","rows_in","rows_good","rows_quarantined"])
        .withColumn("logged_at", current_timestamp())
        .write.format("delta").mode("append").saveAsTable(BATCHLOG))
    print(f"  ▶ batch {batch_id}: in={rows_in} good={rows_good} quarantined={rows_quar}")

print("✓ process_batch ορίστηκε")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Run 1: τρέξε το streaming upsert
# MAGIC
# MAGIC `foreachBatch` → η συνάρτησή σου · `availableNow` → επεξεργάσου ό,τι υπάρχει & σταμάτα.

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
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Incremental + DQ + dedup + exactly-once
# MAGIC
# MAGIC Append στο source: id=5 update· **2 versions** του 10001 (dedup κρατά τη νέα)· **1 κακή** γραμμή
# MAGIC (id=10002, `audit_outcome="???"`) → πρέπει να πάει **quarantine**, ΟΧΙ στο Silver.

# COMMAND ----------

# DBTITLE 1,Append νέα δεδομένα (έτοιμο)
tmpl = spark.table(SRC).filter("request_id = 1").limit(1)
upd5    = spark.table(SRC).filter("request_id = 5").limit(1).withColumn("audit_outcome", lit("rejected"))
new_old = tmpl.withColumn("request_id", lit(10001)).withColumn("audit_outcome", lit("passed")).withColumn("request_timestamp", to_timestamp(lit("2024-01-01 00:00:00")))
new_new = tmpl.withColumn("request_id", lit(10001)).withColumn("audit_outcome", lit("rejected")).withColumn("request_timestamp", to_timestamp(lit("2024-12-31 00:00:00")))
bad_row = tmpl.withColumn("request_id", lit(10002)).withColumn("audit_outcome", lit("???"))   # ⚠️ κακή
upd5.unionByName(new_old).unionByName(new_new).unionByName(bad_row).write.format("delta").mode("append").saveAsTable(SRC)
print("✓ Appended 4 rows (1 update + 2 versions του 10001 + 1 κακή)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Run 2 (incremental) — γράψε όλο το writeStream

# COMMAND ----------

(spark.readStream.format("______").table(SRC).writeStream     # TODO 4a: delta
    .foreachBatch(________________)                           # TODO 4b: process_batch
    .option("checkpointLocation", ________)                   # TODO 4c: CKPT
    .trigger(availableNow=True)
    .start()).awaitTermination()
run2 = spark.table(SILVER).count()
print(f"✓ Run 2: Silver = {run2} (+1 νέο· η κακή πήγε quarantine)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — Run 3 (idempotency): ίδιο checkpoint, χωρίς νέα δεδομένα

# COMMAND ----------

(spark.readStream.format("delta").table(SRC).writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", ________)                   # TODO 5: CKPT (ίδιο → exactly-once)
    .trigger(availableNow=True)
    .start()).awaitTermination()
run3 = spark.table(SILVER).count()
print(f"✓ Run 3 (no new data): Silver = {run3} (ίδιο με run 2)")

spark.sql(f"SELECT request_id, audit_outcome FROM {SILVER} WHERE request_id IN (5,10001,10002) ORDER BY request_id").show()
print("=== Gold ==="); spark.table(GOLD).orderBy(col("total_requests").desc()).show(truncate=False)
print("=== Quarantine ==="); spark.table(QUARANTINE).select("request_id","audit_outcome").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 3

# COMMAND ----------

silver_has = lambda c: c in spark.table(SILVER).columns
gold_cols = set(spark.table(GOLD).columns)
results = {
    "Run 1: Silver = 10000":            run1 == 10000,
    "Run 2: Silver = 10001 (+1 καλό)":  run2 == 10001,
    "Run 3 idempotent (= run 2)":       run3 == run2,
    "Dedup κράτησε τη νέα του 10001":   spark.sql(f"SELECT audit_outcome o FROM {SILVER} WHERE request_id=10001").collect()[0]["o"] == "rejected",
    "id=5 → rejected (update)":          spark.sql(f"SELECT audit_outcome o FROM {SILVER} WHERE request_id=5").collect()[0]["o"] == "rejected",
    "Κακή (10002) ΔΕΝ μπήκε στο Silver": spark.sql(f"SELECT count(*) c FROM {SILVER} WHERE request_id=10002").collect()[0]["c"] == 0,
    "Κακή (10002) μπήκε στο Quarantine": spark.sql(f"SELECT count(*) c FROM {QUARANTINE} WHERE request_id=10002").collect()[0]["c"] >= 1,
    "Καμία διπλοεγγραφή request_id":     spark.sql(f"SELECT count(*) c FROM (SELECT request_id FROM {SILVER} GROUP BY request_id HAVING count(*)>1)").collect()[0]["c"] == 0,
    "Gold = 5 service types":            spark.table(GOLD).count() == 5,
    "Gold έχει pct_flagged":             "pct_flagged" in gold_cols,
    "Batch log έχει ≥ 2 batches":        spark.table(BATCHLOG).count() >= 2,
}
print("=" * 58)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 58)
print("🎉 Τέλος Μέρους 3!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise4_SCD2_Bonus_STARTER`
# MAGIC
# MAGIC Ο upsert κρατά μόνο την τρέχουσα κατάσταση. Στο Μέρος 4 (bonus): **SCD Type 2** — incremental
# MAGIC MERGE που κρατάει **ιστορικό** (version/valid_from/valid_to/is_current).
