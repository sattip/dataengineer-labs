# Databricks notebook source
# MAGIC %md
# MAGIC # 🌊 Άσκηση Ημέρα 4 — Μέρος 3/4: Structured Streaming + foreachBatch MERGE
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~60' · **Δυσκολία:** ⭐⭐⭐ Hard
# MAGIC > Self-contained.
# MAGIC
# MAGIC ## 📖 Πού είμαστε
# MAGIC
# MAGIC Auto Loader = incremental **αρχεία**. Τώρα: **Structured Streaming** = το ίδιο μοντέλο για
# MAGIC **συνεχή** incremental ροή από οποιοδήποτε source (Delta, Kafka, files). Και το production
# MAGIC pattern για **streaming upsert**: `foreachBatch` + `MERGE` — κάθε micro-batch κάνει upsert
# MAGIC στο Silver, με **exactly-once** εγγύηση μέσω checkpoint.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - `spark.readStream` από Delta table (streaming source) · incremental = μόνο νέα commits.
# MAGIC - **`foreachBatch`** — εφαρμόζεις *batch* λογική (όπως MERGE) μέσα σε stream.
# MAGIC - Streaming **upsert** (MERGE μέσα στο foreachBatch).
# MAGIC - **Exactly-once / idempotency** — re-run δεν διπλοεγγράφει (checkpoint).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + source + empty Silver (έτοιμο)

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

SRC    = "workspace.aade.kep_stream_src"
SILVER = "workspace.aade.kep_silver_stream"
CKPT   = f"{VOLUME}/_checkpoints/kep_silver_stream"

# Clean state
dbutils.fs.rm(CKPT, recurse=True)
spark.sql(f"DROP TABLE IF EXISTS {SILVER}")
spark.sql(f"DROP TABLE IF EXISTS {SRC}")

# Source Delta table (append-only log)
(spark.read.option("header","true").option("inferSchema","true")
 .csv(f"{VOLUME}/kep_requests.csv")
 .write.format("delta").mode("overwrite").saveAsTable(SRC))

# Empty Silver με ίδιο schema (ο target του upsert)
spark.sql(f"CREATE TABLE {SILVER} AS SELECT * FROM {SRC} WHERE 1=0")
print(f"✓ Source = {spark.table(SRC).count()} · Silver = {spark.table(SILVER).count()} (κενό)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Streaming read από Delta
# MAGIC
# MAGIC Ένα Delta table μπορεί να είναι **streaming source**: `spark.readStream.format("delta").table(SRC)`.
# MAGIC Το stream διαβάζει **μόνο νέα** commits (append) — δεν ξαναδιαβάζει τα παλιά (το checkpoint θυμάται).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Streaming reader από το Delta source

# COMMAND ----------

stream_df = spark.readStream.format("______").table(SRC)   # TODO 1: delta
print("isStreaming =", stream_df.isStreaming)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — `foreachBatch` + MERGE = streaming upsert
# MAGIC
# MAGIC Το `foreachBatch(fn)` καλεί τη `fn(batch_df, batch_id)` για **κάθε** micro-batch — εκεί τρέχεις
# MAGIC κανονική **batch** λογική (το MERGE δεν δουλεύει απευθείας σε stream, αλλά μέσα σε foreachBatch ναι).
# MAGIC ```python
# MAGIC def upsert(batch_df, batch_id):
# MAGIC     batch_df.createOrReplaceTempView("updates")
# MAGIC     spark.sql("MERGE INTO silver t USING updates s ON t.id=s.id WHEN MATCHED ... WHEN NOT MATCHED ...")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Η συνάρτηση upsert (MERGE)
# MAGIC
# MAGIC Συμπληρώστε τα MERGE clauses (upsert όλων των στηλών με `*`).

# COMMAND ----------

def upsert_to_silver(batch_df, batch_id):
    batch_df.createOrReplaceTempView("kep_updates")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {SILVER} t
        USING kep_updates s
        ON t.request_id = s.request_id
        WHEN __________ THEN UPDATE SET *              -- TODO 2a: MATCHED
        WHEN __________________ THEN INSERT *          -- TODO 2b: NOT MATCHED
    """)

print("✓ upsert function ορίστηκε")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Τρέξτε το streaming upsert (run 1)
# MAGIC
# MAGIC Συμπληρώστε `foreachBatch`, checkpoint, trigger.

# COMMAND ----------

q = (
    stream_df.writeStream
    .foreachBatch(________________)                   # TODO 3a: upsert_to_silver
    .option("checkpointLocation", CKPT)
    .trigger(__________=True)                          # TODO 3b: availableNow
    .start()
)
q.awaitTermination()
run1 = spark.table(SILVER).count()
print(f"✓ Run 1: Silver = {run1}")   # 10000 (όλα τα distinct request_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — Incremental + exactly-once
# MAGIC
# MAGIC Τώρα προσθέτουμε **νέα δεδομένα** στο source (append): 1 update (υπάρχον id=5) + 1 νέο (id=10001).
# MAGIC Ξανατρέχοντας τον **ίδιο** stream, διαβάζει **μόνο** τα 2 νέα commits → το MERGE κάνει
# MAGIC update+insert. Re-run χωρίς νέα δεδομένα → **τίποτα** (idempotent).

# COMMAND ----------

# Append νέα δεδομένα στο source. Τα φτιάχνουμε από υπάρχουσες γραμμές ώστε να ταιριάζει το schema:
from pyspark.sql.functions import lit
upd = spark.table(SRC).filter("request_id = 5").limit(1).withColumn("audit_outcome", lit("rejected"))   # update id=5
new = (spark.table(SRC).filter("request_id = 1").limit(1)
       .withColumn("request_id", lit(10001)).withColumn("audit_outcome", lit("passed")))                # new id=10001
upd.unionByName(new).write.format("delta").mode("append").saveAsTable(SRC)

q2 = (
    spark.readStream.format("delta").table(SRC).writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", CKPT)
    .trigger(availableNow=True)
    .start()
)
q2.awaitTermination()
run2 = spark.table(SILVER).count()
print(f"✓ Run 2: Silver = {run2} (+1 νέο id=10001· το id=5 έγινε update)")
spark.sql(f"SELECT request_id, audit_outcome FROM {SILVER} WHERE request_id IN (5,10001) ORDER BY request_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 3

# COMMAND ----------

results = {
    "Run 1: Silver = 10000":          run1 == 10000,
    "Run 2: Silver = 10001 (+1)":     run2 == 10001,
    "id=5 έγινε update → rejected":    spark.sql(f"SELECT audit_outcome o FROM {SILVER} WHERE request_id=5").collect()[0]["o"] == "rejected",
    "id=10001 προστέθηκε":            spark.sql(f"SELECT count(*) c FROM {SILVER} WHERE request_id=10001").collect()[0]["c"] == 1,
    "Καμία διπλοεγγραφή request_id":  spark.sql(f"SELECT count(*) c FROM (SELECT request_id FROM {SILVER} GROUP BY request_id HAVING count(*)>1)").collect()[0]["c"] == 0,
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
# MAGIC Το MERGE-upsert κρατάει **μόνο την τρέχουσα** κατάσταση. Στο Μέρος 4 (bonus): **SCD Type 2** —
# MAGIC incremental MERGE που κρατάει **ιστορικό** (valid_from/valid_to/is_current).
