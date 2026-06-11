# Databricks notebook source
# MAGIC %md
# MAGIC # 🕰️ Άσκηση Ημέρα 4 — Μέρος 4/4 (Bonus): SCD Type 2 (Incremental με Ιστορικό)
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~50' · **Δυσκολία:** ⭐⭐⭐⭐ Advanced
# MAGIC > Self-contained. Το πιο εξελιγμένο incremental pattern.
# MAGIC
# MAGIC ## 📖 Το πρόβλημα
# MAGIC
# MAGIC Ο upsert (Μέρος 1 & 3) κρατάει **μόνο την τρέχουσα** τιμή — χάνεις την ιστορία. Αλλά συχνά
# MAGIC θες να ξέρεις *«ποιο ήταν το audit_outcome του αιτήματος **τον Μάρτιο**;»* (audit/compliance).
# MAGIC Το **SCD Type 2** κρατάει **κάθε version** μιας εγγραφής με `valid_from` / `valid_to` / `is_current`.
# MAGIC
# MAGIC | request_id | audit_outcome | valid_from | valid_to | is_current |
# MAGIC |---|---|---|---|---|
# MAGIC | 2 | passed | Jan 1 | **Mar 5** | **false** |  ← παλιά version (κλειστή) |
# MAGIC | 2 | rejected | **Mar 5** | null | **true** |  ← τρέχουσα |
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - Το **SCD2 MERGE pattern** (2-part source με `mergeKey`).
# MAGIC - Πώς «κλείνεις» την παλιά version (`is_current=false`, `valid_to=now`) και ανοίγεις νέα.
# MAGIC - Γιατί χρειάζεται το `mergeKey = null` trick για να γίνει το insert.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + initial dimension (έτοιμο)

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
DIM = "workspace.aade.dim_request_scd2"
spark.sql(f"DROP TABLE IF EXISTS {DIM}")

# Initial load — 5 αιτήματα, όλα «τρέχοντα»
initial = spark.createDataFrame(
    [(1,"passed"),(2,"passed"),(3,"flagged"),(4,"passed"),(5,"rejected")],
    ["request_id","audit_outcome"]
).withColumn("valid_from", current_timestamp()) \
 .withColumn("valid_to", lit(None).cast("timestamp")) \
 .withColumn("is_current", lit(True))

initial.write.format("delta").mode("overwrite").saveAsTable(DIM)
print(f"✓ Initial dim: {spark.table(DIM).count()} γραμμές (όλες is_current=true)")
spark.table(DIM).orderBy("request_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Τα νέα δεδομένα (incremental batch)
# MAGIC
# MAGIC Σήμερα: το audit ξανάγινε για τα **2** και **3** (άλλαξε outcome), και ήρθε νέο αίτημα **6**.

# COMMAND ----------

changes = spark.createDataFrame(
    [(2,"rejected"),(3,"passed"),(6,"passed")],
    ["request_id","audit_outcome"]
)
changes.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Το 2-part source (το «κόλπο» του SCD2)
# MAGIC
# MAGIC Ένα MERGE ταιριάζει **μία** target γραμμή ανά source γραμμή — άρα δεν μπορεί ταυτόχρονα να
# MAGIC *κλείσει* την παλιά **και** να *εισάγει* τη νέα για το ίδιο key. Λύση: φτιάχνουμε source με
# MAGIC **δύο** γραμμές για κάθε αλλαγμένο key:
# MAGIC 1. μία με `mergeKey = request_id` → θα **ματσάρει & κλείσει** την παλιά,
# MAGIC 2. μία με `mergeKey = NULL` → δεν ματσάρει ποτέ → θα **εισαχθεί** ως νέα version.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Φτιάξτε το 2-part staged source

# COMMAND ----------

# Μέρος Α: «close» rows — mergeKey = request_id
close_rows = changes.withColumn("mergeKey", col("request_id"))

# Μέρος Β: «insert» rows — ΜΟΝΟ για keys που υπάρχουν & άλλαξαν, με mergeKey = NULL
existing_changed = (
    changes.alias("c")
    .join(spark.table(DIM).filter("is_current = true").alias("d"), "request_id")
    .where(col("c.audit_outcome") != col("d.audit_outcome"))
    .select("c.request_id", "c.audit_outcome")
    .withColumn("mergeKey", lit(______).cast("int"))     # TODO 1: τιμή που ΔΕΝ ματσάρει ποτέ → None
)

staged = close_rows.unionByName(existing_changed)
staged.createOrReplaceTempView("scd2_staged")
print("Staged rows (close + insert):")
staged.orderBy("request_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Το SCD2 MERGE
# MAGIC
# MAGIC Συμπληρώστε: το `is_current` flag στο match, το close (`is_current=false`, `valid_to=now`),
# MAGIC και το insert της νέας version (`is_current=true`).

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {DIM} t
    USING scd2_staged s
    ON t.request_id = s.mergeKey AND t.is_current = ______           -- TODO 2a: true (μόνο τρέχουσες)
    WHEN MATCHED AND t.audit_outcome <> s.audit_outcome THEN
        UPDATE SET t.is_current = ______, t.valid_to = current_timestamp()   -- TODO 2b: false (κλείσε την παλιά)
    WHEN NOT MATCHED THEN
        INSERT (request_id, audit_outcome, valid_from, valid_to, is_current)
        VALUES (s.request_id, s.audit_outcome, current_timestamp(), NULL, ______)   -- TODO 2c: true (νέα τρέχουσα)
""")
print("✓ SCD2 MERGE ολοκληρώθηκε")
spark.table(DIM).orderBy("request_id", "valid_from").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 4

# COMMAND ----------

total      = spark.table(DIM).count()
current_n  = spark.table(DIM).filter("is_current = true").count()
history_n  = spark.table(DIM).filter("is_current = false").count()
id2_current = spark.sql(f"SELECT audit_outcome o FROM {DIM} WHERE request_id=2 AND is_current=true").collect()
id2_closed  = spark.sql(f"SELECT count(*) c FROM {DIM} WHERE request_id=2 AND is_current=false AND valid_to IS NOT NULL").collect()[0]["c"]

results = {
    "Σύνολο = 8 (5 + 2 νέες versions + 1 νέο)": total == 8,
    "Τρέχουσες (is_current) = 6":               current_n == 6,
    "Ιστορικές (κλειστές) = 2":                 history_n == 2,
    "id=2 τρέχον outcome = rejected":           len(id2_current) == 1 and id2_current[0]["o"] == "rejected",
    "id=2 παλιά version κλειστή (valid_to set)": id2_closed == 1,
    "id=6 (νέο) προστέθηκε ως current":         spark.sql(f"SELECT count(*) c FROM {DIM} WHERE request_id=6 AND is_current=true").collect()[0]["c"] == 1,
}
print("=" * 55)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 55)
print("🏆 BONUS ΟΛΟΚΛΗΡΩΘΗΚΕ — ξέρετε SCD2!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Σύνοψη Ημέρας 4 — Full Load vs Incremental
# MAGIC
# MAGIC ```
# MAGIC Μέρος 1: Full load (overwrite) vs Incremental (watermark append + MERGE upsert)
# MAGIC Μέρος 2: Auto Loader — incremental ingestion ΑΡΧΕΙΩΝ (checkpoint = αυτόματο watermark)
# MAGIC Μέρος 3: Structured Streaming + foreachBatch MERGE = streaming upsert (exactly-once)
# MAGIC Μέρος 4: SCD Type 2 — incremental ΜΕ ιστορικό (valid_from/valid_to/is_current)
# MAGIC ```
# MAGIC **Ο κανόνας:** full load όταν είναι μικρό/απλό· incremental όταν μετράει το κόστος/χρόνος.
# MAGIC
# MAGIC **Cleanup (προαιρετικό):**
# MAGIC ```python
# MAGIC # for t in ["kep_requests_src","kep_bronze_full","kep_bronze_incr","kep_watermark",
# MAGIC #           "kep_bronze_autoloader","kep_stream_src","kep_silver_stream","dim_request_scd2"]:
# MAGIC #     spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")
# MAGIC ```
