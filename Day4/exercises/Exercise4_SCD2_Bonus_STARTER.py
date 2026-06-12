# Databricks notebook source
# MAGIC %md
# MAGIC # 🕰️ Άσκηση Ημέρα 4 — Μέρος 4/4 (Bonus): SCD Type 2 (Incremental με Ιστορικό)
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~60' · **Δυσκολία:** ⭐⭐⭐⭐ Advanced · **~12 TODOs**
# MAGIC > Self-contained. Το πιο εξελιγμένο incremental pattern.
# MAGIC
# MAGIC ## 📖 Το πρόβλημα
# MAGIC
# MAGIC Ο upsert κρατά **μόνο την τρέχουσα** τιμή — χάνεις ιστορία. Το **SCD Type 2** κρατά **κάθε version**
# MAGIC με `version` / `valid_from` / `valid_to` / `is_current`, ώστε να απαντάς *«ποιο ήταν το outcome
# MAGIC **τον Μάρτιο**;»*. Θα φτιάξετε μια **επαναχρησιμοποιήσιμη** `apply_scd2()` που:
# MAGIC - ανιχνεύει αλλαγή σε **πολλαπλά** πεδία (`audit_outcome` **ή** `wait_time_minutes`),
# MAGIC - **κλείνει** την παλιά version και ανοίγει **νέα** με αυξημένο `version`,
# MAGIC - την τρέχουμε για **δύο** ημέρες αλλαγών (ώστε μια εγγραφή να φτάσει 3 versions).
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - Το **2-part source** trick (`mergeKey`) — close-old + insert-new σε ένα MERGE.
# MAGIC - Multi-attribute change detection · version numbering.
# MAGIC - Query «as of» / πλήρες ιστορικό μιας εγγραφής.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + initial dimension (έτοιμο)

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
DIM = "workspace.aade.dim_request_scd2"
spark.sql(f"DROP TABLE IF EXISTS {DIM}")

# Initial dimension — 6 αιτήματα, version=1, όλα current
initial = spark.createDataFrame(
    [(1,"passed",30),(2,"passed",40),(3,"flagged",50),
     (4,"passed",60),(5,"rejected",70),(6,"passed",80)],
    ["request_id","audit_outcome","wait_time_minutes"]
).withColumn("version", lit(1)) \
 .withColumn("valid_from", current_timestamp()) \
 .withColumn("valid_to", lit(None).cast("timestamp")) \
 .withColumn("is_current", lit(True))
initial.write.format("delta").mode("overwrite").saveAsTable(DIM)
print(f"✓ Initial dim: {spark.table(DIM).count()} (όλα is_current=true, version=1)")
spark.table(DIM).orderBy("request_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Το 2-part source + reusable apply_scd2()
# MAGIC
# MAGIC Ένα MERGE ματσάρει **μία** target γραμμή ανά source γραμμή → δεν κλείνει & εισάγει ταυτόχρονα.
# MAGIC Λύση: για κάθε αλλαγμένο key φτιάχνουμε **δύο** source rows:
# MAGIC 1. `mergeKey = request_id` → **ματσάρει & κλείνει** την παλιά,
# MAGIC 2. `mergeKey = NULL` → δεν ματσάρει ποτέ → **εισάγεται** ως νέα version.
# MAGIC
# MAGIC Η αλλαγή ανιχνεύεται σε **οποιοδήποτε** από τα tracked πεδία (`audit_outcome` ή `wait_time_minutes`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Η συνάρτηση apply_scd2()

# COMMAND ----------

def apply_scd2(changes):
    """changes: DataFrame [request_id, audit_outcome, wait_time_minutes]"""
    current = spark.table(DIM).filter("is_current = true")

    # Part A: «close» rows — mergeKey = request_id (version placeholder=1 για brand-new keys)
    close_rows = changes.withColumn("mergeKey", col("request_id")).withColumn("new_version", lit(1))

    # Part B: «insert new version» rows — ΜΟΝΟ keys που υπάρχουν & άλλαξαν (mergeKey=NULL)
    changed = (
        changes.alias("c")
        .join(current.alias("d"), "request_id")
        .where(
            (col("c.audit_outcome") != col("d.audit_outcome")) |
            (col("c.wait_time_minutes") ___ col("d.wait_time_minutes"))        # TODO 1a: != (δεύτερο tracked πεδίο)
        )
        .select("c.request_id", "c.audit_outcome", "c.wait_time_minutes",
                (col("d.version") + ___).alias("new_version"))                 # TODO 1b: 1 (αύξησε version)
        .withColumn("mergeKey", lit(______).cast("int"))                       # TODO 1c: None (δεν ματσάρει ποτέ)
    )

    staged = close_rows.unionByName(changed)
    staged.createOrReplaceTempView("scd2_staged")

    spark.sql(f"""
        MERGE INTO {DIM} t
        USING scd2_staged s
        ON t.request_id = s.mergeKey AND t.is_current = ______                 -- TODO 1d: true
        WHEN MATCHED AND (t.audit_outcome <> s.audit_outcome
                          OR t.wait_time_minutes ___ s.wait_time_minutes) THEN -- TODO 1e: <>
            UPDATE SET t.is_current = ______, t.valid_to = current_timestamp() -- TODO 1f: false (κλείσε)
        WHEN NOT MATCHED THEN
            INSERT (request_id, audit_outcome, wait_time_minutes, version, valid_from, valid_to, is_current)
            VALUES (s.request_id, s.audit_outcome, s.wait_time_minutes, s.new_version,
                    current_timestamp(), NULL, ______)                          -- TODO 1g: true (νέα current)
    """)
    print("  ✓ apply_scd2 εφαρμόστηκε")

print("✓ apply_scd2() ορίστηκε")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Ημέρα 2 αλλαγές
# MAGIC
# MAGIC id=2 άλλαξε outcome· id=3 άλλαξε **μόνο wait_time** (δοκιμάζει το OR)· id=7 νέο.

# COMMAND ----------

day2 = spark.createDataFrame(
    [(2,"rejected",40),(3,"flagged",999),(7,"passed",25)],
    ["request_id","audit_outcome","wait_time_minutes"]
)
________(day2)                                          # TODO 2: apply_scd2(day2)
print(f"Μετά Ημέρα 2: total={spark.table(DIM).count()} · current={spark.table(DIM).filter('is_current').count()}")
# Αναμένεται: total=9, current=7

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Ημέρα 3 αλλαγές
# MAGIC
# MAGIC id=2 ξανά (wait change → 3η version)· id=8 νέο.

# COMMAND ----------

day3 = spark.createDataFrame(
    [(2,"rejected",500),(8,"flagged",15)],
    ["request_id","audit_outcome","wait_time_minutes"]
)
________(day3)                                          # TODO 3: apply_scd2(day3)
print(f"Μετά Ημέρα 3: total={spark.table(DIM).count()} · current={spark.table(DIM).filter('is_current').count()}")
# Αναμένεται: total=11, current=8

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Πλήρες ιστορικό του request_id=2 («as of» όλες οι versions)
# MAGIC
# MAGIC Δείξτε όλες τις versions του 2, ταξινομημένες κατά version.

# COMMAND ----------

print("=== Ιστορικό request_id=2 (3 versions) ===")
(spark.table(DIM)
    .filter(col("request_id") == ___)                  # TODO 4: 2
    .orderBy("version")
    .select("request_id","version","audit_outcome","wait_time_minutes","valid_to","is_current")
    .show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 4

# COMMAND ----------

total     = spark.table(DIM).count()
current_n = spark.table(DIM).filter("is_current = true").count()
history_n = spark.table(DIM).filter("is_current = false").count()
id2_versions = spark.table(DIM).filter("request_id=2").count()
id2_current  = spark.sql(f"SELECT version v, audit_outcome o FROM {DIM} WHERE request_id=2 AND is_current=true").collect()
multi_current = spark.sql(f"SELECT count(*) c FROM (SELECT request_id FROM {DIM} WHERE is_current=true GROUP BY request_id HAVING count(*)>1)").collect()[0]["c"]

results = {
    "Total = 11":                       total == 11,
    "Current = 8 (ids 1-8)":            current_n == 8,
    "History (closed) = 3":             history_n == 3,
    "id=2 έχει 3 versions":             id2_versions == 3,
    "id=2 current version = 3":         len(id2_current)==1 and id2_current[0]["v"] == 3,
    "1 current ανά key (no dup)":       multi_current == 0,
    "id=3 wait-only change tracked":    spark.sql(f"SELECT count(*) c FROM {DIM} WHERE request_id=3").collect()[0]["c"] == 2,
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
# MAGIC Μέρος 1: Full (overwrite) vs Incremental (watermark append + MERGE upsert) + audit log
# MAGIC Μέρος 2: Auto Loader — incremental ΑΡΧΕΙΩΝ (checkpoint, rescued data, Silver agg)
# MAGIC Μέρος 3: Structured Streaming + foreachBatch (dedup + MERGE + Gold + exactly-once)
# MAGIC Μέρος 4: SCD Type 2 — incremental ΜΕ ιστορικό (version/valid_from/valid_to/is_current)
# MAGIC ```
# MAGIC **Cleanup (προαιρετικό):**
# MAGIC ```python
# MAGIC # for t in ["kep_requests_src","kep_bronze_full","kep_bronze_incr","kep_watermark","etl_audit_log",
# MAGIC #           "kep_bronze_autoloader","kep_silver_by_service","kep_stream_src","kep_silver_stream",
# MAGIC #           "kep_gold_service_live","kep_stream_batchlog","dim_request_scd2"]:
# MAGIC #     spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")
# MAGIC ```
