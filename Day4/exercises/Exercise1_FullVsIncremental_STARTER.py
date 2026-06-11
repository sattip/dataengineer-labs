# Databricks notebook source
# MAGIC %md
# MAGIC # 🔁 Άσκηση Ημέρα 4 — Μέρος 1/4: Full Load vs Incremental Load
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~75' · **Δυσκολία:** ⭐⭐ Medium
# MAGIC > **Στυλ:** Συμπληρώνετε τα `_____` σε κάθε `# TODO`. Πάνω από κάθε TODO υπάρχει 🧠 ΕΝΝΟΙΑ.
# MAGIC
# MAGIC ## 📖 Το Σενάριο
# MAGIC
# MAGIC Τα **ΚΕΠ** στέλνουν στην ΑΑΔΕ τα αιτήματα πολιτών (`kep_requests.csv`, **10.000** αιτήματα).
# MAGIC Κάθε μέρα έρχονται **νέα** αιτήματα. Το ερώτημα-κλειδί κάθε Data Engineer:
# MAGIC
# MAGIC > **Ξαναφορτώνω τα ΠΑΝΤΑ κάθε φορά (full load), ή μόνο ό,τι ΑΛΛΑΞΕ (incremental);**
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - **Full load** (overwrite) — απλό αλλά ακριβό· επεξεργάζεται **όλα** κάθε φορά.
# MAGIC - **Incremental load** με **high-water-mark** — επεξεργάζεται **μόνο τα νέα**.
# MAGIC - **Incremental upsert** με `MERGE` — διαχειρίζεται και **updates**, όχι μόνο inserts.
# MAGIC - Πώς να **αποθηκεύεις το watermark** ώστε το pipeline να ξέρει «πού έμεινα».
# MAGIC - Το trade-off: rows processed (full vs incremental) = κόστος & χρόνος.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + source table (έτοιμο)

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col, max as spark_max, lit, current_timestamp

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

SRC          = "workspace.aade.kep_requests_src"
BRONZE_FULL  = "workspace.aade.kep_bronze_full"
BRONZE_INCR  = "workspace.aade.kep_bronze_incr"
WATERMARK    = "workspace.aade.kep_watermark"

src = (spark.read.option("header","true").option("inferSchema","true")
       .csv(f"{VOLUME}/kep_requests.csv"))
src.write.format("delta").mode("overwrite").saveAsTable(SRC)
print(f"✓ Source: {spark.table(SRC).count()} αιτήματα (request_id 1..10000)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — FULL LOAD (truncate-and-reload)
# MAGIC
# MAGIC Ο πιο απλός τρόπος: κάθε run **σβήνει** τον target και τον ξαναγράφει από το πλήρες source.
# MAGIC ```python
# MAGIC source.write.format("delta").mode("overwrite").saveAsTable(target)
# MAGIC ```
# MAGIC ✅ Απλό, πάντα σωστό, idempotent.
# MAGIC ❌ Επεξεργάζεται **ΟΛΑ** τα δεδομένα κάθε φορά — με 10εκ. γραμμές, σπατάλη χρόνου/κόστους.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Full load στο Bronze
# MAGIC
# MAGIC Συμπληρώστε το write mode που **αντικαθιστά** πλήρως τον target.

# COMMAND ----------

spark.table(SRC).write.format("delta").mode("__________").saveAsTable(BRONZE_FULL)   # TODO 1: overwrite
full_processed = spark.table(SRC).count()
print(f"FULL LOAD: επεξεργάστηκε {full_processed} γραμμές → Bronze = {spark.table(BRONZE_FULL).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — INCREMENTAL με high-water-mark
# MAGIC
# MAGIC «High-water-mark» = το **τελευταίο σημείο** που επεξεργαστήκαμε (π.χ. max `request_id` ή max timestamp).
# MAGIC Το αποθηκεύουμε σε ένα μικρό table. Σε κάθε run:
# MAGIC 1. Διάβασε το watermark (π.χ. `last_id = 8000`).
# MAGIC 2. Φέρε από το source **μόνο** `request_id > last_id`.
# MAGIC 3. Γράψε (append) μόνο αυτά.
# MAGIC 4. Ενημέρωσε το watermark στο νέο max.
# MAGIC
# MAGIC Έτσι κάθε run «θυμάται πού έμεινε» και δεν ξαναδιαβάζει τα παλιά.

# COMMAND ----------

# DBTITLE 1,Προσομοίωση: «χθεσινό» initial load (id <= 8000) + watermark (έτοιμο)
# Φορτώνουμε τα πρώτα 8000 σαν να ήταν η χθεσινή φόρτωση
spark.table(SRC).filter(col("request_id") <= 8000) \
    .write.format("delta").mode("overwrite").saveAsTable(BRONZE_INCR)

# Watermark table = το τελευταίο id που επεξεργαστήκαμε
spark.createDataFrame([("kep", 8000)], ["table_name", "last_id"]) \
    .write.format("delta").mode("overwrite").saveAsTable(WATERMARK)

print(f"Initial (χθες): Bronze_incr = {spark.table(BRONZE_INCR).count()} · watermark = 8000")
print("Σήμερα έφτασαν τα αιτήματα 8001..10000 (στο source).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Incremental load: φέρε ΜΟΝΟ τα νέα
# MAGIC
# MAGIC Διαβάστε το watermark, φιλτράρετε το source `request_id > last_id`, και κάντε **append**.

# COMMAND ----------

# 2a. Διάβασε το τρέχον watermark
last_id = spark.table(WATERMARK).filter(col("table_name") == "kep") \
              .select("last_id").collect()[0]["last_id"]
print(f"Watermark: {last_id}")

# 2b. Φέρε μόνο τα νέα (request_id > watermark)
new_batch = spark.table(SRC).filter(col("request_id") ___ last_id)   # TODO 2a: τελεστής «μεγαλύτερο από»
incr_processed = new_batch.count()
print(f"INCREMENTAL: επεξεργάζεται {incr_processed} νέες γραμμές (αντί για {full_processed}!)")

# 2c. Append μόνο τα νέα
new_batch.write.format("delta").mode("________").saveAsTable(BRONZE_INCR)   # TODO 2b: append

# 2d. Ενημέρωσε το watermark στο νέο max
new_max = spark.table(SRC).select(spark_max("request_id")).collect()[0][0]
spark.createDataFrame([("kep", new_max)], ["table_name", "last_id"]) \
    .write.format("delta").mode("overwrite").saveAsTable(WATERMARK)
print(f"Bronze_incr τώρα = {spark.table(BRONZE_INCR).count()} · νέο watermark = {new_max}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — Incremental APPEND vs UPSERT
# MAGIC
# MAGIC Το append-by-watermark είναι τέλειο για **insert-only** δεδομένα. Αλλά τι γίνεται αν μια
# MAGIC **υπάρχουσα** εγγραφή **αλλάξει** (π.χ. το `audit_outcome` ενός παλιού αιτήματος ενημερώθηκε);
# MAGIC Το watermark-append δεν θα το πιάσει (το id δεν είναι «νέο»). Λύση: **`MERGE`** (upsert) —
# MAGIC update αν υπάρχει, insert αν όχι.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Incremental UPSERT με MERGE
# MAGIC
# MAGIC Το `daily_changes` έχει 1 **update** (id=5, νέο outcome) + 2 **νέα** (id 10001,10002).
# MAGIC Συμπληρώστε τα MERGE clauses.

# COMMAND ----------

daily_changes = spark.createDataFrame([
    (5,     "rejected"),    # υπάρχον → update
    (10001, "passed"),      # νέο → insert
    (10002, "flagged"),     # νέο → insert
], ["request_id", "audit_outcome"])
daily_changes.createOrReplaceTempView("kep_daily_changes")

before = spark.table(BRONZE_INCR).count()
spark.sql(f"""
    MERGE INTO {BRONZE_INCR} t
    USING kep_daily_changes s
    ON t.request_id = s.request_id
    WHEN __________ THEN UPDATE SET t.audit_outcome = s.audit_outcome              -- TODO 3a: MATCHED
    WHEN __________________ THEN INSERT (request_id, audit_outcome)                -- TODO 3b: NOT MATCHED
        VALUES (s.request_id, s.audit_outcome)
""")
after = spark.table(BRONZE_INCR).count()
print(f"Πριν MERGE: {before} · Μετά: {after} (+2 inserts, 1 update χωρίς αύξηση)")
spark.sql(f"SELECT request_id, audit_outcome FROM {BRONZE_INCR} WHERE request_id IN (5,10001,10002) ORDER BY request_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 1

# COMMAND ----------

expected_new = spark.table(SRC).filter("request_id > 8000").count()   # 2000
results = {
    "Full load Bronze = 10000":          spark.table(BRONZE_FULL).count() == 10000,
    "Incremental επεξεργάστηκε μόνο τα νέα": incr_processed == expected_new,
    "Incremental << Full (κόστος)":      incr_processed < full_processed,
    "Bronze_incr = 10000 (8000+2000)":   spark.table(BRONZE_INCR).filter("request_id <= 10000").count() == 10000,
    "Watermark ενημερώθηκε σε 10000":    spark.table(WATERMARK).filter("table_name='kep'").select("last_id").collect()[0]["last_id"] == 10000,
    "MERGE: id=5 → rejected":            spark.sql(f"SELECT audit_outcome o FROM {BRONZE_INCR} WHERE request_id=5").collect()[0]["o"] == "rejected",
    "MERGE: id=10001 προστέθηκε":        spark.sql(f"SELECT count(*) c FROM {BRONZE_INCR} WHERE request_id=10001").collect()[0]["c"] == 1,
}
print("=" * 55)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 55)
print(f"💡 Full load επεξεργάστηκε {full_processed} · Incremental μόνο {incr_processed} → {round(100*(1-incr_processed/full_processed))}% λιγότερη δουλειά")
print("🎉 Τέλος Μέρους 1!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise2_AutoLoader_STARTER`
# MAGIC
# MAGIC Το watermark δουλεύει, αλλά πρέπει να το διαχειριζόμαστε εμείς. Στο Μέρος 2: **Auto Loader** —
# MAGIC το Databricks κρατάει αυτόματα «τι αρχεία έχω ήδη διαβάσει» μέσω **checkpoint**, για incremental
# MAGIC ingestion **αρχείων** που προσγειώνονται συνεχώς.
