# Databricks notebook source
# MAGIC %md
# MAGIC # 🔁 Άσκηση Ημέρα 4 — Μέρος 1/4: Full Load vs Incremental Load
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~80' · **Δυσκολία:** ⭐⭐ Medium · **~16 TODOs**
# MAGIC > **Στυλ:** Συμπληρώνετε τα `_____`. Πάνω από κάθε ομάδα TODO υπάρχει 🧠 ΕΝΝΟΙΑ.
# MAGIC
# MAGIC ## 📖 Το Σενάριο
# MAGIC
# MAGIC Τα **ΚΕΠ** στέλνουν στην ΑΑΔΕ τα αιτήματα πολιτών (`kep_requests.csv`, **10.000** αιτήματα).
# MAGIC Κάθε μέρα έρχονται **νέα** και ενημερώνονται **υπάρχοντα**. Θα χτίσετε **τρεις** στρατηγικές
# MAGIC φόρτωσης, θα τις **μετρήσετε** με ένα audit log, και θα αποφασίσετε ποια συμφέρει.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - Typed ingestion (cast timestamps/numerics) + **audit columns** (`_run_id`, `_load_type`, `_loaded_at`).
# MAGIC - **Audit log table** — μετράμε rows_processed ανά run (η βάση κάθε observable pipeline).
# MAGIC - **Full load** (overwrite) με validation gate.
# MAGIC - **Incremental** με **high-water-mark** (read → filter → append → advance watermark).
# MAGIC - **Incremental upsert** με `MERGE` (πολλαπλές στήλες, late updates).
# MAGIC - **Reconciliation** — απόδειξη ότι incremental == full, με πολύ λιγότερη δουλειά.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + typed source (έτοιμο)

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

SRC          = "workspace.aade.kep_requests_src"
BRONZE_FULL  = "workspace.aade.kep_bronze_full"
BRONZE_INCR  = "workspace.aade.kep_bronze_incr"
WATERMARK    = "workspace.aade.kep_watermark"
AUDIT_LOG    = "workspace.aade.etl_audit_log"

# Typed source (request_timestamp -> timestamp, numerics -> proper types, + request_date)
raw = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{VOLUME}/kep_requests.csv")
src = (raw
    .withColumn("request_timestamp", to_timestamp("request_timestamp"))
    .withColumn("request_date", to_date("request_timestamp"))
    .withColumn("wait_time_minutes", col("wait_time_minutes").cast("int"))
    .withColumn("final_decision_amount", col("final_decision_amount").cast("double")))
src.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SRC)
print(f"✓ Source: {spark.table(SRC).count()} αιτήματα")
src.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Audit log: «πόση δουλειά έκανε κάθε run;»
# MAGIC
# MAGIC Κάθε σοβαρό pipeline κρατάει μετρικές: ποιο run, τι τύπος φόρτωσης, πόσες γραμμές επεξεργάστηκε,
# MAGIC πότε. Έτσι μπορούμε να **συγκρίνουμε** full vs incremental (κόστος) και να κάνουμε debugging.
# MAGIC Θα φτιάξουμε ένα `etl_audit_log` table + μια helper `log_load(...)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Audit log table + helper

# COMMAND ----------

# 1a: φτιάξτε (idempotent) το audit-log table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_LOG} (
        run_id          STRING,
        load_type       STRING,
        target_table    STRING,
        rows_processed  BIGINT,
        rows_in_target  BIGINT,
        logged_at       TIMESTAMP
    ) USING ________                              -- TODO 1a: delta
""")

def log_load(run_id, load_type, target, rows_processed):
    rows_in_target = spark.table(target).count()
    row = spark.createDataFrame(
        [(run_id, load_type, target, int(rows_processed), int(rows_in_target))],
        ["run_id", "load_type", "target_table", "rows_processed", "rows_in_target"]
    ).withColumn("logged_at", current_timestamp())
    # 1b: γράψτε (append) τη γραμμή μετρικής στο audit log
    row.write.format("delta").mode("________").saveAsTable(AUDIT_LOG)   # TODO 1b: append
    print(f"  📊 [{load_type}] processed={rows_processed} target={rows_in_target}")

print("✓ audit log + helper έτοιμα")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — FULL LOAD (truncate-and-reload)
# MAGIC
# MAGIC Κάθε run **αντικαθιστά** πλήρως τον target από το πλήρες source. ✅ Απλό/idempotent.
# MAGIC ❌ Επεξεργάζεται **ΟΛΑ** κάθε φορά. Προσθέτουμε audit columns + ένα validation gate
# MAGIC (το pipeline «σπάει» αν ο target δεν έχει όσες περιμέναμε).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Full load με audit columns + validation

# COMMAND ----------

run_full = uuid.uuid4().hex[:8]

audited = (spark.table(SRC)
    .withColumn("_run_id",   lit(run_full))
    .withColumn("_load_type", lit("________"))         # TODO 2a: "FULL"
    .withColumn("_loaded_at", ________()))             # TODO 2b: current_timestamp

(audited.write.format("delta")
    .mode("__________")                                # TODO 2c: overwrite
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_FULL))

full_processed = spark.table(SRC).count()
log_load(run_full, "FULL", BRONZE_FULL, full_processed)

# Validation gate: ο target ΠΡΕΠΕΙ να έχει όσες το source
target_rows = spark.table(BRONZE_FULL).count()
assert target_rows ________ full_processed, f"FULL load mismatch: {target_rows} != {full_processed}"   # TODO 2d: ==
print(f"✅ FULL load OK: {target_rows} rows (run {run_full})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — INCREMENTAL με high-water-mark
# MAGIC
# MAGIC «Watermark» = το τελευταίο σημείο που επεξεργαστήκαμε (εδώ: max `request_id`). Σε κάθε run:
# MAGIC **read watermark → filter source > watermark → append μόνο τα νέα → advance watermark**.
# MAGIC Έτσι το pipeline «θυμάται πού έμεινε» και δεν ξαναδιαβάζει τα παλιά.

# COMMAND ----------

# DBTITLE 1,Προσομοίωση «χθεσινής» κατάστασης (έτοιμο)
# Φορτώνουμε τα πρώτα 8000 σαν χθεσινή φόρτωση + watermark=8000
(spark.table(SRC).filter(col("request_id") <= 8000)
    .withColumn("_run_id", lit("seed")).withColumn("_load_type", lit("SEED"))
    .withColumn("_loaded_at", current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_INCR))
spark.sql(f"CREATE TABLE IF NOT EXISTS {WATERMARK} (table_name STRING, last_id BIGINT) USING delta")
spark.sql(f"DELETE FROM {WATERMARK} WHERE table_name = 'kep'")
spark.createDataFrame([("kep", 8000)], ["table_name", "last_id"]).write.mode("append").saveAsTable(WATERMARK)
print(f"χθες: Bronze_incr={spark.table(BRONZE_INCR).count()} · watermark=8000 · σήμερα ήρθαν 8001..10000")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Incremental load (watermark append)

# COMMAND ----------

run_incr = uuid.uuid4().hex[:8]

# 3a: διάβασε το τρέχον watermark
last_id = spark.table(WATERMARK).filter(col("table_name") == "kep").select("________").collect()[0][0]   # TODO 3a: last_id
print(f"Watermark = {last_id}")

# 3b: φέρε ΜΟΝΟ τα νέα (request_id > watermark) + audit columns
new_batch = (spark.table(SRC).filter(col("request_id") ___ last_id)                # TODO 3b: >
    .withColumn("_run_id", lit(run_incr))
    .withColumn("_load_type", lit("INCREMENTAL"))
    .withColumn("_loaded_at", current_timestamp()))
incr_processed = new_batch.count()

# 3c: append μόνο τα νέα
new_batch.write.format("delta").mode("________").saveAsTable(BRONZE_INCR)          # TODO 3c: append

# 3d: advance το watermark στο νέο max
new_max = spark.table(SRC).select(spark_max("request_id")).collect()[0][0]
spark.sql(f"DELETE FROM {WATERMARK} WHERE table_name = 'kep'")
spark.createDataFrame([("kep", int(new_max))], ["table_name", "last_id"]).write.mode("append").saveAsTable(WATERMARK)

log_load(run_incr, "INCREMENTAL", BRONZE_INCR, incr_processed)
print(f"✅ INCREMENTAL: {incr_processed} νέες (αντί {full_processed}) · νέο watermark={new_max}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — Incremental APPEND vs UPSERT
# MAGIC
# MAGIC Το watermark-append πιάνει **νέα** rows, ΟΧΙ **αλλαγές** σε παλιά (το id δεν είναι «νέο»).
# MAGIC Για late updates → **`MERGE`** (upsert): update αν υπάρχει, insert αν όχι. Εδώ ενημερώνουμε
# MAGIC **δύο** στήλες (`audit_outcome` + `wait_time_minutes`) — ρεαλιστικό «διορθωτικό» batch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Incremental UPSERT με MERGE (πολλαπλές στήλες)

# COMMAND ----------

run_merge = uuid.uuid4().hex[:8]
daily_changes = spark.createDataFrame([
    (5,     "rejected", 200),    # υπάρχον → update (outcome + wait_time)
    (42,    "flagged",  150),    # υπάρχον → update
    (10001, "passed",    30),    # νέο → insert
    (10002, "flagged",   45),    # νέο → insert
], ["request_id", "audit_outcome", "wait_time_minutes"])
daily_changes.createOrReplaceTempView("kep_daily_changes")

before = spark.table(BRONZE_INCR).count()
spark.sql(f"""
    MERGE INTO {BRONZE_INCR} t
    USING kep_daily_changes s
    ON t.request_id ___ s.request_id                                       -- TODO 4a: =
    WHEN ________ THEN UPDATE SET                                          -- TODO 4b: MATCHED
        t.audit_outcome = s.audit_outcome,
        t.wait_time_minutes = s.wait_time_minutes,
        t._load_type = 'CORRECTION',
        t._loaded_at = current_timestamp()
    WHEN ________________ THEN INSERT                                      -- TODO 4c: NOT MATCHED
        (request_id, audit_outcome, wait_time_minutes, _run_id, _load_type, _loaded_at)
        VALUES (s.request_id, s.audit_outcome, s.wait_time_minutes,
                '{run_merge}', 'INCREMENTAL', current_timestamp())
""")
after = spark.table(BRONZE_INCR).count()
log_load(run_merge, "UPSERT", BRONZE_INCR, daily_changes.count())
print(f"MERGE: {before} → {after} (+2 inserts, 2 updates)")
spark.sql(f"SELECT request_id, audit_outcome, wait_time_minutes, _load_type FROM {BRONZE_INCR} WHERE request_id IN (5,42,10001,10002) ORDER BY request_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 5 — Reconciliation & cost report
# MAGIC
# MAGIC Απόδειξη ορθότητας: το incremental target (current state) πρέπει να ταυτίζεται με το full.
# MAGIC Και από το audit log βλέπουμε **πόσο** φθηνότερο ήταν.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — Reconciliation + cost από το audit log

# COMMAND ----------

# 5a: πόσες distinct εγγραφές (current) σε καθένα; (αγνοώντας τα >10000 του merge demo)
full_distinct = spark.table(BRONZE_FULL).filter("request_id <= 10000").select("request_id").distinct().count()
incr_distinct = spark.table(BRONZE_INCR).filter("request_id <= 10000").select("request_id").distinct().count()
print(f"Full distinct (≤10000): {full_distinct} · Incr distinct (≤10000): {incr_distinct}")

# 5b: cost report — άθροισε rows_processed ανά load_type από το audit log
print("\n=== AUDIT LOG: rows_processed ανά load_type ===")
(spark.table(AUDIT_LOG)
    .groupBy("________")                                  # TODO 5: "load_type"
    .agg(count("*").alias("runs"),
         spark_sum("rows_processed").alias("total_rows"))
    .orderBy("load_type").show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 1

# COMMAND ----------

from pyspark.sql.functions import sum as _sum
expected_new = spark.table(SRC).filter("request_id > 8000").count()   # 2000
audit_full = spark.table(AUDIT_LOG).filter("load_type='FULL'").select(_sum("rows_processed")).collect()[0][0] or 0
audit_incr = spark.table(AUDIT_LOG).filter("load_type='INCREMENTAL'").select(_sum("rows_processed")).collect()[0][0] or 0

results = {
    "Source typed (request_timestamp = timestamp)": dict(spark.table(SRC).dtypes)["request_timestamp"] == "timestamp",
    "Audit log έχει ≥ 3 runs":          spark.table(AUDIT_LOG).count() >= 3,
    "Full load Bronze = 10000":         spark.table(BRONZE_FULL).filter("request_id<=10000").count() == 10000,
    "Incremental processed = νέα μόνο":  incr_processed == expected_new,
    "Incremental << Full (κόστος)":      audit_incr < audit_full,
    "Reconciliation: incr == full":      incr_distinct == full_distinct == 10000,
    "MERGE: id=5 → rejected/wait=200":   spark.sql(f"SELECT audit_outcome o, wait_time_minutes w FROM {BRONZE_INCR} WHERE request_id=5").collect()[0].asDict() == {"o":"rejected","w":200},
    "MERGE: id=10001 inserted":          spark.sql(f"SELECT count(*) c FROM {BRONZE_INCR} WHERE request_id=10001").collect()[0]["c"] == 1,
    "Watermark advanced → 10000":        spark.table(WATERMARK).filter("table_name='kep'").select("last_id").collect()[0]["last_id"] == 10000,
}
print("=" * 58)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 58)
print(f"💡 Full processed {audit_full} · Incremental {audit_incr} → {round(100*(1-audit_incr/audit_full))}% λιγότερη δουλειά")
print("🎉 Τέλος Μέρους 1!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise2_AutoLoader_STARTER`
# MAGIC
# MAGIC Διαχειριστήκαμε **εμείς** το watermark. Στο Μέρος 2: **Auto Loader** — το checkpoint κρατάει
# MAGIC αυτόματα ποια **αρχεία** διαβάστηκαν, για incremental ingestion αρχείων που προσγειώνονται συνεχώς.
