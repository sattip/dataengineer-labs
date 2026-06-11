# Databricks notebook source
# MAGIC %md
# MAGIC # ⏮️ Άσκηση Ημέρα 3 — Μέρος 2/3: Time Travel + Maintenance
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~70' · **Δυσκολία:** ⭐⭐⭐ Medium-Hard
# MAGIC > Self-contained: φτιάχνει δικό του fresh table ώστε οι versions να είναι ντετερμινιστικές.
# MAGIC
# MAGIC ## 📖 Πού είμαστε
# MAGIC
# MAGIC Κάθε write σε Delta δημιουργεί **version**. Αυτό μας δίνει υπερδυνάμεις: να **γυρίσουμε
# MAGIC πίσω στον χρόνο** (audit, «ωχ έσβησα κατά λάθος»), και να **συντηρήσουμε** το table για
# MAGIC ταχύτητα & κόστος.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - **Time travel:** `VERSION AS OF` / `TIMESTAMP AS OF` — query παλιάς κατάστασης.
# MAGIC - **`RESTORE`** — επαναφορά table σε προηγούμενη version.
# MAGIC - **Small files problem** → `OPTIMIZE` (compaction) → `ZORDER BY` (data skipping).
# MAGIC - **`VACUUM`** — διαγραφή ορφανών files (κόστος + GDPR), και η **παγίδα retention**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + fresh table (έτοιμο)

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col, lit

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3"
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve(f"{REPO}/declarations.csv", f"{VOLUME}/declarations.csv")

TBL = "workspace.aade.tax_declarations_tt"   # ξεχωριστό table για αυτό το μέρος
raw = spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
base = raw.select(
    col("ΔηλωσηID").cast("int").alias("declaration_id"),
    col("ΑΦΜ").cast("string").alias("afm"),
    col("Κατηγορία_Φόρου").alias("tax_category"),
    col("Ποσό_EUR").cast("double").alias("amount_eur"),
    col("Κατάσταση").alias("status"),
    col("Περιφέρεια").alias("region"),
)
base.write.format("delta").mode("overwrite").saveAsTable(TBL)   # ← VERSION 0 (300 γραμμές)
print(f"✓ {TBL} version 0: {spark.table(TBL).count()} γραμμές")

# Δημιουργούμε ιστορικό: 2 ακόμα versions
spark.sql(f"UPDATE {TBL} SET status='Εγκεκριμένη' WHERE declaration_id=1")   # version 1
spark.sql(f"DELETE FROM {TBL} WHERE status='Απορριφθείσα'")                  # version 2
print(f"Τρέχουσες γραμμές (μετά update+delete): {spark.table(TBL).count()}")  # 266

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Δες το ιστορικό versions
# MAGIC
# MAGIC *Hint:* `DESCRIBE HISTORY`.

# COMMAND ----------

display(spark.sql(f"DESCRIBE ________ {TBL}"))   # TODO 1: HISTORY — δες versions 0,1,2 (operation: WRITE, UPDATE, DELETE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Time Travel
# MAGIC
# MAGIC Query οποιασδήποτε παλιάς κατάστασης:
# MAGIC ```sql
# MAGIC SELECT * FROM t VERSION AS OF 0;            -- η αρχική
# MAGIC SELECT * FROM t TIMESTAMP AS OF '2026-06-10';  -- ή με χρόνο
# MAGIC ```
# MAGIC Χρήσεις: audit («πώς ήταν χθες;»), reproducibility, recovery.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Query την αρχική version (0) και σύγκρινε

# COMMAND ----------

v0_count = spark.sql(f"SELECT count(*) c FROM {TBL} VERSION ___ ___ 0").collect()[0]["c"]   # TODO 2: AS OF
now_count = spark.table(TBL).count()
print(f"Version 0 (αρχική): {v0_count} γραμμές")    # 300
print(f"Τώρα (current):     {now_count} γραμμές")    # 266
print(f"Διαφορά (διαγραμμένες απορριφθείσες): {v0_count - now_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — `RESTORE`
# MAGIC
# MAGIC «Ωχ, δεν έπρεπε να σβήσω τις απορριφθείσες!» — μία εντολή το διορθώνει:
# MAGIC ```sql
# MAGIC RESTORE TABLE t TO VERSION AS OF 0;
# MAGIC ```
# MAGIC Το RESTORE είναι κι αυτό **νέα version** (δεν χάνεις το ιστορικό — μπορείς να ξανα-restore).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Επαναφορά στην version 0

# COMMAND ----------

spark.sql(f"_______ TABLE {TBL} TO VERSION AS OF 0")   # TODO 3: RESTORE
print(f"Μετά το RESTORE: {spark.table(TBL).count()} γραμμές")   # 300 ξανά

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — Small files problem → `OPTIMIZE`
# MAGIC
# MAGIC Πολλά μικρά writes (streaming/micro-batches) → εκατοντάδες μικρά Parquet files → αργά queries
# MAGIC (overhead ανοίγματος). Το `OPTIMIZE` τα **συμπυκνώνει** (compaction) σε λίγα μεγάλα.
# MAGIC Το `DESCRIBE DETAIL` δείχνει το `numFiles`.

# COMMAND ----------

# DBTITLE 1,Δημιουργία small-files problem (έτοιμο)
for i in range(6):
    one = base.limit(1).withColumn("declaration_id", lit(9100 + i))
    one.write.format("delta").mode("append").saveAsTable(TBL)
files_before = spark.sql(f"DESCRIBE DETAIL {TBL}").collect()[0]["numFiles"]
print(f"numFiles ΠΡΙΝ το OPTIMIZE: {files_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — OPTIMIZE (compaction) + Z-ORDER
# MAGIC
# MAGIC Συμπληρώστε την εντολή compaction, και μετά το `ZORDER BY` σε στήλη που φιλτράρουμε συχνά (`region`).

# COMMAND ----------

spark.sql(f"________ {TBL}")                          # TODO 4a: OPTIMIZE (compaction)
files_after = spark.sql(f"DESCRIBE DETAIL {TBL}").collect()[0]["numFiles"]
print(f"numFiles ΜΕΤΑ το OPTIMIZE: {files_after}")    # πολύ λιγότερα (συνήθως 1)

# Z-ORDER: co-locate δεδομένα ανά region → data skipping σε WHERE region=...
spark.sql(f"OPTIMIZE {TBL} ______ BY (region)")      # TODO 4b: ZORDER
print("✓ Z-ORDER ολοκληρώθηκε")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — `VACUUM` & η παγίδα retention
# MAGIC
# MAGIC Το OPTIMIZE αφήνει τα **παλιά** files (για time travel). Το `VACUUM` τα διαγράφει οριστικά
# MAGIC → λιγότερο κόστος + **GDPR** (τα σβησμένα PII φεύγουν στ' αλήθεια). ΠΑΓΙΔΑ: το default
# MAGIC retention είναι **168 ώρες (7 μέρες)** — VACUUM δεν σβήνει files νεότερα από αυτό (αλλιώς θα
# MAGIC έσπαγε running queries / time travel). Το `DRY RUN` δείχνει τι ΘΑ έσβηνε χωρίς να σβήσει.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — VACUUM (DRY RUN — ασφαλές)
# MAGIC
# MAGIC Συμπληρώστε το keyword που κάνει το VACUUM **μη καταστροφικό** (προεπισκόπηση).

# COMMAND ----------

print("=== VACUUM DRY RUN (δεν σβήνει — δείχνει υποψήφια files) ===")
display(spark.sql(f"VACUUM {TBL} RETAIN 168 HOURS ___ ___"))   # TODO 5: DRY RUN
# Σημ.: για πραγματικό vacuum < 168h θα χρειαζόταν:
#   SET spark.databricks.delta.retentionDurationCheck.enabled = false;  (προσοχή σε production!)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 2

# COMMAND ----------

results = {
    "History έχει ≥ 3 versions":     spark.sql(f"DESCRIBE HISTORY {TBL}").count() >= 3,
    "Time travel v0 = 300":          v0_count == 300,
    "RESTORE → ≥ 300 γραμμές":       spark.table(TBL).count() >= 300,
    "OPTIMEZE μείωσε τα files":      files_after < files_before,
    "Z-ORDER καταγράφηκε":           spark.sql(f"DESCRIBE HISTORY {TBL}").filter("operation = 'OPTIMIZE'").count() >= 1,
}
print("=" * 50)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 50)
print("🎉 Τέλος Μέρους 2!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise3_CDF_Incremental_STARTER`
# MAGIC
# MAGIC Ξέρουμε να αλλάζουμε & να συντηρούμε ένα table. Στο Μέρος 3: **Change Data Feed** —
# MAGIC πώς «ακούμε» μόνο τις αλλαγές και χτίζουμε **incremental** Gold refresh (production pattern).
