# Databricks notebook source
# MAGIC %md
# MAGIC # 🔺 Άσκηση Ημέρα 3 — Μέρος 1/3: Delta DML + MERGE
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~60' · **Δυσκολία:** ⭐⭐ Medium
# MAGIC > **Στυλ:** Συμπληρώνετε τα `_____` σε κάθε `# TODO`. Πάνω από κάθε TODO υπάρχει ένα 🧠 ΕΝΝΟΙΑ.
# MAGIC
# MAGIC ## 📖 Πού είμαστε
# MAGIC
# MAGIC Στις Ημέρες 1-2 φτιάξαμε medallion tables. Τώρα μαθαίνουμε **τι κάνει το Delta** πραγματικά
# MAGIC ξεχωριστό από απλά Parquet: **ACID transactions, UPDATE/DELETE/MERGE, history**. Αυτά είναι
# MAGIC αδύνατα σε plain Parquet (immutable files) — με Delta γίνονται με μία εντολή.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - Το **`_delta_log`** — γιατί το Delta = Parquet + transaction log.
# MAGIC - `DESCRIBE DETAIL` / `DESCRIBE HISTORY` — τι κρύβεται κάτω από ένα table.
# MAGIC - `UPDATE` / `DELETE` πάνω σε Delta (αδύνατα σε Parquet).
# MAGIC - **`MERGE INTO`** (upsert) — η flagship εντολή: «update αν υπάρχει, insert αν όχι».
# MAGIC - **Schema evolution** — προσθήκη στήλης χωρίς rewrite.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + download + base Silver table (έτοιμο)
# MAGIC
# MAGIC Φτιάχνει το `workspace.aade.tax_declarations_silver` (300 δηλώσεις) που θα δουλέψουμε.

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3"
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve(f"{REPO}/declarations.csv", f"{VOLUME}/declarations.csv")

SILVER = "workspace.aade.tax_declarations_silver"

# Build a clean Silver Delta table (ingestion ήδη καλύφθηκε στην Ημέρα 1)
raw = spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
silver = raw.select(
    col("ΔηλωσηID").cast("int").alias("declaration_id"),
    col("ΑΦΜ").cast("string").alias("afm"),
    col("Επωνυμία").alias("business_name"),
    col("Κατηγορία_Φόρου").alias("tax_category"),
    col("Ποσό_EUR").cast("double").alias("amount_eur"),
    col("Κατάσταση").alias("status"),
    col("Περιφέρεια").alias("region"),
    col("Φορ_Ετος").cast("int").alias("tax_year"),
)
silver.write.format("delta").mode("overwrite").saveAsTable(SILVER)
print(f"✓ {SILVER}: {spark.table(SILVER).count()} γραμμές")   # 300

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Delta = Parquet + `_delta_log`
# MAGIC
# MAGIC Ένα Delta table είναι Parquet files **+** ένας φάκελος `_delta_log/` με JSON commits.
# MAGIC Κάθε write είναι ένα **atomic transaction** (ACID). Αυτό το log δίνει: time travel,
# MAGIC concurrent-safe writes, UPDATE/DELETE, και ιστορικό. `DESCRIBE DETAIL` δείχνει μεταδεδομένα
# MAGIC (format, αριθμός files, μέγεθος), `DESCRIBE HISTORY` δείχνει κάθε version.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Δες τα μεταδεδομένα & το ιστορικό
# MAGIC
# MAGIC Συμπληρώστε τις δύο `DESCRIBE` εντολές. *Hint:* `DETAIL` και `HISTORY`.

# COMMAND ----------

print("=== DESCRIBE DETAIL (format, files, size) ===")
display(spark.sql(f"DESCRIBE ________ {SILVER}"))     # TODO 1a: DETAIL

print("=== DESCRIBE HISTORY (versions) ===")
display(spark.sql(f"DESCRIBE ________ {SILVER}"))     # TODO 1b: HISTORY

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — `UPDATE` & `DELETE` (αδύνατα σε plain Parquet)
# MAGIC
# MAGIC Σε Parquet, για να αλλάξεις 1 γραμμή ξαναγράφεις όλο το αρχείο. Σε Delta:
# MAGIC ```sql
# MAGIC UPDATE table SET col = value WHERE condition;
# MAGIC DELETE FROM table WHERE condition;
# MAGIC ```
# MAGIC Κάθε ένα δημιουργεί **νέα version** (το παλιό παραμένει για time travel).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — UPDATE μία δήλωση
# MAGIC
# MAGIC Η δήλωση `declaration_id = 1` εγκρίθηκε. Αλλάξτε το `status` της σε `'Εγκεκριμένη'`.

# COMMAND ----------

spark.sql(f"""
    ______ {SILVER}
    SET status = 'Εγκεκριμένη'
    WHERE declaration_id = 1
""")                                                  # TODO 2: UPDATE
print("status της δήλωσης 1:")
spark.sql(f"SELECT declaration_id, status FROM {SILVER} WHERE declaration_id = 1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — DELETE μία δήλωση
# MAGIC
# MAGIC Η δήλωση `declaration_id = 2` ακυρώθηκε λανθασμένα και πρέπει να αφαιρεθεί.

# COMMAND ----------

spark.sql(f"""
    ______ FROM {SILVER}
    WHERE declaration_id = 2
""")                                                  # TODO 3: DELETE
print(f"Γραμμές μετά το DELETE: {spark.table(SILVER).count()}")   # 299

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — `MERGE INTO` (upsert) — η flagship εντολή
# MAGIC
# MAGIC Το **MERGE** συγχωνεύει «daily updates» σε ένα table σε μία ατομική πράξη:
# MAGIC ```sql
# MAGIC MERGE INTO target t
# MAGIC USING source s  ON t.key = s.key
# MAGIC WHEN MATCHED      THEN UPDATE SET ...
# MAGIC WHEN NOT MATCHED  THEN INSERT (...) VALUES (...)
# MAGIC ```
# MAGIC «Αν το κλειδί υπάρχει → ενημέρωσέ το· αν όχι → πρόσθεσέ το». Είναι ο τυπικός τρόπος για
# MAGIC incremental loads (CDC, daily batches) χωρίς duplicates.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — MERGE daily updates
# MAGIC
# MAGIC Το `updates` έχει 3 **υπάρχουσες** δηλώσεις (3,4,5 → update) και 2 **νέες** (9001,9002 → insert).
# MAGIC Συμπληρώστε το join key και τα MATCHED/NOT MATCHED clauses.

# COMMAND ----------

updates = spark.createDataFrame([
    (3, "Εγκεκριμένη", 31369.74),
    (4, "Εγκεκριμένη", 12000.00),
    (5, "Εγκεκριμένη",  8500.00),
    (9001, "Εκκρεμής",  1000.00),
    (9002, "Εκκρεμής",  2000.00),
], ["declaration_id", "status", "amount_eur"])
updates.createOrReplaceTempView("declaration_updates")

spark.sql(f"""
    MERGE INTO {SILVER} t
    USING declaration_updates s
    ON t.declaration_id ___ s.declaration_id                                  -- TODO 4a: τελεστής ισότητας (=)
    WHEN __________ THEN UPDATE SET t.status = s.status, t.amount_eur = s.amount_eur   -- TODO 4b: MATCHED
    WHEN __________________ THEN INSERT (declaration_id, status, amount_eur)           -- TODO 4c: NOT MATCHED
        VALUES (s.declaration_id, s.status, s.amount_eur)
""")
print(f"Γραμμές μετά το MERGE: {spark.table(SILVER).count()}")   # 301 (299 + 2 inserts)
spark.sql(f"SELECT declaration_id, status, amount_eur FROM {SILVER} WHERE declaration_id IN (3,9001,9002) ORDER BY declaration_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — Schema Evolution
# MAGIC
# MAGIC Οι ανάγκες αλλάζουν — θέλουμε νέα στήλη `review_note`. Σε Delta προστίθεται **χωρίς rewrite**:
# MAGIC ```sql
# MAGIC ALTER TABLE t ADD COLUMNS (review_note string);
# MAGIC ```
# MAGIC (Εναλλακτικά, σε write: `.option("mergeSchema","true")`.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — Προσθέστε στήλη `review_note`

# COMMAND ----------

spark.sql(f"ALTER TABLE {SILVER} ___ COLUMNS (review_note string)")   # TODO 5: ADD
print("Στήλες τώρα:", spark.table(SILVER).columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 1

# COMMAND ----------

detail = spark.sql(f"DESCRIBE DETAIL {SILVER}").collect()[0]
results = {
    "Format = delta":               detail["format"] == "delta",
    "declaration_id=1 εγκρίθηκε":    spark.sql(f"SELECT status FROM {SILVER} WHERE declaration_id=1").collect()[0]["status"] == "Εγκεκριμένη",
    "declaration_id=2 διαγράφηκε":   spark.sql(f"SELECT count(*) c FROM {SILVER} WHERE declaration_id=2").collect()[0]["c"] == 0,
    "MERGE → 301 γραμμές":           spark.table(SILVER).count() == 301,
    "Νέα δήλωση 9001 υπάρχει":       spark.sql(f"SELECT count(*) c FROM {SILVER} WHERE declaration_id=9001").collect()[0]["c"] == 1,
    "Στήλη review_note προστέθηκε":  "review_note" in spark.table(SILVER).columns,
    "History > 1 version":           spark.sql(f"DESCRIBE HISTORY {SILVER}").count() > 1,
}
print("=" * 50)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 50)
print("🎉 Τέλος Μέρους 1!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise2_TimeTravel_Maintenance_STARTER`
# MAGIC
# MAGIC Κάναμε πολλές αλλαγές → το table έχει **ιστορικό** και **πολλά μικρά files**. Στο Μέρος 2:
# MAGIC time travel (γύρνα πίσω στο χρόνο), OPTIMIZE/ZORDER (ταχύτητα), VACUUM (καθάρισμα).
