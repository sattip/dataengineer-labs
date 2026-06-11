# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 Άσκηση Ημέρα 4 — Μέρος 2/4: Auto Loader (Incremental File Ingestion)
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~75' · **Δυσκολία:** ⭐⭐⭐ Hard
# MAGIC > Self-contained.
# MAGIC
# MAGIC ## 📖 Το πρόβλημα
# MAGIC
# MAGIC Στο Μέρος 1 κρατούσαμε **εμείς** το watermark. Αλλά όταν τα ΚΕΠ ρίχνουν **αρχεία** σε ένα
# MAGIC folder συνεχώς (κάθε ώρα ένα νέο CSV), πώς ξέρουμε ποια έχουμε ήδη διαβάσει; Το **Auto Loader**
# MAGIC (`cloudFiles`) το κάνει **αυτόματα**: κρατάει σε ένα **checkpoint** ποια αρχεία επεξεργάστηκε,
# MAGIC ώστε κάθε run να διαβάζει **μόνο τα ΝΕΑ** — exactly-once, χωρίς χειροκίνητο watermark.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - `spark.readStream.format("cloudFiles")` — η μηχανή incremental file ingestion.
# MAGIC - `checkpointLocation` — η «μνήμη» του τι έχει ήδη διαβαστεί.
# MAGIC - `schemaLocation` + αυτόματο schema inference/evolution.
# MAGIC - `Trigger.AvailableNow` — «επεξεργάσου ό,τι υπάρχει τώρα και σταμάτα» (ιδανικό για notebooks).
# MAGIC - Πώς ένα **2ο batch αρχείων** επεξεργάζεται incrementally (μόνο τα νέα).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + clean state + land batch 1 (έτοιμο)

# COMMAND ----------

import urllib.request, os, pandas as pd

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

LANDING   = f"{VOLUME}/kep_landing"
SCHEMA_LOC = f"{VOLUME}/_schemas/kep_autoloader"
CKPT       = f"{VOLUME}/_checkpoints/kep_autoloader"
BRONZE     = "workspace.aade.kep_bronze_autoloader"

# Clean state για επαναληψιμότητα
for p in [LANDING, SCHEMA_LOC, CKPT]:
    dbutils.fs.rm(p, recurse=True)
os.makedirs(LANDING, exist_ok=True)
spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")

# Land BATCH 1 = 2 αρχεία (rows 1..4000, 4001..6000)
pdf = pd.read_csv(f"{VOLUME}/kep_requests.csv")
pdf.iloc[0:4000].to_csv(f"{LANDING}/kep_batch1a.csv", index=False)
pdf.iloc[4000:6000].to_csv(f"{LANDING}/kep_batch1b.csv", index=False)
print("✓ Batch 1 landed: 2 αρχεία (6000 γραμμές) στο", LANDING)
print("Αρχεία:", [f.name for f in dbutils.fs.ls(LANDING)])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Ο Auto Loader stream reader
# MAGIC
# MAGIC ```python
# MAGIC spark.readStream.format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "csv")            # τι τύπος αρχείων
# MAGIC     .option("cloudFiles.schemaLocation", SCHEMA_LOC)  # πού κρατά το schema (evolution)
# MAGIC     .option("header", "true")
# MAGIC     .load(LANDING)                                  # ο φάκελος που «ακούει»
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Φτιάξτε τον Auto Loader reader

# COMMAND ----------

df_stream = (
    spark.readStream
    .format("__________")                                 # TODO 1a: cloudFiles
    .option("cloudFiles.format", "___")                   # TODO 1b: csv
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(LANDING)
)
print("✓ Stream reader έτοιμος (isStreaming =", df_stream.isStreaming, ")")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Το checkpoint + Trigger.AvailableNow
# MAGIC
# MAGIC - **`checkpointLocation`**: εδώ ο Auto Loader γράφει ΠΟΙΑ αρχεία διάβασε. Χωρίς αυτό, δεν υπάρχει
# MAGIC   incremental — κάθε run θα ξαναδιάβαζε τα πάντα.
# MAGIC - **`trigger(availableNow=True)`**: επεξεργάσου όλα τα διαθέσιμα αρχεία **μία φορά** και σταμάτα
# MAGIC   (σε αντίθεση με συνεχές streaming). Ιδανικό για batch-style incremental σε notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Γράψτε στο Bronze με checkpoint + availableNow

# COMMAND ----------

q = (
    df_stream.writeStream
    .format("delta")
    .option("checkpointLocation", __________)             # TODO 2a: CKPT
    .trigger(__________=True)                             # TODO 2b: availableNow
    .toTable(BRONZE)
)
q.awaitTermination()   # περίμενε να τελειώσει το batch
count_after_b1 = spark.table(BRONZE).count()
print(f"✓ Μετά το BATCH 1: Bronze = {count_after_b1} γραμμές")   # 6000

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Land BATCH 2 και ξανατρέξε (incremental!)
# MAGIC
# MAGIC Προσγειώνουμε 2 νέα αρχεία (rows 6001..8000, 8001..10000). Ο **ίδιος** stream με το **ίδιο
# MAGIC checkpoint** θα διαβάσει **μόνο** τα νέα. Συμπληρώστε ξανά checkpoint + trigger.

# COMMAND ----------

# Land BATCH 2 = 2 νέα αρχεία
pdf.iloc[6000:8000].to_csv(f"{LANDING}/kep_batch2a.csv", index=False)
pdf.iloc[8000:10000].to_csv(f"{LANDING}/kep_batch2b.csv", index=False)
print("✓ Batch 2 landed. Σύνολο αρχείων:", len([f.name for f in dbutils.fs.ls(LANDING)]))

q2 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(LANDING)
    .writeStream
    .format("delta")
    .option("checkpointLocation", __________)             # TODO 3a: CKPT (ίδιο!)
    .trigger(__________=True)                             # TODO 3b: availableNow
    .toTable(BRONZE)
)
q2.awaitTermination()
count_after_b2 = spark.table(BRONZE).count()
print(f"✓ Μετά το BATCH 2: Bronze = {count_after_b2} γραμμές")   # 10000
print(f"   → επεξεργάστηκε μόνο {count_after_b2 - count_after_b1} νέες (ΟΧΙ ξανά τις 6000)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 2

# COMMAND ----------

results = {
    "Batch 1 → Bronze = 6000":        count_after_b1 == 6000,
    "Batch 2 → Bronze = 10000":       count_after_b2 == 10000,
    "Incremental: +4000 (όχι +10000)": (count_after_b2 - count_after_b1) == 4000,
    "Checkpoint δημιουργήθηκε":        len(dbutils.fs.ls(CKPT)) > 0,
    "4 αρχεία στο landing":            len([f.name for f in dbutils.fs.ls(LANDING) if f.name.endswith('.csv')]) == 4,
}
print("=" * 55)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 55)
print("🎉 Τέλος Μέρους 2!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise3_Streaming_Merge_STARTER`
# MAGIC
# MAGIC Ο Auto Loader φέρνει incrementally τα **αρχεία**. Στο Μέρος 3: full Structured Streaming +
# MAGIC `foreachBatch` + `MERGE` = streaming **upsert** σε Silver (το production streaming pattern).
