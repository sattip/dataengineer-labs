# Databricks notebook source
# MAGIC %md
# MAGIC # Άσκηση #5 — OPTIMIZE, Z-ORDER & VACUUM (Ημέρα 3)
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC
# MAGIC > **GitHub** (share this στο Zoom chat):
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/Optimize_Vacuum_Notebook.py
# MAGIC > ```
# MAGIC
# MAGIC **Catalog/Schema**: `workspace.aade`
# MAGIC **Volume**: `/Volumes/workspace/aade/aade_data/`
# MAGIC **Διάρκεια**: ~20 min
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Σενάριο
# MAGIC
# MAGIC Είσαι **Data Engineer της ΑΑΔΕ**. Το `tax_declarations_silver` έχει γεμίσει με μικρά Parquet files μετά από εκατοντάδες micro-batch writes. Queries τρέχουν αργά. Πρέπει να:
# MAGIC
# MAGIC 1. **Καταλάβεις το πρόβλημα** — small files via `DESCRIBE DETAIL`
# MAGIC 2. **OPTIMIZE** — compaction (πολλά μικρά files → λίγα μεγάλα)
# MAGIC 3. **Z-ORDER BY** — co-locate δεδομένα ανά query column (data skipping)
# MAGIC 4. **Μετρήσεις improvement** — query plan + file count
# MAGIC 5. **VACUUM** — διαγραφή ορφανών παλιών files (cost + GDPR)
# MAGIC
# MAGIC ## 📋 Notebook structure
# MAGIC
# MAGIC | Cell | Step | Time |
# MAGIC |---|------|------|
# MAGIC | 0 | Setup + download από GitHub | 30s |
# MAGIC | 0.5 | Bootstrap silver table (αν λείπει) | 1 min |
# MAGIC | 1 | Δημιούργησε small-files problem (10 micro-batches) | 30s |
# MAGIC | 2 | DESCRIBE DETAIL — μέτρα file count + size | 5s |
# MAGIC | 3 | Query baseline — μέτρα ταχύτητα πριν | 10s |
# MAGIC | 4 | OPTIMIZE — compaction | 30s |
# MAGIC | 5 | DESCRIBE DETAIL — αλλαγή στο file count | 5s |
# MAGIC | 6 | OPTIMIZE με ZORDER BY | 30s |
# MAGIC | 7 | Query post-optimize — μέτρα improvement | 10s |
# MAGIC | 8 | VACUUM DRY RUN — δες τι θα διαγραφεί | 5s |
# MAGIC | 9 | VACUUM — διαγραφή ορφανών files | 30s |
# MAGIC | 10 | Auto-optimize settings (production) | — |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0 — Setup + Download από GitHub

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
print("✅ workspace.aade.aade_data ready\n")

import urllib.request, os

REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3"
VOL = "/Volumes/workspace/aade/aade_data"

files = ["declarations.csv", "doy.csv", "employees.csv", "taxpayers.csv"]
for fname in files:
    target = f"{VOL}/{fname}"
    if os.path.exists(target):
        print(f"⏭️  {fname} (already in volume)")
    else:
        urllib.request.urlretrieve(f"{REPO}/{fname}", target)
        print(f"✅ {fname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0.5 — Bootstrap silver (αν λείπει)
# MAGIC
# MAGIC Αν έχει εκτελεστεί η Άσκηση 3 ή 4, το silver υπάρχει ήδη και η εκτέλεση παραλείπεται.

# COMMAND ----------

from pyspark.sql.functions import lit, col

existing = {t.tableName for t in spark.sql("SHOW TABLES IN workspace.aade").collect()}
if "tax_declarations_silver" in existing:
    cnt = spark.sql("SELECT COUNT(*) AS n FROM workspace.aade.tax_declarations_silver").collect()[0]["n"]
    print(f"✅ tax_declarations_silver υπάρχει ({cnt} rows)")
else:
    print("⚠️  silver λείπει — bootstrapping από declarations.csv...")
    df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOL}/declarations.csv"))
    (df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("workspace.aade.tax_declarations_silver"))
    print(f"   ✅ Bootstrapped {df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 1 — Δημιούργησε το "small-files problem"
# MAGIC
# MAGIC Σε production, αυτό συμβαίνει αυτόματα από:
# MAGIC - **Streaming writes** (κάθε micro-batch = 1+ νέα files)
# MAGIC - **Append jobs** που τρέχουν συχνά
# MAGIC - **MERGE** operations που γράφουν small Parquet files
# MAGIC
# MAGIC Εδώ θα το προσομοιώσουμε: 10 διαδοχικά appends 30 rows το καθένα → 10+ small files.

# COMMAND ----------

# Διαβάζουμε το silver, κάνουμε 10 micro-batch appends 30 rows το καθένα
silver_data = spark.table("workspace.aade.tax_declarations_silver")
batch_size = 30

# Παίρνουμε 30 rows ως πρότυπο για micro-batches
template = silver_data.limit(batch_size)

print("Writing 10 micro-batches (small files problem)...")
for i in range(10):
    template.write.format("delta").mode("append") \
        .saveAsTable("workspace.aade.tax_declarations_silver")
print("✅ 10 micro-batches γράφτηκαν.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 2 — DESCRIBE DETAIL: μέτρα το πρόβλημα
# MAGIC
# MAGIC Το `DESCRIBE DETAIL` δίνει:
# MAGIC - **`numFiles`** — πόσα Parquet files έχει το table
# MAGIC - **`sizeInBytes`** — total μέγεθος
# MAGIC - **Average file size** = `sizeInBytes / numFiles`
# MAGIC
# MAGIC **Best practice**: target file size ~128–256 MB ανά Parquet. Αν average <10 MB → small files problem.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.aade.tax_declarations_silver

# COMMAND ----------

# Programmatic check
detail = spark.sql("DESCRIBE DETAIL workspace.aade.tax_declarations_silver").collect()[0]
num_files = detail["numFiles"]
size_bytes = detail["sizeInBytes"]
avg_size_kb = size_bytes / num_files / 1024 if num_files else 0

print(f"📊 Files:          {num_files}")
print(f"📊 Total size:     {size_bytes / 1024:.1f} KB")
print(f"📊 Avg file size:  {avg_size_kb:.1f} KB  ← μικρό = πρόβλημα!")
print(f"\n   Target: ~128 MB per file. Με {num_files} files και {size_bytes/1024:.1f} KB total,")
print(f"   κάθε query θα κάνει I/O σε όλα τα files. Slow.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 3 — Query baseline (πριν OPTIMIZE)
# MAGIC
# MAGIC Τρέχουμε ένα filter query και μετράμε χρόνο.

# COMMAND ----------

import time

start = time.time()
result_before = spark.sql("""
  SELECT COUNT(*) AS cnt
  FROM workspace.aade.tax_declarations_silver
  WHERE Κατηγορία_Φόρου = 'ΦΠΑ'
""").collect()
elapsed_before = time.time() - start

print(f"⏱️  Query χρόνος ΠΡΙΝ: {elapsed_before*1000:.0f} ms")
print(f"   Result: {result_before[0]['cnt']} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 4 — OPTIMIZE: compaction
# MAGIC
# MAGIC Το `OPTIMIZE` διαβάζει όλα τα small Parquet files και τα ξαναγράφει σε **λίγα μεγάλα files** (~128 MB target).
# MAGIC
# MAGIC ⚠️ Σε **Free Edition / Serverless**: το command είναι λίγο αργό σε small tables — αυτό είναι expected. Σε production με GBs δεδομένων, το όφελος είναι τεράστιο.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.aade.tax_declarations_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 5 — DESCRIBE DETAIL: μετά το OPTIMIZE

# COMMAND ----------

detail_after = spark.sql("DESCRIBE DETAIL workspace.aade.tax_declarations_silver").collect()[0]
num_files_after = detail_after["numFiles"]
size_bytes_after = detail_after["sizeInBytes"]
avg_kb_after = size_bytes_after / num_files_after / 1024 if num_files_after else 0

print(f"📊 Files (μετά):       {num_files_after}  (πριν: {num_files})")
print(f"📊 Total size (μετά):  {size_bytes_after / 1024:.1f} KB")
print(f"📊 Avg file size:      {avg_kb_after:.1f} KB")
print(f"\n   Reduction: {num_files - num_files_after} files λιγότερα")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6 — OPTIMIZE με ZORDER BY: data skipping
# MAGIC
# MAGIC Το `ZORDER BY` δίνει επιπλέον boost για queries που φιλτράρουν σε **συγκεκριμένο column**.
# MAGIC
# MAGIC **Πώς δουλεύει**:
# MAGIC - Co-locates rows με ίδιες τιμές στο ZORDER column μαζί στα ίδια Parquet files
# MAGIC - Όταν κάνεις `WHERE Κατηγορία_Φόρου = 'ΦΠΑ'`, το Spark διαβάζει **μόνο** τα files που έχουν 'ΦΠΑ' (data skipping via min/max stats)
# MAGIC - Σε large tables: 10–100× speedup
# MAGIC
# MAGIC **Best practice**:
# MAGIC - ZORDER BY στα 1–4 πιο "hot" filter columns (αυτά που μπαίνουν συχνά σε WHERE)
# MAGIC - Όχι σε high-cardinality cols (π.χ. ΑΦΜ — εκατομμύρια unique values, δεν επιφέρει βελτίωση)
# MAGIC - Όχι σε low-cardinality cols με 2-3 values (π.χ. boolean — partition αντί ZORDER)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.aade.tax_declarations_silver
# MAGIC ZORDER BY (Κατηγορία_Φόρου, Φορ_Ετος)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 7 — Query post-optimize: μέτρα το improvement

# COMMAND ----------

start = time.time()
result_after = spark.sql("""
  SELECT COUNT(*) AS cnt
  FROM workspace.aade.tax_declarations_silver
  WHERE Κατηγορία_Φόρου = 'ΦΠΑ'
""").collect()
elapsed_after = time.time() - start

print(f"⏱️  Query χρόνος ΜΕΤΑ:  {elapsed_after*1000:.0f} ms")
print(f"⏱️  Query χρόνος ΠΡΙΝ:  {elapsed_before*1000:.0f} ms")
if elapsed_after < elapsed_before:
    speedup = elapsed_before / elapsed_after
    print(f"\n🚀 Speedup: {speedup:.2f}× ταχύτερο")
else:
    print("\nℹ️  Σε Free Edition με τόσο μικρό dataset (300 rows) η διαφορά είναι μέσα στο noise.")
    print("   Σε production GB+ datasets, το ZORDER δίνει 10–100× speedup.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 8 — VACUUM DRY RUN: τι θα διαγραφεί;
# MAGIC
# MAGIC Μετά το OPTIMIZE, τα **παλιά small Parquet files** δεν διαγράφονται αμέσως — μένουν στο storage για να υποστηρίξουν **time travel**.
# MAGIC
# MAGIC Το `VACUUM` τα διαγράφει **οριστικά**. Default retention: **7 ημέρες** (168 hours).
# MAGIC
# MAGIC **`DRY RUN`** = δες τι θα διαγραφόταν χωρίς να το κάνεις πραγματικά.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM workspace.aade.tax_declarations_silver DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 9 — VACUUM (πραγματική διαγραφή)
# MAGIC
# MAGIC ⚠️ **Default retention 7 days**: το VACUUM δεν διαγράφει files <7 ημέρες παλιά. Αυτό προστατεύει time travel.
# MAGIC
# MAGIC Για demo, θα επιτρέψουμε retention 0 hours (διαγραφή τώρα). **ΣΕ PRODUCTION ΜΗΝ ΤΟ ΚΑΝΕΙΣ** — χάνεις time travel για αυτή τη διάρκεια.

# COMMAND ----------

# Σε Free Edition / production default, χρειάζεται να επιτρέψουμε retention <168 hours
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM workspace.aade.tax_declarations_silver RETAIN 0 HOURS

# COMMAND ----------

# Επανέφερε retention safety check (production hygiene)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
print("✅ Retention check επανήλθε σε ON.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 10 — Production best practices
# MAGIC
# MAGIC ### Auto-Optimize (γράψε μία φορά, αυτόματο πάντα)
# MAGIC
# MAGIC Για να μην χρειάζεται να τρέχεις OPTIMIZE manually:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable auto-optimize σε νέα δεδομένα
# MAGIC ALTER TABLE workspace.aade.tax_declarations_silver
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Επεξήγηση
# MAGIC
# MAGIC | Property | Τι κάνει |
# MAGIC |---|---|
# MAGIC | `optimizeWrite` | Στο write time, "παίρνει" rows και τα γράφει σε ~128 MB chunks αντί για όσα output partitions έχει το job |
# MAGIC | `autoCompact` | Μετά από κάθε write, αν εντοπίσει small files, τα κάνει merge αυτόματα |
# MAGIC
# MAGIC ### Recommended cadence για manual maintenance
# MAGIC
# MAGIC | Operation | Πότε | Σχόλιο |
# MAGIC |---|---|---|
# MAGIC | `OPTIMIZE` | Καθημερινά (off-hours job) | Καλύτερα μέσω scheduled job, όχι manual |
# MAGIC | `OPTIMIZE ZORDER BY` | Εβδομαδιαία | ZORDER είναι ακριβό — δεν θες κάθε μέρα |
# MAGIC | `VACUUM` | Εβδομαδιαία | Default retention 7 days. Καθαρίζει cost/storage. |
# MAGIC | `ANALYZE TABLE COMPUTE STATISTICS` | Μετά από major data shifts | Ενημερώνει query planner stats |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Wrap-up
# MAGIC
# MAGIC | Πρόβλημα | Command | Πότε |
# MAGIC |---|---|---|
# MAGIC | Πολλά μικρά Parquet files (από streaming/append) | `OPTIMIZE` | Καθημερινά |
# MAGIC | Slow filter queries σε hot column | `OPTIMIZE ... ZORDER BY (col)` | Εβδομαδιαία |
# MAGIC | Ορφανά files ξεχειλίζουν το storage / GDPR cleanup | `VACUUM` | Εβδομαδιαία |
# MAGIC | Δες πόσα files / πόσο size έχει το table | `DESCRIBE DETAIL <table>` | Όποτε |
# MAGIC | Auto-care, γράψε μία φορά | `ALTER TABLE ... SET TBLPROPERTIES (autoOptimize.*)` | Set up once |
# MAGIC
# MAGIC ### Κλειστή ερώτηση για τους trainees
# MAGIC
# MAGIC > "Έχεις 1 TB πίνακα. Streaming γράφει 1000 rows/sec σε αυτόν, 24/7. Πόσα files θα είχες σε 30 ημέρες χωρίς maintenance, και πώς το λύνεις;"
# MAGIC
# MAGIC **Απάντηση**: Με micro-batch trigger 1 sec → ~2.6M files. Λύσεις:
# MAGIC 1. `delta.autoOptimize.optimizeWrite = true` (write-time compaction → ~10× λιγότερα files)
# MAGIC 2. `autoCompact = true` (post-write compaction)
# MAGIC 3. Καθημερινό scheduled `OPTIMIZE` job
# MAGIC 4. Εβδομαδιαίο `VACUUM` για cost
