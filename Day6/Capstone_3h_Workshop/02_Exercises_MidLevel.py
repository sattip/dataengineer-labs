# Databricks notebook source
# MAGIC %md
# MAGIC # 📝 Day 6 Exercises — Mid-Level (Block B)
# MAGIC
# MAGIC **Duration:** 60 minutes (Hour 2)
# MAGIC **Mode:** Hands-on — εσείς γράφετε τον κώδικα
# MAGIC **Prerequisites:** Ολοκληρώσατε το `01_Exercises_Foundations.py`
# MAGIC
# MAGIC ## 🎯 Tasks σε αυτό το block
# MAGIC | # | Exercise | Concept | Time |
# MAGIC |---|---|---|---|
# MAGIC | 5 | Auto Loader streaming Bronze | Incremental file ingestion | 15' |
# MAGIC | 6 | foreachBatch + MERGE σε Silver | Streaming + transactional upsert | 20' |
# MAGIC | 7 | Schema Evolution | New column σε source, auto-merge | 12' |
# MAGIC | 8 | Time Travel + Rollback | Delta versioning, RESTORE | 13' |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧰 Setup (same as Block A)

# COMMAND ----------

import os
import logging
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, to_timestamp, upper, length
)
from delta.tables import DeltaTable

for n in ("pyspark.sql.connect.client.core", "pyspark.sql.connect",
          "pyspark", "py4j", "grpc"):
    logging.getLogger(n).setLevel(logging.CRITICAL)

YOUR_NAME = "george"  # ← change me (ίδιο όπως στο Block A)
SCHEMA = f"workspace.day6_{YOUR_NAME}"
volume_path = f"/Volumes/workspace/day6_{YOUR_NAME}/raw"

# Folder για streaming source files
streaming_src = f"{volume_path}/streaming_src"
checkpoint_dir = f"{volume_path}/checkpoints"
os.makedirs(streaming_src, exist_ok=True)
os.makedirs(checkpoint_dir, exist_ok=True)

print(f"✅ Setup ready")
print(f"   Streaming source: {streaming_src}")
print(f"   Checkpoints:      {checkpoint_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Exercise 5 — Auto Loader Streaming Bronze (15 min)
# MAGIC
# MAGIC ### Σενάριο
# MAGIC Καθημερινά το TAXIS γράφει νέα CSV files στο data lake. Πρέπει να φτιάξετε
# MAGIC **streaming bronze table** που τα πιάνει incrementally μέσω Auto Loader.

# COMMAND ----------

# Setup: Generate Batch 1 files (μην το αλλάξετε)
def generate_batch(batch_num: int, n_rows: int = 30):
    np.random.seed(batch_num * 100 + hash(YOUR_NAME) % 100)
    df_batch = pd.DataFrame({
        "statement_id": [f"BT{batch_num:02d}{i:04d}" for i in range(n_rows)],
        "afm":          [f"{900000000 + batch_num*1000 + i:09d}" for i in range(n_rows)],
        "fiscal_year":  [2025] * n_rows,
        "region":       np.random.choice(["Αττική", "Θεσσαλονίκη", "Κρήτη"], n_rows),
        "tax_amount":   np.round(np.random.uniform(800, 25000, n_rows), 2),
        "status":       np.random.choice(["Submitted", "Approved"], n_rows),
        "submitted_at": [(datetime.utcnow() - timedelta(hours=i)).isoformat() for i in range(n_rows)],
    })
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = f"{streaming_src}/batch_{batch_num:02d}_{ts}.csv"
    df_batch.to_csv(path, index=False)
    return path

# Batch 1
batch1_path = generate_batch(1)
print(f"✅ Batch 1 written: {batch1_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your task
# MAGIC Φτιάξτε streaming query με Auto Loader:
# MAGIC
# MAGIC ### Requirements
# MAGIC - Source: `streaming_src` (path με όλα τα CSV files)
# MAGIC - Schema location: `{checkpoint_dir}/bronze_stream/_schema`
# MAGIC - Checkpoint: `{checkpoint_dir}/bronze_stream`
# MAGIC - Target table: `{SCHEMA}.bronze_stream_tax`
# MAGIC - Trigger: `availableNow=True` (run-and-stop)
# MAGIC - Add columns: `_ingested_at`, `_source_file`
# MAGIC
# MAGIC ### 📖 Worked Example (Auto Loader για JSON files)
# MAGIC
# MAGIC Παρόμοιο pattern, αλλά για JSON αντί για CSV:
# MAGIC
# MAGIC ```python
# MAGIC src   = "/Volumes/demo/raw/events"
# MAGIC chkpt = "/Volumes/demo/raw/checkpoints/bronze_events"
# MAGIC
# MAGIC (spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")              # ⬅️ json
# MAGIC     .option("cloudFiles.schemaLocation", f"{chkpt}/_schema")
# MAGIC     .option("cloudFiles.inferColumnTypes", "true")
# MAGIC     .load(src)
# MAGIC     .withColumn("_ingested_at", current_timestamp())
# MAGIC     .writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", chkpt)
# MAGIC     .trigger(availableNow=True)
# MAGIC     .toTable("workspace.demo.bronze_events")
# MAGIC     .awaitTermination())
# MAGIC ```
# MAGIC
# MAGIC ### 💡 Detailed Hints για το exercise
# MAGIC
# MAGIC **Full Auto Loader pattern:**
# MAGIC ```python
# MAGIC chk_path = f"{checkpoint_dir}/bronze_stream"
# MAGIC
# MAGIC query = (spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "csv")
# MAGIC     .option("cloudFiles.schemaLocation", f"{chk_path}/_schema")
# MAGIC     .option("header", "true")
# MAGIC     .option("cloudFiles.inferColumnTypes", "true")
# MAGIC     .load(streaming_src)
# MAGIC     .withColumn("_ingested_at", current_timestamp())
# MAGIC     .withColumn("_source_file", col("_metadata.file_path"))
# MAGIC     .writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", chk_path)
# MAGIC     .option("mergeSchema", "true")
# MAGIC     .trigger(availableNow=True)
# MAGIC     .toTable(f"{SCHEMA}.bronze_stream_tax"))
# MAGIC
# MAGIC query.awaitTermination()      # ⬅️ ΑΠΑΡΑΙΤΗΤΟ
# MAGIC ```
# MAGIC
# MAGIC **Σημείωση**: ίδιος `checkpointLocation` σε κάθε run = incremental. Διαφορετικό = full re-read.

# COMMAND ----------

# TODO: Φτιάξτε Auto Loader streaming query που γράφει σε bronze_stream_tax

# Your code here:




# Verification — Batch 1
print("\n=== Verification — Exercise 5 (Batch 1) ===")
try:
    cnt_b1 = spark.table(f"{SCHEMA}.bronze_stream_tax").count()
    cols = spark.table(f"{SCHEMA}.bronze_stream_tax").columns
    print(f"  Rows after batch 1: {cnt_b1}        {'✅' if cnt_b1 == 30 else '❌ expected 30'}")
    print(f"  Has _ingested_at:    {'_ingested_at' in cols}  {'✅' if '_ingested_at' in cols else '❌'}")
    print(f"  Has _source_file:    {'_source_file' in cols}  {'✅' if '_source_file' in cols else '❌'}")
except Exception as e:
    print(f"  ❌ Error: {e}")

# COMMAND ----------

# Batch 2 — τρέξτε το ίδιο query ξανά, πρέπει να γίνει incremental
batch2_path = generate_batch(2)
print(f"✅ Batch 2 written: {batch2_path}")

# TODO: Re-run το ίδιο streaming query (copy-paste από το προηγούμενο cell)
#       Πρέπει να διπλασιαστούν τα rows (60 total).

# Your code here:




# Verification — Batch 2
print("\n=== Verification — Exercise 5 (incremental Batch 2) ===")
try:
    cnt_b2 = spark.table(f"{SCHEMA}.bronze_stream_tax").count()
    print(f"  Rows after batch 2: {cnt_b2}        {'✅' if cnt_b2 == 60 else '❌ expected 60 — incremental δεν δούλεψε;'}")
except Exception as e:
    print(f"  ❌ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Exercise 6 — foreachBatch + MERGE (20 min)
# MAGIC
# MAGIC ### Σενάριο
# MAGIC Το streaming bronze γράφει append-only. Για **Silver** χρειαζόμαστε MERGE
# MAGIC ώστε να κρατάμε **την πιο πρόσφατη version** ανά `statement_id`.
# MAGIC Αυτό γίνεται με **`foreachBatch`** που δίνει access στο microbatch ως DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your task
# MAGIC 1. Φτιάξτε αρχικά empty Silver table:
# MAGIC    `CREATE TABLE {SCHEMA}.silver_stream_tax (...)`
# MAGIC 2. Φτιάξτε function `merge_to_silver(microbatch_df, batch_id)` που:
# MAGIC    - Παίρνει microbatch
# MAGIC    - Deduplicate σε επίπεδο microbatch (αν ίδιο statement_id εμφανίζεται 2 φορές, κρατάει το latest)
# MAGIC    - MERGE INTO silver_stream_tax
# MAGIC 3. Συνδέστε με streaming query: `.foreachBatch(merge_to_silver).trigger(availableNow=True)`
# MAGIC
# MAGIC ### 📖 Worked Example (foreachBatch με MERGE)
# MAGIC
# MAGIC Streaming events → MERGE into customers table:
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC def update_customers(microbatch_df, batch_id):
# MAGIC     # 1. Dedupe σε microbatch (latest event wins)
# MAGIC     w = Window.partitionBy("customer_id").orderBy(col("event_ts").desc())
# MAGIC     dedup = (microbatch_df
# MAGIC         .withColumn("rn", F.row_number().over(w))
# MAGIC         .filter("rn = 1")
# MAGIC         .drop("rn"))
# MAGIC
# MAGIC     # 2. MERGE
# MAGIC     tgt = DeltaTable.forName(microbatch_df.sparkSession, "workspace.demo.customers")
# MAGIC     (tgt.alias("t")
# MAGIC         .merge(dedup.alias("s"), "t.customer_id = s.customer_id")
# MAGIC         .whenMatchedUpdateAll()
# MAGIC         .whenNotMatchedInsertAll()
# MAGIC         .execute())
# MAGIC     print(f"  Batch {batch_id}: merged {dedup.count()} customers")
# MAGIC
# MAGIC # Stream
# MAGIC (spark.readStream
# MAGIC     .table("workspace.demo.bronze_events")
# MAGIC     .writeStream
# MAGIC     .foreachBatch(update_customers)
# MAGIC     .option("checkpointLocation", "/Volumes/.../checkpoints/silver_merge")
# MAGIC     .trigger(availableNow=True)
# MAGIC     .start()
# MAGIC     .awaitTermination())
# MAGIC ```
# MAGIC
# MAGIC ### 💡 Detailed Hints για το exercise
# MAGIC
# MAGIC **Function signature**:
# MAGIC ```python
# MAGIC def merge_to_silver(microbatch_df, batch_id):
# MAGIC     # microbatch_df: DataFrame
# MAGIC     # batch_id: integer (incremental ανά batch)
# MAGIC     ...
# MAGIC ```
# MAGIC
# MAGIC **Dedupe pattern (latest wins):**
# MAGIC ```python
# MAGIC w = Window.partitionBy("statement_id").orderBy(col("_ingested_at").desc())
# MAGIC dedup = (microbatch_df
# MAGIC     .withColumn("_rn", row_number().over(w))
# MAGIC     .filter("_rn = 1")
# MAGIC     .drop("_rn")
# MAGIC     .withColumn("_merged_at", current_timestamp()))
# MAGIC ```
# MAGIC
# MAGIC **MERGE μέσα στη function:**
# MAGIC ```python
# MAGIC target = DeltaTable.forName(
# MAGIC     microbatch_df.sparkSession,    # ⬅️ ΟΧΙ global spark!
# MAGIC     f"{SCHEMA}.silver_stream_tax"
# MAGIC )
# MAGIC (target.alias("t")
# MAGIC     .merge(dedup.alias("s"), "t.statement_id = s.statement_id")
# MAGIC     .whenMatchedUpdateAll()
# MAGIC     .whenNotMatchedInsertAll()
# MAGIC     .execute())
# MAGIC ```
# MAGIC
# MAGIC **Stream connection:**
# MAGIC ```python
# MAGIC (spark.readStream
# MAGIC     .table(f"{SCHEMA}.bronze_stream_tax")           # ⬅️ read από bronze
# MAGIC     .writeStream
# MAGIC     .foreachBatch(merge_to_silver)
# MAGIC     .option("checkpointLocation", f"{checkpoint_dir}/silver_merge")
# MAGIC     .trigger(availableNow=True)
# MAGIC     .start()
# MAGIC     .awaitTermination())
# MAGIC ```

# COMMAND ----------

# Step 1: Create Silver shell
spark.sql(f"""
    CREATE OR REPLACE TABLE {SCHEMA}.silver_stream_tax (
        statement_id STRING,
        afm STRING,
        fiscal_year INT,
        region STRING,
        tax_amount DOUBLE,
        status STRING,
        submitted_at STRING,
        _ingested_at TIMESTAMP,
        _merged_at TIMESTAMP
    ) USING DELTA
    COMMENT 'Silver layer — deduplicated tax declarations via streaming MERGE'
""")
print(f"✅ Silver shell created: {SCHEMA}.silver_stream_tax")

# COMMAND ----------

# TODO: Define merge_to_silver function and run streaming query

# Your code here:




# Verification
print("\n=== Verification — Exercise 6 ===")
try:
    silver_cnt = spark.table(f"{SCHEMA}.silver_stream_tax").count()
    bronze_cnt = spark.table(f"{SCHEMA}.bronze_stream_tax").count()
    has_merged_at = "_merged_at" in spark.table(f"{SCHEMA}.silver_stream_tax").columns
    print(f"  Bronze rows:  {bronze_cnt}")
    print(f"  Silver rows:  {silver_cnt}     {'✅' if silver_cnt > 0 else '❌'}")
    print(f"  _merged_at:   {has_merged_at}  {'✅' if has_merged_at else '❌'}")

    # Verify ιστορικότητα MERGE
    print(f"\n  Delta history (latest 3 operations):")
    spark.sql(f"DESCRIBE HISTORY {SCHEMA}.silver_stream_tax").select(
        "version", "timestamp", "operation"
    ).limit(3).show(truncate=False)
except Exception as e:
    print(f"  ❌ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Exercise 7 — Schema Evolution (12 min)
# MAGIC
# MAGIC ### Σενάριο
# MAGIC Το TAXIS αναβαθμίζει το API και προσθέτει νέα στήλη `tax_office_code` στα CSV.
# MAGIC Το Auto Loader πρέπει να την δεχτεί **αυτόματα** χωρίς να σπάσει το pipeline.

# COMMAND ----------

# Setup Batch 3 με extra column
def generate_batch_with_new_col(batch_num: int = 3, n_rows: int = 20):
    df_batch = pd.DataFrame({
        "statement_id": [f"BT{batch_num:02d}{i:04d}" for i in range(n_rows)],
        "afm":          [f"{900000000 + batch_num*1000 + i:09d}" for i in range(n_rows)],
        "fiscal_year":  [2025] * n_rows,
        "region":       np.random.choice(["Αττική", "Θεσσαλονίκη"], n_rows),
        "tax_amount":   np.round(np.random.uniform(800, 25000, n_rows), 2),
        "status":       ["Approved"] * n_rows,
        "submitted_at": [(datetime.utcnow() - timedelta(hours=i)).isoformat() for i in range(n_rows)],
        "tax_office_code": [f"DOY-{(i % 50) + 1:03d}" for i in range(n_rows)],  # ⬅️ ΝΕΟ
    })
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = f"{streaming_src}/batch_{batch_num:02d}_{ts}_v2.csv"
    df_batch.to_csv(path, index=False)
    return path

batch3_path = generate_batch_with_new_col()
print(f"✅ Batch 3 with NEW column written: {batch3_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your task
# MAGIC Τρέξτε το **ίδιο streaming query του Exercise 5** ΑΛΛΑ προσθέστε:
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# MAGIC .option("mergeSchema", "true")  # στο writeStream
# MAGIC ```
# MAGIC
# MAGIC ⚠️ **ΠΡΟΣΟΧΗ**: το Auto Loader πιθανώς θα σταματήσει με `UnknownFieldException`
# MAGIC την πρώτη φορά. Αυτό είναι **expected**! Το stream auto-restarts μόλις
# MAGIC update-άρει το schema. Τρέξτε ξανά το cell.
# MAGIC
# MAGIC ### Expected outcome
# MAGIC - Bronze table τώρα έχει 10 columns (8 original + 2 metadata) + το νέο `tax_office_code`
# MAGIC - Rows από batch 1 + 2: `tax_office_code` = NULL
# MAGIC - Rows από batch 3: `tax_office_code` = `DOY-XXX`

# COMMAND ----------

# TODO: Auto Loader stream με schema evolution enabled

# Your code here (μπορείτε να χρησιμοποιήσετε try/except γιατί η πρώτη εκτέλεση πιθανώς θα σπάσει):




# Verification
print("\n=== Verification — Exercise 7 ===")
try:
    cols = spark.table(f"{SCHEMA}.bronze_stream_tax").columns
    has_new_col = "tax_office_code" in cols
    cnt = spark.table(f"{SCHEMA}.bronze_stream_tax").count()
    null_old = spark.sql(f"""
        SELECT COUNT(*) AS c
        FROM {SCHEMA}.bronze_stream_tax
        WHERE tax_office_code IS NULL
    """).first()["c"] if has_new_col else 0
    print(f"  Total rows:                {cnt}")
    print(f"  New column 'tax_office_code': {'present' if has_new_col else 'MISSING'} {'✅' if has_new_col else '❌'}")
    if has_new_col:
        print(f"  Old rows με NULL στο νέο col: {null_old}  ({'✅' if null_old >= 60 else '❌ expected ~60'})")
except Exception as e:
    print(f"  ❌ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Exercise 8 — Time Travel & Rollback (13 min)
# MAGIC
# MAGIC ### Σενάριο
# MAGIC Κάποιος (κατά λάθος) έκανε `UPDATE` που μηδένισε όλα τα `tax_amount` στο
# MAGIC Silver. Πρέπει να **rollback** σε προηγούμενη version.

# COMMAND ----------

# Setup: simulate κακή ενημέρωση
print("Βρώμικη ενημέρωση εν εξελίξει...")
spark.sql(f"UPDATE {SCHEMA}.silver_stream_tax SET tax_amount = 0")

# Δείτε state
print("\nState μετά τη ζημιά:")
spark.sql(f"SELECT statement_id, tax_amount FROM {SCHEMA}.silver_stream_tax LIMIT 5").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your task — 3 sub-tasks
# MAGIC
# MAGIC 1. **Διερεύνηση**: τρέξτε `DESCRIBE HISTORY` για να βρείτε:
# MAGIC    - Σε ποια version έγινε το `UPDATE`
# MAGIC    - Ποια version έχουμε τώρα
# MAGIC
# MAGIC 2. **Verify**: query τη previous version με `VERSION AS OF X` και επιβεβαιώστε
# MAGIC    ότι το `tax_amount` ΔΕΝ είναι 0.
# MAGIC
# MAGIC 3. **Rollback**: τρέξτε `RESTORE TABLE ... TO VERSION AS OF X` για να επιστρέψετε.
# MAGIC
# MAGIC ### 📖 Worked Example (Time Travel + RESTORE)
# MAGIC
# MAGIC ```python
# MAGIC # 1. Δες πλήρες ιστορικό
# MAGIC spark.sql("DESCRIBE HISTORY workspace.demo.products").show(truncate=False)
# MAGIC # Output:
# MAGIC #   version | timestamp           | operation
# MAGIC #   0       | 2026-04-30 10:00    | CREATE TABLE
# MAGIC #   1       | 2026-04-30 10:05    | WRITE
# MAGIC #   2       | 2026-04-30 10:10    | MERGE
# MAGIC #   3       | 2026-04-30 10:15    | UPDATE       ← το «κακό» update
# MAGIC
# MAGIC # 2. Verify ότι version 2 έχει σωστά data
# MAGIC spark.sql("SELECT * FROM workspace.demo.products VERSION AS OF 2").show()
# MAGIC
# MAGIC # 3. Rollback σε version 2
# MAGIC spark.sql("RESTORE TABLE workspace.demo.products TO VERSION AS OF 2")
# MAGIC
# MAGIC # 4. Verify ότι τώρα είμαι σε version 2 (αλλά καταγράφεται νέο version 4 για audit)
# MAGIC spark.sql("DESCRIBE HISTORY workspace.demo.products").show()
# MAGIC # version 4 operation = RESTORE
# MAGIC ```
# MAGIC
# MAGIC ### 💡 Detailed Hints για το exercise
# MAGIC
# MAGIC **Find the bad version:**
# MAGIC ```python
# MAGIC history = spark.sql(f"DESCRIBE HISTORY {SCHEMA}.silver_stream_tax")
# MAGIC history.select("version", "timestamp", "operation").show(truncate=False)
# MAGIC
# MAGIC # Programmatic: το πιο πρόσφατο update είναι το πρώτο row
# MAGIC latest_op = history.first()
# MAGIC bad_version = latest_op["version"]              # το UPDATE
# MAGIC good_version = bad_version - 1                  # το προηγούμενο
# MAGIC ```
# MAGIC
# MAGIC **Verify previous version:**
# MAGIC ```python
# MAGIC spark.sql(f\"\"\"
# MAGIC     SELECT statement_id, tax_amount
# MAGIC     FROM {SCHEMA}.silver_stream_tax VERSION AS OF {good_version}
# MAGIC     LIMIT 10
# MAGIC \"\"\").show()
# MAGIC ```
# MAGIC
# MAGIC **Restore:**
# MAGIC ```python
# MAGIC spark.sql(f"RESTORE TABLE {SCHEMA}.silver_stream_tax TO VERSION AS OF {good_version}")
# MAGIC ```
# MAGIC
# MAGIC **Σημαντικό**: το RESTORE δεν διαγράφει το «κακό» version — απλώς δημιουργεί νέο version
# MAGIC που είναι copy του παλιού καλού. Όλο το history διατηρείται.

# COMMAND ----------

# TODO: Sub-task 1 — DESCRIBE HISTORY
# Your code here:




# COMMAND ----------

# TODO: Sub-task 2 — Verify previous version έχει σωστά data

# Your code here (use VERSION AS OF):




# COMMAND ----------

# TODO: Sub-task 3 — Rollback με RESTORE TABLE

# Your code here:




# Verification (αυτό μπορείτε να το αλλάξετε αν έχετε άλλη version)
print("\n=== Verification — Exercise 8 ===")
try:
    nonzero = spark.sql(f"""
        SELECT COUNT(*) AS c FROM {SCHEMA}.silver_stream_tax WHERE tax_amount > 0
    """).first()["c"]
    total = spark.sql(f"SELECT COUNT(*) AS c FROM {SCHEMA}.silver_stream_tax").first()["c"]
    print(f"  Rows με tax_amount > 0:  {nonzero} / {total}")
    if nonzero == total and total > 0:
        print(f"  ✅ Rollback successful — όλα τα tax_amount αποκαταστάθηκαν")
    else:
        print(f"  ❌ Δεν έγινε successful rollback — επανεκτέλεση RESTORE")
except Exception as e:
    print(f"  ❌ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Block B Complete
# MAGIC
# MAGIC Έχετε τώρα δοκιμάσει:
# MAGIC - 📡 **Auto Loader streaming** με `availableNow` trigger
# MAGIC - 🔄 **foreachBatch + MERGE** για streaming upserts
# MAGIC - 🌱 **Schema evolution** για νέα columns από source
# MAGIC - ⏱️ **Time travel + RESTORE** για disaster recovery
# MAGIC
# MAGIC ### 🔍 Reflection questions
# MAGIC 1. Πότε **όχι** schema evolution; (hint: production governance)
# MAGIC 2. Τι κάνει το `dropDuplicates()` σε microbatch vs το MERGE condition;
# MAGIC 3. Πότε χρειάζεστε `RESTORE` vs απλό `DELETE FROM` + re-MERGE;
# MAGIC
# MAGIC ### Next:
# MAGIC Πηγαίνετε στο `03_Capstone_MiniProject.py` — final 60-min χωρίς hints.
