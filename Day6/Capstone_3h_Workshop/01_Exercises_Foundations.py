# Databricks notebook source
# MAGIC %md
# MAGIC # 📝 Day 6 Exercises — Foundations (Block A)
# MAGIC
# MAGIC **Duration:** 60 minutes (Hour 1)
# MAGIC **Mode:** Hands-on — εσείς γράφετε τον κώδικα στα TODO blocks
# MAGIC **Prerequisites:** Διαβάσατε το `00_Reading_Material.md`
# MAGIC
# MAGIC ## 🎯 Tasks σε αυτό το block
# MAGIC | # | Exercise | Concept | Time |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Ingest CSV → Bronze Delta | Read files, write Delta με metadata | 12' |
# MAGIC | 2 | Apply data quality | Filter invalid, quarantine pattern | 12' |
# MAGIC | 3 | Silver upsert με MERGE | DeltaTable API + dedupe | 18' |
# MAGIC | 4 | Gold aggregation με window | Window functions + groupBy | 18' |
# MAGIC
# MAGIC ## ⚠️ Πώς να το χρησιμοποιήσετε
# MAGIC 1. **Διαβάστε** την περιγραφή κάθε exercise
# MAGIC 2. **Δείτε τα hints** — δείχνουν ποια functions χρειάζεστε
# MAGIC 3. **Γράψτε τον κώδικα** στα `# TODO:` σχόλια
# MAGIC 4. **Τρέξτε** το cell και ελέγξτε αν το expected output ταιριάζει
# MAGIC 5. Αν κολλήσετε > 5 λεπτά, ρωτήστε τον trainer ή κοιτάξτε το `04_Solutions.py`
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day6/Capstone_3h_Workshop/01_Exercises_Foundations.py
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧰 Setup (Run this first — δεν χρειάζεται να αλλάξετε τίποτα)

# COMMAND ----------

import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, upper, trim, length,
    row_number, lag, sum as spark_sum, avg, count, desc
)
from pyspark.sql.window import Window

# Suppress noise
for n in ("pyspark.sql.connect.client.core", "pyspark.sql.connect",
          "pyspark", "py4j", "grpc"):
    logging.getLogger(n).setLevel(logging.CRITICAL)

# ⚠️ Personalize — αλλάξτε σε δικό σας όνομα (lowercase)
YOUR_NAME = "george"  # ← change me
SCHEMA = f"workspace.day6_{YOUR_NAME}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.raw")
volume_path = f"/Volumes/workspace/day6_{YOUR_NAME}/raw"
os.makedirs(volume_path, exist_ok=True)

# Generate seed CSV (φορολογικές δηλώσεις με σκόπιμα errors)
np.random.seed(hash(YOUR_NAME) % 1000)
rows = 200
afms = [f"{900000000 + i:09d}" for i in range(rows)]

data = pd.DataFrame({
    "statement_id": [f"TX{i:05d}" for i in range(rows)],
    "afm":          [afms[i] if i % 25 != 0 else None for i in range(rows)],  # 4% null
    "fiscal_year":  [2025] * rows,
    "region":       np.random.choice(
                        ["Αττική", "Θεσσαλονίκη", "Κρήτη", "Πάτρα", "Λάρισα", "INVALID"],
                        rows, p=[0.35, 0.20, 0.15, 0.12, 0.10, 0.08]),
    "tax_amount":   [np.round(np.random.uniform(800, 25000), 2) if i % 33 != 0
                     else -1000.0  # 3% negative (invalid)
                     for i in range(rows)],
    "status":       np.random.choice(["Submitted", "Approved", "Rejected"], rows),
    "submitted_at": [(datetime.utcnow() - timedelta(days=np.random.randint(0, 60))).isoformat()
                     for _ in range(rows)],
})

csv_path = f"{volume_path}/tax_declarations.csv"
data.to_csv(csv_path, index=False)
print(f"✅ Setup complete. Schema: {SCHEMA}")
print(f"   Source CSV: {csv_path} ({len(data)} rows)")
print(f"   Expected issues: ~{sum(1 for x in data['afm'] if x is None)} NULL ΑΦΜ, "
      f"~{sum(1 for x in data['tax_amount'] if x < 0)} negative amounts, "
      f"~{sum(1 for x in data['region'] if x == 'INVALID')} invalid regions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Exercise 1 — Ingest CSV → Bronze Delta (12 min)
# MAGIC
# MAGIC ### Σενάριο
# MAGIC Έχετε ένα CSV file με φορολογικές δηλώσεις στο volume. Πρέπει να το διαβάσετε
# MAGIC και να το γράψετε ως **Bronze Delta table** με τα παρακάτω requirements:
# MAGIC
# MAGIC ### Requirements
# MAGIC - Όνομα table: `{SCHEMA}.bronze_tax_declarations`
# MAGIC - Πρόσθεστε στήλη `_ingested_at` με current timestamp
# MAGIC - Πρόσθεστε στήλη `_source_file` με το path του CSV (hint: `_metadata.file_path`)
# MAGIC - TBLPROPERTIES: `layer='bronze'`, `data_owner='aade'`, `pii_present='true'`
# MAGIC - COMMENT: 'Bronze layer — raw tax declarations από TAXIS source'
# MAGIC
# MAGIC ### Hints
# MAGIC - `spark.read.csv()` ή SQL `read_files()`
# MAGIC - `withColumn()` για νέες στήλες
# MAGIC - `.write.format("delta").mode("overwrite").saveAsTable()`
# MAGIC - Ή SQL `CREATE OR REPLACE TABLE ... AS SELECT ...`
# MAGIC
# MAGIC ### Expected output
# MAGIC - 200 rows στο Bronze
# MAGIC - 9 columns συνολικά (7 από CSV + 2 metadata)
# MAGIC - DESCRIBE TABLE EXTENDED δείχνει τα TBLPROPERTIES

# COMMAND ----------

# TODO: Διαβάστε το CSV από `csv_path` και γράψτε το ως Bronze Delta table.
#       Προσθέστε τις 2 metadata columns και τα TBLPROPERTIES.

# Your code here:




# Verification (μην το αλλάξετε)
print("=== Verification — Exercise 1 ===")
try:
    df_bronze = spark.table(f"{SCHEMA}.bronze_tax_declarations")
    row_count = df_bronze.count()
    cols = df_bronze.columns
    has_ingested = "_ingested_at" in cols
    has_source = "_source_file" in cols
    print(f"  Rows:              {row_count}            {'✅' if row_count == 200 else '❌ expected 200'}")
    print(f"  _ingested_at col:  {'present' if has_ingested else 'MISSING'}  {'✅' if has_ingested else '❌'}")
    print(f"  _source_file col:  {'present' if has_source else 'MISSING'}   {'✅' if has_source else '❌'}")
except Exception as e:
    print(f"  ❌ Table not found ή error: {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Exercise 2 — Data Quality με Quarantine Pattern (12 min)
# MAGIC
# MAGIC ### Σενάριο
# MAGIC Το Bronze περιέχει invalid rows (NULL ΑΦΜ, αρνητικά ποσά, invalid regions).
# MAGIC Δεν θέλουμε να τα drop-άρουμε σιωπηλά — θέλουμε **quarantine** ώστε να μπορούμε
# MAGIC να τα ελέγξουμε αργότερα.
# MAGIC
# MAGIC ### Requirements
# MAGIC Φτιάξτε 2 tables:
# MAGIC 1. **`{SCHEMA}.silver_tax_clean`** — μόνο valid rows. Κανόνες:
# MAGIC    - `afm IS NOT NULL`
# MAGIC    - `LENGTH(afm) = 9`
# MAGIC    - `tax_amount >= 0`
# MAGIC    - `region != 'INVALID'`
# MAGIC    - `status` σε UPPERCASE
# MAGIC    - `submitted_at` ως TIMESTAMP (όχι string)
# MAGIC
# MAGIC 2. **`{SCHEMA}.quarantine_tax`** — όλα τα invalid rows + extra column `_reason` που
# MAGIC    εξηγεί γιατί απορρίφθηκαν.
# MAGIC
# MAGIC ### Hints
# MAGIC - `.filter()` + `~` (not) για inverse
# MAGIC - `when().otherwise()` για το `_reason` column
# MAGIC - `to_timestamp()` για conversion
# MAGIC - Combine πολλαπλά conditions: `& (col1) & (col2)`
# MAGIC
# MAGIC ### Expected output
# MAGIC - silver_tax_clean: ~165-175 rows (depends on overlaps μεταξύ invalid types)
# MAGIC - quarantine_tax: ~25-35 rows
# MAGIC - Sum = 200

# COMMAND ----------

# TODO: Διαβάστε το bronze table, split σε valid/invalid, γράψτε σε 2 πίνακες.
#       Στο quarantine βάλτε ένα _reason column που εξηγεί γιατί.

# Your code here:




# Verification
print("=== Verification — Exercise 2 ===")
try:
    silver_cnt = spark.table(f"{SCHEMA}.silver_tax_clean").count()
    quarantine_cnt = spark.table(f"{SCHEMA}.quarantine_tax").count()
    total = silver_cnt + quarantine_cnt
    has_reason = "_reason" in spark.table(f"{SCHEMA}.quarantine_tax").columns
    print(f"  Silver clean rows:    {silver_cnt}")
    print(f"  Quarantined rows:     {quarantine_cnt}")
    print(f"  Total:                {total}              {'✅' if total == 200 else '❌ Σύνολο πρέπει = 200'}")
    print(f"  _reason in quarantine: {'present' if has_reason else 'MISSING'} {'✅' if has_reason else '❌'}")
    if has_reason and quarantine_cnt > 0:
        print("\n  Reasons breakdown:")
        spark.table(f"{SCHEMA}.quarantine_tax").groupBy("_reason").count().show(truncate=False)
except Exception as e:
    print(f"  ❌ Error: {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Exercise 3 — Silver Upsert με MERGE (18 min)
# MAGIC
# MAGIC ### Σενάριο
# MAGIC Έρχονται καθημερινά **incremental updates** από TAXIS με νέες δηλώσεις +
# MAGIC corrections παλιών. Πρέπει να κάνετε **MERGE INTO** στο Silver:
# MAGIC - Αν `statement_id` υπάρχει + νέο `submitted_at` πιο πρόσφατο → **UPDATE**
# MAGIC - Αν `statement_id` δεν υπάρχει → **INSERT**
# MAGIC - Αν `statement_id` υπάρχει αλλά νέο `submitted_at` πιο παλιό → **SKIP** (όχι downgrade)
# MAGIC
# MAGIC ### Setup (έτοιμο — μην το αλλάξετε)
# MAGIC Παράγουμε ένα incremental batch:

# COMMAND ----------

# Setup incremental data
incremental_data = [
    # 2 corrections — overwrite υπάρχουσες
    ("TX00001", "900000001", 2025, "ΑΤΤΙΚΗ", 9999.99, "APPROVED", datetime.utcnow().isoformat()),
    ("TX00002", "900000002", 2025, "ΑΤΤΙΚΗ", 5500.00, "APPROVED", datetime.utcnow().isoformat()),
    # 3 new
    ("TX99001", "900099001", 2025, "ΚΡΗΤΗ",   3200.50, "SUBMITTED", datetime.utcnow().isoformat()),
    ("TX99002", "900099002", 2025, "ΠΑΤΡΑ",   7800.00, "APPROVED",  datetime.utcnow().isoformat()),
    ("TX99003", "900099003", 2025, "ΑΤΤΙΚΗ",  12000.00, "APPROVED", datetime.utcnow().isoformat()),
    # 1 STALE — έχει submitted_at παλαιότερο από αυτό που υπάρχει
    ("TX00010", "900000010", 2025, "ΑΤΤΙΚΗ", 100.00, "REJECTED", "2020-01-01T00:00:00"),
]

incremental_df = spark.createDataFrame(
    incremental_data,
    "statement_id STRING, afm STRING, fiscal_year INT, region STRING, "
    "tax_amount DOUBLE, status STRING, submitted_at STRING"
).withColumn("submitted_at", F.to_timestamp("submitted_at"))

print("Incremental batch:")
incremental_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your task
# MAGIC Γράψτε τον MERGE statement που:
# MAGIC - Updates υπάρχουσες rows **μόνο αν** `incremental.submitted_at > existing.submitted_at`
# MAGIC - Inserts νέες rows
# MAGIC - Δεν αγγίζει stale rows
# MAGIC
# MAGIC ### Hints
# MAGIC - `DeltaTable.forName(spark, ...)`
# MAGIC - `.merge(source, condition)`
# MAGIC - `.whenMatchedUpdate(condition=..., set={...})`
# MAGIC - `.whenNotMatchedInsertAll()`
# MAGIC - Σε python-side: `incremental_df.alias("s")` και target `.alias("t")`
# MAGIC - Conditions ως **strings**: `"s.submitted_at > t.submitted_at"`
# MAGIC
# MAGIC ### Expected behavior
# MAGIC - TX00001: tax_amount γίνεται 9999.99 (update)
# MAGIC - TX00002: tax_amount γίνεται 5500.00 (update)
# MAGIC - TX99001, TX99002, TX99003: inserted (silver count +3)
# MAGIC - TX00010: ΔΕΝ αλλάζει (stale skip)

# COMMAND ----------

from delta.tables import DeltaTable

# Capture state πριν το MERGE
print("Πριν το MERGE:")
before_state = spark.sql(f"""
    SELECT statement_id, tax_amount, submitted_at
    FROM {SCHEMA}.silver_tax_clean
    WHERE statement_id IN ('TX00001', 'TX00002', 'TX00010')
    ORDER BY statement_id
""").toPandas()
print(before_state.to_string(index=False))

# TODO: Γράψτε MERGE INTO που updates υπάρχουσες (μόνο αν incremental είναι newer)
#       και inserts νέες. Σκεφτείτε το condition προσεκτικά!

# Your code here:




# Verification
print("\n\n=== Verification — Exercise 3 ===")
try:
    after_state = spark.sql(f"""
        SELECT statement_id, tax_amount, submitted_at
        FROM {SCHEMA}.silver_tax_clean
        WHERE statement_id IN ('TX00001', 'TX00002', 'TX00010',
                                'TX99001', 'TX99002', 'TX99003')
        ORDER BY statement_id
    """).toPandas()
    print("Μετά το MERGE:")
    print(after_state.to_string(index=False))

    # Specific checks
    rows = after_state.set_index("statement_id")
    check_update1 = abs(rows.loc["TX00001", "tax_amount"] - 9999.99) < 0.01
    check_update2 = abs(rows.loc["TX00002", "tax_amount"] - 5500.00) < 0.01
    check_inserts = all(s in after_state["statement_id"].values for s in ["TX99001", "TX99002", "TX99003"])
    check_stale_skip = "TX00010" in after_state.index and rows.loc["TX00010", "tax_amount"] != 100.0

    print(f"\n  TX00001 updated to 9999.99:  {'✅' if check_update1 else '❌'}")
    print(f"  TX00002 updated to 5500.00:  {'✅' if check_update2 else '❌'}")
    print(f"  TX99001/2/3 inserted:        {'✅' if check_inserts else '❌'}")
    print(f"  TX00010 NOT downgraded:      {'✅' if check_stale_skip else '❌ Το stale row υπέγραψε existing!'}")
except Exception as e:
    print(f"  ❌ Error: {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Exercise 4 — Gold με Window Functions (18 min)
# MAGIC
# MAGIC ### Σενάριο
# MAGIC Φτιάξτε **Gold analytical table** `{SCHEMA}.gold_taxpayer_metrics` με 1 row ανά ΑΦΜ
# MAGIC και τα παρακάτω columns:
# MAGIC
# MAGIC | Column | Computation |
# MAGIC |---|---|
# MAGIC | `afm` | partition key |
# MAGIC | `total_declarations` | COUNT(*) |
# MAGIC | `total_tax` | SUM(tax_amount) |
# MAGIC | `approved_count` | COUNT WHERE status='APPROVED' |
# MAGIC | `rejection_rate` | rejected_count / total_declarations |
# MAGIC | `region_avg_tax` | AVG tax της Περιφέρειας του (window function!) |
# MAGIC | `tax_vs_region_avg` | (taxpayer_total / region_avg) σαν ratio |
# MAGIC | `is_above_region_avg` | boolean: tax_vs_region_avg > 1.0 |
# MAGIC | `computed_at` | current_timestamp() |
# MAGIC
# MAGIC ### Hints
# MAGIC - `groupBy("afm")` + aggregations
# MAGIC - Window για region average: `Window.partitionBy("region")`
# MAGIC - Για να βάλεις και region στο aggregation result: keep region στο first groupBy
# MAGIC - `case when status='APPROVED' then 1 else 0 end` για counting

# COMMAND ----------

# TODO: Φτιάξτε το gold_taxpayer_metrics table

# Your code here:




# Verification
print("=== Verification — Exercise 4 ===")
try:
    gold = spark.table(f"{SCHEMA}.gold_taxpayer_metrics")
    cnt = gold.count()
    required_cols = ["afm", "total_declarations", "total_tax", "approved_count",
                     "rejection_rate", "region_avg_tax", "tax_vs_region_avg",
                     "is_above_region_avg", "computed_at"]
    missing_cols = [c for c in required_cols if c not in gold.columns]
    print(f"  Rows:           {cnt}")
    print(f"  Missing cols:   {missing_cols if missing_cols else 'none'}  {'✅' if not missing_cols else '❌'}")

    if not missing_cols:
        print("\n=== Top 10 taxpayers by total tax ===")
        gold.orderBy(col("total_tax").desc()).show(10, truncate=False)

        # Sanity: region_avg_tax πρέπει να είναι ίδιο για όλους τους ΑΦΜ ίδιας Περιφέρειας
        regions_check = gold.select("region_avg_tax").distinct().count()
        unique_afms = gold.select("afm").distinct().count()
        print(f"\n  Unique ΑΦΜ:           {unique_afms}")
        print(f"  Unique region averages: {regions_check} (πρέπει < unique ΑΦΜ)")
except Exception as e:
    print(f"  ❌ Error: {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Block A Complete
# MAGIC
# MAGIC Συγχαρητήρια! Έχετε τώρα:
# MAGIC - 🥉 Bronze table με metadata
# MAGIC - 🥈 Silver clean + quarantine pattern
# MAGIC - 🥈 Silver με MERGE upsert
# MAGIC - 🥇 Gold analytical metrics με window function
# MAGIC
# MAGIC ### 🔍 Reflection questions (2 min — κρατήστε notes)
# MAGIC 1. Γιατί προτιμάμε quarantine αντί για drop;
# MAGIC 2. Πότε χρησιμοποιούμε `whenMatchedUpdate(condition=...)` αντί για `whenMatchedUpdateAll()`;
# MAGIC 3. Πώς θα έκανες το `gold_taxpayer_metrics` ως streaming materialized view;
# MAGIC
# MAGIC ### Next:
# MAGIC Πηγαίνετε στο `02_Exercises_MidLevel.py` για Block B (πιο σύνθετα challenges).
