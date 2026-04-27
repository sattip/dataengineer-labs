# Databricks notebook source
# MAGIC %md
# MAGIC # Άσκηση #6 — Change Data Feed (CDF) & Incremental ETL (Ημέρα 3)
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC
# MAGIC > **GitHub** (share this στο Zoom chat):
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/CDF_Incremental_Notebook.py
# MAGIC > ```
# MAGIC
# MAGIC **Catalog/Schema**: `workspace.aade`
# MAGIC **Volume**: `/Volumes/workspace/aade/aade_data/`
# MAGIC **Διάρκεια**: ~25 min
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Σενάριο
# MAGIC
# MAGIC Είσαι **Data Engineer της ΑΑΔΕ**. Έχεις το `tax_declarations_silver` (300 rows) και πρέπει να φτιάξεις ένα **Gold aggregation table** (`tax_kpis_gold`) που τροφοδοτεί το dashboard του Υπουργού.
# MAGIC
# MAGIC ### Το πρόβλημα
# MAGIC
# MAGIC Κάθε φορά που αλλάζει το silver (νέες δηλώσεις, διορθώσεις, ακυρώσεις), **δεν θες να ξανατρέξεις full reload** του Gold — είναι αργό + ακριβό σε compute. Θες **incremental refresh**: μόνο τα rows που άλλαξαν.
# MAGIC
# MAGIC ### Η λύση: Change Data Feed (CDF)
# MAGIC
# MAGIC Το Delta CDF δίνει per-row change records (INSERT, UPDATE_PRE, UPDATE_POST, DELETE) που μπορείς να καταναλώσεις για να ενημερώσεις downstream tables incremental.
# MAGIC
# MAGIC Σε αυτή την άσκηση θα:
# MAGIC 1. **Enable CDF** στο silver
# MAGIC 2. **Bootstrap Gold** με full snapshot
# MAGIC 3. **Simulate changes** (INSERT + UPDATE + DELETE) στο silver
# MAGIC 4. **Read CDF** με `table_changes()`
# MAGIC 5. **Apply changes** στο Gold με MERGE (incremental)
# MAGIC 6. **Verify consistency** (Gold = aggregation του current silver)
# MAGIC
# MAGIC ## 📋 Notebook structure
# MAGIC
# MAGIC | Cell | Step | Time |
# MAGIC |---|------|------|
# MAGIC | 0 | Setup + download | 30s |
# MAGIC | 0.5 | Bootstrap silver | 1 min |
# MAGIC | 1 | Enable CDF στο silver | 5s |
# MAGIC | 2 | Bootstrap Gold (full aggregation) | 10s |
# MAGIC | 3 | Simulate INSERT στο silver | 10s |
# MAGIC | 4 | Simulate UPDATE στο silver | 10s |
# MAGIC | 5 | Simulate DELETE στο silver | 10s |
# MAGIC | 6 | Read CDF με `table_changes()` | 5s |
# MAGIC | 7 | Apply CDF στο Gold (MERGE) | 30s |
# MAGIC | 8 | Verify consistency | 5s |
# MAGIC | 9 | Production patterns (streaming CDF) | — |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0 — Setup + Download

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

import urllib.request, os
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3"
VOL = "/Volumes/workspace/aade/aade_data"

for fname in ["declarations.csv", "doy.csv", "employees.csv", "taxpayers.csv"]:
    target = f"{VOL}/{fname}"
    if os.path.exists(target):
        print(f"⏭️  {fname}")
    else:
        urllib.request.urlretrieve(f"{REPO}/{fname}", target)
        print(f"✅ {fname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0.5 — Bootstrap silver (αν λείπει)

# COMMAND ----------

existing = {t.tableName for t in spark.sql("SHOW TABLES IN workspace.aade").collect()}
if "tax_declarations_silver" in existing:
    cnt = spark.sql("SELECT COUNT(*) AS n FROM workspace.aade.tax_declarations_silver").collect()[0]["n"]
    print(f"✅ tax_declarations_silver υπάρχει ({cnt} rows)")
else:
    print("⚠️  silver λείπει — bootstrapping...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{VOL}/declarations.csv")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable("workspace.aade.tax_declarations_silver")
    print(f"   ✅ Bootstrapped {df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 1 — Enable CDF στο silver
# MAGIC
# MAGIC Το CDF δεν είναι default-on. Πρέπει να το ενεργοποιήσουμε με `TBLPROPERTIES`. **Από εκεί και πέρα**, κάθε write (INSERT/UPDATE/DELETE/MERGE) καταγράφει per-row change records.
# MAGIC
# MAGIC ⚠️ Tο CDF καταγράφει αλλαγές **από εδώ και μετά** — δεν retroactively για παλιές versions.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.aade.tax_declarations_silver
# MAGIC SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')

# COMMAND ----------

# Καταγραφή starting version — από εκεί θα διαβάζουμε CDF
start_version = spark.sql("DESCRIBE HISTORY workspace.aade.tax_declarations_silver").collect()[0]["version"]
print(f"📌 CDF enabled. Starting version: {start_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 2 — Bootstrap Gold με full aggregation
# MAGIC
# MAGIC Πρώτη φορά: κάνουμε full read του silver και υπολογίζουμε aggregations ανά **Κατηγορία_Φόρου × Φορ_Ετος**.
# MAGIC
# MAGIC Από εδώ και πέρα, μόνο **incremental refresh**.

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, count, col, lit, current_timestamp

gold_df = (spark.table("workspace.aade.tax_declarations_silver")
    .groupBy("`Κατηγορία_Φόρου`", "`Φορ_Ετος`")
    .agg(
        spark_sum("`Ποσό_EUR`").alias("total_eur"),
        count("*").alias("declaration_count")
    )
    .withColumn("last_updated", current_timestamp())
)

(gold_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.aade.tax_kpis_gold"))

print(f"✅ Gold bootstrapped: {gold_df.count()} aggregation rows")
gold_df.orderBy(col("total_eur").desc()).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 3 — Simulate INSERT (3 νέες δηλώσεις ΦΠΑ)
# MAGIC
# MAGIC Πρωί Δευτέρας: ήρθαν 3 νέες δηλώσεις ΦΠΑ που πρέπει να μπουν στο silver.

# COMMAND ----------

silver_schema = spark.table("workspace.aade.tax_declarations_silver").schema

new_rows_template = spark.table("workspace.aade.tax_declarations_silver").limit(3).toPandas().to_dict("records")
new_data = [
    {"ΔηλωσηID": 90001, "Κατηγορία_Φόρου": "ΦΠΑ", "Φορ_Ετος": 2024, "Ποσό_EUR": 5000.0, "Επωνυμία": "NEW_RECORD_01"},
    {"ΔηλωσηID": 90002, "Κατηγορία_Φόρου": "ΦΠΑ", "Φορ_Ετος": 2024, "Ποσό_EUR": 7500.0, "Επωνυμία": "NEW_RECORD_02"},
    {"ΔηλωσηID": 90003, "Κατηγορία_Φόρου": "Εισοδήματος", "Φορ_Ετος": 2024, "Ποσό_EUR": 3200.0, "Επωνυμία": "NEW_RECORD_03"},
]
for tmpl, override in zip(new_rows_template, new_data):
    tmpl.update(override)

inserts_df = spark.createDataFrame(new_rows_template, schema=silver_schema)
inserts_df.write.format("delta").mode("append").saveAsTable("workspace.aade.tax_declarations_silver")
print(f"✅ INSERT: 3 νέες δηλώσεις προστέθηκαν στο silver.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 4 — Simulate UPDATE (διόρθωση ποσού σε υπάρχουσα δήλωση)
# MAGIC
# MAGIC Φορολογικός υπάλληλος εντόπισε λάθος σε δήλωση ID 1 — διορθώνει το `Ποσό_EUR` σε διπλάσιο.
# MAGIC
# MAGIC > **Note**: το CSV έχει IDs 1–300. Επιλέγουμε ID 1 ώστε το UPDATE να βρει σίγουρα 1 row.

# COMMAND ----------

# Verify ότι το ID που πρόκειται να update υπάρχει
target_id = 1
exists = spark.sql(f"SELECT COUNT(*) AS n FROM workspace.aade.tax_declarations_silver WHERE `ΔηλωσηID` = {target_id}").collect()[0]["n"]
assert exists > 0, f"ΔηλωσηID = {target_id} δεν υπάρχει στο silver — το UPDATE θα επηρεάσει 0 rows"
print(f"✅ ΔηλωσηID = {target_id} υπάρχει ({exists} row). Συνεχίζουμε στο UPDATE.")

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE workspace.aade.tax_declarations_silver
# MAGIC SET `Ποσό_EUR` = `Ποσό_EUR` * 2
# MAGIC WHERE `ΔηλωσηID` = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 5 — Simulate DELETE (ακύρωση δήλωσης)
# MAGIC
# MAGIC Φορολογούμενος ακύρωσε τη δήλωση ID 2 — διαγραφή.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM workspace.aade.tax_declarations_silver
# MAGIC WHERE `ΔηλωσηID` = 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6 — Read CDF με `table_changes()`
# MAGIC
# MAGIC Το CDF function επιστρέφει **change records** για κάθε row που άλλαξε. Έχει 4 τύπους στο `_change_type`:
# MAGIC
# MAGIC | `_change_type` | Σημασία |
# MAGIC |---|---|
# MAGIC | `insert` | Νέο row |
# MAGIC | `update_preimage` | Προηγούμενη τιμή του row (πριν το update) |
# MAGIC | `update_postimage` | Νέα τιμή του row (μετά το update) |
# MAGIC | `delete` | Διαγραμμένο row |
# MAGIC
# MAGIC Συν 2 metadata columns: `_commit_version`, `_commit_timestamp`.

# COMMAND ----------

changes_df = spark.sql(f"""
  SELECT
    _change_type,
    _commit_version,
    `ΔηλωσηID`,
    `Κατηγορία_Φόρου`,
    `Φορ_Ετος`,
    `Ποσό_EUR`,
    `Επωνυμία`
  FROM table_changes('workspace.aade.tax_declarations_silver', {start_version + 1})
  ORDER BY _commit_version, _change_type
""")

print(f"📋 CDF changes από version {start_version + 1}:")
changes_df.show(truncate=False)

# Πόσες αλλαγές ανά τύπο
print("\nΣύνοψη ανά τύπο αλλαγής:")
changes_df.groupBy("_change_type").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 7 — Apply CDF στο Gold (incremental MERGE)
# MAGIC
# MAGIC ### Στρατηγική
# MAGIC
# MAGIC Επειδή το Gold είναι **aggregation**, δεν μπορούμε να εφαρμόσουμε row-level changes 1-1. Αντιθέτως:
# MAGIC
# MAGIC 1. Βρίσκουμε **ποια aggregation keys** (Κατηγορία × Έτος) επηρεάστηκαν από αλλαγές
# MAGIC 2. Για αυτά τα keys, **ξανατρέχουμε aggregation** μόνο από το current silver
# MAGIC 3. **MERGE** στο Gold: update τα affected, leave the rest untouched
# MAGIC
# MAGIC Αυτό = **partial recompute**. Cost: O(affected_keys), όχι O(all_keys).

# COMMAND ----------

# 1. Βρίσκουμε affected aggregation keys από CDF
affected_keys = (changes_df
    .select("`Κατηγορία_Φόρου`", "`Φορ_Ετος`")
    .distinct())

print("🎯 Affected aggregation keys:")
affected_keys.show()

# 2. Recompute aggregation ΜΟΝΟ για αυτά τα keys
silver = spark.table("workspace.aade.tax_declarations_silver")
recomputed = (silver
    .join(affected_keys, on=["Κατηγορία_Φόρου", "Φορ_Ετος"], how="inner")
    .groupBy("`Κατηγορία_Φόρου`", "`Φορ_Ετος`")
    .agg(
        spark_sum("`Ποσό_EUR`").alias("total_eur"),
        count("*").alias("declaration_count")
    )
    .withColumn("last_updated", current_timestamp())
)

# Stage σε temp view για το MERGE
recomputed.createOrReplaceTempView("gold_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. MERGE στο Gold: update τα affected, ignore τα υπόλοιπα
# MAGIC MERGE INTO workspace.aade.tax_kpis_gold AS target
# MAGIC USING gold_updates AS source
# MAGIC ON  target.`Κατηγορία_Φόρου` = source.`Κατηγορία_Φόρου`
# MAGIC AND target.`Φορ_Ετος`        = source.`Φορ_Ετος`
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   total_eur = source.total_eur,
# MAGIC   declaration_count = source.declaration_count,
# MAGIC   last_updated = source.last_updated
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 8 — Verify consistency
# MAGIC
# MAGIC Σύγκρινε Gold (incremental) με ένα **fresh full recompute** του silver. Πρέπει να συμφωνούν.

# COMMAND ----------

# Fresh aggregation από silver
fresh = (spark.table("workspace.aade.tax_declarations_silver")
    .groupBy("`Κατηγορία_Φόρου`", "`Φορ_Ετος`")
    .agg(
        spark_sum("`Ποσό_EUR`").alias("total_eur_fresh"),
        count("*").alias("count_fresh")
    ))

# Read τρέχον Gold
gold_now = spark.table("workspace.aade.tax_kpis_gold").select(
    "`Κατηγορία_Φόρου`", "`Φορ_Ετος`",
    col("total_eur").alias("total_eur_gold"),
    col("declaration_count").alias("count_gold")
)

# Diff
comparison = fresh.join(gold_now, on=["Κατηγορία_Φόρου", "Φορ_Ετος"], how="full")
comparison = comparison.withColumn(
    "match",
    (col("total_eur_fresh") == col("total_eur_gold")) &
    (col("count_fresh") == col("count_gold"))
)

print("📊 Consistency check (Gold vs full recompute):")
comparison.show(truncate=False)

mismatches = comparison.filter(~col("match")).count()
if mismatches == 0:
    print("✅ PASS — Gold είναι consistent με full recompute.")
else:
    print(f"❌ FAIL — {mismatches} mismatches!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 9 — Production patterns
# MAGIC
# MAGIC ### Pattern 1: Streaming CDF
# MAGIC
# MAGIC Σε real production, αντί για manual `table_changes()`, χρησιμοποιείς **streaming**:
# MAGIC
# MAGIC ```python
# MAGIC stream = (spark.readStream
# MAGIC   .format("delta")
# MAGIC   .option("readChangeFeed", "true")
# MAGIC   .table("workspace.aade.tax_declarations_silver"))
# MAGIC
# MAGIC stream.writeStream.foreachBatch(apply_changes_to_gold).start()
# MAGIC ```
# MAGIC
# MAGIC Το Gold ενημερώνεται αυτόματα κάθε φορά που γράφει το silver — **near real-time KPIs**.
# MAGIC
# MAGIC ### Pattern 2: CDC από / προς external systems
# MAGIC
# MAGIC | Use case | Pattern |
# MAGIC |---|---|
# MAGIC | Sync Delta → Postgres | `table_changes()` → JDBC writes ανά τύπο (INSERT/UPDATE/DELETE) |
# MAGIC | Sync Delta → Kafka | streaming CDF → Kafka producer με change_type ως key |
# MAGIC | Sync external → Delta | Kafka/CDC tool (Debezium) → MERGE στο Delta |
# MAGIC
# MAGIC ### Πότε χρησιμοποιείς CDF
# MAGIC
# MAGIC | ✅ Καλό | ❌ Όχι ιδανικό |
# MAGIC |---|---|
# MAGIC | Incremental ETL με <50% changes ανά batch | Full reload faster αν αλλάζει >80% του data |
# MAGIC | Sync downstream tables / external systems | Big bulk loads (full overwrite) |
# MAGIC | Audit trail per-row changes | Time travel based recovery (use VERSION AS OF) |
# MAGIC | CDC pipelines (push to Kafka/Postgres) | One-time analytics |
# MAGIC
# MAGIC ### Cost considerations
# MAGIC
# MAGIC - CDF γράφει **extra files** (`_change_data` directory) — ~10–20% overhead στο storage
# MAGIC - `table_changes()` διαβάζει transaction log + change files — fast
# MAGIC - Default retention: ίδιο με Delta history (30 days). Configurable με `delta.changeDataFeed.retentionDuration`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Wrap-up
# MAGIC
# MAGIC | Δυνατότητα | Command | Use case |
# MAGIC |---|---|---|
# MAGIC | **Enable CDF** | `ALTER TABLE ... SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')` | One-time setup |
# MAGIC | **Read changes** | `SELECT * FROM table_changes(table, start_version)` | Per-batch reads |
# MAGIC | **Streaming changes** | `spark.readStream...option('readChangeFeed', 'true')` | Continuous CDC |
# MAGIC | **Apply changes** | MERGE με affected keys → partial recompute | Incremental Gold refresh |
# MAGIC
# MAGIC ### Κλειστή ερώτηση για τους trainees
# MAGIC
# MAGIC > "Έχεις 50M-row silver table που ενημερώνεται 1000 rows/min. Gold KPI table πρέπει να ενημερώνεται κάθε 5 min. **Ποιο είναι 10× φθηνότερο: full recompute κάθε 5 min ή CDF-based MERGE;**"
# MAGIC
# MAGIC **Απάντηση**: CDF-based — γιατί διαβάζεις ~5000 rows αλλαγές αντί για 50M. Cost = O(changes), όχι O(total).
