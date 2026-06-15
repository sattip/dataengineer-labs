# Databricks notebook source
# MAGIC %md
# MAGIC # 🧊 Άσκηση Ημέρα 5 — Μέρος 5/6 (Advanced): Liquid Clustering & Data Skipping
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~75' · **Δυσκολία:** ⭐⭐⭐⭐ Advanced · **~9 TODOs**
# MAGIC > Self-contained · τρέχει σε **Serverless** (Delta features, χωρίς RDD/cache).
# MAGIC
# MAGIC ## 📖 Πού πάμε πιο πέρα
# MAGIC
# MAGIC Στο Μέρος 1 είδαμε **partitioning** (`partitionBy`). Έχει όμως προβλήματα: άκαμπτοι φάκελοι,
# MAGIC over/under-partitioning, κακό για high-cardinality στήλες. Η σύγχρονη απάντηση της Databricks
# MAGIC είναι το **Liquid Clustering** (`CLUSTER BY`): αυτο-ισορροπούμενο, πολυδιάστατο, αλλάζει χωρίς
# MAGIC rewrite. Θα δούμε επίσης **data skipping** (στατιστικά → διαβάζει λιγότερα files) και
# MAGIC **Deletion Vectors** (γρήγορα DELETE/UPDATE χωρίς να ξαναγράφεις files).
# MAGIC
# MAGIC ## 🎯 Τι θα μάθεις
# MAGIC
# MAGIC - **Liquid Clustering** (`CLUSTER BY`) vs partitioning — πότε & γιατί.
# MAGIC - `OPTIMIZE` σε clustered table (clustering compaction).
# MAGIC - **Data skipping** via min/max stats (`DESCRIBE DETAIL`, file pruning).
# MAGIC - **Deletion Vectors** — soft deletes χωρίς rewrite.
# MAGIC - `ALTER TABLE ... CLUSTER BY` (αλλαγή clustering keys χωρίς rewrite).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + synthetic source (έτοιμο)

# COMMAND ----------

import io, contextlib
from pyspark.sql.functions import col, count

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
SRC       = "workspace.aade.lc_source"
CLUSTERED = "workspace.aade.lc_clustered"

def get_plan(df):  # serverless-safe (χωρίς _jdf)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()

N = 3_000_000
(spark.range(N)
    .withColumn("afm",        (col("id") % 100000 + 100000000).cast("string"))
    .withColumn("region_id",  (col("id") % 8).cast("int"))
    .withColumn("service_id", (col("id") % 5).cast("int"))
    .withColumn("amount_eur", (col("id") % 1000 + 1).cast("double"))
 .write.format("delta").mode("overwrite").saveAsTable(SRC))
print(f"✓ Source: {spark.table(SRC).count():,} γραμμές")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Liquid Clustering (`CLUSTER BY`)
# MAGIC
# MAGIC Αντί για άκαμπτους φακέλους (`partitionBy`), το `CLUSTER BY` ομαδοποιεί «κοντινά» δεδομένα σε
# MAGIC files **δυναμικά & πολυδιάστατα**. Πλεονεκτήματα: καλό και για high-cardinality, αλλάζεις keys
# MAGIC χωρίς rewrite, καμία over-partitioning. Σύνταξη:
# MAGIC ```sql
# MAGIC CREATE TABLE t CLUSTER BY (c1, c2) AS SELECT ...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Φτιάξε clustered table (CLUSTER BY region_id, service_id)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CLUSTERED}
    ____________ (region_id, service_id)        -- TODO 1a: CLUSTER BY
    AS SELECT * FROM {SRC}
""")
detail = spark.sql(f"DESCRIBE DETAIL {CLUSTERED}").collect()[0]
print(f"clusteringColumns = {detail['clusteringColumns']}")   # αναμένεται [region_id, service_id]
print(f"numFiles = {detail['numFiles']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — `OPTIMIZE` σε clustered table
# MAGIC
# MAGIC Το `OPTIMIZE` σε liquid-clustered table κάνει **clustering** (συγκεντρώνει κοντινά δεδομένα στα
# MAGIC ίδια files) + compaction. Μετά, queries με φίλτρο στα clustering keys διαβάζουν **λιγότερα files**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — OPTIMIZE & δες το ιστορικό

# COMMAND ----------

spark.sql(f"________ {CLUSTERED}")                            # TODO 2a: OPTIMIZE
opt_ran = spark.sql(f"DESCRIBE ________ {CLUSTERED}").filter("operation = 'OPTIMIZE'").count()   # TODO 2b: HISTORY
print(f"OPTIMIZE operations στο history: {opt_ran}")
files_after = spark.sql(f"DESCRIBE DETAIL {CLUSTERED}").collect()[0]["numFiles"]
print(f"numFiles μετά OPTIMIZE: {files_after}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — Data skipping (min/max stats → file pruning)
# MAGIC
# MAGIC Το Delta κρατά **min/max** ανά file για τις πρώτες στήλες. Ένα query `WHERE region_id=3` διαβάζει
# MAGIC **μόνο** τα files που μπορεί να περιέχουν region_id=3 (data skipping). Στο `explain(formatted)`
# MAGIC το βλέπεις ως `PushedFilters` / `DataFilters`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Query με φίλτρο στα clustering keys

# COMMAND ----------

q = spark.table(CLUSTERED).filter((col("__________") == 3) & (col("service_id") == ___))   # TODO 3a: region_id · TODO 3b: 2
plan = get_plan(q)
print("PushedFilters/DataFilters στο plan;",
      ("PushedFilters" in plan) or ("DataFilters" in plan) or ("region_id" in plan))
print(f"Γραμμές (region_id=3, service_id=2): {q.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — Deletion Vectors (soft deletes)
# MAGIC
# MAGIC Κανονικά ένα `DELETE` ξαναγράφει ολόκληρα files. Με **Deletion Vectors** το Delta **σημειώνει**
# MAGIC ποιες γραμμές διαγράφηκαν (χωρίς rewrite) → πολύ ταχύτερα DELETE/UPDATE (merge-on-read).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Ενεργοποίησε Deletion Vectors & κάνε DELETE

# COMMAND ----------

# 4a: ενεργοποίησε deletion vectors
spark.sql(f"ALTER TABLE {CLUSTERED} SET TBLPROPERTIES (delta.________________ = true)")   # TODO 4a: enableDeletionVectors
before = spark.table(CLUSTERED).count()

# 4b: διέγραψε όλα τα service_id = 0
spark.sql(f"________ FROM {CLUSTERED} WHERE service_id = 0")                               # TODO 4b: DELETE
after = spark.table(CLUSTERED).count()
print(f"Πριν: {before:,} · Μετά DELETE: {after:,} (διαγράφηκαν {before-after:,})")

dv_on = spark.sql(f"SHOW TBLPROPERTIES {CLUSTERED}").filter("key='delta.enableDeletionVectors'").collect()
print("Deletion Vectors:", dv_on[0]["value"] if dv_on else "n/a")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 5 — Αλλαγή clustering keys χωρίς rewrite
# MAGIC
# MAGIC Σε αντίθεση με partitioning (που απαιτεί πλήρες rewrite), τα clustering keys αλλάζουν **άμεσα**:
# MAGIC `ALTER TABLE t CLUSTER BY (νέες στήλες)`. Τα νέα δεδομένα clustered με τα νέα keys.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — Άλλαξε τα clustering keys

# COMMAND ----------

spark.sql(f"ALTER TABLE {CLUSTERED} ____________ (afm)")      # TODO 5a: CLUSTER BY
new_detail = spark.sql(f"DESCRIBE DETAIL {CLUSTERED}").collect()[0]
print(f"Νέα clusteringColumns = {new_detail['clusteringColumns']}")   # [afm]

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 5

# COMMAND ----------

results = {
    "clusteringColumns = region_id+service_id (αρχικά)": set(detail["clusteringColumns"]) == {"region_id","service_id"},
    "OPTIMIZE έτρεξε":                  opt_ran >= 1,
    "Query φίλτρο επέστρεψε γραμμές":   q.count() > 0,
    "Deletion Vectors = true":          bool(dv_on) and dv_on[0]["value"] == "true",
    "DELETE μείωσε τις γραμμές":        after < before,
    "Clustering keys άλλαξαν σε [afm]":  set(new_detail["clusteringColumns"]) == {"afm"},
}
print("=" * 56)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 56)
print("🎉 Τέλος Μέρους 5!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise6_PII_Tokenization_ABAC_STARTER`
# MAGIC
# MAGIC Advanced security: **pseudonymization** (sha2 hashing ΑΦΜ) και **ABAC** row-level security με
# MAGIC πίνακα εξουσιοδοτήσεων (`current_user()`) — αντί για hardcoded κανόνες.
