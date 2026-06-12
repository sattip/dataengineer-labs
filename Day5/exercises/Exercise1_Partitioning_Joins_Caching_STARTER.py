# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ Άσκηση Ημέρα 5 — Μέρος 1/4: Partitioning, Joins & Caching
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~80' · **Δυσκολία:** ⭐⭐⭐ Hard · **~12 TODOs**
# MAGIC > ⚠️ **Cluster:** οι μετρήσεις απόδοσης φαίνονται καλύτερα σε **classic/dedicated cluster**.
# MAGIC > Σε Serverless τα νούμερα partitions/plans ισχύουν, αλλά οι χρόνοι είναι ενδεικτικοί.
# MAGIC
# MAGIC ## 📖 Το Σενάριο
# MAGIC
# MAGIC Έχεις ένα **μεγάλο** fact table με αιτήματα πολιτών (2 εκατ. γραμμές) και ένα **μικρό** dim
# MAGIC με περιφέρειες. Τα queries τρέχουν αργά. Θα μάθεις τα 3 βασικά εργαλεία επιτάχυνσης:
# MAGIC **partitioning, broadcast join, caching** — και πώς να **διαβάζεις το query plan** (`explain`).
# MAGIC
# MAGIC ## 🎯 Τι θα μάθεις
# MAGIC
# MAGIC - `repartition` vs `coalesce` (πλήθος & shuffle).
# MAGIC - **Broadcast join** (μεγάλο ⨝ μικρό) — αποφυγή ακριβού shuffle.
# MAGIC - **Caching** δεδομένων που ξαναχρησιμοποιούνται.
# MAGIC - **`explain()`** — να εντοπίζεις `BroadcastHashJoin` / `Exchange` (shuffle).
# MAGIC - **Partitioned write** (Delta `partitionBy`) για partition pruning.
# MAGIC - **perf log** — μετράμε partitions/χρόνο ανά βήμα.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + synthetic data (έτοιμο)

# COMMAND ----------

import time, io, contextlib
from pyspark.sql.functions import col, broadcast, count, sum as spark_sum, avg, current_timestamp, lit, spark_partition_id

# --- Serverless-safe helpers (χωρίς RDD / _jdf) ---
def num_partitions(df):
    """Πλήθος partitions χωρίς RDD (το .rdd ΔΕΝ επιτρέπεται σε serverless)."""
    return df.select(spark_partition_id().alias("_pid")).distinct().count()

def get_plan(df):
    """Πιάνει το physical plan ως string μέσω explain() (serverless-safe)."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
FACT   = "workspace.aade.perf_requests_fact"
DIM    = "workspace.aade.perf_regions_dim"
PARTED = "workspace.aade.perf_requests_partitioned"
PERFLOG = "workspace.aade.perf_log"

N = 2_000_000
# Big fact table (deterministic — χωρίς randomness)
fact = (spark.range(N)
    .withColumn("afm",        (col("id") % 100000 + 100000000).cast("string"))
    .withColumn("region_id",  (col("id") % 8).cast("int"))
    .withColumn("service_id", (col("id") % 5).cast("int"))
    .withColumn("amount_eur", (col("id") % 1000 + 1).cast("double")))
fact.write.format("delta").mode("overwrite").saveAsTable(FACT)

# Small dim table (broadcastable)
regions = spark.createDataFrame(
    list(enumerate(["Αττική","Κεντρική Μακεδονία","Θεσσαλία","Δυτική Ελλάδα",
                    "Κρήτη","Ιόνια Νησιά","Πελοπόννησος","Ήπειρος"])),
    ["region_id","region_name"])
regions.write.format("delta").mode("overwrite").saveAsTable(DIM)

spark.sql(f"CREATE TABLE IF NOT EXISTS {PERFLOG} (step STRING, n_partitions INT, rows BIGINT, duration_ms BIGINT, logged_at TIMESTAMP) USING delta")

def timed(step, df_action):
    t0 = time.time(); result = df_action(); ms = int((time.time()-t0)*1000)
    print(f"  ⏱️ {step}: {ms} ms"); return result, ms

print(f"✓ Fact={spark.table(FACT).count():,} · Dim={spark.table(DIM).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Partitions: `repartition` vs `coalesce`
# MAGIC
# MAGIC Ένα DataFrame είναι χωρισμένο σε **partitions** (κομμάτια που τρέχουν παράλληλα).
# MAGIC - **`repartition(n)`** — αλλάζει σε **ακριβώς n** partitions· κάνει **full shuffle** (ακριβό αλλά ισορροπημένο).
# MAGIC - **`coalesce(n)`** — **μειώνει** partitions **χωρίς** full shuffle (φθηνό, για να μαζέψεις μικρά).
# MAGIC
# MAGIC `num_partitions(df)` (helper, serverless-safe) δείχνει το τρέχον πλήθος.
# MAGIC > ⚠️ Σε **serverless** το `df.rdd.getNumPartitions()` ΔΕΝ επιτρέπεται (RDD API). Γι' αυτό
# MAGIC > μετράμε με τη `spark_partition_id()` μέσα στον helper `num_partitions()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — repartition & coalesce

# COMMAND ----------

base = spark.table(FACT)
print(f"Αρχικά partitions: {num_partitions(base)}")

# 1a: ξανα-διαμοίρασε σε ΑΚΡΙΒΩΣ 16 partitions (full shuffle)
rep = base.____________(16)                       # TODO 1a: repartition
n_rep = num_partitions(rep)
print(f"Μετά repartition(16): {n_rep}")

# 1b: μείωσε σε 4 partitions ΧΩΡΙΣ full shuffle
col4 = rep.____________(4)                         # TODO 1b: coalesce
n_col = num_partitions(col4)
print(f"Μετά coalesce(4): {n_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Broadcast join (μεγάλο ⨝ μικρό)
# MAGIC
# MAGIC Όταν ενώνεις ένα **τεράστιο** fact με ένα **μικρό** dim, ο default (sort-merge) join κάνει
# MAGIC **shuffle** και των δύο → αργό. Με **`broadcast(small)`** ο Spark στέλνει αντίγραφο του μικρού
# MAGIC σε κάθε node → **κανένα shuffle** του μεγάλου. Στο `explain()` βλέπεις **`BroadcastHashJoin`**
# MAGIC αντί για `SortMergeJoin` + `Exchange`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Broadcast join + διάβασε το plan

# COMMAND ----------

dim = spark.table(DIM)

# 2a: κάνε join με broadcast hint στο μικρό dim
joined = spark.table(FACT).join(____________(dim), on="__________", how="____")   # TODO 2a: broadcast · 2b: "region_id" · 2c: "left"

# 2b: δες το physical plan (serverless-safe μέσω explain)
plan = get_plan(joined)
has_broadcast = "BroadcastHashJoin" in plan
print(f"Plan περιέχει BroadcastHashJoin; → {has_broadcast}")

# σύγκριση: χωρίς broadcast (default)
plain = spark.table(FACT).join(dim, on="region_id", how="left")
print(f"Default join → BroadcastHashJoin; {('BroadcastHashJoin' in get_plan(plain))} (μπορεί να το επιλέξει αυτόματα ο AQE)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — «Caching» (materialization) σε Serverless
# MAGIC
# MAGIC Όταν ξαναχρησιμοποιείς ένα ακριβό αποτέλεσμα, θες να μην το ξανα-υπολογίζεις.
# MAGIC ⚠️ Σε **Serverless** το `.cache()` / `.persist()` **ΔΕΝ υποστηρίζεται** (`PERSIST TABLE not supported`)
# MAGIC — το Serverless κάνει auto-caching. Ο serverless-safe τρόπος να «υλοποιήσεις & επαναχρησιμοποιήσεις»
# MAGIC είναι να **γράψεις σε Delta table** και να διαβάζεις από αυτό.
# MAGIC > 💡 Σε **classic cluster** θα έγραφες `agg.cache()` + `agg.is_cached`. Εδώ κάνουμε materialize.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Materialize ένα reused aggregate (serverless-safe)

# COMMAND ----------

agg = (joined.groupBy("____________")                                     # TODO 3a: region_name
       .agg(count("*").alias("n"), spark_sum("__________").alias("total")))   # TODO 3b: amount_eur

# 1η χρήση (cold) — υπολογισμός από το joined (shuffle + join)
_, ms1 = timed("cold (από joined)", lambda: agg.count())

# 3c: «cache» serverless-safe = MATERIALIZE σε Delta table
AGG_TBL = "workspace.aade.perf_agg_materialized"
agg.write.format("delta").mode("__________").saveAsTable(AGG_TBL)   # TODO 3c: overwrite
agg_fast = spark.table(AGG_TBL)

# 2η χρήση — διαβάζει από το materialized Delta (γρήγορα, χωρίς ξανα-υπολογισμό)
_, ms2 = timed("materialized (από Delta)", lambda: agg_fast.orderBy(col("total").desc()).collect())
print(f"materialized table: {AGG_TBL}")
agg_fast.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — Partitioned write (partition pruning)
# MAGIC
# MAGIC Γράφοντας ένα Delta table **`partitionBy("region_name")`**, τα δεδομένα χωρίζονται σε φακέλους
# MAGIC ανά περιφέρεια. Ένα query `WHERE region_name='Αττική'` διαβάζει **μόνο** εκείνον τον φάκελο
# MAGIC (**partition pruning**) → πολύ λιγότερο I/O.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Γράψε partitioned & επιβεβαίωσε pruning

# COMMAND ----------

# 4a: γράψε partitioned by region_name
(joined.write.format("delta").mode("overwrite")
    .____________("region_name")                   # TODO 4a: partitionBy
    .saveAsTable(PARTED))

# 4b: επιβεβαίωσε τα partition columns
detail = spark.sql(f"DESCRIBE DETAIL {PARTED}").collect()[0]
print(f"partitionColumns = {detail['partitionColumns']}")

# 4c: query με φίλτρο — δες partition pruning στο plan
one_region = spark.table(PARTED).filter(col("region_name") == "________")   # TODO 4b: "Αττική" (partition pruning)
pruned_plan = get_plan(one_region)
print("PartitionFilters στο plan;", "PartitionFilters" in pruned_plan or "region_name" in pruned_plan)
print(f"Γραμμές Αττικής: {one_region.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — Κατέγραψε perf metrics στο perf_log

# COMMAND ----------

(spark.createDataFrame(
    [("repartition16", n_rep, spark.table(FACT).count(), 0),
     ("coalesce4",     n_col, spark.table(FACT).count(), 0),
     ("agg_cold",          None, agg.count(),       ms1),
     ("agg_materialized",  None, agg_fast.count(),  ms2)],
    ["step","n_partitions","rows","duration_ms"])
 .withColumn("logged_at", current_timestamp())
 .write.format("delta").mode("________").saveAsTable(PERFLOG))   # TODO 5: append
print("📊 perf_log:")
spark.table(PERFLOG).orderBy("logged_at").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 1

# COMMAND ----------

results = {
    "repartition → 16 partitions":      n_rep == 16,
    "coalesce → ≤ 16 (μείωση)":         n_col <= 16,
    "Broadcast join στο plan":          has_broadcast,
    "Materialized table δημιουργήθηκε":  spark.catalog.tableExists("workspace.aade.perf_agg_materialized"),
    "Materialized == agg (ορθότητα)":    spark.table("workspace.aade.perf_agg_materialized").count() == agg.count(),
    "Partitioned by region_name":       detail["partitionColumns"] == ["region_name"],
    "perf_log ≥ 4 βήματα":              spark.table(PERFLOG).count() >= 4,
}
print("=" * 55)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 55)
print("🎉 Τέλος Μέρους 1!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise2_DataSkew_STARTER`
# MAGIC
# MAGIC Οι joins/aggregations γίνονται **πολύ** αργές όταν τα δεδομένα είναι **skewed** (ένα κλειδί
# MAGIC έχει το 90% των γραμμών). Στο Μέρος 2: πώς το **εντοπίζεις** και το διορθώνεις με **salting**.
