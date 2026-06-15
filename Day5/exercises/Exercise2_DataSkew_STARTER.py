# Databricks notebook source
# MAGIC %md
# MAGIC # 🧨 Άσκηση Ημέρα 5 — Μέρος 2/4: Data Skew (Detection + Salting)
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~75' · **Δυσκολία:** ⭐⭐⭐⭐ Advanced · **~10 TODOs**
# MAGIC > Self-contained (synthetic skewed data).
# MAGIC
# MAGIC ## 📖 Το πρόβλημα
# MAGIC
# MAGIC Στην ΑΑΔΕ, λίγα ΑΦΜ (π.χ. μεγάλες δημόσιες επιχειρήσεις) έχουν **δυσανάλογα** πολλές
# MAGIC εγγραφές. Όταν κάνεις `groupBy(afm)` ή join, **ένα** task παίρνει το **90%** της δουλειάς ενώ
# MAGIC τα υπόλοιπα κάθονται → το job «κολλάει» στο 99%. Αυτό λέγεται **data skew**.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθεις
# MAGIC
# MAGIC - Πώς να **εντοπίζεις** skew (κατανομή κλειδιού + μέγεθος partitions).
# MAGIC - Γιατί ένα «καυτό» κλειδί κάνει ένα partition τεράστιο.
# MAGIC - **Salting** — σπάμε το καυτό κλειδί σε N «κουβάδες» → ισορροπία.
# MAGIC - **Two-stage aggregation** με salt, και **απόδειξη ορθότητας** (salted == plain).
# MAGIC - Το **AQE skew-join** (αυτόματος χειρισμός).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + SKEWED synthetic data (έτοιμο)

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, count, sum as spark_sum, max as spark_max, avg,
    spark_partition_id, round as spark_round
)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
FACT = "workspace.aade.skew_fact"
N = 2_000_000
HOT_AFM = "100000000"   # το «καυτό» κλειδί
SALT_N = 16

# 90% των γραμμών έχουν το HOT_AFM· το υπόλοιπο 10% κατανέμεται σε 100.000 ΑΦΜ
fact = (spark.range(N)
    .withColumn("afm",
        when(col("id") % 10 != 0, lit(HOT_AFM))
        .otherwise((col("id") % 100000 + 200000000).cast("string")))
    .withColumn("amount_eur", (col("id") % 1000 + 1).cast("double")))
fact.write.format("delta").mode("overwrite").saveAsTable(FACT)
print(f"✓ Fact={spark.table(FACT).count():,} (90% στο HOT_AFM={HOT_AFM})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Εντοπισμός skew: κατανομή κλειδιού
# MAGIC
# MAGIC Πρώτο βήμα: δες πόσες γραμμές έχει κάθε κλειδί. Αν το top κλειδί έχει **τάξεις μεγέθους**
# MAGIC περισσότερες από τον μέσο όρο → skew. **Skew ratio = max_count / avg_count**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Εντόπισε το καυτό κλειδί

# COMMAND ----------

# 1a: μέτρα γραμμές ανά afm
key_dist = spark.table(FACT).groupBy("______").count()                  # TODO 1a: afm
top = key_dist.orderBy(col("count").______()).first()                   # TODO 1b: desc (το μεγαλύτερο πρώτο)
max_count = top["count"]
avg_count = key_dist.agg(avg("count")).collect()[0][0]
skew_ratio = max_count / avg_count
print(f"Top κλειδί: {top['afm']} με {max_count:,} γραμμές")
print(f"Μέσος όρος/κλειδί: {avg_count:,.1f}  →  SKEW RATIO = {skew_ratio:,.0f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Γιατί είναι αργό: ένα τεράστιο partition
# MAGIC
# MAGIC Όταν κάνεις shuffle με κλειδί το `afm` (π.χ. σε groupBy/join), **όλες** οι γραμμές του ίδιου
# MAGIC afm πάνε στο **ίδιο** partition. Το καυτό κλειδί → ένα partition με ~1.8M γραμμές, ενώ τα
# MAGIC υπόλοιπα σχεδόν άδεια. `spark_partition_id()` μας δείχνει το μέγεθος κάθε partition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Δες την ανισορροπία partitions (πριν το fix)

# COMMAND ----------

# 2a: ξανα-διαμοίρασε ΜΕ κλειδί το afm (έτσι θα στοιβαχτεί το καυτό κλειδί)
skewed_rep = spark.table(FACT).repartition(col("________"))             # TODO 2a: afm
part_sizes = skewed_rep.groupBy(spark_partition_id().alias("pid")).count()
skew_max_part = part_sizes.agg(________("count")).collect()[0][0]        # TODO 2b: spark_max (μεγαλύτερο partition)
print(f"Μεγαλύτερο partition (skewed): {skew_max_part:,} γραμμές  ← εδώ κολλάει το job")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — Salting: σπάμε το καυτό κλειδί
# MAGIC
# MAGIC **Salting** = προσθέτουμε ένα τυχαίο-ντετερμινιστικό «αλάτι» (`salt = 0..N-1`) στο κλειδί.
# MAGIC Έτσι το καυτό `afm` γίνεται `afm + salt` → σπάει σε **N κουβάδες** που μοιράζονται σε N tasks.
# MAGIC Για **aggregation** το κάνουμε σε **δύο στάδια**:
# MAGIC 1. `groupBy(afm, salt)` → μερικά αθροίσματα (κατανεμημένα),
# MAGIC 2. `groupBy(afm)` πάνω στα μερικά → τελικό άθροισμα.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Salted two-stage aggregation

# COMMAND ----------

# Plain aggregation (το «αργό» reference) — άθροισμα ανά afm
plain = spark.table(FACT).groupBy("afm").agg(spark_sum("amount_eur").alias("total"))
plain_hot = plain.filter(col("afm") == HOT_AFM).collect()[0]["total"]

# 3a: πρόσθεσε στήλη salt = id % SALT_N
salted = spark.table(FACT).withColumn("salt", (col("id") % ________).cast("int"))   # TODO 3a: SALT_N

# 3b: ΣΤΑΔΙΟ 1 — group by (afm, salt) → μερικά αθροίσματα
stage1 = salted.groupBy("afm", "______").agg(spark_sum("amount_eur").alias("partial"))   # TODO 3b: salt

# 3c: ΣΤΑΔΙΟ 2 — group by afm → άθροισε τα μερικά
stage2 = stage1.groupBy("______").agg(spark_sum("partial").alias("total"))               # TODO 3c: afm
salted_hot = stage2.filter(col("afm") == HOT_AFM).collect()[0]["total"]

print(f"Plain  total (hot key): {plain_hot:,.0f}")
print(f"Salted total (hot key): {salted_hot:,.0f}")
print(f"Ίδιο αποτέλεσμα; → {plain_hot == salted_hot}  ✅ (correctness)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Δες τη βελτιωμένη ισορροπία partitions (μετά το salting)

# COMMAND ----------

# 4a: ξανα-διαμοίρασε με κλειδί (afm, salt) → το καυτό σπάει σε SALT_N κουβάδες
salted_rep = salted.repartition(col("afm"), col("________"))            # TODO 4a: salt
salt_sizes = salted_rep.groupBy(spark_partition_id().alias("pid")).count()
salt_max_part = salt_sizes.agg(________("count")).collect()[0][0]        # TODO 4b: spark_max
print(f"Μεγαλύτερο partition (skewed): {skew_max_part:,}")
print(f"Μεγαλύτερο partition (salted): {salt_max_part:,}  ← πολύ πιο ισορροπημένο")
print(f"Βελτίωση: {skew_max_part/salt_max_part:,.1f}x μικρότερο hot partition")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — Ο αυτόματος τρόπος: AQE skew join
# MAGIC
# MAGIC Το **Adaptive Query Execution (AQE)** εντοπίζει & σπάει skewed partitions **αυτόματα** σε joins.
# MAGIC Είναι ενεργό by default στα σύγχρονα runtimes. Το manual salting το χρειάζεσαι όταν AQE δεν
# MAGIC αρκεί (π.χ. heavy aggregations, ή παλιά runtimes).

# COMMAND ----------

# ⚠️ Serverless: η ανάγνωση κάποιων Spark configs είναι αποκλεισμένη → try/except
for k in ["spark.sql.adaptive.enabled", "spark.sql.adaptive.skewJoin.enabled"]:
    try:
        print(f"  {k} = {spark.conf.get(k)}")
    except Exception:
        print(f"  {k} = (μη-αναγνώσιμο σε serverless· είναι ON by default)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 2

# COMMAND ----------

results = {
    "Skew εντοπίστηκε (ratio > 100x)":      skew_ratio > 100,
    "Hot key ~1.8M γραμμές":                max_count >= 1_700_000,
    "Salted == Plain (correctness)":        plain_hot == salted_hot,
    "Salted partition < skewed partition":  salt_max_part < skew_max_part,
    "Βελτίωση ισορροπίας ≥ 3x":             (skew_max_part / salt_max_part) >= 3,
}
print("=" * 55)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 55)
print("🎉 Τέλος Μέρους 2!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise3_Masking_RowLevel_STARTER`
# MAGIC
# MAGIC Φτιάξαμε γρήγορα pipelines. Στο Μέρος 3 αλλάζουμε θέμα σε **Security**: column masking & row-level
# MAGIC security για **PII** (ΑΦΜ, εισοδήματα) — ποιος βλέπει τι.
