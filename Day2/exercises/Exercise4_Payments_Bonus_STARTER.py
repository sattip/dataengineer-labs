# Databricks notebook source
# MAGIC %md
# MAGIC # 💳 Άσκηση Ημέρα 2 — Bonus (4/4): Payments Pipeline (Capstone)
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~45' · **Δυσκολία:** ⭐⭐ Medium (consolidation)
# MAGIC > **Στόχος:** Όχι νέες έννοιες — **εφαρμόζετε μόνοι σας** ό,τι μάθατε στα Μέρη 1-3, σε ΝΕΟ dataset.
# MAGIC
# MAGIC ## 📖 Σενάριο
# MAGIC
# MAGIC Η ΑΑΔΕ θέλει καθημερινή ανάλυση των **πληρωμών** φορολογουμένων. Το `payments.csv` έχει
# MAGIC **250 πληρωμές** με ~23 «ελαφριά» DQ issues. Διαφορά από το myDATA: **λιγότερη καθοδήγηση** —
# MAGIC εδώ θυμάστε τα patterns και τα γράφετε. (Αν κολλήσετε, δείτε τα Μέρη 1-3.)
# MAGIC
# MAGIC ## 🎯 Τι θα κάνετε (ένα ενιαίο pipeline)
# MAGIC
# MAGIC 1. **Read** → 2. **Cleanse** (null afm, trim, negatives, status→UNKNOWN, dedup)
# MAGIC → 3. **Gold** (`payment_method × region`) → 4. **Insights** (top method, failure rate) → 5. **Delta**.
# MAGIC
# MAGIC ## 📐 Schema του `payments.csv`
# MAGIC `payment_id, payment_date, afm, declaration_id, amount_eur, payment_method, channel, status, region`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + download (έτοιμο, μην το αλλάξετε)

# COMMAND ----------

import urllib.request, os
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2"
VOLUME = "/Volumes/workspace/aade/aade_data"
for fname in ["payments.csv", "taxpayers.csv"]:
    target = f"{VOLUME}/{fname}"
    if not os.path.exists(target):
        urllib.request.urlretrieve(f"{REPO}/{fname}", target); print(f"✅ {fname}")
    else:
        print(f"⏭️  {fname}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, row_number, trim, lit
)
from pyspark.sql.window import Window
print("✓ ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Read το CSV
# MAGIC
# MAGIC Θυμηθείτε τις δύο options από το Μέρος 1.

# COMMAND ----------

payments_raw = (
    spark.read
         .option("__________", "true")   # TODO 1a: header
         .option("__________", "true")   # TODO 1b: inferSchema
         .csv(f"{VOLUME}/payments.csv")
)
print(f"Raw: {payments_raw.count()} rows")   # 250
payments_raw.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 Υπενθύμιση — τα status εδώ είναι **Αγγλικά**
# MAGIC
# MAGIC Έγκυρα: `Confirmed`, `Pending`, `Failed`. Προσοχή: το dataset έχει `'pending '` (πεζά + κενό)
# MAGIC και `'??'` — αυτά είναι **άκυρα** → πάνε σε `UNKNOWN`. (Δεν κάνουμε trim στο status εδώ,
# MAGIC οπότε το `'pending '` μένει άκυρο — ρεαλιστικό.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Cleansing (όλα μαζί)
# MAGIC
# MAGIC Συμπληρώστε. Κανόνες: drop NULL afm · trim `payment_method` · drop `amount_eur <= 0` ·
# MAGIC άκυρο status → `"UNKNOWN"`.

# COMMAND ----------

valid_status = ["Confirmed", "Pending", "Failed"]

payments_clean = (
    payments_raw
    .filter(col("afm").__________)                       # TODO 2a: «δεν είναι NULL»
    .withColumn("payment_method", ______(col("payment_method")))   # TODO 2b: trim
    .filter(col("amount_eur") ___ 0)                     # TODO 2c: «> 0»
    .withColumn("status",
        when(col("status").______(valid_status), col("status"))    # TODO 2d: isin
        .otherwise(lit("__________")))                   # TODO 2e: "UNKNOWN"
)
print(f"Μετά cleanse (πριν dedup): {payments_clean.count()}")   # 240

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Dedup με Window (κράτα το νεότερο ανά payment_id)
# MAGIC
# MAGIC Το ίδιο pattern με το Μέρος 2: `partitionBy(key).orderBy(date desc)` → `row_number` → `rn==1`.

# COMMAND ----------

w = Window.partitionBy("__________").orderBy(col("payment_date").______())   # TODO 3a: key · TODO 3b: desc
payments_dedup = (
    payments_clean
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == ___)        # TODO 3c
    .drop("rn")
)
print(f"Raw {payments_raw.count()} → Clean {payments_clean.count()} → Dedup {payments_dedup.count()}")   # 250 → 240 → 235

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Gold: aggregation ανά `payment_method × region`
# MAGIC
# MAGIC Conditional counts: confirmed / failed / pending με το `sum(when(...,1).otherwise(0))` pattern.

# COMMAND ----------

gold = (
    payments_dedup
    .groupBy("______________", "______")               # TODO 4a: payment_method, region
    .agg(
        count("*").alias("total_payments"),
        spark_sum("amount_eur").alias("total_eur"),
        avg("amount_eur").alias("avg_eur"),
        spark_sum(when(col("status") == "Confirmed", 1).otherwise(0)).alias("confirmed"),
        spark_sum(when(col("status") == "______", 1).otherwise(0)).alias("failed"),    # TODO 4b: "Failed"
        spark_sum(when(col("status") == "______", 1).otherwise(0)).alias("pending"),   # TODO 4c: "Pending"
    )
    .orderBy(______("total_eur"))                       # TODO 4d: desc
)
print("=== GOLD — Payments ανά Method × Region ===")
gold.show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — Insight: Failure rate ανά method
# MAGIC
# MAGIC failure_pct = failed / total × 100. Συμπληρώστε τον υπολογισμό.

# COMMAND ----------

print("=== Failure rate ανά payment_method ===")
(
    gold.groupBy("payment_method")
        .agg(spark_sum("failed").alias("failed"),
             spark_sum("total_payments").alias("total"))
        .withColumn("failure_pct",
            (col("______") / col("______") * 100).cast("decimal(5,2)"))   # TODO 5: failed / total
        .orderBy(desc("failure_pct"))
        .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 6 — Delta write (Gold)

# COMMAND ----------

gold.write.format("______").mode("__________").saveAsTable("workspace.aade.payments_gold")   # TODO 6: "delta", "overwrite"
print("✓ Gold saved: workspace.aade.payments_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check (τελικό capstone)

# COMMAND ----------

results = {
    "Raw = 250":                     payments_raw.count() == 250,
    "Clean (pre-dedup) = 240":       payments_clean.count() == 240,
    "Dedup = 235":                   payments_dedup.count() == 235,
    "Καμία NULL afm":                payments_dedup.filter(col("afm").isNull()).count() == 0,
    "Κανένα amount <= 0":            payments_dedup.filter(col("amount_eur") <= 0).count() == 0,
    "Κανένα διπλό payment_id":       payments_dedup.groupBy("payment_id").count().filter(col("count") > 1).count() == 0,
    "Gold table υπάρχει":            spark.catalog.tableExists("workspace.aade.payments_gold"),
    "Gold έχει failure metrics":     set(["confirmed","failed","pending"]).issubset(set(gold.columns)),
}
print("=" * 50)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 50)
print("🏆 CAPSTONE ΟΛΟΚΛΗΡΩΘΗΚΕ!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎓 Συγχαρητήρια
# MAGIC
# MAGIC Εφαρμόσατε **μόνοι σας** ολόκληρο DQ pipeline σε νέο dataset: read → cleanse → dedup →
# MAGIC aggregate → insight → Delta. Αυτό ακριβώς είναι η καθημερινή δουλειά ενός Data Engineer.
# MAGIC
# MAGIC **Bonus για extra χρόνο:** εμπλουτίστε με `taxpayers` (left join σε `afm`) και προσθέστε
# MAGIC ανάλυση ανά `sector`. ⚠️ Πολλά afm των payments **δεν** υπάρχουν στο master → θα δείτε
# MAGIC NULL `business_name` (γι' αυτό **left** join, ποτέ inner εδώ).
