# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2 — ΑΣΚΗΣΗ Payments ΑΑΔΕ (Ημέρα 2 · Slide 49)
# MAGIC **Ρόλος 3: Μηχανικοί Δεδομένων**
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC
# MAGIC > **GitHub** (share this στο Zoom chat):
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2/Lab2_EXERCISE2_Payments.py
# MAGIC > ```
# MAGIC
# MAGIC ## Σενάριο
# MAGIC
# MAGIC Η ΑΑΔΕ χρειάζεται καθημερινή ανάλυση των **πληρωμών** από φορολογούμενους.
# MAGIC Έχεις ένα CSV με 250 πληρωμές που έχει 23 light DQ issues.
# MAGIC
# MAGIC **Στόχος**: cleansing → enrichment με taxpayers → aggregation ανά payment method × region → Gold Delta.
# MAGIC
# MAGIC ## Προαπαιτούμενα
# MAGIC
# MAGIC Cell 0 παρακάτω κατεβάζει αυτόματα τα CSVs (`payments.csv` + 4 master) από το GitHub repo στο Volume `/Volumes/workspace/aade/aade_data/`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0 — Setup + Download CSVs από GitHub

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

import urllib.request, os

REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2"
VOLUME = "/Volumes/workspace/aade/aade_data"

files = ["payments.csv", "taxpayers.csv", "doy.csv", "employees.csv"]
for fname in files:
    target = f"{VOLUME}/{fname}"
    if os.path.exists(target):
        print(f"⏭️  {fname} (already in volume)")
    else:
        urllib.request.urlretrieve(f"{REPO}/{fname}", target)
        print(f"✅ {fname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 1 — Imports & Setup

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, row_number,
    trim, lit, regexp_replace
)
from pyspark.sql.window import Window

print("✓ Ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 2 — Read CSV + Schema Verification

# COMMAND ----------

payments_raw = spark.read.csv(
    f"{VOLUME}/payments.csv",
    header=True, inferSchema=True
)

print(f"Raw payments: {payments_raw.count()} rows")
payments_raw.printSchema()
payments_raw.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 3 — Cleansing
# MAGIC
# MAGIC Εφαρμόζουμε:
# MAGIC - Drop NULL afm (critical key)
# MAGIC - Trim whitespace στο payment_method
# MAGIC - Drop negative amounts (business rule)
# MAGIC - Normalize status — map bad values → "UNKNOWN"
# MAGIC - Deduplicate on payment_id — keep latest by date

# COMMAND ----------

payments_clean = (
    payments_raw
    .filter(col("afm").isNotNull())
    .withColumn("payment_method", trim(col("payment_method")))
    .filter(col("amount_eur") > 0)
    .withColumn("status",
        when(col("status").isin(["Confirmed", "Pending", "Failed"]),
             col("status"))
        .otherwise(lit("UNKNOWN")))
)

# Dedup on payment_id — keep latest
w = Window.partitionBy("payment_id").orderBy(col("payment_date").desc())
payments_dedup = (
    payments_clean
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

print(f"Raw:    {payments_raw.count()}")
print(f"Clean:  {payments_clean.count()}")
print(f"Dedup:  {payments_dedup.count()}")
print(f"Removed: {payments_raw.count() - payments_dedup.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 4 — Enrichment: JOIN με taxpayers

# COMMAND ----------

taxpayers = spark.read.csv(
    f"{VOLUME}/taxpayers.csv",
    header=True, inferSchema=True
)

payments_enriched = payments_dedup.join(
    taxpayers.select(
        col("ΑΦΜ").cast("string").alias("afm"),
        col("Επωνυμία").alias("business_name"),
        col("Κλάδος").alias("sector"),
    ),
    on="afm", how="left"
)

print(f"Enriched: {payments_enriched.count()} rows")
payments_enriched.select(
    "payment_id", "afm", "business_name", "sector",
    "amount_eur", "payment_method", "status", "region"
).show(8, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 5 — Gold Aggregation: Payment Method × Region

# COMMAND ----------

gold = (
    payments_enriched.groupBy("payment_method", "region")
    .agg(
        count("*").alias("total_payments"),
        spark_sum("amount_eur").alias("total_eur"),
        avg("amount_eur").alias("avg_payment_eur"),
        spark_sum(when(col("status") == "Confirmed", 1).otherwise(0)).alias("confirmed"),
        spark_sum(when(col("status") == "Failed", 1).otherwise(0)).alias("failed"),
        spark_sum(when(col("status") == "Pending", 1).otherwise(0)).alias("pending"),
    )
    .orderBy(desc("total_eur"))
)

print("=== GOLD — Payments ανά Method × Region ===")
gold.show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6 — Delta Write + Verification

# COMMAND ----------

# Delta writes
payments_enriched.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.aade.payments_clean")

gold.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.aade.payments_gold")

print("✓ Delta tables created:")
print("  - workspace.aade.payments_clean")
print("  - workspace.aade.payments_gold")

# Verify
print("\n=== Summary ===")
print(f"Clean payments: {spark.table('workspace.aade.payments_clean').count()}")
print(f"Gold rows:      {spark.table('workspace.aade.payments_gold').count()}")

# Top 3 insights για business
print("\n=== Business Insights ===")

print("\n1. Top Method:")
gold.groupBy("payment_method").agg(
    spark_sum("total_eur").alias("total_eur"),
    spark_sum("total_payments").alias("count")
).orderBy(desc("total_eur")).show(3, truncate=False)

print("2. Failure Rate ανά Method:")
gold.groupBy("payment_method").agg(
    spark_sum("confirmed").alias("confirmed"),
    spark_sum("failed").alias("failed"),
    spark_sum("total_payments").alias("total"),
).withColumn("failure_pct",
    (col("failed") / col("total") * 100).cast("decimal(5,2)")
).orderBy(desc("failure_pct")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση
# MAGIC
# MAGIC **Τι πέτυχες σε 15 λεπτά:**
# MAGIC - ✓ Read CSV με 250 payments
# MAGIC - ✓ Cleansed 23 DQ issues (nulls, dups, negatives, whitespace, bad status)
# MAGIC - ✓ Enriched με taxpayers (business_name, sector)
# MAGIC - ✓ Aggregated ανά payment_method × region
# MAGIC - ✓ Saved 2 Delta tables
# MAGIC - ✓ Business insights: top methods, failure rates
# MAGIC
# MAGIC **Expected output highlights:**
# MAGIC - Web Banking = top method σε total volume
# MAGIC - Failed rate ~30% (realistic για test data)
# MAGIC - Attica + Κεντρική Μακεδονία = top regions
# MAGIC
# MAGIC **Cleanup (uncomment αν θες να καθαρίσεις μετά):**

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS workspace.aade.payments_clean")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.payments_gold")
# print("✓ Cleaned up")
