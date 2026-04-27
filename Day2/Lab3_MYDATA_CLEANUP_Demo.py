# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3 — myDATA Invoices CLEANUP (Ημέρα 2)
# MAGIC **Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ**
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC
# MAGIC > **GitHub** (share this στο Zoom chat):
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2/Lab3_MYDATA_CLEANUP_Demo.py
# MAGIC > ```
# MAGIC
# MAGIC Πραγματικό end-to-end Data Quality + Cleansing σενάριο πάνω σε **myDATA τιμολόγια**
# MAGIC με σκόπιμα **10 τύπους DQ issues**. Περιλαμβάνει detection → cleansing → enrichment → Gold.
# MAGIC
# MAGIC ## Σενάριο (ΑΑΔΕ)
# MAGIC
# MAGIC Το myDATA export «αδειάζεται» σε CSV κάθε βράδυ. Το dataset έχει 100 τιμολόγια με
# MAGIC **35 injected issues** σε 10 κατηγορίες:
# MAGIC
# MAGIC | # | Issue | Πλήθος | Σοβαρότητα |
# MAGIC |---|-------|--------|-------------|
# MAGIC | 1 | NULL issuer_afm (critical key) | 5 | 🔴 Drop |
# MAGIC | 2 | Duplicate invoice_id | 3 | 🔴 Dedup |
# MAGIC | 3 | Bad AFM format (<9 digits) | 4 | 🟡 Flag |
# MAGIC | 4 | Negative net_amount | 3 | 🔴 Drop or absolute |
# MAGIC | 5 | Future issue_date | 4 | 🟡 Flag |
# MAGIC | 6 | Invalid status enum | 3 | 🟡 Map to UNKNOWN |
# MAGIC | 7 | Whitespace padding σε issuer_name | 5 | 🟢 Trim |
# MAGIC | 8 | Bad date format (YYYY/MM/DD) | 3 | 🟢 Normalize |
# MAGIC | 9 | NULL vat_amount | 2 | 🟡 Recompute |
# MAGIC | 10 | Orphan receiver_afm (not in master) | 3 | 🟡 Flag |
# MAGIC
# MAGIC ## Προαπαιτούμενα
# MAGIC
# MAGIC Cell 0 παρακάτω κατεβάζει αυτόματα όλα τα CSVs:
# MAGIC - `mydata_invoices_MESSY.csv` → νέο volume `mydata_raw`
# MAGIC - 4 master CSVs (`taxpayers`, `doy`, `employees`, `declarations`) → `aade_data`
# MAGIC
# MAGIC ## Delta Tables που θα δημιουργηθούν
# MAGIC
# MAGIC - `workspace.aade.mydata_raw` (raw ingest — Bronze)
# MAGIC - `workspace.aade.mydata_quarantine` (failed DQ rows — για investigation)
# MAGIC - `workspace.aade.mydata_clean` (Silver — καθαρά)
# MAGIC - `workspace.aade.mydata_gold` (Gold — aggregated)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + Download CSVs από GitHub

# COMMAND ----------

import urllib.request, os

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.mydata_raw")

REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2"
MYDATA_VOLUME = "/Volumes/workspace/aade/mydata_raw"
MASTER_VOLUME = "/Volumes/workspace/aade/aade_data"

# Master files → aade_data
for fname in ["taxpayers.csv", "doy.csv", "employees.csv", "declarations.csv"]:
    target = f"{MASTER_VOLUME}/{fname}"
    if os.path.exists(target):
        print(f"⏭️  {fname} (master, already in volume)")
    else:
        urllib.request.urlretrieve(f"{REPO}/{fname}", target)
        print(f"✅ {fname}")

# myDATA messy file → mydata_raw
mydata_target = f"{MYDATA_VOLUME}/mydata_invoices_MESSY.csv"
if os.path.exists(mydata_target):
    print(f"⏭️  mydata_invoices_MESSY.csv (already in volume)")
else:
    urllib.request.urlretrieve(f"{REPO}/mydata_invoices_MESSY.csv", mydata_target)
    print(f"✅ mydata_invoices_MESSY.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 1 — Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, row_number,
    trim, regexp_replace, lit, to_date, current_date, length,
    regexp_extract, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, DateType

print("✓ Setup ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Cell 2 — Bronze: Raw Ingest

# COMMAND ----------

df_raw = spark.read.csv(
    f"{MYDATA_VOLUME}/mydata_invoices_MESSY.csv",
    header=True,
    inferSchema=True,
)

print(f"Raw invoices: {df_raw.count()} rows")
df_raw.printSchema()
df_raw.show(5, truncate=False)

# Save as Bronze
df_raw.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.aade.mydata_raw")
print("✓ Bronze saved: workspace.aade.mydata_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Cell 3 — Detection: Εντοπισμός όλων των DQ Issues

# COMMAND ----------

print("=" * 60)
print("DQ REPORT — myDATA Invoices")
print("=" * 60)

# Check 1: NULLs σε critical fields
print("\n[1] NULLs ανά στήλη:")
null_counts = df_raw.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df_raw.columns
])
null_counts.show()

# Check 2: Duplicate invoice_id
print("[2] Duplicate invoice_id:")
dups = df_raw.groupBy("invoice_id").count().filter(col("count") > 1)
print(f"   → {dups.count()} invoice_ids εμφανίζονται > 1 φορά")
dups.show(truncate=False)

# Check 3: Bad AFM format (not 9 digits)
print("[3] Bad issuer_afm (not 9 digits):")
bad_afm = df_raw.filter(
    col("issuer_afm").isNotNull() &
    (~col("issuer_afm").rlike(r"^\d{9}$"))
)
print(f"   → {bad_afm.count()} rows με κακό ΑΦΜ")
bad_afm.select("invoice_id", "issuer_afm").show(truncate=False)

# Check 4: Negative amounts
print("[4] Negative net_amount:")
neg = df_raw.filter(col("net_amount") < 0)
print(f"   → {neg.count()} rows με αρνητικό ποσό")
neg.select("invoice_id", "net_amount").show()

# Check 5: Future dates — προϋποθέτει σωστό format
print("[5] Future issue_date (>= σήμερα):")
# Parse και δες ποιες είναι future (μόνο για valid format)
parseable = df_raw.withColumn(
    "parsed_date", to_date(col("issue_date"), "yyyy-MM-dd")
)
future = parseable.filter(col("parsed_date") > current_date())
print(f"   → {future.count()} rows με future date")

# Check 6: Invalid status
print("[6] Invalid status:")
valid_statuses = ["Υποβληθέν", "Ακυρωμένο", "Εκκρεμές"]
invalid_status = df_raw.filter(~col("status").isin(valid_statuses))
print(f"   → {invalid_status.count()} rows με άγνωστο status")
invalid_status.groupBy("status").count().show()

# Check 7: Whitespace in issuer_name
print("[7] issuer_name με leading/trailing whitespace:")
ws = df_raw.filter(col("issuer_name") != trim(col("issuer_name")))
print(f"   → {ws.count()} rows χρειάζονται trim")

# Check 8: Bad date format (not YYYY-MM-DD)
print("[8] Bad date format (not YYYY-MM-DD):")
bad_date = df_raw.filter(~col("issue_date").rlike(r"^\d{4}-\d{2}-\d{2}$"))
print(f"   → {bad_date.count()} rows με λάθος date format")
bad_date.select("invoice_id", "issue_date").show(truncate=False)

# Check 9: NULL vat_amount
print("[9] NULL vat_amount:")
null_vat = df_raw.filter(col("vat_amount").isNull())
print(f"   → {null_vat.count()} rows χωρίς vat_amount")

# Check 10: Orphan receiver_afm (not in taxpayers master)
taxpayers = spark.read.csv(
    f"{MASTER_VOLUME}/taxpayers.csv", header=True, inferSchema=True
)
valid_afms = taxpayers.select(col("ΑΦΜ").cast("string").alias("ΑΦΜ"))
print("[10] Orphan receiver_afm (not in taxpayers master):")
orphan = df_raw.select("invoice_id", "receiver_afm") \
    .join(valid_afms, df_raw.receiver_afm == valid_afms.ΑΦΜ, "left_anti")
print(f"   → {orphan.count()} rows με άγνωστο receiver_afm")
orphan.show()

print("\n" + "=" * 60)
print("✓ DQ Detection complete")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨 Cell 4 — Quarantine: Φυγή προβληματικών rows

# COMMAND ----------

# Flag each problematic row με reason(s)
flagged = (
    df_raw
    .withColumn("has_null_afm",
        col("issuer_afm").isNull())
    .withColumn("has_bad_afm",
        col("issuer_afm").isNotNull() &
        (~col("issuer_afm").rlike(r"^\d{9}$")))
    .withColumn("has_negative_amount",
        col("net_amount") < 0)
    .withColumn("has_bad_date",
        ~col("issue_date").rlike(r"^\d{4}-\d{2}-\d{2}$"))
    .withColumn("has_invalid_status",
        ~col("status").isin(["Υποβληθέν", "Ακυρωμένο", "Εκκρεμές"]))
)

# Quarantine = rows με ΟΠΟΙΟΔΗΠΟΤΕ critical issue
quarantine = flagged.filter(
    col("has_null_afm") | col("has_bad_afm") |
    col("has_negative_amount") | col("has_bad_date") |
    col("has_invalid_status")
)

print(f"Quarantined rows: {quarantine.count()}")
quarantine.select(
    "invoice_id", "issuer_afm", "net_amount", "issue_date", "status",
    "has_null_afm", "has_bad_afm", "has_negative_amount",
    "has_bad_date", "has_invalid_status"
).show(20, truncate=False)

# Save quarantine table για investigation
quarantine.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.aade.mydata_quarantine")
print("✓ Quarantine saved: workspace.aade.mydata_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cell 5 — Cleansing Pipeline
# MAGIC
# MAGIC Εφαρμόζουμε τους κανόνες καθαρισμού:
# MAGIC - Drop NULL issuer_afm
# MAGIC - Dedup on invoice_id (keep first by issue_date)
# MAGIC - Normalize bad AFMs → NULL (flag)
# MAGIC - Drop negative amounts (business rule)
# MAGIC - Normalize date formats (YYYY/MM/DD → YYYY-MM-DD)
# MAGIC - Map invalid status → "UNKNOWN"
# MAGIC - Trim whitespace
# MAGIC - Recompute NULL vat_amount from net × rate

# COMMAND ----------

VAT_RATES = {
    "ΦΠΑ 24%": 0.24, "ΦΠΑ 13%": 0.13, "ΦΠΑ 6%": 0.06, "Απαλλαγή": 0.0,
}

# Build the vat_rate column via case expression
vat_rate_expr = when(col("vat_category") == "ΦΠΑ 24%", lit(0.24)) \
    .when(col("vat_category") == "ΦΠΑ 13%", lit(0.13)) \
    .when(col("vat_category") == "ΦΠΑ 6%", lit(0.06)) \
    .when(col("vat_category") == "Απαλλαγή", lit(0.0)) \
    .otherwise(lit(None))

clean = (
    df_raw
    # 1. Drop NULL critical fields
    .filter(col("issuer_afm").isNotNull() & col("invoice_id").isNotNull())
    # 2. Normalize date format (YYYY/MM/DD → YYYY-MM-DD)
    .withColumn("issue_date", regexp_replace(col("issue_date"), "/", "-"))
    # 3. Trim whitespace
    .withColumn("issuer_name", trim(col("issuer_name")))
    # 4. Flag bad AFMs (< 9 digits) → NULL
    .withColumn("issuer_afm",
        when(col("issuer_afm").rlike(r"^\d{9}$"), col("issuer_afm"))
        .otherwise(lit(None)))
    # 5. Drop negative amounts (business rule — cannot be negative)
    .filter(col("net_amount") >= 0)
    # 6. Map invalid status → UNKNOWN
    .withColumn("status",
        when(col("status").isin(["Υποβληθέν", "Ακυρωμένο", "Εκκρεμές"]),
             col("status"))
        .otherwise(lit("UNKNOWN")))
    # 7. Recompute vat_amount when NULL
    .withColumn("vat_rate", vat_rate_expr)
    .withColumn("vat_amount",
        when(col("vat_amount").isNull() & col("vat_rate").isNotNull(),
             (col("net_amount") * col("vat_rate")).cast(DoubleType()))
        .otherwise(col("vat_amount")))
    .withColumn("total_amount",
        (col("net_amount") + col("vat_amount")).cast(DoubleType()))
    .drop("vat_rate")
)

# 8. Dedup on invoice_id — keep latest by issue_date
w = Window.partitionBy("invoice_id").orderBy(col("issue_date").desc())
clean_dedup = (
    clean
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

print(f"Before clean:  {df_raw.count()}")
print(f"After cleanse: {clean_dedup.count()}")
print(f"Removed:       {df_raw.count() - clean_dedup.count()} rows")

clean_dedup.show(5, truncate=False)

# Save as Silver
clean_dedup.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.aade.mydata_clean")
print("✓ Silver saved: workspace.aade.mydata_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔗 Cell 6 — Enrichment: JOIN με taxpayers + doy

# COMMAND ----------

taxpayers_enrich = taxpayers.select(
    col("ΑΦΜ").cast("string").alias("issuer_afm"),
    col("Επωνυμία").alias("official_name"),
    col("Κλάδος").alias("sector"),
    col("Περιφέρεια").alias("region"),
    col("ΔΟΥID").alias("ΔΟΥID"),
)

doy = spark.read.csv(f"{MASTER_VOLUME}/doy.csv", header=True, inferSchema=True)
doy_enrich = doy.select(col("ΔΟΥID"), col("ΔΟΥ_Ονομα").alias("doy_name"))

enriched = (
    clean_dedup
    .join(taxpayers_enrich, on="issuer_afm", how="left")
    .join(doy_enrich, on="ΔΟΥID", how="left")
)

print(f"Enriched rows: {enriched.count()}")
enriched.select(
    "invoice_id", "issuer_afm", "official_name", "sector",
    "region", "doy_name", "invoice_type",
    "net_amount", "vat_amount", "total_amount", "status"
).show(8, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Cell 7 — Gold: Aggregated KPIs

# COMMAND ----------

gold = (
    enriched.groupBy("sector", "region")
    .agg(
        count("*").alias("invoice_count"),
        spark_sum("net_amount").alias("total_net_eur"),
        spark_sum("vat_amount").alias("total_vat_eur"),
        spark_sum("total_amount").alias("total_with_vat_eur"),
        avg("net_amount").alias("avg_invoice_eur"),
        spark_sum(when(col("status") == "Υποβληθέν", 1).otherwise(0)).alias("submitted"),
        spark_sum(when(col("status") == "Ακυρωμένο", 1).otherwise(0)).alias("cancelled"),
        spark_sum(when(col("status") == "Εκκρεμές", 1).otherwise(0)).alias("pending"),
        spark_sum(when(col("status") == "UNKNOWN", 1).otherwise(0)).alias("unknown_status"),
    )
    .orderBy(desc("total_with_vat_eur"))
)

print("=== GOLD — myDATA ανά Κλάδο × Περιφέρεια ===")
gold.show(20, truncate=False)

gold.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.aade.mydata_gold")
print("✓ Gold saved: workspace.aade.mydata_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Cell 8 — Before vs After Comparison

# COMMAND ----------

print("=" * 60)
print("BEFORE vs AFTER CLEANUP")
print("=" * 60)

raw_count = spark.table("workspace.aade.mydata_raw").count()
quarantine_count = spark.table("workspace.aade.mydata_quarantine").count()
clean_count = spark.table("workspace.aade.mydata_clean").count()

print(f"\n Raw (Bronze):         {raw_count:5d} rows")
print(f" Quarantine:           {quarantine_count:5d} rows ({100*quarantine_count/raw_count:.0f}%)")
print(f" Clean (Silver):       {clean_count:5d} rows ({100*clean_count/raw_count:.0f}%)")
print(f" Rows Lost:            {raw_count - clean_count:5d}")

print("\n=== Tables στο workspace.aade ===")
spark.sql("SHOW TABLES IN workspace.aade").show()

print("\n=== Top 3 Σύνολα ανά Κλάδο ===")
enriched.groupBy("sector") \
    .agg(
        spark_sum("total_amount").alias("total_eur"),
        count("*").alias("count")
    ) \
    .orderBy(desc("total_eur")) \
    .show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση
# MAGIC
# MAGIC **Τι πέτυχες:**
# MAGIC - ✓ Bronze: raw ingest (100 rows)
# MAGIC - ✓ 10 DQ checks με detailed report
# MAGIC - ✓ Quarantine table (problematic rows)
# MAGIC - ✓ Cleansing με 8 rules
# MAGIC - ✓ Silver: clean Delta table
# MAGIC - ✓ Enrichment με taxpayers + doy
# MAGIC - ✓ Gold: aggregated KPIs
# MAGIC - ✓ 4 Delta tables production-ready

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cell 9 — CLEANUP (ΠΡΟΣΟΧΗ: διαγράφει τα tables!)
# MAGIC
# MAGIC Τρέξε το **μόνο μετά το τέλος του demo** για να καθαρίσεις το workspace.

# COMMAND ----------

# Uncomment τα παρακάτω για cleanup:

# print("Dropping tables...")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.mydata_gold")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.mydata_clean")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.mydata_quarantine")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.mydata_raw")
# print("✓ All mydata_* tables dropped")
#
# # Αν θες να καθαρίσεις και το Volume:
# # dbutils.fs.rm("/Volumes/workspace/aade/mydata_raw/mydata_invoices_MESSY.csv")
# # spark.sql("DROP VOLUME IF EXISTS workspace.aade.mydata_raw")
#
# print("✓ Cleanup complete. Ready για next run.")

print("⚠️ Cell 9 είναι commented out. Uncomment αν θες cleanup.")
