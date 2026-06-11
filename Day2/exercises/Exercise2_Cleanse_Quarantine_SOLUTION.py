# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 2/3: Quarantine + Cleansing (Bronze → Silver)

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, row_number,
    trim, regexp_replace, lit, to_date, current_date, length, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

MASTER_VOLUME = "/Volumes/workspace/aade/aade_data"
valid_statuses = ["Υποβληθέν", "Ακυρωμένο", "Εκκρεμές"]
df_raw = spark.table("workspace.aade.mydata_raw")

# COMMAND ----------

# TODO 1 — flag rows
flagged = (
    df_raw
    .withColumn("has_null_afm",        col("issuer_afm").isNull())
    .withColumn("has_bad_afm",         col("issuer_afm").isNotNull() &
                                       (~col("issuer_afm").cast("string").rlike(r"^\d{9}$")))
    .withColumn("has_negative_amount", col("net_amount") < 0)
    .withColumn("has_bad_date",        ~col("issue_date").rlike(r"^\d{4}-\d{2}-\d{2}$"))
    .withColumn("has_invalid_status",  ~col("status").isin(valid_statuses))
)

# COMMAND ----------

# TODO 2 — quarantine table
quarantine = flagged.filter(
    col("has_null_afm") | col("has_bad_afm") | col("has_negative_amount") |
    col("has_bad_date") | col("has_invalid_status")
)
print(f"Quarantined: {quarantine.count()}")
quarantine.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mydata_quarantine")

# COMMAND ----------

# TODO 3 — cleansing
vat_rate_expr = (
    when(col("vat_category") == "ΦΠΑ 24%", lit(0.24))
    .when(col("vat_category") == "ΦΠΑ 13%", lit(0.13))
    .when(col("vat_category") == "ΦΠΑ 6%",  lit(0.06))
    .when(col("vat_category") == "Απαλλαγή", lit(0.0))
    .otherwise(lit(None))
)

clean = (
    df_raw
    .filter(col("issuer_afm").isNotNull() & col("invoice_id").isNotNull())
    .withColumn("issue_date", regexp_replace(col("issue_date"), "/", "-"))
    .withColumn("issuer_name", trim(col("issuer_name")))
    .withColumn("issuer_afm",
        when(col("issuer_afm").cast("string").rlike(r"^\d{9}$"), col("issuer_afm"))
        .otherwise(lit(None)))
    .filter(col("net_amount") >= 0)
    .withColumn("status",
        when(col("status").isin(valid_statuses), col("status"))
        .otherwise(lit("UNKNOWN")))
    .withColumn("vat_rate", vat_rate_expr)
    .withColumn("vat_amount",
        when(col("vat_amount").isNull() & col("vat_rate").isNotNull(),
             (col("net_amount") * col("vat_rate")).cast(DoubleType()))
        .otherwise(col("vat_amount")))
    .withColumn("total_amount",
        (col("net_amount") + col("vat_amount")).cast(DoubleType()))
    .drop("vat_rate")
)

# COMMAND ----------

# TODO 4 — dedup με Window
w = Window.partitionBy("invoice_id").orderBy(col("issue_date").desc())
clean_dedup = (
    clean
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)
print(f"Πριν: {clean.count()}  Μετά: {clean_dedup.count()}")
clean_dedup.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mydata_clean")
print("✓ Silver saved")

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 2 ολοκληρώθηκε")
