# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 3/3: Enrichment + Gold + Insights

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, round as spark_round, broadcast
)

MASTER_VOLUME = "/Volumes/workspace/aade/aade_data"
clean_dedup = spark.table("workspace.aade.mydata_clean")
taxpayers   = spark.read.csv(f"{MASTER_VOLUME}/taxpayers.csv", header=True, inferSchema=True)
doy         = spark.read.csv(f"{MASTER_VOLUME}/doy.csv",       header=True, inferSchema=True)

# COMMAND ----------

# TODO 1 — prep taxpayers
taxpayers_enrich = taxpayers.select(
    col("ΑΦΜ").cast("string").alias("issuer_afm"),
    col("Επωνυμία").alias("official_name"),
    col("Κλάδος").alias("sector"),
    col("Περιφέρεια").alias("region"),
    col("ΔΟΥID").alias("ΔΟΥID"),
)

# COMMAND ----------

# TODO 2 — joins (left + broadcast)
doy_enrich = doy.select(col("ΔΟΥID"), col("ΔΟΥ_Ονομα").alias("doy_name"))
enriched = (
    clean_dedup
    .join(broadcast(taxpayers_enrich), on="issuer_afm", how="left")
    .join(broadcast(doy_enrich),       on="ΔΟΥID",      how="left")
)
print(f"Enriched: {enriched.count()} (== Silver {clean_dedup.count()})")

# COMMAND ----------

# TODO 3 — Gold aggregation
gold = (
    enriched
    .groupBy("sector", "region")
    .agg(
        count("*").alias("invoice_count"),
        spark_sum("net_amount").alias("total_net_eur"),
        spark_sum("vat_amount").alias("total_vat_eur"),
        spark_sum("total_amount").alias("total_with_vat_eur"),
        avg("net_amount").alias("avg_invoice_eur"),
        spark_sum(when(col("status") == "Υποβληθέν", 1).otherwise(0)).alias("submitted"),
        spark_sum(when(col("status") == "Ακυρωμένο", 1).otherwise(0)).alias("cancelled"),
        spark_sum(when(col("status") == "Εκκρεμές",  1).otherwise(0)).alias("pending"),
        spark_sum(when(col("status") == "UNKNOWN",   1).otherwise(0)).alias("unknown_status"),
    )
    .orderBy(desc("total_with_vat_eur"))
)
gold.show(20, truncate=False)

# COMMAND ----------

# TODO 4 — Gold write
gold.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mydata_gold")
print("✓ Gold saved")

# COMMAND ----------

# TODO 5 — top sectors
(
    gold.groupBy("sector")
        .agg(spark_sum("total_with_vat_eur").alias("revenue"),
             spark_sum("invoice_count").alias("invoices"))
        .orderBy(desc("revenue"))
        .show(5, truncate=False)
)

# COMMAND ----------

# TODO 6 — before vs after
raw_n    = spark.table("workspace.aade.mydata_raw").count()
quar_n   = spark.table("workspace.aade.mydata_quarantine").count()
silver_n = spark.table("workspace.aade.mydata_clean").count()
gold_n   = spark.table("workspace.aade.mydata_gold").count()
print(f"Bronze {raw_n} → Quarantine {quar_n} + Silver {silver_n} → Gold {gold_n}")

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 3 ολοκληρώθηκε — όλο το pipeline έτοιμο")
