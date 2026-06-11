# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 2/3: Medallion (Bronze → Silver → Gold)

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, current_timestamp
)

CATALOG       = "workspace"
SCHEMA_BRONZE = "aade_bronze"
SCHEMA_SILVER = "aade_silver"
SCHEMA_GOLD   = "aade_gold"
LANDING_PATH  = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/landing"
BRONZE_TBL = f"{CATALOG}.{SCHEMA_BRONZE}.declarations_raw"
SILVER_TBL = f"{CATALOG}.{SCHEMA_SILVER}.declarations_clean"
GOLD_TBL   = f"{CATALOG}.{SCHEMA_GOLD}.declarations_by_category_region"

df = spark.read.option("header","true").option("inferSchema","true").csv(f"{LANDING_PATH}/declarations.csv")

# COMMAND ----------

# TODO 1 — Bronze
bronze = (
    df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)
bronze.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TBL)
print(f"✓ Bronze: {spark.table(BRONZE_TBL).count()}")

# COMMAND ----------

# TODO 2 — Silver
silver = (
    spark.table(BRONZE_TBL)
    .select(
        col("ΔηλωσηID").cast("int").alias("declaration_id"),
        col("Ημερομηνία").cast("date").alias("declaration_date"),
        col("ΑΦΜ").cast("string").alias("afm"),
        col("Επωνυμία").alias("business_name"),
        col("ΔΟΥID").cast("int").alias("doy_id"),
        col("Κατηγορία_Φόρου").alias("tax_category"),
        col("Βάση_Φόρου").cast("double").alias("tax_base"),
        col("Συντελεστής_Pct").cast("double").alias("rate_pct"),
        col("Ποσό_EUR").cast("double").alias("amount_eur"),
        col("Κατάσταση").alias("status"),
        col("Περιφέρεια").alias("region"),
        col("Πόλη").alias("city"),
        col("Φορ_Ετος").cast("int").alias("tax_year"),
    )
    .filter(col("amount_eur") >= 0)
)
silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_TBL)
print(f"✓ Silver: {spark.table(SILVER_TBL).count()}")
spark.table(SILVER_TBL).printSchema()

# COMMAND ----------

# TODO 3 — Gold
gold = (
    spark.table(SILVER_TBL)
    .groupBy("tax_category", "region")
    .agg(
        count("*").alias("n_declarations"),
        spark_sum("amount_eur").alias("total_tax_eur"),
        avg("amount_eur").alias("avg_tax_eur"),
        spark_sum(when(col("status") == "Εγκεκριμένη", 1).otherwise(0)).alias("approved"),
        spark_sum(when(col("status") == "Απορριφθείσα", 1).otherwise(0)).alias("rejected"),
        spark_sum(when(col("status") == "Εκκρεμής", 1).otherwise(0)).alias("pending"),
    )
    .orderBy(desc("total_tax_eur"))
)
gold.write.format("delta").mode("overwrite").saveAsTable(GOLD_TBL)
gold.show(20, truncate=False)

# COMMAND ----------

# TODO 4 — insight
(
    spark.table(GOLD_TBL)
    .groupBy("tax_category")
    .agg(spark_sum("total_tax_eur").alias("revenue"), spark_sum("approved").alias("approved"))
    .orderBy(desc("revenue"))
    .show(truncate=False)
)

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 2 ολοκληρώθηκε")
