# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Bonus (4/4): Payments Pipeline (Capstone)

# COMMAND ----------

import urllib.request, os
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2"
VOLUME = "/Volumes/workspace/aade/aade_data"
for fname in ["payments.csv", "taxpayers.csv"]:
    target = f"{VOLUME}/{fname}"
    if not os.path.exists(target):
        urllib.request.urlretrieve(f"{REPO}/{fname}", target)
print("✓ data ready")

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, row_number, trim, lit
)
from pyspark.sql.window import Window

# COMMAND ----------

# TODO 1 — read
payments_raw = (
    spark.read.option("header", "true").option("inferSchema", "true")
         .csv(f"{VOLUME}/payments.csv")
)
print(f"Raw: {payments_raw.count()}")

# COMMAND ----------

# TODO 2 — cleanse
valid_status = ["Confirmed", "Pending", "Failed"]
payments_clean = (
    payments_raw
    .filter(col("afm").isNotNull())
    .withColumn("payment_method", trim(col("payment_method")))
    .filter(col("amount_eur") > 0)
    .withColumn("status",
        when(col("status").isin(valid_status), col("status"))
        .otherwise(lit("UNKNOWN")))
)
print(f"Clean: {payments_clean.count()}")

# COMMAND ----------

# TODO 3 — dedup
w = Window.partitionBy("payment_id").orderBy(col("payment_date").desc())
payments_dedup = (
    payments_clean
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)
print(f"Raw {payments_raw.count()} → Clean {payments_clean.count()} → Dedup {payments_dedup.count()}")

# COMMAND ----------

# TODO 4 — gold
gold = (
    payments_dedup
    .groupBy("payment_method", "region")
    .agg(
        count("*").alias("total_payments"),
        spark_sum("amount_eur").alias("total_eur"),
        avg("amount_eur").alias("avg_eur"),
        spark_sum(when(col("status") == "Confirmed", 1).otherwise(0)).alias("confirmed"),
        spark_sum(when(col("status") == "Failed", 1).otherwise(0)).alias("failed"),
        spark_sum(when(col("status") == "Pending", 1).otherwise(0)).alias("pending"),
    )
    .orderBy(desc("total_eur"))
)
gold.show(15, truncate=False)

# COMMAND ----------

# TODO 5 — failure rate
(
    gold.groupBy("payment_method")
        .agg(spark_sum("failed").alias("failed"), spark_sum("total_payments").alias("total"))
        .withColumn("failure_pct", (col("failed") / col("total") * 100).cast("decimal(5,2)"))
        .orderBy(desc("failure_pct"))
        .show(truncate=False)
)

# COMMAND ----------

# TODO 6 — delta write
gold.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.payments_gold")
print("✓ Gold saved")

# COMMAND ----------

print("🏆 ΛΥΣΗ Capstone ολοκληρώθηκε")
