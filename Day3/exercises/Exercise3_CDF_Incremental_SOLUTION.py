# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 3/3: Change Data Feed + Incremental ETL

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col, when, sum as spark_sum, lit

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3"
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve(f"{REPO}/declarations.csv", f"{VOLUME}/declarations.csv")

SRC  = "workspace.aade.declarations_cdf"
GOLD = "workspace.aade.revenue_by_region_gold"
raw = spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
src = raw.select(
    col("ΔηλωσηID").cast("int").alias("declaration_id"),
    col("Ποσό_EUR").cast("double").alias("amount_eur"),
    col("Περιφέρεια").alias("region"),
)
src.write.format("delta").mode("overwrite").saveAsTable(SRC)

# COMMAND ----------

# TODO 1 — enable CDF
spark.sql(f"ALTER TABLE {SRC} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
start_v = spark.sql(f"DESCRIBE HISTORY {SRC}").selectExpr("max(version) v").collect()[0]["v"]
print(f"start_version = {start_v}")

# COMMAND ----------

# initial gold + changes
gold0 = spark.table(SRC).groupBy("region").agg(spark_sum("amount_eur").alias("total_amount"))
gold0.write.format("delta").mode("overwrite").saveAsTable(GOLD)

spark.sql(f"INSERT INTO {SRC} VALUES (9001, 5000.0, 'Αττική'), (9002, 3000.0, 'Κρήτη')")
spark.sql(f"UPDATE {SRC} SET amount_eur = amount_eur + 1000 WHERE declaration_id IN (10, 11)")
spark.sql(f"DELETE FROM {SRC} WHERE declaration_id IN (20, 21)")

# COMMAND ----------

# TODO 2 — read change feed
changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", start_v + 1)
    .table(SRC)
)
changes.groupBy("_change_type").count().show()

# COMMAND ----------

# TODO 3 — net delta
signed = changes.withColumn(
    "signed_amount",
    when(col("_change_type").isin("insert", "update_postimage"), col("amount_eur"))
    .when(col("_change_type").isin("delete", "update_preimage"), -col("amount_eur"))
    .otherwise(lit(0.0))
)
net = signed.groupBy("region").agg(spark_sum("signed_amount").alias("net_delta"))
net.createOrReplaceTempView("region_net_delta")
net.show(truncate=False)

# COMMAND ----------

# TODO 4 — merge deltas into gold
spark.sql(f"""
    MERGE INTO {GOLD} g
    USING region_net_delta d
    ON g.region = d.region
    WHEN MATCHED THEN UPDATE SET g.total_amount = g.total_amount + d.net_delta
    WHEN NOT MATCHED THEN INSERT (region, total_amount) VALUES (d.region, d.net_delta)
""")

# COMMAND ----------

# validation: incremental == full recompute (tolerance)
incremental = {r["region"]: r["total_amount"] for r in spark.table(GOLD).collect()}
full = {r["region"]: r["total"] for r in spark.table(SRC).groupBy("region").agg(spark_sum("amount_eur").alias("total")).collect()}
all_regions = set(incremental) | set(full)
match = all(abs(incremental.get(r,0.0) - full.get(r,0.0)) < 0.01 for r in all_regions)
print("Incremental == Full recompute:", match)

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 3 ολοκληρώθηκε — Ημέρα 3 complete")
