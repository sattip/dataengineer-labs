# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 2/3: Time Travel + Maintenance

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col, lit

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3"
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve(f"{REPO}/declarations.csv", f"{VOLUME}/declarations.csv")

TBL = "workspace.aade.tax_declarations_tt"
raw = spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
base = raw.select(
    col("ΔηλωσηID").cast("int").alias("declaration_id"),
    col("ΑΦΜ").cast("string").alias("afm"),
    col("Κατηγορία_Φόρου").alias("tax_category"),
    col("Ποσό_EUR").cast("double").alias("amount_eur"),
    col("Κατάσταση").alias("status"),
    col("Περιφέρεια").alias("region"),
)
base.write.format("delta").mode("overwrite").saveAsTable(TBL)   # v0
spark.sql(f"UPDATE {TBL} SET status='Εγκεκριμένη' WHERE declaration_id=1")  # v1
spark.sql(f"DELETE FROM {TBL} WHERE status='Απορριφθείσα'")                 # v2
print(f"current: {spark.table(TBL).count()}")  # 266

# COMMAND ----------

# TODO 1 — history
display(spark.sql(f"DESCRIBE HISTORY {TBL}"))

# COMMAND ----------

# TODO 2 — time travel
v0_count = spark.sql(f"SELECT count(*) c FROM {TBL} VERSION AS OF 0").collect()[0]["c"]
print(f"v0={v0_count}  now={spark.table(TBL).count()}")   # 300 vs 266

# COMMAND ----------

# TODO 3 — restore
spark.sql(f"RESTORE TABLE {TBL} TO VERSION AS OF 0")
print(f"after restore: {spark.table(TBL).count()}")   # 300

# COMMAND ----------

# small files
for i in range(6):
    base.limit(1).withColumn("declaration_id", lit(9100 + i)).write.format("delta").mode("append").saveAsTable(TBL)
files_before = spark.sql(f"DESCRIBE DETAIL {TBL}").collect()[0]["numFiles"]
print(f"files before: {files_before}")

# COMMAND ----------

# TODO 4 — OPTIMIZE + ZORDER
spark.sql(f"OPTIMIZE {TBL}")
files_after = spark.sql(f"DESCRIBE DETAIL {TBL}").collect()[0]["numFiles"]
print(f"files after: {files_after}")
spark.sql(f"OPTIMIZE {TBL} ZORDER BY (region)")
print("✓ ZORDER done")

# COMMAND ----------

# TODO 5 — VACUUM dry run
display(spark.sql(f"VACUUM {TBL} RETAIN 168 HOURS DRY RUN"))

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 2 ολοκληρώθηκε")
