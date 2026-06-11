# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 1/3: Delta DML + MERGE

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3"
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve(f"{REPO}/declarations.csv", f"{VOLUME}/declarations.csv")

SILVER = "workspace.aade.tax_declarations_silver"
raw = spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
silver = raw.select(
    col("ΔηλωσηID").cast("int").alias("declaration_id"),
    col("ΑΦΜ").cast("string").alias("afm"),
    col("Επωνυμία").alias("business_name"),
    col("Κατηγορία_Φόρου").alias("tax_category"),
    col("Ποσό_EUR").cast("double").alias("amount_eur"),
    col("Κατάσταση").alias("status"),
    col("Περιφέρεια").alias("region"),
    col("Φορ_Ετος").cast("int").alias("tax_year"),
)
silver.write.format("delta").mode("overwrite").saveAsTable(SILVER)
print(f"✓ {spark.table(SILVER).count()}")

# COMMAND ----------

# TODO 1 — describe
display(spark.sql(f"DESCRIBE DETAIL {SILVER}"))
display(spark.sql(f"DESCRIBE HISTORY {SILVER}"))

# COMMAND ----------

# TODO 2 — UPDATE
spark.sql(f"UPDATE {SILVER} SET status = 'Εγκεκριμένη' WHERE declaration_id = 1")
spark.sql(f"SELECT declaration_id, status FROM {SILVER} WHERE declaration_id = 1").show()

# COMMAND ----------

# TODO 3 — DELETE
spark.sql(f"DELETE FROM {SILVER} WHERE declaration_id = 2")
print(f"count: {spark.table(SILVER).count()}")   # 299

# COMMAND ----------

# TODO 4 — MERGE
updates = spark.createDataFrame([
    (3, "Εγκεκριμένη", 31369.74),
    (4, "Εγκεκριμένη", 12000.00),
    (5, "Εγκεκριμένη",  8500.00),
    (9001, "Εκκρεμής",  1000.00),
    (9002, "Εκκρεμής",  2000.00),
], ["declaration_id", "status", "amount_eur"])
updates.createOrReplaceTempView("declaration_updates")

spark.sql(f"""
    MERGE INTO {SILVER} t
    USING declaration_updates s
    ON t.declaration_id = s.declaration_id
    WHEN MATCHED THEN UPDATE SET t.status = s.status, t.amount_eur = s.amount_eur
    WHEN NOT MATCHED THEN INSERT (declaration_id, status, amount_eur)
        VALUES (s.declaration_id, s.status, s.amount_eur)
""")
print(f"count: {spark.table(SILVER).count()}")   # 301
spark.sql(f"SELECT declaration_id, status, amount_eur FROM {SILVER} WHERE declaration_id IN (3,9001,9002) ORDER BY declaration_id").show()

# COMMAND ----------

# TODO 5 — schema evolution
spark.sql(f"ALTER TABLE {SILVER} ADD COLUMNS (review_note string)")
print(spark.table(SILVER).columns)

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 1 ολοκληρώθηκε")
