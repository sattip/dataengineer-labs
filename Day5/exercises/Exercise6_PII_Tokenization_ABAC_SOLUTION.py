# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 6/6 (Advanced): PII Tokenization & ABAC

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col, sha2, concat, lit, length

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve("https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/declarations.csv",
                               f"{VOLUME}/declarations.csv")

SILVER, SHARED, ENTITLEMENTS, ABAC_VIEW = (
    "workspace.aade.tok_declarations","workspace.aade.tok_declarations_shared",
    "workspace.aade.entitlements","workspace.aade.tok_declarations_abac")

(spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
 .select(col("ΑΦΜ").cast("string").alias("afm"), col("Επωνυμία").alias("business_name"),
         col("Ποσό_EUR").cast("double").alias("tax_amount_eur"), col("Περιφέρεια").alias("region"))
 .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(SILVER))

# COMMAND ----------

# TODO 1 — pseudonymized shared view
spark.sql(f"""
    CREATE OR REPLACE VIEW {SHARED} AS
    SELECT sha2(afm, 256) AS afm_token, business_name, tax_amount_eur, region
    FROM {SILVER}
""")
spark.table(SHARED).show(3, truncate=False)

# COMMAND ----------

# TODO 2 — deterministic & 1:1
n_afm   = spark.table(SILVER).select("afm").distinct().count()
n_token = spark.table(SHARED).select("afm_token").distinct().count()
token_len = spark.table(SHARED).select(length("afm_token").alias("L")).first()["L"]
print(f"afm={n_afm} token={n_token} len={token_len}")

# COMMAND ----------

# TODO 3 — salted hashing
SALT = "aade_2026_secret"
salted = spark.table(SILVER).withColumn("afm_token_salted", sha2(concat(lit(SALT), col("afm")), 256))
sample_afm = spark.table(SILVER).select("afm").orderBy("afm").first()["afm"]
unsalted_t = spark.sql(f"SELECT sha2('{sample_afm}', 256) AS t").first()["t"]
salted_t   = salted.filter(col("afm")==sample_afm).select("afm_token_salted").first()["afm_token_salted"]
print("unsalted==salted:", unsalted_t == salted_t)

# COMMAND ----------

# TODO 4 — entitlements + ABAC view
spark.sql(f"CREATE OR REPLACE TABLE {ENTITLEMENTS} (user_email STRING, allowed_region STRING) USING delta")
spark.sql(f"INSERT INTO {ENTITLEMENTS} SELECT current_user(), 'Αττική'")
spark.sql(f"""
    CREATE OR REPLACE VIEW {ABAC_VIEW} AS
    SELECT * FROM {SILVER} s
    WHERE s.region IN (SELECT allowed_region FROM {ENTITLEMENTS} WHERE user_email = current_user())
""")
abac_regions = [r["region"] for r in spark.table(ABAC_VIEW).select("region").distinct().collect()]
print("ABAC regions:", abac_regions)

# COMMAND ----------

# TODO 5 — sensitivity tag
try:
    spark.sql(f"ALTER TABLE {SILVER} ALTER COLUMN afm SET TAGS ('sensitivity' = 'PII')")
    print("tagged"); tag_ok = True
except Exception as e:
    tag_ok = None; print("skip tag:", str(e)[:120])

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 6 ολοκληρώθηκε — Day 5 advanced complete")
