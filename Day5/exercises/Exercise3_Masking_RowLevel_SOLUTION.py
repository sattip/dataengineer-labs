# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 3/4: Column Masking + Row-Level Security

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve("https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/declarations.csv",
                               f"{VOLUME}/declarations.csv")

SILVER, VIEW_MASKED, VIEW_MYREGION = (
    "workspace.aade.pii_declarations","workspace.aade.pii_declarations_masked","workspace.aade.pii_declarations_myregion")

(spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
 .select(col("ΑΦΜ").cast("string").alias("afm"), col("Επωνυμία").alias("business_name"),
         col("Ποσό_EUR").cast("double").alias("tax_amount_eur"),
         col("Περιφέρεια").alias("region"), col("Κατάσταση").alias("status"))
 .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(SILVER))
print(f"✓ Silver {spark.table(SILVER).count()}")

# COMMAND ----------

# TODO 1 — masked dynamic view
spark.sql(f"""
    CREATE OR REPLACE VIEW {VIEW_MASKED} AS
    SELECT
        CASE WHEN is_account_group_member('aade_pii_unmasked') THEN afm
             ELSE concat('***', substr(afm, 7, 3)) END AS afm,
        business_name,
        CASE WHEN is_account_group_member('aade_pii_unmasked') THEN tax_amount_eur
             ELSE NULL END AS tax_amount_eur,
        region, status
    FROM {SILVER}
""")
spark.table(VIEW_MASKED).show(5, truncate=False)

# COMMAND ----------

# TODO 2 — row-filtered view
spark.sql(f"""
    CREATE OR REPLACE VIEW {VIEW_MYREGION} AS
    SELECT * FROM {SILVER}
    WHERE is_account_group_member('aade_all_regions') OR region = 'Αττική'
""")
myregion_count = spark.table(VIEW_MYREGION).count()
attica_count   = spark.table(SILVER).filter(col("region")=="Αττική").count()
print("myregion:", myregion_count, "attica:", attica_count)

# COMMAND ----------

# TODO 3 — UC column mask (production)
try:
    spark.sql("""CREATE OR REPLACE FUNCTION workspace.aade.mask_afm(a STRING)
                 RETURN CASE WHEN is_account_group_member('aade_pii_unmasked') THEN a
                             ELSE concat('***', substr(a,7,3)) END""")
    spark.sql(f"ALTER TABLE {SILVER} ALTER COLUMN afm SET MASK workspace.aade.mask_afm")
    spark.table(SILVER).select("afm","business_name").show(3, truncate=False)
    uc_mask_ok = spark.sql(f"SELECT afm FROM {SILVER} LIMIT 1").collect()[0]["afm"].startswith("***")
except Exception as e:
    uc_mask_ok = None; print("skip mask:", str(e)[:120])

# COMMAND ----------

# TODO 4 — UC row filter (production) + cleanup
try:
    spark.sql("""CREATE OR REPLACE FUNCTION workspace.aade.region_filter(r STRING)
                 RETURN is_account_group_member('aade_all_regions') OR r = 'Αττική'""")
    spark.sql(f"ALTER TABLE {SILVER} SET ROW FILTER workspace.aade.region_filter ON (region)")
    print("row filter visible rows:", spark.table(SILVER).count())
    spark.sql(f"ALTER TABLE {SILVER} DROP ROW FILTER")
    spark.sql(f"ALTER TABLE {SILVER} ALTER COLUMN afm DROP MASK")
except Exception as e:
    print("skip row filter:", str(e)[:120])

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 3 ολοκληρώθηκε")
