# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 4/4: Governance & Audit

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col, count, sum as spark_sum

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve("https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/declarations.csv",
                               f"{VOLUME}/declarations.csv")
GOLD = "workspace.aade.gov_revenue_by_region"
(spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
 .select(col("ΑΦΜ").cast("string").alias("afm"), col("Ποσό_EUR").cast("double").alias("tax_amount_eur"),
         col("Περιφέρεια").alias("region"))
 .groupBy("region").agg(count("*").alias("n"), spark_sum("tax_amount_eur").alias("total_eur"))
 .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(GOLD))

# COMMAND ----------

# TODO 1 — RBAC matrix
roles = spark.createDataFrame([
    ("Data Engineer","READ/WRITE","READ/WRITE","READ/WRITE","Πλήρες control"),
    ("Data Steward", "READ","READ/WRITE","READ","Quality + curation"),
    ("Data Analyst", "—","READ","READ","BI + ad-hoc"),
    ("Executive","—","—","READ","Dashboards"),
    ("Auditor (DPO)","READ-meta","READ-meta","READ-meta","Audit logs"),
    ("Citizen (GDPR)","—","—","—","Right to erasure"),
], ["role","bronze","silver","gold","notes"])
display(roles)

# COMMAND ----------

# TODO 2 — GRANT
GROUP = "account users"
try:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA workspace.aade TO `{GROUP}`")
    spark.sql(f"GRANT SELECT ON TABLE {GOLD} TO `{GROUP}`")
    grants_ok = True
    display(spark.sql(f"SHOW GRANTS ON TABLE {GOLD}"))
except Exception as e:
    grants_ok = None; print("skip grant:", str(e)[:120])

# COMMAND ----------

# TODO 3 — REVOKE
try:
    spark.sql(f"REVOKE SELECT ON TABLE {GOLD} FROM `{GROUP}`"); print("revoked")
except Exception as e:
    print("skip revoke:", str(e)[:120])

# COMMAND ----------

# TODO 4 — information_schema: count objects
n_tables = spark.sql("SELECT count(*) c FROM workspace.information_schema.tables WHERE table_schema='aade'").collect()[0]["c"]
print("objects in aade:", n_tables)

# COMMAND ----------

# TODO 5 — PII discovery
pii = spark.sql("""
    SELECT table_name, column_name, data_type
    FROM workspace.information_schema.columns
    WHERE table_schema='aade'
      AND lower(column_name) IN ('afm','amka','email','tax_amount_eur','income','phone','iban')
    ORDER BY table_name, column_name
""")
print("PII columns:", pii.count()); pii.show(50, truncate=False)

# COMMAND ----------

# TODO 6 — audit (system tables)
try:
    audit = spark.sql("""
        SELECT event_time, user_identity.email AS user, action_name
        FROM system.access.audit
        WHERE action_name IN ('getTable','generateTemporaryTableCredential','commandSubmit')
        ORDER BY event_time DESC LIMIT 10
    """)
    audit.show(truncate=False); audit_ok = True
except Exception as e:
    audit_ok = None; print("skip audit:", str(e)[:120])

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 4 ολοκληρώθηκε — Day 5 complete")
