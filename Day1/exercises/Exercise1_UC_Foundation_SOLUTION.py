# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 1/3: Unity Catalog Foundation

# COMMAND ----------

CATALOG        = "workspace"
SCHEMA_BRONZE  = "aade_bronze"
SCHEMA_SILVER  = "aade_silver"
SCHEMA_GOLD    = "aade_gold"
VOLUME_FQN     = f"{CATALOG}.{SCHEMA_BRONZE}.landing"
LANDING_PATH   = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/landing"

# COMMAND ----------

# TODO 1 — schemas
for schema, desc in [
    (SCHEMA_BRONZE, "Raw ingested data — immutable, full history"),
    (SCHEMA_SILVER, "Cleaned, typed, validated, deduplicated"),
    (SCHEMA_GOLD,   "Aggregated, business-ready για BI"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{desc}'")
    print(f"✅ Schema: {CATALOG}.{schema}")

# COMMAND ----------

# TODO 2 — volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME_FQN} COMMENT 'Landing zone για raw CSVs'")
print(f"✅ Volume ready: {LANDING_PATH}")

# COMMAND ----------

import urllib.request, os
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day1"
for fname in ["declarations.csv", "doy.csv", "employees.csv", "taxpayers.csv"]:
    target = f"{LANDING_PATH}/{fname}"
    if not os.path.exists(target):
        urllib.request.urlretrieve(f"{REPO}/{fname}", target)
print("✓ data ready")

# COMMAND ----------

# TODO 3 — read
df = (
    spark.read.option("header", "true").option("inferSchema", "true")
         .csv(f"{LANDING_PATH}/declarations.csv")
)
print(f"Δηλώσεις: {df.count()} γραμμές")
df.printSchema()

# COMMAND ----------

# TODO 4 — show
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA_BRONZE}"))
display(dbutils.fs.ls(LANDING_PATH))

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 1 ολοκληρώθηκε")
