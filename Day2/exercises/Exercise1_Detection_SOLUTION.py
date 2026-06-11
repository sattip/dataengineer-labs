# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 1/3: Bronze Ingestion + DQ Detection
# MAGIC Πλήρης λύση όλων των TODO. Συγκρίνετε με το STARTER σας.

# COMMAND ----------

import urllib.request, os

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.mydata_raw")

REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2"
MYDATA_VOLUME = "/Volumes/workspace/aade/mydata_raw"
MASTER_VOLUME = "/Volumes/workspace/aade/aade_data"

for fname in ["taxpayers.csv", "doy.csv", "employees.csv", "declarations.csv"]:
    target = f"{MASTER_VOLUME}/{fname}"
    if not os.path.exists(target):
        urllib.request.urlretrieve(f"{REPO}/{fname}", target)

mydata_target = f"{MYDATA_VOLUME}/mydata_invoices_MESSY.csv"
if not os.path.exists(mydata_target):
    urllib.request.urlretrieve(f"{REPO}/mydata_invoices_MESSY.csv", mydata_target)
print("✓ data ready")

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, row_number,
    trim, regexp_replace, lit, to_date, current_date, current_timestamp,
    length, regexp_extract, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, DateType

# COMMAND ----------

# TODO 1 — Bronze ingest
df_raw = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{MYDATA_VOLUME}/mydata_invoices_MESSY.csv")
)
print(f"Raw invoices: {df_raw.count()} rows")
df_raw.printSchema()

# COMMAND ----------

# TODO 2 — audit metadata + Bronze write
df_bronze = (
    df_raw
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)
df_bronze.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mydata_raw")
print("✓ Bronze saved")

# COMMAND ----------

# TODO 3 — NULL counts
null_counts = df_raw.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df_raw.columns
])
null_counts.show()

# COMMAND ----------

# TODO 4 — duplicate invoice_id
dups = df_raw.groupBy("invoice_id").count().filter(col("count") > 1)
print(f"Duplicates: {dups.count()}")
dups.show(truncate=False)

# COMMAND ----------

# TODO 5 — bad AFM
bad_afm = df_raw.filter(
    col("issuer_afm").isNotNull() &
    (~col("issuer_afm").cast("string").rlike(r"^\d{9}$"))
)
print(f"Bad AFM: {bad_afm.count()}")
bad_afm.select("invoice_id", "issuer_afm").show(truncate=False)

# COMMAND ----------

# TODO 6 — negative amounts
neg = df_raw.filter(col("net_amount") < 0)
print(f"Negative: {neg.count()}")

# COMMAND ----------

# TODO 7 — future dates
parseable = df_raw.withColumn("parsed_date", expr("try_to_date(issue_date, 'yyyy-MM-dd')"))
future = parseable.filter(col("parsed_date") > current_date())
print(f"Future dates: {future.count()}")

# COMMAND ----------

# TODO 8 — invalid status
valid_statuses = ["Υποβληθέν", "Ακυρωμένο", "Εκκρεμές"]
invalid_status = df_raw.filter(~col("status").isin(valid_statuses))
print(f"Invalid status: {invalid_status.count()}")
invalid_status.groupBy("status").count().show()

# COMMAND ----------

# TODO 9 — whitespace
ws = df_raw.filter(col("issuer_name") != trim(col("issuer_name")))
print(f"Need trim: {ws.count()}")

# COMMAND ----------

# TODO 10 — bad date format
bad_date = df_raw.filter(~col("issue_date").rlike(r"^\d{4}-\d{2}-\d{2}$"))
print(f"Bad date format: {bad_date.count()}")
bad_date.select("invoice_id", "issue_date").show(truncate=False)

# COMMAND ----------

# TODO 11 — null vat
null_vat = df_raw.filter(col("vat_amount").isNull())
print(f"Null vat: {null_vat.count()}")

# COMMAND ----------

# TODO 12 — orphans (left_anti)
taxpayers = spark.read.csv(f"{MASTER_VOLUME}/taxpayers.csv", header=True, inferSchema=True)
valid_afms = taxpayers.select(col("ΑΦΜ").cast("string").alias("ΑΦΜ"))
orphan = df_raw.select("invoice_id", "receiver_afm").join(
    valid_afms, df_raw.receiver_afm == valid_afms.ΑΦΜ, "left_anti"
)
print(f"Orphans: {orphan.count()}")
orphan.show()

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 1 ολοκληρώθηκε")
