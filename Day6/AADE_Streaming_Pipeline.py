# Databricks notebook source
# MAGIC %md
# MAGIC # 🏛️ Day 6 Capstone — End-to-End Streaming Pipeline (ΑΑΔΕ)
# MAGIC
# MAGIC **Ρόλος:** Lead Data Engineer — Data Platform ΑΑΔΕ
# MAGIC **Διάρκεια:** ~45'
# MAGIC **Περιβάλλον:** Databricks Free Edition (Serverless) με Unity Catalog
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day6/AADE_Streaming_Pipeline.py
# MAGIC > ```
# MAGIC
# MAGIC ## 🎯 Στόχος
# MAGIC > Φτιάχνουμε **end-to-end streaming pipeline** που ενώνει 4 πηγές δεδομένων ΑΑΔΕ
# MAGIC > (TAXIS, myDATA, ΚΕΠ, e-ΕΦΚΑ) σε medallion architecture, με streaming bronze
# MAGIC > ingestion, MERGE-based silver, και αναλυτικά gold tables.
# MAGIC
# MAGIC ## 📚 Τι είναι «medallion architecture»;
# MAGIC > Pattern Databricks για ETL pipelines σε **3 ποιοτικά επίπεδα**:
# MAGIC > - **🥉 Bronze**: Raw data όπως ήρθε (append-only, immutable)
# MAGIC > - **🥈 Silver**: Καθαρισμένα + ενοποιημένα δεδομένα (deduplicated, validated)
# MAGIC > - **🥇 Gold**: Aggregated business-ready tables (KPIs, dashboards, ML features)
# MAGIC
# MAGIC ## 📚 Τι είναι «streaming pipeline»;
# MAGIC > Pipeline που **τρέχει συνεχώς** και επεξεργάζεται **νέα δεδομένα μόλις φτάσουν** —
# MAGIC > σε αντίθεση με batch (που τρέχει 1× τη μέρα). Το Databricks προσφέρει:
# MAGIC > - **Structured Streaming**: API για streaming queries
# MAGIC > - **Auto Loader (cloudFiles)**: αυτόματο ingestion νέων αρχείων
# MAGIC > - **availableNow trigger**: process all current data + exit (good για demos)
# MAGIC > - **continuous trigger**: 24/7 streaming (production)
# MAGIC
# MAGIC ## 🏗️ Pipeline Architecture
# MAGIC ```
# MAGIC                        ┌──────────────┐
# MAGIC  TAXIS    ─CSV files──▶│ Auto Loader  │──▶ bronze_taxis ───┐
# MAGIC  myDATA   ─CSV files──▶│ (streaming)  │──▶ bronze_mydata ──┤
# MAGIC  ΚΕΠ      ─CSV files──▶│              │──▶ bronze_kep ─────┤  Silver MERGE
# MAGIC  e-ΕΦΚΑ   ─CSV files──▶│              │──▶ bronze_efka ────┤  + Quality
# MAGIC                        └──────────────┘                    │  Expectations
# MAGIC                                                            ▼
# MAGIC                                  ┌──────────────────────────────────┐
# MAGIC                                  │  silver_tax_declarations_clean   │
# MAGIC                                  │  silver_invoices_clean           │
# MAGIC                                  │  silver_kep_events_clean         │
# MAGIC                                  │  silver_efka_contributions_clean │
# MAGIC                                  └──────────────┬───────────────────┘
# MAGIC                                                 │ JOIN + AGGREGATE
# MAGIC                                                 ▼
# MAGIC                                  ┌──────────────────────────────────┐
# MAGIC                                  │  gold_citizen_360                │
# MAGIC                                  │  gold_daily_kpis                 │
# MAGIC                                  │  gold_audit_trail                │
# MAGIC                                  │  gold_data_quality_summary       │
# MAGIC                                  └──────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## 📊 Τι θα δείξει το pipeline
# MAGIC | Layer | Tables | Mode |
# MAGIC |---|---|---|
# MAGIC | 🥉 Bronze | 4 streaming tables | Append με Auto Loader |
# MAGIC | 🥈 Silver | 4 cleansed tables | MERGE με data quality expectations |
# MAGIC | 🥇 Gold | 4 analytical tables | Aggregations + JOIN |
# MAGIC | 📈 Monitoring | DQ summary + audit | Per-batch metrics |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 0: Setup — Schema, Volume, και suppression για clean output

# COMMAND ----------

import os
import logging

# Σιγάζουμε noisy GRPC warnings (Spark Connect cosmetic issue σε Free Edition)
logging.getLogger("pyspark.sql.connect.client.core").setLevel(logging.CRITICAL)
logging.getLogger("py4j").setLevel(logging.CRITICAL)
logging.getLogger("grpc").setLevel(logging.CRITICAL)

# Unity Catalog setup (idempotent)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

# Καθαρίζουμε προηγούμενα streaming checkpoints για clean rerun
volume_root = "/Volumes/workspace/aade/aade_data"
streaming_root = f"{volume_root}/streaming"
os.makedirs(streaming_root, exist_ok=True)

# Folders για κάθε source (input)
sources = ["taxis", "mydata", "kep", "efka"]
for src in sources:
    os.makedirs(f"{streaming_root}/raw/{src}", exist_ok=True)
    os.makedirs(f"{streaming_root}/checkpoints/{src}", exist_ok=True)

print(f"✓ Schema workspace.aade & Volume έτοιμα")
print(f"✓ Streaming root: {streaming_root}")
print(f"✓ Sources: {', '.join(sources)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 1: Mock Source Data Generator (Batch 1)
# MAGIC
# MAGIC ### 📚 Πώς προσομοιώνουμε streaming;
# MAGIC > Σε production τα δεδομένα έρχονται από Kafka, Event Hubs, ή τα σώζουν εξωτερικά
# MAGIC > systems σε ένα data lake folder. Εμείς **γράφουμε CSV files** στο volume με
# MAGIC > timestamp στο όνομα — το Auto Loader τα **πιάνει αυτόματα** ως νέα events.
# MAGIC >
# MAGIC > Θα τρέξουμε δύο batches: ένα τώρα (Batch 1) για να αρχίσει το pipeline,
# MAGIC > και ένα αργότερα (Batch 2) για να δούμε **incremental processing**.

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)
batch_id = 1
batch_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def write_source(name, df):
    path = f"{streaming_root}/raw/{name}/batch_{batch_id}_{batch_ts}.csv"
    df.to_csv(path, index=False)
    print(f"  ✓ {name:8s} → {len(df):4d} rows | {path.replace(volume_root, '/Volumes/.../aade_data')}")


# 1.1 TAXIS declarations
taxis_data = []
afms = [f"{900000000 + i:09d}" for i in range(1, 21)]
for i in range(50):
    taxis_data.append({
        "statement_id": f"TX{batch_id:02d}{i:04d}",
        "afm": np.random.choice(afms) if np.random.random() > 0.05 else None,  # 5% nulls
        "fiscal_year": 2025,
        "tax_category": np.random.choice(["IncomeTax", "VAT", "PropertyTax"]),
        "tax_base": round(np.random.uniform(5000, 80000), 2),
        "tax_amount": round(np.random.uniform(800, 18000), 2),
        "status": np.random.choice(["Submitted", "Approved", "Rejected"], p=[0.5, 0.4, 0.1]),
        "submitted_at": (datetime.utcnow() - timedelta(hours=np.random.randint(0, 48))).isoformat(),
    })
taxis_df = pd.DataFrame(taxis_data)
write_source("taxis", taxis_df)

# 1.2 myDATA invoices
mydata_data = []
for i in range(80):
    issuer = np.random.choice(afms)
    receiver = np.random.choice([a for a in afms if a != issuer])
    net = round(np.random.uniform(50, 5000), 2)
    vat = round(net * 0.24, 2)
    mydata_data.append({
        "invoice_id": f"INV{batch_id:02d}{i:04d}",
        "issuer_afm": issuer,
        "receiver_afm": receiver,
        "invoice_date": (datetime.utcnow() - timedelta(days=np.random.randint(0, 30))).date().isoformat(),
        "net_amount": net,
        "vat_amount": vat,
        "total_amount": round(net + vat, 2),
        "transmission_status": np.random.choice(["Accepted", "Rejected", "Pending"], p=[0.85, 0.05, 0.10]),
    })
write_source("mydata", pd.DataFrame(mydata_data))

# 1.3 ΚΕΠ events (real-time citizen requests)
kep_data = []
event_types = ["BirthCertificate", "TaxClearance", "ResidencePermit", "ConfirmationOfStudies", "FamilyStatus"]
for i in range(120):
    kep_data.append({
        "event_id": f"KEP{batch_id:02d}{i:05d}",
        "afm": np.random.choice(afms),
        "event_type": np.random.choice(event_types),
        "kep_office": np.random.choice(["Athens-Center", "Thessaloniki", "Patras", "Heraklion", "Larissa"]),
        "event_ts": (datetime.utcnow() - timedelta(minutes=np.random.randint(0, 720))).isoformat(),
        "status": np.random.choice(["Completed", "InProgress", "Failed"], p=[0.85, 0.12, 0.03]),
        "duration_seconds": np.random.randint(120, 3600),
    })
write_source("kep", pd.DataFrame(kep_data))

# 1.4 e-ΕΦΚΑ contributions
efka_data = []
for i in range(60):
    efka_data.append({
        "contribution_id": f"EFK{batch_id:02d}{i:04d}",
        "afm": np.random.choice(afms),
        "contribution_month": (datetime.utcnow() - timedelta(days=np.random.randint(0, 90))).strftime("%Y-%m"),
        "category": np.random.choice(["Employee", "SelfEmployed", "Pensioner"]),
        "gross_income": round(np.random.uniform(800, 6000), 2),
        "contribution_amount": round(np.random.uniform(120, 1200), 2),
        "payment_status": np.random.choice(["Paid", "Pending", "Overdue"], p=[0.80, 0.15, 0.05]),
    })
write_source("efka", pd.DataFrame(efka_data))

print(f"\n✓ Batch {batch_id} written → 4 sources, {50+80+120+60} total rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 2: Bronze Layer — Auto Loader Streaming Ingestion
# MAGIC
# MAGIC ### 📚 Τι είναι «Auto Loader»;
# MAGIC > Databricks streaming source (`cloudFiles` format) που **παρακολουθεί** ένα folder
# MAGIC > στο cloud storage και **διαβάζει αυτόματα** κάθε νέο αρχείο όταν εμφανιστεί.
# MAGIC > Πλεονεκτήματα έναντι naive `spark.read`:
# MAGIC > - **Exactly-once** processing με checkpoint
# MAGIC > - **Schema inference + evolution** — όταν προστεθούν νέες στήλες
# MAGIC > - **Scalable** — χειρίζεται εκατομμύρια αρχείων (file notification mode)
# MAGIC > - **Resumable** — αν crashάρει, ξεκινά από το τελευταίο checkpoint
# MAGIC
# MAGIC ### 📚 Τι είναι «trigger=availableNow»;
# MAGIC > **Trigger.AvailableNow** = «επεξεργάσου ό,τι υπάρχει τώρα και τερμάτισε».
# MAGIC > Ιδανικό για:
# MAGIC > - Notebook demos (δεν τρέχει για πάντα)
# MAGIC > - Scheduled jobs (καθημερινά processing batches σαν να ήταν stream)
# MAGIC > - Cost optimization (δεν κρατάει cluster up 24/7)
# MAGIC >
# MAGIC > Αντίθετο: **Trigger.Continuous** ή default (microbatch κάθε 500ms) για real-time.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col


def bronze_stream(source_name, target_table):
    """Generic Auto Loader → Bronze table function."""
    src_path = f"{streaming_root}/raw/{source_name}"
    chk_path = f"{streaming_root}/checkpoints/{source_name}"

    stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{chk_path}/_schema")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(src_path)
        .withColumn("_ingested_at", current_timestamp())
        # _metadata είναι UC-compatible (αντικαθιστά το input_file_name() που δεν υποστηρίζεται)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source", lit(source_name))
    )

    query = (
        stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", chk_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    return query


print("=== Starting 4 Bronze streaming queries (trigger=availableNow) ===\n")

queries = []
queries.append(bronze_stream("taxis",  "workspace.aade.bronze_taxis"))
queries.append(bronze_stream("mydata", "workspace.aade.bronze_mydata"))
queries.append(bronze_stream("kep",    "workspace.aade.bronze_kep"))
queries.append(bronze_stream("efka",   "workspace.aade.bronze_efka"))

# Wait για ολοκλήρωση όλων των streaming queries
for q in queries:
    q.awaitTermination()

print("\n✓ Όλα τα Bronze streams ολοκληρώθηκαν")
print("\n=== Bronze row counts ===")
for src in sources:
    cnt = spark.table(f"workspace.aade.bronze_{src}").count()
    print(f"  bronze_{src:8s}: {cnt:5d} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 3: Silver Layer — Cleansing + MERGE με Data Quality
# MAGIC
# MAGIC ### 📚 Τι είναι το «Silver layer»;
# MAGIC > Καθαρά, validated, deduplicated δεδομένα έτοιμα για analytics. Εφαρμόζουμε:
# MAGIC > - **Filter invalid rows** (π.χ. NULL ΑΦΜ, αρνητικά ποσά)
# MAGIC > - **Standardize formats** (timestamps, capitalize, trim whitespace)
# MAGIC > - **Deduplicate** (αν το ίδιο statement_id έρθει 2 φορές)
# MAGIC > - **Track quality metrics** (πόσα rows πέρασαν, πόσα έπεσαν, γιατί)
# MAGIC >
# MAGIC > Συνήθως **MERGE INTO** (upsert): αν υπάρχει το ID → update, αλλιώς insert.
# MAGIC
# MAGIC ### 📚 Τι είναι «expectations»;
# MAGIC > Data quality rules σε pipeline. Στο DLT υπάρχει `@expect_or_drop` decorator.
# MAGIC > Εδώ τις υλοποιούμε χειροκίνητα: count προβληματικές γραμμές, log them, drop ή quarantine.

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, trim, upper, to_timestamp, length, current_timestamp, lit, count
)
from delta.tables import DeltaTable

# Helper για data quality logging
dq_records = []


def log_dq(source, rule, failed_count, total_count):
    dq_records.append({
        "source": source,
        "rule": rule,
        "failed_count": failed_count,
        "total_count": total_count,
        "failure_pct": round(failed_count / max(total_count, 1) * 100, 2),
        "checked_at": datetime.utcnow(),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3α. Silver: TAXIS Declarations
# MAGIC
# MAGIC **Quality rules:**
# MAGIC - `afm` πρέπει να είναι 9 ψηφία (όχι NULL)
# MAGIC - `tax_amount >= 0`
# MAGIC - `status ∈ {Submitted, Approved, Rejected}`

# COMMAND ----------

bronze_taxis = spark.table("workspace.aade.bronze_taxis")
total_taxis = bronze_taxis.count()

# Quality checks (track only — not drop yet)
fail_null_afm = bronze_taxis.filter(col("afm").isNull()).count()
fail_invalid_amount = bronze_taxis.filter(col("tax_amount") < 0).count()
fail_invalid_afm_format = bronze_taxis.filter(
    col("afm").isNotNull() & (length(col("afm")) != 9)
).count()

log_dq("taxis", "afm_not_null", fail_null_afm, total_taxis)
log_dq("taxis", "tax_amount_non_negative", fail_invalid_amount, total_taxis)
log_dq("taxis", "afm_9_digits", fail_invalid_afm_format, total_taxis)

# Cleansing
silver_taxis = (bronze_taxis
    .filter(col("afm").isNotNull())
    .filter(length(col("afm")) == 9)
    .filter(col("tax_amount") >= 0)
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("submitted_at", to_timestamp(col("submitted_at")))
    .withColumn("_silver_at", current_timestamp())
    .dropDuplicates(["statement_id"])
)

target_table = "workspace.aade.silver_tax_declarations_clean"

# MERGE pattern: upsert by statement_id
if spark.catalog.tableExists(target_table):
    delta = DeltaTable.forName(spark, target_table)
    (delta.alias("t")
        .merge(silver_taxis.alias("s"), "t.statement_id = s.statement_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"✓ MERGE silver_tax_declarations_clean — upserted {silver_taxis.count()} rows")
else:
    silver_taxis.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"✓ CREATE silver_tax_declarations_clean — {silver_taxis.count()} rows")

print(f"\n  Quality drops: {fail_null_afm} null AFMs, {fail_invalid_afm_format} bad-format AFMs, {fail_invalid_amount} negative amounts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3β. Silver: myDATA Invoices

# COMMAND ----------

bronze_mydata = spark.table("workspace.aade.bronze_mydata")
total_mydata = bronze_mydata.count()

# Quality
fail_total_mismatch = bronze_mydata.filter(
    (col("net_amount") + col("vat_amount") - col("total_amount")).cast("decimal(18,2)") != 0
).count()
fail_negative_total = bronze_mydata.filter(col("total_amount") < 0).count()

log_dq("mydata", "total_equals_net_plus_vat", fail_total_mismatch, total_mydata)
log_dq("mydata", "total_non_negative", fail_negative_total, total_mydata)

silver_mydata = (bronze_mydata
    .filter(col("total_amount") >= 0)
    .filter(col("issuer_afm").isNotNull() & col("receiver_afm").isNotNull())
    .filter(col("issuer_afm") != col("receiver_afm"))  # cannot self-invoice
    .withColumn("transmission_status", upper(trim(col("transmission_status"))))
    .withColumn("_silver_at", current_timestamp())
    .dropDuplicates(["invoice_id"])
)

target = "workspace.aade.silver_invoices_clean"
if spark.catalog.tableExists(target):
    delta = DeltaTable.forName(spark, target)
    (delta.alias("t")
        .merge(silver_mydata.alias("s"), "t.invoice_id = s.invoice_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"✓ MERGE silver_invoices_clean — upserted {silver_mydata.count()} rows")
else:
    silver_mydata.write.format("delta").mode("overwrite").saveAsTable(target)
    print(f"✓ CREATE silver_invoices_clean — {silver_mydata.count()} rows")

print(f"  Quality issues: {fail_total_mismatch} total-amount mismatches, {fail_negative_total} negative totals")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3γ. Silver: ΚΕΠ Events

# COMMAND ----------

bronze_kep = spark.table("workspace.aade.bronze_kep")
total_kep = bronze_kep.count()

fail_duration_outlier = bronze_kep.filter(
    (col("duration_seconds") < 30) | (col("duration_seconds") > 7200)
).count()

log_dq("kep", "duration_in_reasonable_range", fail_duration_outlier, total_kep)

silver_kep = (bronze_kep
    .filter(col("afm").isNotNull())
    .filter(col("duration_seconds") >= 30)
    .filter(col("duration_seconds") <= 7200)
    .withColumn("event_ts", to_timestamp(col("event_ts")))
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("_silver_at", current_timestamp())
    .dropDuplicates(["event_id"])
)

target = "workspace.aade.silver_kep_events_clean"
if spark.catalog.tableExists(target):
    delta = DeltaTable.forName(spark, target)
    (delta.alias("t")
        .merge(silver_kep.alias("s"), "t.event_id = s.event_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"✓ MERGE silver_kep_events_clean — upserted {silver_kep.count()} rows")
else:
    silver_kep.write.format("delta").mode("overwrite").saveAsTable(target)
    print(f"✓ CREATE silver_kep_events_clean — {silver_kep.count()} rows")

print(f"  Quality issues: {fail_duration_outlier} duration outliers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3δ. Silver: e-ΕΦΚΑ Contributions

# COMMAND ----------

bronze_efka = spark.table("workspace.aade.bronze_efka")
total_efka = bronze_efka.count()

fail_zero_income = bronze_efka.filter(col("gross_income") <= 0).count()
fail_high_contribution_ratio = bronze_efka.filter(
    col("contribution_amount") / col("gross_income") > 0.5
).count()

log_dq("efka", "income_positive", fail_zero_income, total_efka)
log_dq("efka", "contribution_ratio_under_50pct", fail_high_contribution_ratio, total_efka)

silver_efka = (bronze_efka
    .filter(col("gross_income") > 0)
    .withColumn("category", upper(trim(col("category"))))
    .withColumn("payment_status", upper(trim(col("payment_status"))))
    .withColumn("_silver_at", current_timestamp())
    .dropDuplicates(["contribution_id"])
)

target = "workspace.aade.silver_efka_contributions_clean"
if spark.catalog.tableExists(target):
    delta = DeltaTable.forName(spark, target)
    (delta.alias("t")
        .merge(silver_efka.alias("s"), "t.contribution_id = s.contribution_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"✓ MERGE silver_efka_contributions_clean — upserted {silver_efka.count()} rows")
else:
    silver_efka.write.format("delta").mode("overwrite").saveAsTable(target)
    print(f"✓ CREATE silver_efka_contributions_clean — {silver_efka.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 4: Gold Layer — Analytical Aggregations
# MAGIC
# MAGIC ### 📚 Τι είναι το «Gold layer»;
# MAGIC > Business-ready πίνακες που τροφοδοτούν dashboards, reports, ML features.
# MAGIC > **Star schema** ή **flat denormalized tables** — όχι πρωτογενή δεδομένα.
# MAGIC > Συνήθως αγγίζουν JOIN πολλαπλών silver tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4α. Gold: Citizen 360 (one row per ΑΦΜ)
# MAGIC
# MAGIC Κάθε φορολογούμενος συγκεντρωτικά: σύνολο φόρων, τιμολογίων, ΚΕΠ events, ΕΦΚΑ.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.aade.gold_citizen_360 AS
# MAGIC WITH tax_agg AS (
# MAGIC   SELECT afm,
# MAGIC          COUNT(*)                       AS total_declarations,
# MAGIC          SUM(tax_amount)                AS total_tax_paid,
# MAGIC          AVG(tax_amount)                AS avg_tax_per_declaration,
# MAGIC          SUM(CASE WHEN status='APPROVED' THEN 1 ELSE 0 END) AS approved_count,
# MAGIC          SUM(CASE WHEN status='REJECTED' THEN 1 ELSE 0 END) AS rejected_count
# MAGIC   FROM workspace.aade.silver_tax_declarations_clean
# MAGIC   GROUP BY afm
# MAGIC ),
# MAGIC invoice_agg AS (
# MAGIC   SELECT issuer_afm AS afm,
# MAGIC          COUNT(*)                       AS invoices_issued,
# MAGIC          SUM(total_amount)              AS total_invoiced
# MAGIC   FROM workspace.aade.silver_invoices_clean
# MAGIC   GROUP BY issuer_afm
# MAGIC ),
# MAGIC kep_agg AS (
# MAGIC   SELECT afm,
# MAGIC          COUNT(*)                       AS kep_events_count,
# MAGIC          AVG(duration_seconds)          AS avg_kep_duration_sec
# MAGIC   FROM workspace.aade.silver_kep_events_clean
# MAGIC   GROUP BY afm
# MAGIC ),
# MAGIC efka_agg AS (
# MAGIC   SELECT afm,
# MAGIC          SUM(contribution_amount)       AS total_efka_contributions,
# MAGIC          MAX(category)                  AS efka_category
# MAGIC   FROM workspace.aade.silver_efka_contributions_clean
# MAGIC   GROUP BY afm
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(t.afm, i.afm, k.afm, e.afm) AS afm,
# MAGIC   COALESCE(t.total_declarations, 0)    AS total_declarations,
# MAGIC   COALESCE(t.total_tax_paid, 0)        AS total_tax_paid,
# MAGIC   COALESCE(t.approved_count, 0)        AS approved_count,
# MAGIC   COALESCE(t.rejected_count, 0)        AS rejected_count,
# MAGIC   COALESCE(i.invoices_issued, 0)       AS invoices_issued,
# MAGIC   COALESCE(i.total_invoiced, 0)        AS total_invoiced,
# MAGIC   COALESCE(k.kep_events_count, 0)      AS kep_events_count,
# MAGIC   ROUND(COALESCE(k.avg_kep_duration_sec, 0), 1) AS avg_kep_duration_sec,
# MAGIC   COALESCE(e.total_efka_contributions, 0) AS total_efka_contributions,
# MAGIC   COALESCE(e.efka_category, 'Unknown') AS efka_category,
# MAGIC   current_timestamp()                   AS computed_at
# MAGIC FROM tax_agg t
# MAGIC FULL OUTER JOIN invoice_agg i ON t.afm = i.afm
# MAGIC FULL OUTER JOIN kep_agg     k ON COALESCE(t.afm, i.afm) = k.afm
# MAGIC FULL OUTER JOIN efka_agg    e ON COALESCE(t.afm, i.afm, k.afm) = e.afm

# COMMAND ----------

print("=== gold_citizen_360 — Top 10 ΑΦΜ by total_tax_paid ===")
spark.sql("""
    SELECT afm, total_declarations, total_tax_paid, invoices_issued,
           kep_events_count, total_efka_contributions
    FROM workspace.aade.gold_citizen_360
    ORDER BY total_tax_paid DESC
    LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4β. Gold: Daily KPIs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.aade.gold_daily_kpis AS
# MAGIC WITH tax_daily AS (
# MAGIC   SELECT date(submitted_at) AS day,
# MAGIC          COUNT(*) AS declarations,
# MAGIC          SUM(tax_amount) AS tax_collected
# MAGIC   FROM workspace.aade.silver_tax_declarations_clean
# MAGIC   GROUP BY date(submitted_at)
# MAGIC ),
# MAGIC kep_daily AS (
# MAGIC   SELECT date(event_ts) AS day,
# MAGIC          COUNT(*) AS kep_events,
# MAGIC          AVG(duration_seconds) AS avg_duration
# MAGIC   FROM workspace.aade.silver_kep_events_clean
# MAGIC   GROUP BY date(event_ts)
# MAGIC ),
# MAGIC invoice_daily AS (
# MAGIC   SELECT invoice_date AS day,
# MAGIC          COUNT(*) AS invoices,
# MAGIC          SUM(total_amount) AS gmv
# MAGIC   FROM workspace.aade.silver_invoices_clean
# MAGIC   GROUP BY invoice_date
# MAGIC )
# MAGIC SELECT
# MAGIC   COALESCE(t.day, k.day, i.day) AS day,
# MAGIC   COALESCE(t.declarations, 0)   AS declarations,
# MAGIC   COALESCE(t.tax_collected, 0)  AS tax_collected,
# MAGIC   COALESCE(k.kep_events, 0)     AS kep_events,
# MAGIC   ROUND(COALESCE(k.avg_duration, 0), 1) AS avg_kep_duration_sec,
# MAGIC   COALESCE(i.invoices, 0)       AS invoices,
# MAGIC   COALESCE(i.gmv, 0)            AS gross_invoice_value
# MAGIC FROM tax_daily t
# MAGIC FULL OUTER JOIN kep_daily      k ON t.day = k.day
# MAGIC FULL OUTER JOIN invoice_daily  i ON COALESCE(t.day, k.day) = i.day
# MAGIC ORDER BY day DESC

# COMMAND ----------

print("=== gold_daily_kpis ===")
spark.table("workspace.aade.gold_daily_kpis").show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4γ. Gold: Audit Trail (lineage + processing metrics)

# COMMAND ----------

audit_records = []
for src in sources:
    bronze_count = spark.table(f"workspace.aade.bronze_{src}").count()
    silver_table = {
        "taxis": "silver_tax_declarations_clean",
        "mydata": "silver_invoices_clean",
        "kep": "silver_kep_events_clean",
        "efka": "silver_efka_contributions_clean",
    }[src]
    silver_count = spark.table(f"workspace.aade.{silver_table}").count()
    audit_records.append({
        "source": src,
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "drop_rate_pct": round((bronze_count - silver_count) / max(bronze_count, 1) * 100, 2),
        "processed_at": datetime.utcnow(),
        "pipeline_run_id": batch_ts,
    })

audit_df = spark.createDataFrame(pd.DataFrame(audit_records))
audit_df.write.format("delta").mode("append").saveAsTable("workspace.aade.gold_audit_trail")

print("=== Pipeline Audit Trail ===")
spark.sql("""
    SELECT pipeline_run_id, source, bronze_count, silver_count, drop_rate_pct
    FROM workspace.aade.gold_audit_trail
    ORDER BY processed_at DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4δ. Gold: Data Quality Summary

# COMMAND ----------

dq_df = spark.createDataFrame(pd.DataFrame(dq_records))
dq_df.write.format("delta").mode("append").saveAsTable("workspace.aade.gold_data_quality_summary")

print("=== Data Quality Summary ===")
spark.sql("""
    SELECT source, rule, failed_count, total_count, failure_pct
    FROM workspace.aade.gold_data_quality_summary
    ORDER BY checked_at DESC, source
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 5: Incremental Run (Batch 2)
# MAGIC
# MAGIC Γράφουμε **νέα δεδομένα** στα ίδια folders. Το Auto Loader **θα τα πιάσει
# MAGIC αυτόματα** και θα τα προσθέσει incrementally. Δεν χρειάζεται να ξανακάνουμε
# MAGIC nothing — το checkpoint ξέρει τι έχει ήδη επεξεργαστεί.

# COMMAND ----------

# Generate Batch 2
batch_id = 2
batch_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
np.random.seed(99)

# Mini batches
taxis_b2 = pd.DataFrame([{
    "statement_id": f"TX{batch_id:02d}{i:04d}",
    "afm": np.random.choice(afms),
    "fiscal_year": 2025,
    "tax_category": np.random.choice(["IncomeTax", "VAT", "PropertyTax"]),
    "tax_base": round(np.random.uniform(5000, 80000), 2),
    "tax_amount": round(np.random.uniform(800, 18000), 2),
    "status": np.random.choice(["Submitted", "Approved", "Rejected"], p=[0.5, 0.4, 0.1]),
    "submitted_at": datetime.utcnow().isoformat(),
} for i in range(25)])
write_source("taxis", taxis_b2)

mydata_b2 = pd.DataFrame([{
    "invoice_id": f"INV{batch_id:02d}{i:04d}",
    "issuer_afm": np.random.choice(afms),
    "receiver_afm": np.random.choice(afms),
    "invoice_date": datetime.utcnow().date().isoformat(),
    "net_amount": round(np.random.uniform(50, 5000), 2),
    "vat_amount": round(np.random.uniform(12, 1200), 2),
    "total_amount": round(np.random.uniform(62, 6200), 2),
    "transmission_status": np.random.choice(["Accepted", "Rejected", "Pending"], p=[0.85, 0.05, 0.10]),
} for i in range(40)])
write_source("mydata", mydata_b2)

print(f"\n✓ Batch 2 written")

# Re-run the bronze streams (incremental)
print("\n=== Re-running Bronze streams (incremental) ===")
queries2 = []
queries2.append(bronze_stream("taxis",  "workspace.aade.bronze_taxis"))
queries2.append(bronze_stream("mydata", "workspace.aade.bronze_mydata"))
queries2.append(bronze_stream("kep",    "workspace.aade.bronze_kep"))
queries2.append(bronze_stream("efka",   "workspace.aade.bronze_efka"))
for q in queries2:
    q.awaitTermination()

print("\n=== Bronze counts μετά το Batch 2 ===")
for src in sources:
    cnt = spark.table(f"workspace.aade.bronze_{src}").count()
    print(f"  bronze_{src:8s}: {cnt:5d} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 6: Pipeline Health Dashboard

# COMMAND ----------

import matplotlib.pyplot as plt

# Pipeline DAG-style summary
print("=" * 70)
print("                  AADE STREAMING PIPELINE — RUN SUMMARY")
print("=" * 70)
for src in sources:
    bronze = spark.table(f"workspace.aade.bronze_{src}").count()
    silver_table = {
        "taxis": "silver_tax_declarations_clean",
        "mydata": "silver_invoices_clean",
        "kep": "silver_kep_events_clean",
        "efka": "silver_efka_contributions_clean",
    }[src]
    silver = spark.table(f"workspace.aade.{silver_table}").count()
    drop_pct = (bronze - silver) / max(bronze, 1) * 100
    bar = "█" * int(silver / max(bronze, 1) * 30)
    print(f"  {src:8s}  bronze: {bronze:5d}  →  silver: {silver:5d}  ({100-drop_pct:5.1f}% pass)  {bar}")
print("=" * 70)
print(f"  gold_citizen_360         : {spark.table('workspace.aade.gold_citizen_360').count()} ΑΦΜ")
print(f"  gold_daily_kpis          : {spark.table('workspace.aade.gold_daily_kpis').count()} days")
print(f"  gold_audit_trail         : {spark.table('workspace.aade.gold_audit_trail').count()} runs")
print(f"  gold_data_quality_summary: {spark.table('workspace.aade.gold_data_quality_summary').count()} rules")
print("=" * 70)

# COMMAND ----------

# Visualize bronze→silver retention per source
audit_pdf = spark.table("workspace.aade.gold_audit_trail").toPandas()
audit_pdf = audit_pdf.sort_values("processed_at")

fig, ax = plt.subplots(figsize=(12, 5))
agg = audit_pdf.groupby("source").agg(bronze=("bronze_count", "sum"),
                                       silver=("silver_count", "sum")).reset_index()
x = range(len(agg))
ax.bar([i - 0.2 for i in x], agg["bronze"], width=0.4, label="Bronze", color="#CD7F32")
ax.bar([i + 0.2 for i in x], agg["silver"], width=0.4, label="Silver", color="#C0C0C0")
ax.set_xticks(list(x))
ax.set_xticklabels(agg["source"])
ax.set_ylabel("Row count")
ax.set_title("Bronze vs Silver row counts per source (cumulative)")
ax.legend()
for i, row in agg.iterrows():
    ax.text(i - 0.2, row["bronze"] + 2, str(row["bronze"]), ha="center", fontsize=9)
    ax.text(i + 0.2, row["silver"] + 2, str(row["silver"]), ha="center", fontsize=9)
plt.tight_layout()
plt.show()

# COMMAND ----------

# Quality issues breakdown
dq_pdf = spark.table("workspace.aade.gold_data_quality_summary").toPandas()
fig, ax = plt.subplots(figsize=(12, 4.5))
sources_dq = dq_pdf["source"].unique()
colors = ["#4C72B0", "#55A868", "#C44E52", "#8172B2"]
y_pos = 0
labels = []
for src, color in zip(sources_dq, colors):
    sub = dq_pdf[dq_pdf["source"] == src]
    for _, row in sub.iterrows():
        ax.barh(y_pos, row["failed_count"], color=color, alpha=0.85)
        labels.append(f"[{src}] {row['rule']}")
        ax.text(row["failed_count"] + 0.3, y_pos, f"{row['failed_count']} ({row['failure_pct']}%)",
                va="center", fontsize=9)
        y_pos += 1
ax.set_yticks(range(len(labels)))
ax.set_yticklabels(labels, fontsize=9)
ax.set_xlabel("Failed rows")
ax.set_title("Data Quality Failures by Rule")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση
# MAGIC
# MAGIC ### 🎓 Τι παρήγαγε το pipeline
# MAGIC
# MAGIC | Layer | Tables | Type |
# MAGIC |---|---|---|
# MAGIC | 🥉 Bronze | `bronze_taxis`, `bronze_mydata`, `bronze_kep`, `bronze_efka` | Streaming (Auto Loader) |
# MAGIC | 🥈 Silver | `silver_tax_declarations_clean`, `silver_invoices_clean`, `silver_kep_events_clean`, `silver_efka_contributions_clean` | Cleansed + MERGE |
# MAGIC | 🥇 Gold | `gold_citizen_360`, `gold_daily_kpis`, `gold_audit_trail`, `gold_data_quality_summary` | Aggregated + Analytical |
# MAGIC
# MAGIC ### 🧠 Patterns που είδατε
# MAGIC - **Auto Loader (cloudFiles)** για auto-detection νέων αρχείων
# MAGIC - **Trigger.AvailableNow** για run-and-stop streaming στο notebook
# MAGIC - **Schema inference + evolution** μέσω `cloudFiles.schemaLocation`
# MAGIC - **Checkpointing** για exactly-once + resumability
# MAGIC - **MERGE INTO** pattern για upsert σε Silver (Delta Lake)
# MAGIC - **Data Quality expectations** με quarantine/log αντί για fail
# MAGIC - **Medallion architecture** (Bronze → Silver → Gold)
# MAGIC - **Audit trail** με run_id και per-source row counts
# MAGIC - **Incremental processing** — Batch 2 «πέρασε» μόνο τα νέα rows
# MAGIC
# MAGIC ### 📖 Glossary
# MAGIC | Όρος | Σημασία |
# MAGIC |---|---|
# MAGIC | **Streaming pipeline** | Continuous data processing — νέα δεδομένα → άμεση επεξεργασία |
# MAGIC | **Auto Loader** | Databricks streaming source για files (`cloudFiles` format) |
# MAGIC | **Checkpoint** | Persisted state που επιτρέπει στο stream να ξεκινά από εκεί που σταμάτησε |
# MAGIC | **Trigger.AvailableNow** | «Process current data and exit» — για scheduled jobs |
# MAGIC | **Schema evolution** | Νέες στήλες προστίθενται αυτόματα στο target table |
# MAGIC | **MERGE INTO** | SQL upsert: WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT |
# MAGIC | **Medallion** | Bronze (raw) → Silver (clean) → Gold (analytical) pattern |
# MAGIC | **Data quality expectation** | Rule που validates rows (drop / quarantine / log) |
# MAGIC | **Audit trail** | Πίνακας με ιστορικό runs + lineage |
# MAGIC | **Citizen 360** | Συγκεντρωτική όψη φορολογούμενου από όλες τις πηγές |
# MAGIC
# MAGIC ### 🔄 Επόμενα βήματα (production)
# MAGIC - Μετάβαση σε **Delta Live Tables (DLT)** για declarative pipelines + auto DAG view
# MAGIC - Replace mock generator με **real connectors** (TAXIS Database CDC, myDATA API, ΚΕΠ Kafka)
# MAGIC - **continuous trigger** αντί για availableNow για 24/7 streaming
# MAGIC - **Liquid Clustering** για auto-optimize file layout
# MAGIC - **Column-level Lineage** (auto-tracked στο UC)
# MAGIC - **Workflow** με dependencies: bronze → silver → gold ως sequential tasks
# MAGIC - **Alerts** όταν `failure_pct > X%` σε quality summary
# MAGIC
# MAGIC > **🎯 Take-home**: «Ένα pipeline δεν είναι κώδικας. Είναι **συμβόλαιο** με
# MAGIC > τους downstream consumers — ποιο data, σε τι ποιότητα, με τι freshness, με τι SLA.»
