# Databricks notebook source
# MAGIC %md
# MAGIC # 🏛️ AADE DLT Pipeline — Declarative Bronze→Silver→Gold
# MAGIC
# MAGIC # ⛔ ΜΗΝ ΠΑΤΗΣΕΙΣ "RUN ALL" ΕΔΩ! ⛔
# MAGIC
# MAGIC ## 🚨 Αυτό το notebook **ΔΕΝ τρέχεται σαν κανονικό notebook**
# MAGIC
# MAGIC Το `import dlt` δουλεύει **μόνο** μέσα σε DLT runtime — όχι σε regular cluster.
# MAGIC Αν πατήσεις Run, θα δεις: `ModuleNotFoundError: No module named 'dlt'`
# MAGIC
# MAGIC ### ✅ Σωστή χρήση
# MAGIC
# MAGIC 1. Sidebar → **Workflows** → tab **Delta Live Tables** → **Create pipeline**
# MAGIC 2. Στο form:
# MAGIC    - **Pipeline name**: `aade_streaming_pipeline`
# MAGIC    - **Pipeline mode**: Triggered
# MAGIC    - **Source code**: Add → Notebook → επίλεξε **αυτό** το notebook
# MAGIC    - **Catalog**: `workspace`
# MAGIC    - **Target schema**: `aade`
# MAGIC 3. **Create** → **Start**
# MAGIC
# MAGIC Πριν τρέξεις το pipeline: τρέξε πρώτα το `AADE_DLT_Generator` notebook
# MAGIC (αυτό τρέχει κανονικά, σε regular cluster) για να δημιουργηθούν CSV files
# MAGIC στο volume που θα διαβάσει το DLT pipeline.
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day6/AADE_DLT_Pipeline.py
# MAGIC > ```
# MAGIC
# MAGIC ## 📚 Τι είναι DLT (Delta Live Tables);
# MAGIC > Declarative framework για data pipelines στο Databricks. Αντί για imperative
# MAGIC > `readStream/writeStream`, ορίζεις **τι** θέλεις (όχι **πώς**) και το DLT:
# MAGIC > - Φτιάχνει αυτόματα **DAG** από τις dependencies
# MAGIC > - Διαχειρίζεται streaming + materialized views
# MAGIC > - Εφαρμόζει **expectations** (data quality rules)
# MAGIC > - Δείχνει **visual lineage** + per-table metrics στο UI
# MAGIC > - Auto-retry, auto-checkpoint
# MAGIC
# MAGIC ## 🏗️ Pipeline DAG (όπως θα το δεις στο UI)
# MAGIC ```
# MAGIC  taxis CSV ──▶ bronze_taxis  ──▶ silver_tax_declarations_clean ──┐
# MAGIC  mydata CSV ─▶ bronze_mydata ──▶ silver_invoices_clean ──────────┤
# MAGIC  kep CSV ────▶ bronze_kep    ──▶ silver_kep_events_clean ────────┼─▶ gold_citizen_360
# MAGIC  efka CSV ───▶ bronze_efka   ──▶ silver_efka_contributions_clean ┘  gold_daily_kpis
# MAGIC                                                                      gold_data_quality
# MAGIC ```

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, lit, upper, trim, length, to_timestamp,
    sum as F_sum, avg as F_avg, count as F_count, max as F_max, when,
    date_format, coalesce, round as F_round
)

VOLUME_ROOT = "/Volumes/workspace/aade/aade_data/streaming/raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 Bronze Layer — Streaming ingestion με Auto Loader
# MAGIC
# MAGIC Κάθε `@dlt.table` με `spark.readStream` + `cloudFiles` = **streaming table**.
# MAGIC Το DLT ορίζει αυτόματα checkpoint location, schema location, και exactly-once.

# COMMAND ----------

@dlt.table(
    name="bronze_taxis",
    comment="Raw TAXIS declarations — append-only streaming table",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.managed": "true"},
)
def bronze_taxis():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{VOLUME_ROOT}/taxis")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source", lit("taxis"))
    )


@dlt.table(
    name="bronze_mydata",
    comment="Raw myDATA invoices — append-only streaming table",
    table_properties={"quality": "bronze"},
)
def bronze_mydata():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{VOLUME_ROOT}/mydata")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source", lit("mydata"))
    )


@dlt.table(
    name="bronze_kep",
    comment="Raw ΚΕΠ events — append-only streaming table",
    table_properties={"quality": "bronze"},
)
def bronze_kep():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{VOLUME_ROOT}/kep")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source", lit("kep"))
    )


@dlt.table(
    name="bronze_efka",
    comment="Raw e-ΕΦΚΑ contributions — append-only streaming table",
    table_properties={"quality": "bronze"},
)
def bronze_efka():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(f"{VOLUME_ROOT}/efka")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_source", lit("efka"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Silver Layer — Cleansed με `@dlt.expect` rules
# MAGIC
# MAGIC ### 📚 Τι είναι «expectations»;
# MAGIC > Data quality rules σε declarative form:
# MAGIC > - `@dlt.expect("name", "condition")` — log violation
# MAGIC > - `@dlt.expect_or_drop("name", "condition")` — **drop** violating rows
# MAGIC > - `@dlt.expect_or_fail("name", "condition")` — **fail** το pipeline
# MAGIC >
# MAGIC > Στο DLT UI εμφανίζεται tab **"Data Quality"** με per-rule metrics:
# MAGIC > πόσα rows passed, dropped, ή failed.

# COMMAND ----------

@dlt.table(
    name="silver_tax_declarations_clean",
    comment="Validated, deduplicated TAXIS declarations",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_afm_not_null", "afm IS NOT NULL")
@dlt.expect_or_drop("valid_afm_format", "length(afm) = 9")
@dlt.expect_or_drop("non_negative_amount", "tax_amount >= 0")
@dlt.expect("valid_status", "status IN ('SUBMITTED', 'APPROVED', 'REJECTED')")
def silver_tax_declarations_clean():
    return (
        dlt.read_stream("bronze_taxis")
        .withColumn("status", upper(trim(col("status"))))
        .withColumn("submitted_at", to_timestamp(col("submitted_at")))
        .withColumn("_silver_at", current_timestamp())
    )


@dlt.table(
    name="silver_invoices_clean",
    comment="Validated myDATA invoices με total integrity check",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("non_negative_total", "total_amount >= 0")
@dlt.expect_or_drop("issuer_not_null", "issuer_afm IS NOT NULL")
@dlt.expect_or_drop("receiver_not_null", "receiver_afm IS NOT NULL")
@dlt.expect_or_drop("no_self_invoice", "issuer_afm != receiver_afm")
@dlt.expect("total_equals_net_plus_vat", "abs((net_amount + vat_amount) - total_amount) < 0.01")
def silver_invoices_clean():
    return (
        dlt.read_stream("bronze_mydata")
        .withColumn("transmission_status", upper(trim(col("transmission_status"))))
        .withColumn("_silver_at", current_timestamp())
    )


@dlt.table(
    name="silver_kep_events_clean",
    comment="Validated ΚΕΠ events με duration sanity",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("afm_not_null", "afm IS NOT NULL")
@dlt.expect_or_drop("duration_min", "duration_seconds >= 30")
@dlt.expect_or_drop("duration_max", "duration_seconds <= 7200")
@dlt.expect("valid_status", "status IN ('COMPLETED', 'INPROGRESS', 'FAILED')")
def silver_kep_events_clean():
    return (
        dlt.read_stream("bronze_kep")
        .withColumn("event_ts", to_timestamp(col("event_ts")))
        .withColumn("status", upper(trim(col("status"))))
        .withColumn("_silver_at", current_timestamp())
    )


@dlt.table(
    name="silver_efka_contributions_clean",
    comment="Validated e-ΕΦΚΑ contributions",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("positive_income", "gross_income > 0")
@dlt.expect_or_drop("non_negative_contribution", "contribution_amount >= 0")
@dlt.expect("contribution_ratio_sane", "contribution_amount / gross_income <= 0.5")
def silver_efka_contributions_clean():
    return (
        dlt.read_stream("bronze_efka")
        .withColumn("category", upper(trim(col("category"))))
        .withColumn("payment_status", upper(trim(col("payment_status"))))
        .withColumn("_silver_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Gold Layer — Materialized Views (analytical aggregations)
# MAGIC
# MAGIC ### 📚 Streaming table vs Materialized view
# MAGIC > - **Streaming table** (`dlt.read_stream`): processes **νέα δεδομένα μόνο**, append-only
# MAGIC > - **Materialized view** (`dlt.read`): **recomputed** σε κάθε run, full snapshot
# MAGIC >
# MAGIC > Gold layer είναι συνήθως materialized views: aggregations που χρειάζονται
# MAGIC > **όλο** το dataset (όχι incremental).

# COMMAND ----------

@dlt.table(
    name="gold_citizen_360",
    comment="Per-ΑΦΜ 360-degree view: φόροι + τιμολόγια + ΚΕΠ + ΕΦΚΑ",
    table_properties={"quality": "gold"},
)
def gold_citizen_360():
    tax = (dlt.read("silver_tax_declarations_clean")
           .groupBy("afm")
           .agg(
               F_count("*").alias("total_declarations"),
               F_sum("tax_amount").alias("total_tax_paid"),
               F_avg("tax_amount").alias("avg_tax_per_declaration"),
               F_sum(when(col("status") == "APPROVED", 1).otherwise(0)).alias("approved_count"),
               F_sum(when(col("status") == "REJECTED", 1).otherwise(0)).alias("rejected_count"),
           ))

    invoices = (dlt.read("silver_invoices_clean")
                .groupBy(col("issuer_afm").alias("afm"))
                .agg(
                    F_count("*").alias("invoices_issued"),
                    F_sum("total_amount").alias("total_invoiced"),
                ))

    kep = (dlt.read("silver_kep_events_clean")
           .groupBy("afm")
           .agg(
               F_count("*").alias("kep_events_count"),
               F_avg("duration_seconds").alias("avg_kep_duration_sec"),
           ))

    efka = (dlt.read("silver_efka_contributions_clean")
            .groupBy("afm")
            .agg(
                F_sum("contribution_amount").alias("total_efka_contributions"),
                F_max("category").alias("efka_category"),
            ))

    return (tax.alias("t")
            .join(invoices.alias("i"), "afm", "fullouter")
            .join(kep.alias("k"), "afm", "fullouter")
            .join(efka.alias("e"), "afm", "fullouter")
            .select(
                col("afm"),
                coalesce(col("total_declarations"), lit(0)).alias("total_declarations"),
                F_round(coalesce(col("total_tax_paid"), lit(0)), 2).alias("total_tax_paid"),
                coalesce(col("approved_count"), lit(0)).alias("approved_count"),
                coalesce(col("rejected_count"), lit(0)).alias("rejected_count"),
                coalesce(col("invoices_issued"), lit(0)).alias("invoices_issued"),
                F_round(coalesce(col("total_invoiced"), lit(0)), 2).alias("total_invoiced"),
                coalesce(col("kep_events_count"), lit(0)).alias("kep_events_count"),
                F_round(coalesce(col("avg_kep_duration_sec"), lit(0)), 1).alias("avg_kep_duration_sec"),
                F_round(coalesce(col("total_efka_contributions"), lit(0)), 2).alias("total_efka_contributions"),
                coalesce(col("efka_category"), lit("Unknown")).alias("efka_category"),
                current_timestamp().alias("computed_at"),
            ))


@dlt.table(
    name="gold_daily_kpis",
    comment="Daily KPIs across all sources",
    table_properties={"quality": "gold"},
)
def gold_daily_kpis():
    tax_daily = (dlt.read("silver_tax_declarations_clean")
                 .withColumn("day", date_format(col("submitted_at"), "yyyy-MM-dd"))
                 .groupBy("day")
                 .agg(
                     F_count("*").alias("declarations"),
                     F_sum("tax_amount").alias("tax_collected"),
                 ))

    kep_daily = (dlt.read("silver_kep_events_clean")
                 .withColumn("day", date_format(col("event_ts"), "yyyy-MM-dd"))
                 .groupBy("day")
                 .agg(
                     F_count("*").alias("kep_events"),
                     F_avg("duration_seconds").alias("avg_duration"),
                 ))

    invoice_daily = (dlt.read("silver_invoices_clean")
                     .groupBy(col("invoice_date").alias("day"))
                     .agg(
                         F_count("*").alias("invoices"),
                         F_sum("total_amount").alias("gmv"),
                     ))

    return (tax_daily.alias("t")
            .join(kep_daily.alias("k"), "day", "fullouter")
            .join(invoice_daily.alias("i"), "day", "fullouter")
            .select(
                col("day"),
                coalesce(col("declarations"), lit(0)).alias("declarations"),
                F_round(coalesce(col("tax_collected"), lit(0)), 2).alias("tax_collected"),
                coalesce(col("kep_events"), lit(0)).alias("kep_events"),
                F_round(coalesce(col("avg_duration"), lit(0)), 1).alias("avg_kep_duration_sec"),
                coalesce(col("invoices"), lit(0)).alias("invoices"),
                F_round(coalesce(col("gmv"), lit(0)), 2).alias("gross_invoice_value"),
            ))


@dlt.table(
    name="gold_pipeline_health",
    comment="Pipeline-wide row counts per layer + drop rates",
    table_properties={"quality": "gold"},
)
def gold_pipeline_health():
    rows = []
    for layer in ["bronze_taxis", "silver_tax_declarations_clean",
                  "bronze_mydata", "silver_invoices_clean",
                  "bronze_kep", "silver_kep_events_clean",
                  "bronze_efka", "silver_efka_contributions_clean"]:
        df = dlt.read(layer)
        cnt = df.count()
        layer_type = "bronze" if layer.startswith("bronze") else "silver"
        source = layer.replace("bronze_", "").replace("silver_", "").replace("_clean", "")\
            .replace("tax_declarations", "taxis").replace("invoices", "mydata")\
            .replace("kep_events", "kep").replace("efka_contributions", "efka")
        rows.append((source, layer_type, layer, cnt))
    return spark.createDataFrame(rows, "source string, layer string, table_name string, row_count long") \
        .withColumn("computed_at", current_timestamp())
