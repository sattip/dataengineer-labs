# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 LAB 2 — Bronze Ingestion — ΛΥΣΗ (TRAINER ONLY)
# MAGIC
# MAGIC > ⚠️ **TRAINER REFERENCE — Μην το δείξεις στους students.**
# MAGIC > Όλα τα steps + reference example + stretches λυμένα + trainer notes.

# COMMAND ----------

# DBTITLE 1,Auto-bootstrap (έτοιμο)
CATALOG       = "gt_lab"
LANDING_PATH  = f"/Volumes/{CATALOG}/bronze/landing"
GITHUB_BASE   = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day6/data_for_students"
REQUIRED_CSVS = ["citizen_registry.csv", "taxis_declarations.csv",
                 "efka_contributions.csv", "kep_events.csv", "mydata_invoices.csv"]

def _bootstrap():
    """Serverless-safe: writes directly to /Volumes path (no /tmp/ access needed)."""
    import urllib.request
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    for s in ["bronze", "silver", "gold"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")
    try:
        existing = {f.name for f in dbutils.fs.ls(LANDING_PATH)}
    except: existing = set()
    missing = set(REQUIRED_CSVS) - existing
    if not missing:
        print(f"✅ All {len(REQUIRED_CSVS)} CSVs already in volume")
        return
    print(f"⬇️  Downloading {len(missing)} CSVs από GitHub...")
    for f in missing:
        url = f"{GITHUB_BASE}/{f}"
        target = f"{LANDING_PATH}/{f}"
        # Direct stream from URL → volume (no /tmp/, serverless-safe)
        with urllib.request.urlopen(url, timeout=30) as response:
            content = response.read()
        with open(target, "wb") as fp:
            fp.write(content)
        print(f"   ✅ {f}")

_bootstrap()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 1 — Group Design Discussion (no code)
# MAGIC
# MAGIC Πριν κώδικα, ομάδα συμφωνεί σε:
# MAGIC 1. **Naming convention**: `bronze.{source}_{entity}_raw` (ή δικός τους — defend choice)
# MAGIC 2. **Type strategy**: Schemas per source vs all-string (trade-off discussion)
# MAGIC 3. **Audit columns**: minimum `_ingestion_ts` + `_source_file` + ίσως `_pipeline_run_id`
# MAGIC 4. **Function signature**: `def load_to_bronze(source, entity, csv_file, schema=None)`
# MAGIC 5. **Partition strategy**: `_ingestion_date` (low cardinality, time-based queries)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 2 — Reusable Loader Function

# COMMAND ----------

# DBTITLE 1,Solution: Production-grade loader
from pyspark.sql.functions import current_timestamp, input_file_name, to_date, lit
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType, DecimalType
)
import uuid

# Per-source schemas (production: αυτά συνήθως ζουν σε YAML/JSON config files)
SCHEMAS = {
    "citizen": StructType([
        StructField("afm",        StringType(),    False),
        StructField("full_name",  StringType(),    True),
        StructField("region",     StringType(),    True),
        StructField("birth_year", IntegerType(),   True),
        StructField("is_active",  BooleanType(),   True),
        StructField("updated_at", TimestampType(), True),
    ]),
    "taxis": StructType([
        StructField("statement_id",   StringType(),     False),
        StructField("afm",            StringType(),     False),
        StructField("fiscal_year",    IntegerType(),    True),
        StructField("tax_category",   StringType(),     True),
        StructField("tax_base",       DecimalType(18,2), True),
        StructField("tax_amount",     DecimalType(18,2), True),
        StructField("status",         StringType(),     True),
        StructField("submitted_at",   TimestampType(),  True),
        StructField("updated_at",     TimestampType(),  True),
    ]),
    "efka": StructType([
        StructField("contribution_id",         StringType(),     False),
        StructField("afm",                     StringType(),     False),
        StructField("period",                  StringType(),     True),
        StructField("employer_afm",            StringType(),     True),
        StructField("gross_salary",            DecimalType(18,2),True),
        StructField("employee_contribution",   DecimalType(18,2),True),
        StructField("employer_contribution",   DecimalType(18,2),True),
        StructField("status",                  StringType(),     True),
        StructField("updated_at",              TimestampType(),  True),
    ]),
    "kep": StructType([
        StructField("event_id",      StringType(),    False),
        StructField("citizen_afm",   StringType(),    True),
        StructField("service_code",  StringType(),    True),
        StructField("event_type",    StringType(),    True),
        StructField("event_ts",      TimestampType(), True),
        StructField("wait_minutes",  IntegerType(),   True),
        StructField("channel",       StringType(),    True),
        StructField("municipality",  StringType(),    True),
    ]),
    "mydata": StructType([
        StructField("invoice_id",          StringType(),     False),
        StructField("issuer_afm",          StringType(),     False),
        StructField("receiver_afm",        StringType(),     True),
        StructField("invoice_date",        TimestampType(),  True),
        StructField("net_amount",          DecimalType(18,2),True),
        StructField("vat_amount",          DecimalType(18,2),True),
        StructField("total_amount",        DecimalType(18,2),True),
        StructField("transmission_status", StringType(),     True),
        StructField("updated_at",          TimestampType(),  True),
    ]),
}

# Generate pipeline run ID (one per full ingestion cycle)
PIPELINE_RUN_ID = str(uuid.uuid4())


def load_to_bronze(source: str, entity: str, csv_filename: str):
    """
    Load CSV από landing volume σε Bronze Delta table.

    Idempotent: re-run δεν διπλασιάζει (mode='overwrite').
    Includes audit columns: _ingestion_ts, _source_file, _pipeline_run_id, _ingestion_date.
    """
    target_table = f"gt_lab.bronze.{source}_{entity}_raw"
    csv_path     = f"{LANDING_PATH}/{csv_filename}"
    schema       = SCHEMAS.get(source)

    # Read με explicit schema (όχι inferSchema!)
    reader = spark.read.option("header", "true")
    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")  # fallback

    df = reader.csv(csv_path)

    # Audit columns — production essentials
    df_audited = (df
        .withColumn("_ingestion_ts",   current_timestamp())
        .withColumn("_source_file",    input_file_name())
        .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
        .withColumn("_ingestion_date",  to_date(current_timestamp()))
    )

    # Write Bronze Delta με partition + overwrite (idempotency)
    (df_audited.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("_ingestion_date")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table))

    # Production polish — table metadata
    spark.sql(f"""
        ALTER TABLE {target_table}
        SET TBLPROPERTIES (
            'comment' = 'Bronze raw — {source} {entity}',
            'source_system' = '{source}',
            'entity' = '{entity}',
            'layer' = 'bronze',
            'last_pipeline_run' = '{PIPELINE_RUN_ID}'
        )
    """)

    n = spark.table(target_table).count()
    print(f"  ✅ {target_table}: {n} rows")
    return n


print(f"Pipeline run ID: {PIPELINE_RUN_ID}")
print("Function defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer notes σε αυτή τη solution:**
# MAGIC
# MAGIC 1. **SCHEMAS dict**: Production-pattern — schemas ως config (όχι inline). Σε real life: YAML file ή schema registry.
# MAGIC 2. **`PIPELINE_RUN_ID`**: One UUID ανά complete run = lineage gold. Φαίνεται σε όλες τις 5 tables — μπορείς να trace «αυτή η batch αν είχε bug, ποια records επηρέασε».
# MAGIC 3. **`overwriteSchema=true`**: Επιτρέπει schema evolution σε rerun. Trade-off: silently accepts breaking changes — production να βάζεις guards.
# MAGIC 4. **TBLPROPERTIES**: Custom tags (`source_system`, `layer`) επιτρέπουν catalog queries «δείξε όλα τα bronze tables από TAXIS».

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 3 — Loop για 5 πηγές

# COMMAND ----------

# DBTITLE 1,Solution: Load all 5 sources
SOURCES = [
    ("citizen", "registry",     "citizen_registry.csv"),
    ("taxis",   "declarations", "taxis_declarations.csv"),
    ("efka",    "contributions","efka_contributions.csv"),
    ("kep",     "events",       "kep_events.csv"),
    ("mydata",  "invoices",     "mydata_invoices.csv"),
]

print(f"\n🚀 Loading {len(SOURCES)} sources (pipeline_run_id={PIPELINE_RUN_ID})\n")
total_records = 0
for source, entity, csv_file in SOURCES:
    total_records += load_to_bronze(source, entity, csv_file)
print(f"\n✅ Total records ingested: {total_records}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 4 — SHOW TABLES + Counts

# COMMAND ----------

# DBTITLE 1,Solution: Verify all 5 tables
print("=== Bronze tables ===")
display(spark.sql("SHOW TABLES IN gt_lab.bronze"))

# COMMAND ----------

print("\n=== Row counts ===")
from pyspark.sql.functions import lit as l
rows = []
for source, entity, _ in SOURCES:
    table = f"gt_lab.bronze.{source}_{entity}_raw"
    n = spark.table(table).count()
    rows.append((table, n))

display(spark.createDataFrame(rows, ["table", "row_count"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 5 — Idempotency Test

# COMMAND ----------

# DBTITLE 1,Solution: Test idempotency
target_table = "gt_lab.bronze.taxis_declarations_raw"

# Count BEFORE rerun
count_before = spark.table(target_table).count()
print(f"Before: {count_before} rows")

# Re-run loader for same source
print("\nRe-running ingestion για taxis...")
load_to_bronze("taxis", "declarations", "taxis_declarations.csv")

# Count AFTER
count_after = spark.table(target_table).count()
print(f"After:  {count_after} rows")

# Assertion
assert count_before == count_after, \
    f"❌ IDEMPOTENCY BROKEN! {count_before} != {count_after}"
print(f"\n✅ IDEMPOTENT — counts equal ({count_before} == {count_after})")

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer notes:**
# MAGIC - Highlight: αν είχε γίνει `mode("append")`, εδώ θα βλέπαμε `20 == 10` → fail
# MAGIC - Tip: «idempotency είναι requirement, όχι nice-to-have»

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 6 — Inspect ένα table

# COMMAND ----------

# DBTITLE 1,Solution: Deep inspect
print("=== DESCRIBE EXTENDED ===")
display(spark.sql("DESCRIBE EXTENDED gt_lab.bronze.kep_events_raw"))

# COMMAND ----------

print("\n=== Sample records με audit columns ===")
display(spark.table("gt_lab.bronze.kep_events_raw").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✅ AUTO-VERIFICATION

# COMMAND ----------

EXPECTED = {
    "citizen_registry_raw":    9,
    "taxis_declarations_raw":  10,
    "efka_contributions_raw":  7,
    "kep_events_raw":          10,
    "mydata_invoices_raw":     10,
}

checks = []
for table, exp_count in EXPECTED.items():
    full = f"gt_lab.bronze.{table}"
    try:
        actual = spark.table(full).count()
        checks.append((f"{table}: {actual}/{exp_count}", actual == exp_count))
        cols = spark.table(full).columns
        if "_ingestion_ts" not in cols:
            checks.append((f"  ⚠️ {table}: missing _ingestion_ts", False))
        if "_source_file" not in cols:
            checks.append((f"  ⚠️ {table}: missing _source_file", False))
    except Exception as e:
        checks.append((f"{table}: ❌ NOT FOUND", False))

passed = sum(1 for _, ok in checks if ok)
print("=" * 70)
print(" ✅ LAB 2 VERIFICATION")
print("=" * 70)
for n, ok in checks:
    print(f"  {'✅' if ok else '❌'} {n}")
print("=" * 70)
print(f"  {passed}/{len(checks)} passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🚀 STRETCH SOLUTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 STRETCH 1 — Schema Evolution

# COMMAND ----------

# DBTITLE 1,Solution: Schema evolution με mergeSchema
target = "gt_lab.bronze.taxis_declarations_raw"

# Read το original CSV ξανά
df_taxis = (spark.read.schema(SCHEMAS["taxis"])
              .option("header","true")
              .csv(f"{LANDING_PATH}/taxis_declarations.csv"))

# Add artificial new column (simulating source upgrade)
df_with_new = df_taxis.withColumn("tax_year_period", lit("Q1-2026"))

# Append με mergeSchema=true → νέα στήλη προστίθεται
(df_with_new.write
    .format("delta")
    .mode("append")  # APPEND για evolution test
    .option("mergeSchema", "true")
    .saveAsTable(target))

print("✅ Schema evolved — νέα column 'tax_year_period' προστέθηκε")
print("\n=== New schema ===")
display(spark.sql(f"DESCRIBE {target}"))

# Παλιά records έχουν NULL στη νέα στήλη
print("\n=== Παλιά vs νέα records ===")
display(spark.sql(f"""
    SELECT statement_id, status, tax_year_period
    FROM {target}
    ORDER BY tax_year_period NULLS LAST
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer warning:**
# MAGIC - `mergeSchema=true` σιωπηλά accepts breaking changes!
# MAGIC - Production pattern: explicit `addNewColumns` mode + alert
# MAGIC - Bonus question: «Πώς θα ξέρεις ότι schema άλλαξε;» → δες `DESCRIBE HISTORY`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 STRETCH 2 — Auto Loader

# COMMAND ----------

# DBTITLE 1,Solution: Auto Loader pattern
checkpoint = "/Volumes/gt_lab/bronze/_checkpoints/citizen_autoloader"
schema_loc = "/Volumes/gt_lab/bronze/_schemas/citizen"

# Cleanup πρώτη φορά (idempotency για demo)
dbutils.fs.rm(checkpoint, recurse=True)
dbutils.fs.rm(schema_loc, recurse=True)

# Read stream με Auto Loader
df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_loc)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(f"{LANDING_PATH}/citizen_registry*.csv"))

# Write stream στο Bronze Delta (availableNow = run once + stop)
query = (df_stream
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable("gt_lab.bronze.citizen_registry_autoloader"))

query.awaitTermination()

n = spark.table("gt_lab.bronze.citizen_registry_autoloader").count()
print(f"✅ Auto Loader Bronze: {n} rows")

# Re-run idempotency test — Auto Loader ΞΕΡΕΙ ποια files έχει διαβάσει
print("\n🔁 Re-running Auto Loader (πρέπει να μη διπλασιάσει)...")
query2 = (df_stream
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable("gt_lab.bronze.citizen_registry_autoloader"))
query2.awaitTermination()

n2 = spark.table("gt_lab.bronze.citizen_registry_autoloader").count()
print(f"After re-run: {n2} rows")
assert n == n2, f"Idempotency broke: {n} → {n2}"
print(f"✅ Auto Loader idempotent ({n} == {n2})")

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Production talking point:**
# MAGIC - Auto Loader είναι το **production standard** για file-based ingestion
# MAGIC - Tracks state σε checkpoint → exactly-once semantics
# MAGIC - Scale: 1B+ files χωρίς memory issues
# MAGIC - Real prod: trigger `processingTime='1 minute'` αντί για `availableNow`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 STRETCH 3 — Quality Check Function

# COMMAND ----------

# DBTITLE 1,Solution: Quality gate
from pyspark.sql.functions import col, count, when

def quality_check(df, table_name, required_cols=None, null_threshold_pct=5.0):
    """Production quality gate. Raises if critical fail.

    Args:
        df: DataFrame to check
        table_name: για logging
        required_cols: list of cols που ΠΡΕΠΕΙ νο είναι non-null > threshold
        null_threshold_pct: max % nulls allowed
    """
    n_total = df.count()
    if n_total == 0:
        raise ValueError(f"❌ {table_name}: empty DataFrame")

    print(f"\n🔍 Quality check για {table_name} ({n_total} rows):")

    required_cols = required_cols or [c for c in df.columns if c in ("afm","statement_id","invoice_id","event_id","contribution_id")]

    for c in required_cols:
        if c not in df.columns:
            continue
        nulls = df.filter(col(c).isNull()).count()
        null_pct = 100.0 * nulls / n_total
        if null_pct > null_threshold_pct:
            raise ValueError(
                f"❌ {table_name}.{c}: {null_pct:.1f}% nulls "
                f"(threshold {null_threshold_pct}%)"
            )
        icon = "✅" if null_pct == 0 else "⚠️"
        print(f"  {icon} {c}: {null_pct:.2f}% nulls (limit {null_threshold_pct}%)")

    print(f"✅ {table_name}: passed quality gate")
    return df


# Apply σε όλα τα Bronze
for source, entity, _ in SOURCES:
    table = f"gt_lab.bronze.{source}_{entity}_raw"
    df_check = spark.table(table)
    quality_check(df_check, table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 SUPER STRETCH — DESCRIBE HISTORY + Time Travel

# COMMAND ----------

# DBTITLE 1,Solution: Delta time travel diff
target = "gt_lab.bronze.taxis_declarations_raw"

print("=== History (όλες οι versions) ===")
display(spark.sql(f"DESCRIBE HISTORY {target}"))

# COMMAND ----------

# Read version 0 vs current
v0 = spark.read.format("delta").option("versionAsOf", 0).table(target)
v_now = spark.table(target)

print(f"Version 0 count:   {v0.count()}")
print(f"Current count:     {v_now.count()}")

# Diff: τι προστέθηκε
new_records = v_now.exceptAll(v0)
print(f"Records νέα μετά v0: {new_records.count()}")

# Diff αν είχε deletes
deleted = v0.exceptAll(v_now)
print(f"Records που διαγράφηκαν: {deleted.count()}")

# Restore σε version 0 αν χρειαστείς
# spark.sql(f"RESTORE TABLE {target} TO VERSION AS OF 0")

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Production tip:**
# MAGIC - `DESCRIBE HISTORY` είναι το **debug gold** για data incidents
# MAGIC - GDPR audit: «ποιος έγραψε τι, πότε» — όλα εκεί
# MAGIC - `RESTORE` είναι **καταστροφική** — δοκίμασε `versionAsOf` πρώτα

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎓 TRAINER CHEAT SHEET

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pacing (110')
# MAGIC
# MAGIC | Time | Activity | Watch for |
# MAGIC |---|---|---|
# MAGIC | 0-20' | Theory reading | Groups already start discussing — encourage |
# MAGIC | 20-30' | Step 1 (design) | Push για να βγάλουν decisions σε χαρτί |
# MAGIC | 30-65' | Step 2-3 (code function + loop) | Most time goes here. Walk around. |
# MAGIC | 65-75' | Step 4-5 (verify + idempotency) | Many will skip idempotency test — push |
# MAGIC | 75-90' | Step 6 + verification cell | Group debug αν fail counts |
# MAGIC | 90-110' | 2 group presentations (10' each) | Push «explain WHY this choice» |
# MAGIC
# MAGIC ## Top common mistakes
# MAGIC
# MAGIC | Bug | How to spot | Fix |
# MAGIC |---|---|---|
# MAGIC | `mode("append")` | Idempotency test fails (counts double) | Switch to `overwrite` |
# MAGIC | `inferSchema=true` | AFM type = integer | Explicit StructType schema |
# MAGIC | Copy-paste 5 times | Code is verbose, no function | Refactor to def + loop |
# MAGIC | Forget audit cols | Verification cell complains | Add `current_timestamp()` |
# MAGIC | Forget partition | Verification passes but slow queries | Add `partitionBy("_ingestion_date")` |
# MAGIC
# MAGIC ## Group presentation prompts
# MAGIC
# MAGIC Όταν παρουσιάζουν, ρώτησε:
# MAGIC 1. **«Γιατί αυτή η partition key;»** — Test understanding
# MAGIC 2. **«Πώς θα το έκανες με 50 πηγές αντί για 5;»** — Σε stretch direction
# MAGIC 3. **«Αν αύριο TAXIS προσθέσει column, τι σπάει;»** — Schema evolution discussion
# MAGIC 4. **«Πόσο κοστίζει αυτό σε DBUs;»** — Cost awareness
# MAGIC
# MAGIC ## Bridge to Day 2
# MAGIC
# MAGIC «Σήμερα φτιάξαμε Bronze. Αύριο: Bronze → Silver με quality rules, dedup, type enforcement. Όλα τα bad data που σήμερα μπήκε στο Bronze raw, αύριο θα πάει στο quarantine ή θα καθαριστεί.»
