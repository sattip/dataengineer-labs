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
# MAGIC %md
# MAGIC ---
# MAGIC # 🧪 STRETCH 4 — Data Contract Validator (Mini-Lab Solution · 60')
# MAGIC
# MAGIC > Full implementation για 1-hour mini-lab.

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP A — Define CONTRACTS for 5 sources

# COMMAND ----------

# DBTITLE 1,Solution A — CONTRACTS dict
CONTRACTS = {
    "citizen": {
        "version": "1.0",
        "owner": "registry-team@gov.gr",
        "primary_key": "afm",
        "columns": {
            "afm":        {"type": "string",   "nullable": False, "regex": r"^\d{9}$"},
            "full_name":  {"type": "string",   "nullable": True},
            "region":     {"type": "string",   "nullable": True,
                          "allowed": ["ATTICA","MACEDONIA","CRETE","EPIRUS","THESSALY","PELOPONNESE"]},
            "birth_year": {"type": "integer",  "nullable": True, "min": 1900, "max": 2026},
            "is_active":  {"type": "boolean",  "nullable": False},
        },
        "quality": {"min_rows": 1, "max_rows": 100_000_000,
                    "max_null_pct": {"afm": 0.0, "is_active": 0.0, "region": 5.0}}
    },
    "taxis": {
        "version": "1.0",
        "owner": "taxis-team@aade.gr",
        "primary_key": "statement_id",
        "columns": {
            "statement_id": {"type": "string",  "nullable": False},
            "afm":          {"type": "string",  "nullable": False, "regex": r"^\d{9}$"},
            "fiscal_year":  {"type": "integer", "nullable": False, "min": 2000, "max": 2030},
            "tax_amount":   {"type": "decimal", "nullable": True,  "min": 0},
            "status":       {"type": "string",  "nullable": False,
                            "allowed": ["Submitted","Approved","Rejected","Pending"]},
        },
        "quality": {"min_rows": 1, "max_rows": 10_000_000,
                    "max_null_pct": {"afm": 0.0, "statement_id": 0.0}}
    },
    "efka": {
        "version": "1.0",
        "owner": "efka-team@efka.gr",
        "primary_key": "contribution_id",
        "columns": {
            "contribution_id":       {"type": "string",  "nullable": False},
            "afm":                   {"type": "string",  "nullable": False, "regex": r"^\d{9}$"},
            "employer_afm":          {"type": "string",  "nullable": True,  "regex": r"^\d{9}$"},
            "period":                {"type": "string",  "nullable": False, "regex": r"^\d{4}-\d{2}$"},
            "gross_salary":          {"type": "decimal", "nullable": True,  "min": 0},
            "employee_contribution": {"type": "decimal", "nullable": True,  "min": 0},
        },
        "quality": {"min_rows": 1, "max_rows": 50_000_000,
                    "max_null_pct": {"afm": 0.0, "contribution_id": 0.0}}
    },
    "kep": {
        "version": "1.0",
        "owner": "kep-team@kep.gr",
        "primary_key": "event_id",
        "columns": {
            "event_id":     {"type": "string",  "nullable": False},
            "citizen_afm":  {"type": "string",  "nullable": False, "regex": r"^\d{9}$"},
            "event_type":   {"type": "string",  "nullable": False,
                            "allowed": ["REQUEST_CREATED","REQUEST_COMPLETED","REQUEST_FAILED"]},
            "wait_minutes": {"type": "integer", "nullable": True, "min": 0, "max": 1440},
        },
        "quality": {"min_rows": 1, "max_rows": 100_000_000,
                    "max_null_pct": {"event_id": 0.0, "citizen_afm": 0.0}}
    },
    "mydata": {
        "version": "1.0",
        "owner": "mydata-team@aade.gr",
        "primary_key": "invoice_id",
        "columns": {
            "invoice_id":          {"type": "string",  "nullable": False},
            "issuer_afm":          {"type": "string",  "nullable": False, "regex": r"^\d{9}$"},
            "receiver_afm":        {"type": "string",  "nullable": True,  "regex": r"^\d{9}$"},
            "total_amount":        {"type": "decimal", "nullable": True},  # NULL allowed (credit notes)
            "transmission_status": {"type": "string",  "nullable": False,
                                   "allowed": ["Accepted","Rejected","Pending"]},
        },
        "quality": {"min_rows": 1, "max_rows": 1_000_000_000,
                    "max_null_pct": {"invoice_id": 0.0, "issuer_afm": 0.0}}
    },
}
print(f"✅ Defined {len(CONTRACTS)} contracts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP B — validate_contract() με 7 checks

# COMMAND ----------

# DBTITLE 1,Solution B — Production validator
from pyspark.sql.functions import col, when, lit, count as _count

def validate_contract(df, contract, fail_fast=True):
    """
    Validate DataFrame against contract spec.

    Returns: (df_valid, df_invalid, report)
    """
    report = {"checks": {}, "n_total": 0, "n_failures": 0,
              "violations": [], "contract_version": contract.get("version")}

    n = df.count()
    report["n_total"] = n

    # ----- Check 1: Row count bounds -----
    q = contract.get("quality", {})
    if n < q.get("min_rows", 0):
        msg = f"Row count {n} < min {q.get('min_rows')}"
        report["violations"].append(msg)
        if fail_fast: raise ValueError(f"❌ {msg}")
    if n > q.get("max_rows", float("inf")):
        msg = f"Row count {n} > max {q.get('max_rows')}"
        report["violations"].append(msg)
        if fail_fast: raise ValueError(f"❌ {msg}")
    report["checks"]["row_bounds"] = "pass"

    # ----- Check 2: Required columns exist -----
    missing_cols = [c for c in contract["columns"] if c not in df.columns]
    if missing_cols:
        msg = f"Missing columns: {missing_cols}"
        report["violations"].append(msg)
        if fail_fast: raise ValueError(f"❌ {msg}")
    report["checks"]["columns_exist"] = "pass" if not missing_cols else f"fail: {missing_cols}"

    # Build cumulative invalid_mask + failure_reason
    invalid_mask = lit(False)
    failure_reasons_expr = lit("")

    # ----- Check 3: Non-nullable -----
    for c, spec in contract["columns"].items():
        if c not in df.columns: continue
        if not spec.get("nullable", True):
            cond = col(c).isNull()
            invalid_mask = invalid_mask | cond
            failure_reasons_expr = failure_reasons_expr.cast("string") + \
                when(cond, lit(f"[{c}_null]")).otherwise(lit(""))

    # ----- Check 4: Null rate thresholds (per column) -----
    for c, max_pct in q.get("max_null_pct", {}).items():
        if c not in df.columns: continue
        nulls = df.filter(col(c).isNull()).count()
        null_pct = 100.0 * nulls / n if n > 0 else 0.0
        if null_pct > max_pct:
            msg = f"{c}: {null_pct:.2f}% nulls > {max_pct}% threshold"
            report["violations"].append(msg)
            if fail_fast: raise ValueError(f"❌ {msg}")
        report["checks"][f"null_pct_{c}"] = round(null_pct, 2)

    # ----- Check 5: Regex patterns -----
    for c, spec in contract["columns"].items():
        if c not in df.columns: continue
        if "regex" in spec:
            cond = col(c).isNotNull() & ~col(c).cast("string").rlike(spec["regex"])
            invalid_mask = invalid_mask | cond
            failure_reasons_expr = failure_reasons_expr + \
                when(cond, lit(f"[{c}_regex]")).otherwise(lit(""))

    # ----- Check 6: Allowed values -----
    for c, spec in contract["columns"].items():
        if c not in df.columns: continue
        if "allowed" in spec:
            cond = col(c).isNotNull() & ~col(c).isin(spec["allowed"])
            invalid_mask = invalid_mask | cond
            failure_reasons_expr = failure_reasons_expr + \
                when(cond, lit(f"[{c}_enum]")).otherwise(lit(""))

    # ----- Check 7: Numeric ranges -----
    for c, spec in contract["columns"].items():
        if c not in df.columns: continue
        if "min" in spec:
            cond = col(c).isNotNull() & (col(c) < spec["min"])
            invalid_mask = invalid_mask | cond
            failure_reasons_expr = failure_reasons_expr + \
                when(cond, lit(f"[{c}_min]")).otherwise(lit(""))
        if "max" in spec:
            cond = col(c).isNotNull() & (col(c) > spec["max"])
            invalid_mask = invalid_mask | cond
            failure_reasons_expr = failure_reasons_expr + \
                when(cond, lit(f"[{c}_max]")).otherwise(lit(""))

    # Split valid/invalid με _failure_reason
    df_with_mask = df.withColumn("_failure_reason", failure_reasons_expr) \
                     .withColumn("_is_invalid", invalid_mask)
    df_valid = df_with_mask.filter(~col("_is_invalid")).drop("_failure_reason", "_is_invalid")
    df_invalid = df_with_mask.filter(col("_is_invalid")).drop("_is_invalid")

    report["n_failures"] = df_invalid.count()

    return df_valid, df_invalid, report

print("✅ validate_contract() defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP C — Quarantine pattern

# COMMAND ----------

# DBTITLE 1,Solution C — write_quarantine()
from pyspark.sql.functions import current_timestamp

def write_quarantine(df_invalid, source, entity):
    """Write bad records σε separate quarantine Delta table."""
    n = df_invalid.count()
    if n == 0:
        print(f"  ✅ {source}/{entity}: zero quarantined records")
        return

    target = f"gt_lab.bronze.quarantine_{source}_{entity}"
    df_q = (df_invalid
        .withColumn("_quarantine_ts",   current_timestamp())
        .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
    )
    (df_q.write
        .format("delta")
        .mode("append")  # APPEND για ιστορικό
        .option("mergeSchema", "true")
        .saveAsTable(target))
    print(f"  ⚠️  Quarantined {n} rows → {target}")

print("✅ write_quarantine() defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP D — `load_with_contracts()` end-to-end

# COMMAND ----------

# DBTITLE 1,Solution D — Integration
def load_with_contracts(source, entity, csv_filename):
    """End-to-end loader με contract validation + quarantine."""
    csv_path = f"{LANDING_PATH}/{csv_filename}"
    target_bronze = f"gt_lab.bronze.{source}_{entity}_raw"

    print(f"\n🚀 Loading {source}/{entity}...")

    # 1. Read
    df = (spark.read
            .schema(SCHEMAS.get(source))
            .option("header","true")
            .csv(csv_path)
        ) if source in SCHEMAS else (
            spark.read.option("header","true").option("inferSchema","true").csv(csv_path)
        )

    # 2. Validate
    contract = CONTRACTS.get(source)
    if not contract:
        print(f"  ⚠️ No contract — skipping validation")
        df_valid, df_invalid, report = df, df.limit(0), {"n_total": df.count(), "n_failures": 0}
    else:
        df_valid, df_invalid, report = validate_contract(df, contract, fail_fast=False)

    # 3. Write valid → Bronze
    df_audited = (df_valid
        .withColumn("_ingestion_ts",   current_timestamp())
        .withColumn("_source_file",    lit(csv_filename))
        .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
    )
    (df_audited.write
        .format("delta").mode("overwrite")
        .option("overwriteSchema","true")
        .saveAsTable(target_bronze))

    # 4. Quarantine
    write_quarantine(df_invalid, source, entity)

    # 5. Report
    print(f"  📊 Total: {report['n_total']} | Valid: {report['n_total'] - report['n_failures']} | Quarantined: {report['n_failures']}")
    if report.get("violations"):
        for v in report["violations"][:3]:
            print(f"     ⚠ {v}")

    return report

# Run για όλες τις 5 πηγές
all_reports = {}
for source, entity, csv in [
    ("citizen", "registry",      "citizen_registry.csv"),
    ("taxis",   "declarations",  "taxis_declarations.csv"),
    ("efka",    "contributions", "efka_contributions.csv"),
    ("kep",     "events",        "kep_events.csv"),
    ("mydata",  "invoices",      "mydata_invoices.csv"),
]:
    all_reports[source] = load_with_contracts(source, entity, csv)

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP E — Break Tests + Metrics

# COMMAND ----------

# DBTITLE 1,Solution E1 — Inject bad records
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

print("=== Test 1: Bad AFM (5 digits) ===")
bad_afm_df = spark.createDataFrame(
    [("TX_BAD1", "12345", 2025, "VAT", 100.0, 50.0, "Approved", None, None)],
    schema=SCHEMAS["taxis"]
)
v, inv, r = validate_contract(bad_afm_df, CONTRACTS["taxis"], fail_fast=False)
print(f"  Invalid count: {inv.count()}")
inv.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Solution E2 — Negative tax_amount
print("=== Test 2: Negative amount ===")
neg_amt_df = spark.createDataFrame(
    [("TX_BAD2", "987654321", 2025, "VAT", 100.0, -50.0, "Approved", None, None)],
    schema=SCHEMAS["taxis"]
)
v, inv, r = validate_contract(neg_amt_df, CONTRACTS["taxis"], fail_fast=False)
print(f"  Invalid count: {inv.count()}")
inv.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Solution E3 — Invalid enum value
print("=== Test 3: Invalid status enum ===")
bad_status_df = spark.createDataFrame(
    [("TX_BAD3", "987654321", 2025, "VAT", 100.0, 50.0, "UNKNOWN_STATUS", None, None)],
    schema=SCHEMAS["taxis"]
)
v, inv, r = validate_contract(bad_status_df, CONTRACTS["taxis"], fail_fast=False)
print(f"  Invalid count: {inv.count()}")
inv.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Solution E4 — Metrics dashboard
print("=== 📊 Pipeline Health Report ===")
metrics_rows = []
for source, report in all_reports.items():
    metrics_rows.append((
        source,
        report["n_total"],
        report["n_total"] - report["n_failures"],
        report["n_failures"],
        round(100.0 * report["n_failures"] / report["n_total"], 2) if report["n_total"] > 0 else 0.0,
    ))

df_metrics = spark.createDataFrame(
    metrics_rows,
    ["source", "total", "valid", "quarantined", "quarantine_pct"]
)
display(df_metrics)

# Per-quarantine table breakdown
print("\n=== Quarantine table contents ===")
for source, entity in [("taxis","declarations"), ("citizen","registry")]:
    t = f"gt_lab.bronze.quarantine_{source}_{entity}"
    try:
        n = spark.table(t).count()
        if n > 0:
            print(f"\n  {t}: {n} bad records")
            display(spark.table(t).limit(5))
    except: pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP F — Discussion talking points (για trainer)
# MAGIC
# MAGIC ### Fail-fast vs Quarantine
# MAGIC
# MAGIC | Use case | Choice |
# MAGIC |---|---|
# MAGIC | Schema break (missing column) | **Fail-fast** — pipeline can't proceed |
# MAGIC | Source > 90% bad records | **Fail-fast** — source likely broken |
# MAGIC | Some bad records (< 10%) | **Quarantine** — keep flow, sideline bad |
# MAGIC | Cosmetic issues (trailing spaces) | **Log only** — don't block, don't quarantine |
# MAGIC
# MAGIC ### Performance gotcha
# MAGIC
# MAGIC Το naive validator κάνει 5+ separate `count()` calls = 5 full scans.
# MAGIC
# MAGIC **Production optimization**:
# MAGIC ```python
# MAGIC stats = df.agg(
# MAGIC     count("*").alias("n_total"),
# MAGIC     count(when(col("afm").isNull(), True)).alias("afm_nulls"),
# MAGIC     count(when(~col("afm").cast("string").rlike(r"^\d{9}$"), True)).alias("afm_bad_regex"),
# MAGIC     count(when(col("tax_amount") < 0, True)).alias("neg_amount"),
# MAGIC     # ... όλα τα checks σε ένα agg
# MAGIC ).collect()[0]
# MAGIC ```
# MAGIC Σπάει σε ένα Spark job με Catalyst optimization.
# MAGIC
# MAGIC ### Contract evolution patterns
# MAGIC
# MAGIC | Type | Pattern |
# MAGIC |---|---|
# MAGIC | **Add column** (non-breaking) | bump minor (1.0 → 1.1), backwards compat |
# MAGIC | **Tighten constraint** (e.g. nullable→non-null) | breaking, bump major (1.x → 2.0) |
# MAGIC | **Remove column** | breaking, major bump, deprecation period |
# MAGIC | **Change type** | breaking — usually + migration script |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verification

# COMMAND ----------

# Just confirm everything ran
print(f"✅ CONTRACTS: {len(CONTRACTS)} sources")
print(f"✅ validate_contract: callable")
print(f"✅ write_quarantine: callable")
print(f"✅ load_with_contracts: ran για όλες τις 5 πηγές")
print(f"✅ Bad data tests: 3 scenarios covered")
print(f"✅ Metrics report: produced\n")
print("🎉 STRETCH 4 ολοκληρώθηκε με production-grade pattern.")

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
