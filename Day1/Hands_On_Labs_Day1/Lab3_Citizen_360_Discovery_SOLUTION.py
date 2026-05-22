# Databricks notebook source
# MAGIC %md
# MAGIC # 🌐 LAB 3 — Citizen 360 Discovery — ΛΥΣΗ (TRAINER ONLY)
# MAGIC
# MAGIC > ⚠️ **TRAINER REFERENCE — Μην το δείξεις στους students.**
# MAGIC > Όλα τα steps + stretch exercises λυμένα + trainer notes.

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
# MAGIC ## ✅ STEP 1 — Check Bronze prerequisites

# COMMAND ----------

# DBTITLE 1,Solution: Check Bronze
print("=== Bronze tables που υπάρχουν ===")
display(spark.sql("SHOW TABLES IN gt_lab.bronze"))

# Αν λείπουν (Lab 2 δεν έτρεξε ακόμα), φόρτωσε direct από CSV
from pyspark.sql.functions import col, current_timestamp, input_file_name

# Helper για direct CSV load (fallback)
def load_csv_if_needed(table_name, csv_file):
    try:
        spark.table(table_name)
        return spark.table(table_name)
    except:
        print(f"⚠️ {table_name} δεν υπάρχει — load από CSV")
        df = (spark.read.option("header","true").option("inferSchema","true")
              .csv(f"{LANDING_PATH}/{csv_file}"))
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 2 — Inspect bronze schemas

# COMMAND ----------

# DBTITLE 1,Solution: Inspect all 5 Bronze
SOURCES_INFO = [
    ("gt_lab.bronze.citizen_registry_raw",    "citizen_registry.csv",    "afm"),
    ("gt_lab.bronze.taxis_declarations_raw",  "taxis_declarations.csv",  "afm"),
    ("gt_lab.bronze.efka_contributions_raw",  "efka_contributions.csv",  "afm"),
    ("gt_lab.bronze.kep_events_raw",          "kep_events.csv",          "citizen_afm"),
    ("gt_lab.bronze.mydata_invoices_raw",     "mydata_invoices.csv",     "issuer_afm"),
]

dfs = {}
for table, csv_file, afm_col in SOURCES_INFO:
    df = load_csv_if_needed(table, csv_file)
    dfs[table] = df
    print(f"\n=== {table} ===")
    print(f"  AFM column: {afm_col}")
    print(f"  Schema:")
    df.printSchema()
    print(f"  Sample:")
    display(df.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer notes:**
# MAGIC - Highlight: AFM columns έχουν **διαφορετικά ονόματα** ανά source
# MAGIC - Type alignment: σε ορισμένα tables AFM είναι integer (από inferSchema)
# MAGIC - Production pattern: **πάντα cast σε string** όλα τα AFM columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 3 — Aggregate TAXIS

# COMMAND ----------

# DBTITLE 1,Solution: TAXIS aggregations
from pyspark.sql.functions import (
    col, sum as _sum, count, max as _max, avg, when, lit, coalesce
)

taxis = dfs["gt_lab.bronze.taxis_declarations_raw"] \
    .withColumn("afm", col("afm").cast("string"))

taxis_agg = (taxis
    .groupBy("afm")
    .agg(
        _sum("tax_amount").alias("total_tax_paid"),
        count("statement_id").alias("tax_declarations_count"),
        _max("fiscal_year").alias("last_tax_year"),
    )
)
print(f"TAXIS aggregations: {taxis_agg.count()} unique AFMs")
display(taxis_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 4 — Aggregate EFKA

# COMMAND ----------

# DBTITLE 1,Solution: EFKA aggregations
efka = dfs["gt_lab.bronze.efka_contributions_raw"] \
    .withColumn("afm", col("afm").cast("string"))

efka_agg = (efka
    .groupBy("afm")
    .agg(
        _sum("employee_contribution").alias("total_efka_paid"),
        count("contribution_id").alias("efka_periods_count"),
    )
)
print(f"EFKA aggregations: {efka_agg.count()} unique AFMs")
display(efka_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 5 — Aggregate KEP (rename + agg)

# COMMAND ----------

# DBTITLE 1,Solution: KEP aggregations
kep = dfs["gt_lab.bronze.kep_events_raw"] \
    .withColumnRenamed("citizen_afm", "afm") \
    .withColumn("afm", col("afm").cast("string"))

kep_agg = (kep
    .groupBy("afm")
    .agg(
        count("event_id").alias("kep_events_total"),
        avg("wait_minutes").alias("avg_wait_minutes"),
    )
)
print(f"KEP aggregations: {kep_agg.count()} unique AFMs")
display(kep_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer note:** Common mistake — students ξεχνούν το rename και κάνουν join με `citizen_afm == afm` που γίνεται μπερδεμένο.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 6 — Aggregate myDATA (διπλό aggregation)

# COMMAND ----------

# DBTITLE 1,Solution: myDATA — issued + received
mydata = dfs["gt_lab.bronze.mydata_invoices_raw"]

# Issued ως ξεχωριστό aggregation
issued = (mydata
    .withColumn("issuer_afm", col("issuer_afm").cast("string"))
    .groupBy("issuer_afm")
    .agg(count("invoice_id").alias("invoices_issued"))
    .withColumnRenamed("issuer_afm", "afm")
)

# Received
received = (mydata
    .withColumn("receiver_afm", col("receiver_afm").cast("string"))
    .groupBy("receiver_afm")
    .agg(count("invoice_id").alias("invoices_received"))
    .withColumnRenamed("receiver_afm", "afm")
)

# Full outer join σε AFM ώστε να έχουμε όλους τους πολίτες
mydata_agg = (issued
    .join(received, on="afm", how="full_outer")
    .fillna(0, subset=["invoices_issued", "invoices_received"])
)
print(f"myDATA aggregations: {mydata_agg.count()} unique AFMs")
display(mydata_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer talking points:**
# MAGIC - **Full outer** εδώ γιατί ο issuer δεν είναι πάντα και receiver
# MAGIC - `fillna(0)` αντί NULL γιατί count χωρίς activity = 0
# MAGIC - Σε production: φτιάχνεις και `total_vat_issued`, `total_amount_received` κλπ

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 7 — Citizen 360 με LEFT JOINs

# COMMAND ----------

# DBTITLE 1,Solution: Final citizen 360 build
registry = dfs["gt_lab.bronze.citizen_registry_raw"] \
    .withColumn("afm", col("afm").cast("string"))

citizen_360 = (registry
    .join(taxis_agg,  on="afm", how="left")
    .join(efka_agg,   on="afm", how="left")
    .join(kep_agg,    on="afm", how="left")
    .join(mydata_agg, on="afm", how="left")
    # Fill NULLs σε counts (όχι σε amounts — κρατάμε null για clarity)
    .fillna(0, subset=["tax_declarations_count", "efka_periods_count",
                       "kep_events_total", "invoices_issued", "invoices_received"])
)

print(f"Citizen 360 rows: {citizen_360.count()}")
print(f"Unique AFMs: {citizen_360.select('afm').distinct().count()}")
display(citizen_360)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 8 — Save Gold

# COMMAND ----------

# DBTITLE 1,Solution: Write Gold + metadata
target = "gt_lab.gold.citizen_360"
citizen_360.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(target)

# Metadata
spark.sql(f"""
    ALTER TABLE {target}
    SET TBLPROPERTIES (
        'comment' = 'Citizen 360 — unified citizen view across TAXIS/EFKA/KEP/myDATA',
        'layer' = 'gold',
        'pii_classification' = 'high',
        'business_owner' = 'general-secretariat',
        'tech_owner' = 'data-platform-team'
    )
""")

print(f"✅ {target}: {spark.table(target).count()} rows")
display(spark.sql(f"DESCRIBE EXTENDED {target}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 9 — Discovery Analyses

# COMMAND ----------

# DBTITLE 1,Solution 9A — Orphan tax records
print("=== Orphan tax records (TAXIS AFMs που δεν είναι στο Registry) ===")
orphans = (taxis
    .select("afm").distinct()
    .join(registry.select("afm").distinct(), on="afm", how="left_anti")
)
print(f"Orphan AFMs: {orphans.count()}")
display(orphans)

# COMMAND ----------

# DBTITLE 1,Solution 9B — Top 5 taxpayers
print("=== Top 5 taxpayers ===")
top5 = (spark.table(target)
    .filter(col("total_tax_paid").isNotNull())
    .orderBy(col("total_tax_paid").desc())
    .select("afm", "full_name", "region", "total_tax_paid", "tax_declarations_count")
    .limit(5)
)
display(top5)

# COMMAND ----------

# DBTITLE 1,Solution 9C — KEP activity by region
print("=== KEP events ανά region ===")
by_region = (spark.table(target)
    .groupBy("region")
    .agg(
        count("afm").alias("citizens"),
        _sum("kep_events_total").alias("total_kep_events"),
        avg("kep_events_total").alias("avg_kep_per_citizen"),
    )
    .orderBy(col("total_kep_events").desc())
)
display(by_region)

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer talking points:**
# MAGIC - Discovery analyses αποκαλύπτουν data quality issues που δεν είναι obvious
# MAGIC - **Orphan records**: γιατί υπάρχουν AFMs στο TAXIS αλλά όχι στο Registry; → outdated Registry, error, or test data
# MAGIC - Top 5 query: αν φαίνεται outlier (π.χ. €1M φόρος), ίσως είναι error

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🚀 STRETCH SOLUTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 STRETCH 1 — Citizen Risk Score

# COMMAND ----------

# DBTITLE 1,Solution: Risk score
df_with_risk = (spark.table(target)
    .withColumn(
        "risk_score",
        # +1 αν high tax (>10K)
        when(coalesce(col("total_tax_paid"), lit(0)) > 10000, 1).otherwise(0) +
        # +1 αν zero EFKA (suspicious αν έχει tax)
        when((coalesce(col("total_efka_paid"), lit(0)) == 0) &
             (coalesce(col("total_tax_paid"), lit(0)) > 0), 1).otherwise(0) +
        # +1 αν > 5 KEP events
        when(col("kep_events_total") > 5, 1).otherwise(0) +
        # +1 αν issuer mismatch με tax (πολλά τιμολόγια, χαμηλός φόρος)
        when((col("invoices_issued") > 10) &
             (coalesce(col("total_tax_paid"), lit(0)) < 1000), 2).otherwise(0)
    )
    .withColumn(
        "risk_tier",
        when(col("risk_score") >= 3, "high")
        .when(col("risk_score") >= 1, "medium")
        .otherwise("low")
    )
)

display(df_with_risk.select("afm", "full_name", "total_tax_paid",
                              "total_efka_paid", "kep_events_total",
                              "invoices_issued", "risk_score", "risk_tier")
        .orderBy(col("risk_score").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 STRETCH 2 — Active systems array

# COMMAND ----------

# DBTITLE 1,Solution: active_in_systems array
from pyspark.sql.functions import array_remove, array, size

df_active = (spark.table(target)
    .withColumn(
        "active_in_systems",
        array_remove(array(
            when(col("total_tax_paid").isNotNull() & (col("total_tax_paid") > 0), lit("TAXIS")),
            when(col("total_efka_paid").isNotNull() & (col("total_efka_paid") > 0), lit("EFKA")),
            when(col("kep_events_total") > 0, lit("KEP")),
            when((col("invoices_issued") + col("invoices_received")) > 0, lit("myDATA")),
            lit("REGISTRY")  # All citizens are in registry by definition
        ), None)
    )
    .withColumn("systems_count", size(col("active_in_systems")))
)

display(df_active.select("afm", "full_name", "active_in_systems", "systems_count")
        .orderBy(col("systems_count").desc()))

# Insight query
print("\n=== Citizens κατηγοριοποιημένοι κατά systems coverage ===")
display(df_active.groupBy("systems_count").count().orderBy("systems_count"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 STRETCH 3 — Masked PII view για analysts

# COMMAND ----------

# DBTITLE 1,Solution: Masked view
spark.sql(f"""
    CREATE OR REPLACE VIEW gt_lab.gold.citizen_360_masked AS
    SELECT
        -- AFM masked (last 4 digits only)
        CONCAT('***', RIGHT(afm, 4))                  AS afm_masked,
        -- Full name masked (first letter + last name)
        CONCAT(LEFT(SPLIT(full_name, ' ')[0], 1), '. ',
               COALESCE(SPLIT(full_name, ' ')[1], '')) AS name_masked,
        -- All else exposed
        region,
        birth_year,
        is_active,
        total_tax_paid,
        tax_declarations_count,
        last_tax_year,
        total_efka_paid,
        efka_periods_count,
        kep_events_total,
        avg_wait_minutes,
        invoices_issued,
        invoices_received
    FROM gt_lab.gold.citizen_360
""")

print("✅ View created: gt_lab.gold.citizen_360_masked")
display(spark.table("gt_lab.gold.citizen_360_masked"))

# Grant access only to masked view για analysts
try:
    spark.sql("GRANT USAGE ON CATALOG gt_lab TO `analysts`")
    spark.sql("GRANT USAGE ON SCHEMA gt_lab.gold TO `analysts`")
    spark.sql("GRANT SELECT ON VIEW gt_lab.gold.citizen_360_masked TO `analysts`")
    print("✅ Analysts have access ΜΟΝΟ στο masked view")
except Exception as e:
    print(f"⚠️ GRANT skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 SUPER STRETCH — Streaming Gold pseudocode

# COMMAND ----------

# DBTITLE 1,Solution: Streaming citizen_360 pseudocode
PSEUDOCODE = """
# Production pattern για live citizen_360 με DLT:

import dlt
from pyspark.sql.functions import col, sum, count, current_timestamp

# Bronze streams (each table)
@dlt.table
def taxis_stream():
    return spark.readStream.format("delta").table("gt_lab.bronze.taxis_declarations_raw")

@dlt.table
def efka_stream():
    return spark.readStream.format("delta").table("gt_lab.bronze.efka_contributions_raw")

# ... similar for KEP, myDATA, Registry

# Silver streaming aggregations (per source)
@dlt.table
def taxis_per_citizen():
    return (
        dlt.read_stream("taxis_stream")
        .withWatermark("submitted_at", "1 hour")
        .groupBy("afm")
        .agg(sum("tax_amount").alias("total_tax_paid"),
             count("*").alias("tax_count"))
    )

# Gold — incremental MERGE pattern
@dlt.table
def citizen_360_live():
    return (
        dlt.read("registry_stream")
        .join(dlt.read("taxis_per_citizen"),  "afm", "left")
        .join(dlt.read("efka_per_citizen"),   "afm", "left")
        .join(dlt.read("kep_per_citizen"),    "afm", "left")
        .join(dlt.read("mydata_per_citizen"), "afm", "left")
        .withColumn("last_refresh", current_timestamp())
    )

# Operational considerations:
#  - Watermarks για late data tolerance
#  - State management (large aggregations → memory)
#  - Checkpoint location per pipeline
#  - Trigger interval — balance freshness vs cost
#  - Schema evolution — DLT auto-handles
#  - Lineage — DLT auto-tracks
"""
print(PSEUDOCODE)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎓 TRAINER CHEAT SHEET

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pacing (90')
# MAGIC
# MAGIC | Time | Activity | Watch for |
# MAGIC |---|---|---|
# MAGIC | 0-15' | Theory + JOIN refresher | Πιθανώς να χρειαστεί JOIN whiteboard |
# MAGIC | 15-25' | Steps 1-2 (inspect) | Push να βλέπουν data, όχι να σπεύδουν σε code |
# MAGIC | 25-55' | Steps 3-7 (aggregations + joins) | Common: forget rename `citizen_afm` |
# MAGIC | 55-65' | Step 8 (save) + Step 9 (discovery) | Push να ψάχνουν "παράξενα" |
# MAGIC | 65-85' | 2 group presentations | Focus σε **WHY** join strategy + NULL handling |
# MAGIC | 85-90' | Group discussion of findings | Bridge to Day 2 quality |
# MAGIC
# MAGIC ## Top common mistakes
# MAGIC
# MAGIC | Bug | Detection | Fix |
# MAGIC |---|---|---|
# MAGIC | Forget rename `citizen_afm`/`issuer_afm` | Join produces no matches | Rename to `afm` explicitly |
# MAGIC | Type mismatch (int vs string AFM) | Slow join + 0 matches | Cast to string explicitly |
# MAGIC | Row explosion (no aggregation) | Citizen 360 has 1M+ rows for 9 citizens | Aggregate πρώτα, μετά join |
# MAGIC | Inner join αντί left | Lose citizens χωρίς tax | Use LEFT from registry |
# MAGIC | NULL semantics confusion | Counts show NULL αντί 0 | fillna(0) σε count columns |
# MAGIC
# MAGIC ## Discovery moments (highlight αυτά!)
# MAGIC
# MAGIC 1. **«Κοίτα! Ίδιο AFM σε 4 systems»** — first time they realize the power
# MAGIC 2. **Orphan records** — «πραγματικά υπάρχουν!»
# MAGIC 3. **Top taxpayers** — «αυτός βγάζει 10x τους άλλους — γιατί;»
# MAGIC 4. **KEP by region** — «η Attica έχει αναλογικά λιγότερα KEP — γιατί;»
# MAGIC
# MAGIC Αυτά τα moments είναι ΟΤΙ ΧΡΕΙΑΖΕΤΑΙ για να καταλάβουν γιατί data engineering matters.
# MAGIC
# MAGIC ## Bridge to Day 2 & Day 6
# MAGIC
# MAGIC «Σήμερα φτιάξατε το **πρώτο σας Gold table**. Παρατηρήσατε quality issues (orphans, missing data). **Αύριο (Day 2)**: θα φτιάξουμε quality rules για να καθαρίσουμε. **Day 6 capstone**: όλο αυτό το pipeline σε production-grade DLT + Job + monitoring.»
