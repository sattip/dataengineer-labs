# Databricks notebook source
# MAGIC %md
# MAGIC # 🔐 LAB 1 — UC Foundation — ΛΥΣΗ (TRAINER ONLY)
# MAGIC
# MAGIC > ⚠️ **TRAINER REFERENCE — Μην το δείξεις στους students.**
# MAGIC > Όλα τα steps + stretch exercises λυμένα με σχόλια.

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
# MAGIC ## ✅ STEP 1 — Create Catalog

# COMMAND ----------

# DBTITLE 1,Solution: CREATE CATALOG
spark.sql("""
    CREATE CATALOG IF NOT EXISTS gt_lab
    COMMENT 'Grant Thornton lab catalog — Δημόσιος Φορέας Ψηφιακών Υπηρεσιών'
""")
print("✅ Catalog gt_lab δημιουργήθηκε (ή υπήρχε ήδη)")

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer notes:**
# MAGIC - Common student mistakes:
# MAGIC   - Ξεχνούν το `IF NOT EXISTS` → fail σε 2ο run
# MAGIC   - Ξεχνούν το COMMENT (technically OK, αλλά governance loss)
# MAGIC - Highlight: το `IF NOT EXISTS` είναι **production rule**

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 2 — Create 3 Schemas

# COMMAND ----------

# DBTITLE 1,Solution: CREATE 3 SCHEMAS (loop)
SCHEMAS = [
    ("bronze", "Raw ingested data — immutable, full history"),
    ("silver", "Cleaned, validated, deduplicated"),
    ("gold",   "Business-ready aggregations για BI/ML"),
]

for name, comment in SCHEMAS:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS gt_lab.{name}
        COMMENT '{comment}'
    """)
    print(f"  ✅ Schema gt_lab.{name}")

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer notes:**
# MAGIC - Πολλοί students θα γράψουν 3 ξεχωριστά `CREATE SCHEMA` — αποδεκτό αλλά **όχι DRY**
# MAGIC - Loop pattern είναι senior skill. Push για αυτό αν τους ρωτήσουν.
# MAGIC - Ερώτημα να ρωτήσεις: «Γιατί χωριστά schemas + όχι ένα schema με prefix bronze_*?»
# MAGIC   - Απάντηση: governance permissions per layer, καθαρότερο catalog UI

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 3 — Create Volume

# COMMAND ----------

# DBTITLE 1,Solution: CREATE VOLUME
spark.sql("""
    CREATE VOLUME IF NOT EXISTS gt_lab.bronze.landing
    COMMENT 'Landing zone για CSV/JSON raw files'
""")
print("✅ Volume gt_lab.bronze.landing")

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer notes:**
# MAGIC - **Σύγχυση που εμφανίζεται:** "Είναι σαν folder ή σαν table;" → Σαν folder (path-based, files inside)
# MAGIC - Volume vs External Volume: για beginners πάντα **Managed** (UC χειρίζεται storage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 4 — Verify CSVs

# COMMAND ----------

# DBTITLE 1,Solution: List files
files = dbutils.fs.ls(LANDING_PATH)
print(f"📂 Αρχεία στο {LANDING_PATH}:")
for f in files:
    print(f"  • {f.name}  ({f.size:,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 5 — SHOW Catalogs/Schemas/Volumes

# COMMAND ----------

# DBTITLE 1,Solution: 3 SHOW queries
print("=== Catalogs ===")
display(spark.sql("SHOW CATALOGS LIKE 'gt%'"))

# COMMAND ----------

print("=== Schemas στο gt_lab ===")
display(spark.sql("SHOW SCHEMAS IN gt_lab"))

# COMMAND ----------

print("=== Volumes στο gt_lab.bronze ===")
display(spark.sql("SHOW VOLUMES IN gt_lab.bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP 6 — Read citizen_registry.csv

# COMMAND ----------

# DBTITLE 1,Solution: Read CSV + inspect
df_citizens = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{LANDING_PATH}/citizen_registry.csv"))

print(f"📊 Rows: {df_citizens.count()}")
print("📋 Schema:")
df_citizens.printSchema()
display(df_citizens.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer "gotcha" moment:**
# MAGIC - Δείξε τους ότι το `afm` ΕΓΙΝΕ `integer` (όχι string)
# MAGIC - Ρώτησε: «Τι θα γίνει αν είχαμε `afm = "001234567"`;»
# MAGIC - Απάντηση: το integer 1234567 — leading zero **χάθηκε**
# MAGIC - Αυτό σπάει join με dim_citizen
# MAGIC - **Production rule**: ΠΟΤΕ inferSchema σε identifiers / codes

# COMMAND ----------

# MAGIC %md
# MAGIC # ✅ AUTO-VERIFICATION (έτοιμο)

# COMMAND ----------

checks = []
try:
    catalogs = [r['catalog'] for r in spark.sql("SHOW CATALOGS").collect()]
    checks.append(("Catalog gt_lab", "gt_lab" in catalogs))
except: checks.append(("Catalog gt_lab", False))

try:
    schemas = {r['databaseName'] for r in spark.sql("SHOW SCHEMAS IN gt_lab").collect()}
    checks.append(("3 schemas", {"bronze","silver","gold"}.issubset(schemas)))
except: checks.append(("3 schemas", False))

try:
    vols = [r['volume_name'] for r in spark.sql("SHOW VOLUMES IN gt_lab.bronze").collect()]
    checks.append(("Volume landing", "landing" in vols))
except: checks.append(("Volume landing", False))

try:
    files = {f.name for f in dbutils.fs.ls("/Volumes/gt_lab/bronze/landing")}
    expected = {"citizen_registry.csv", "taxis_declarations.csv", "efka_contributions.csv",
                "kep_events.csv", "mydata_invoices.csv"}
    checks.append(("5 CSVs", not (expected - files)))
except: checks.append(("5 CSVs", False))

try:
    n = spark.read.option("header","true").option("inferSchema","true") \
        .csv("/Volumes/gt_lab/bronze/landing/citizen_registry.csv").count()
    checks.append((f"Read works ({n} rows)", n >= 9))
except: checks.append(("Read", False))

passed = sum(1 for _, ok in checks if ok)
print("=" * 60)
for n, ok in checks:
    print(f"  {'✅' if ok else '❌'} {n}")
print("=" * 60)
print(f"  {passed}/{len(checks)} passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🚀 STRETCH SOLUTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 STRETCH 1 — RBAC GRANT/REVOKE

# COMMAND ----------

# DBTITLE 1,Solution: Grant SELECT στο gold για analysts
# Σημείωση: αν το group 'analysts' δεν υπάρχει στο workspace, αυτό σπάει.
# Σε production φτιάχνεις το group πρώτα στο Admin Console.

try:
    # 3-level grant chain — όλα τα 3 χρειάζονται
    spark.sql("GRANT USAGE ON CATALOG gt_lab TO `analysts`")
    spark.sql("GRANT USAGE ON SCHEMA gt_lab.gold TO `analysts`")
    spark.sql("GRANT SELECT ON ALL TABLES IN SCHEMA gt_lab.gold TO `analysts`")
    spark.sql("GRANT SELECT ON FUTURE TABLES IN SCHEMA gt_lab.gold TO `analysts`")
    print("✅ Analysts έχουν read access στο gold schema")

    print("\n=== Current grants στο gt_lab.gold ===")
    display(spark.sql("SHOW GRANTS ON SCHEMA gt_lab.gold"))

except Exception as e:
    print(f"⚠️ GRANT skipped (probably group δεν υπάρχει): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer notes:**
# MAGIC - `FUTURE TABLES` είναι το secret — auto-grant για tables που δεν υπάρχουν ακόμα
# MAGIC - Common error: «GRANT failed» → πάντα ελέγξτε ότι το group existsέ στο workspace πρώτα
# MAGIC - REVOKE pattern (να το αναφέρεις):
# MAGIC   ```
# MAGIC   REVOKE ALL PRIVILEGES ON SCHEMA gt_lab.gold FROM `analysts`
# MAGIC   ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 STRETCH 2 — Explicit Schema Read

# COMMAND ----------

# DBTITLE 1,Solution: Type-safe read
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

citizen_schema = StructType([
    StructField("afm",        StringType(),  False),  # AFM as string!
    StructField("full_name",  StringType(),  True),
    StructField("region",     StringType(),  True),
    StructField("birth_year", IntegerType(), True),
    StructField("is_active",  BooleanType(), True),
    StructField("updated_at", TimestampType(), True),
])

df_typed = (spark.read
    .schema(citizen_schema)
    .option("header", "true")
    .csv(f"{LANDING_PATH}/citizen_registry.csv"))

print("Schema με explicit types:")
df_typed.printSchema()
display(df_typed.limit(5))

# Compare με inferSchema version
print("\n🎯 Παρατήρηση: AFM είναι τώρα string — leading zeros θα διατηρηθούν")

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Trainer talking point:**
# MAGIC - Δείξε side-by-side: `df_citizens.printSchema()` vs `df_typed.printSchema()`
# MAGIC - Highlight ότι το `afm` είναι integer vs string
# MAGIC - "Bug που χάνει χρόνια production δίχως να ανιχνεύεται"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 STRETCH 3 — Table με metadata

# COMMAND ----------

# DBTITLE 1,Solution: Bronze table με TBLPROPERTIES
target = "gt_lab.bronze.citizen_registry_raw"

# Write df_typed σε Delta table
df_typed.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(target)

# Πρόσθεσε metadata
spark.sql(f"""
    ALTER TABLE {target}
    SET TBLPROPERTIES (
        'comment' = 'Raw citizen registry από CRM source',
        'owner' = 'data-platform-team',
        'pii_classification' = 'high',
        'refresh_cadence' = 'daily',
        'created_by' = 'Lab1 UC Foundation'
    )
""")

# Column-level comment
spark.sql(f"""
    ALTER TABLE {target}
    ALTER COLUMN afm
    COMMENT 'Αριθμός Φορολογικού Μητρώου (9 digits, PII high)'
""")

# Verify
print("=== Table metadata ===")
display(spark.sql(f"DESCRIBE EXTENDED {target}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Production tip:**
# MAGIC - `pii_classification` δεν είναι standard tag — εσύ ορίζεις conventions
# MAGIC - Sync με Purview / Collibra catalog για enterprise governance

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 SUPER STRETCH — Parametrization με widgets

# COMMAND ----------

# DBTITLE 1,Solution: dbutils.widgets
# Define widgets στην αρχή
dbutils.widgets.text("catalog_name", "gt_lab", "Catalog name")
dbutils.widgets.dropdown("environment", "dev", ["dev", "test", "prod"])
dbutils.widgets.text("source_file", "citizen_registry.csv", "Source filename")

# Read values
CATALOG_W = dbutils.widgets.get("catalog_name")
ENV_W     = dbutils.widgets.get("environment")
FILE_W    = dbutils.widgets.get("source_file")

print(f"Catalog:     {CATALOG_W}")
print(f"Environment: {ENV_W}")
print(f"File:        {FILE_W}")

# Use σε queries
table_name = f"{CATALOG_W}.bronze.parametrized_demo"
spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} (id INT, env STRING) USING DELTA")
spark.sql(f"INSERT INTO {table_name} VALUES (1, '{ENV_W}')")
display(spark.sql(f"SELECT * FROM {table_name}"))

# Cleanup widgets αν θες
# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC **🎓 Production patterns:**
# MAGIC - Widgets δίνουν UI control panel πάνω από το notebook
# MAGIC - Σε Workflows: γίνονται job parameters
# MAGIC - Best practice: όλα τα paths/catalogs/dates → widgets, ποτέ hardcoded

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎓 TRAINER CHEAT SHEET — Pacing & Common Issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pacing (80')
# MAGIC
# MAGIC | Time | Activity | Watch for |
# MAGIC |---|---|---|
# MAGIC | 0-15' | Theory reading | Όσοι δεν διαβάζουν, push them να επιστρέψουν |
# MAGIC | 15-25' | Steps 1-3 (DDL) | Common: ξεχνούν `IF NOT EXISTS` |
# MAGIC | 25-40' | Steps 4-5 (verify) | Πρέπει `dbutils.fs.ls` να δείχνει 5 αρχεία |
# MAGIC | 40-55' | Step 6 (read) | Highlight inferSchema "gotcha" |
# MAGIC | 55-65' | Verification cell | Auto-checks — δες σκορ ολομέλειας |
# MAGIC | 65-80' | Stretch όσοι θέλουν | Don't push slow students να σπρώχνονται |
# MAGIC
# MAGIC ## Common student blocks
# MAGIC
# MAGIC | Error | Fix |
# MAGIC |---|---|
# MAGIC | `Insufficient privileges to CREATE CATALOG` | Workspace δεν επιτρέπει. Χρησιμοποίησε υπάρχον (`main` ή personal) |
# MAGIC | `Table or view not found: gt_lab.bronze` | Skipped Step 2 — re-run schema creation |
# MAGIC | `Cannot find file in volume` | Bootstrap cell δεν τρέξε σωστά — re-run |
# MAGIC | Auto-verification fails on idempotency | Έγραψαν `mode("append")` αντί για `overwrite` |
# MAGIC
# MAGIC ## Bridge to Lab 2
# MAGIC
# MAGIC «Τώρα που έχουμε το foundation, στο Lab 2 θα φτιάξουμε το REAL ingestion pipeline με 5 πηγές, audit columns, και idempotency.»
