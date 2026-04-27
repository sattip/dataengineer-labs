# Databricks notebook source
# MAGIC %md
# MAGIC # Άσκηση #4 — Lineage & Time Travel (Ημέρα 3)
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC
# MAGIC > **GitHub** (share this στο Zoom chat):
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/Lineage_History_Notebook.py
# MAGIC > ```
# MAGIC
# MAGIC **Catalog/Schema**: `workspace.aade`
# MAGIC **Volume**: `/Volumes/workspace/aade/aade_data/`
# MAGIC **Διάρκεια**: ~20 min
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Σενάριο
# MAGIC
# MAGIC Είσαι **Data Steward της ΑΑΔΕ**. Σήμερα ο automated job έσπρωξε ένα bad batch στο `tax_declarations_silver`. Πρέπει να:
# MAGIC
# MAGIC 1. **Δεις τι άλλαξε** — `DESCRIBE HISTORY`
# MAGIC 2. **Συγκρίνεις versions** — `VERSION AS OF` + `EXCEPT`
# MAGIC 3. **Εντοπίσεις τι έσπασε** — query στο audit table
# MAGIC 4. **Δεις από πού ήρθε το data** — Unity Catalog Lineage UI
# MAGIC 5. **Κάνεις rollback** — `RESTORE TABLE`
# MAGIC
# MAGIC ## 📋 Notebook structure
# MAGIC
# MAGIC | Cell | Step | Time |
# MAGIC |---|------|------|
# MAGIC | 0 | Setup + download από GitHub | 30s |
# MAGIC | 0.5 | Bootstrap silver/quarantine/audit (αν λείπουν) | 1 min |
# MAGIC | 1 | DESCRIBE HISTORY | 5s |
# MAGIC | 2 | Audit log query (business view) | 5s |
# MAGIC | 3 | Time Travel — VERSION AS OF | 5s |
# MAGIC | 4 | Simulate bad batch | 5s |
# MAGIC | 5 | Detect — version diff με EXCEPT | 5s |
# MAGIC | 6 | Unity Catalog Lineage UI walkthrough | 3 min |
# MAGIC | 7 | RESTORE — rollback | 5s |
# MAGIC | 8 | Audit forensics — failed rules σε 24h | 5s |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0 — Setup + Download από GitHub
# MAGIC
# MAGIC Δημιουργεί schema/volume και κατεβάζει τα 5 lab files. **Idempotent** — αν έχει ήδη εκτελεστεί η Άσκηση 3 και τα αρχεία υπάρχουν, η λήψη παραλείπεται.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
print("✅ workspace.aade.aade_data ready\n")

import urllib.request, os

REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3"
VOL = "/Volumes/workspace/aade/aade_data"

files = [
    "declarations.csv",
    "doy.csv",
    "employees.csv",
    "taxpayers.csv",
    "aade_declarations_data_contract.yaml",
]

for fname in files:
    target = f"{VOL}/{fname}"
    if os.path.exists(target):
        print(f"⏭️  {fname} (already in volume)")
    else:
        try:
            urllib.request.urlretrieve(f"{REPO}/{fname}", target)
            print(f"✅ {fname}")
        except Exception as e:
            print(f"❌ {fname}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0.5 — Bootstrap silver/quarantine/audit (αν λείπουν)
# MAGIC
# MAGIC Αν έχει ήδη τρέξει η **Άσκηση 3** (data_contract_validation_notebook), αυτό το cell **δεν κάνει τίποτα**.
# MAGIC
# MAGIC Αν τα tables λείπουν (π.χ. ξεκινάς direct με την Άσκηση 4), δημιουργεί ένα **minimal pipeline** για να έχεις δεδομένα να δουλέψεις.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, when

# Check what already exists
existing = {t.tableName for t in spark.sql("SHOW TABLES IN workspace.aade").collect()}
required = {"tax_declarations_silver", "tax_declarations_quarantine", "data_contract_audit"}
missing = required - existing

if not missing:
    print("✅ Όλα τα prerequisite tables υπάρχουν — προχώρα στο Βήμα 1")
else:
    print(f"⚠️  Λείπουν tables: {missing}")
    print("   Bootstrapping minimal pipeline (~30s)...\n")

    csv_path = f"{VOL}/declarations.csv"

    # Bronze: read raw with backticked Greek headers
    bronze = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path))
    print(f"   📥 Read {bronze.count()} rows from declarations.csv")

    # Inject 5 bad records για το quarantine demo (negative amount)
    bad = bronze.limit(5).withColumn("`Ποσό_EUR`", lit(-1.0))
    combined = bronze.unionByName(bad)

    # Split silver vs quarantine (rule: Ποσό_EUR >= 0)
    silver_df = combined.filter(col("`Ποσό_EUR`") >= 0)
    quarantine_df = (combined.filter(col("`Ποσό_EUR`") < 0)
        .withColumn("rule_failed", lit("no_negative_amount"))
        .withColumn("quarantined_at", current_timestamp()))

    # Write silver
    (silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("workspace.aade.tax_declarations_silver"))

    # Write quarantine
    (quarantine_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("workspace.aade.tax_declarations_quarantine"))

    # Audit table — minimal version
    audit_rows = [
        ("run_001", "schema_match",       "critical", True,  0, 305),
        ("run_001", "no_negative_amount", "critical", False, 5, 305),
        ("run_001", "afm_format",         "warning",  True,  0, 305),
        ("run_001", "doy_in_range",       "warning",  True,  0, 305),
    ]
    audit_df = spark.createDataFrame(audit_rows, ["run_id", "rule_id", "rule_severity", "passed", "invalid_count", "total_rows"])
    audit_df = audit_df.withColumn("run_timestamp", current_timestamp())

    (audit_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("workspace.aade.data_contract_audit"))

    print(f"   ✅ silver:     {silver_df.count()} rows")
    print(f"   ✅ quarantine: {quarantine_df.count()} rows")
    print(f"   ✅ audit:      {audit_df.count()} entries")
    print("\n   Bootstrap complete. Ready για lineage exercise.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 0 — Verify prerequisites
# MAGIC
# MAGIC Επιβεβαίωσε ότι τα 3 tables είναι έτοιμα.

# COMMAND ----------

required_tables = ["tax_declarations_silver", "tax_declarations_quarantine", "data_contract_audit"]
for t in required_tables:
    full = f"workspace.aade.{t}"
    cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {full}").collect()[0]["n"]
    print(f"✅ {full}: {cnt} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 1 — Delta History (πλήρες audit trail)
# MAGIC
# MAGIC Κάθε Delta table κρατάει **transaction log**. Δείχνει:
# MAGIC - Ποιος έτρεξε το operation (`userName`)
# MAGIC - Πότε (`timestamp`)
# MAGIC - Τι (`operation`: WRITE, MERGE, DELETE, RESTORE...)
# MAGIC - Πόσα rows προσθέθηκαν/αφαιρέθηκαν (`operationMetrics`)
# MAGIC
# MAGIC Default retention: **30 ημέρες** (configurable με `delta.logRetentionDuration`).

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.aade.tax_declarations_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 2 — Audit log (business-level history)
# MAGIC
# MAGIC Το `data_contract_audit` είναι ο **business view**: ποιο rule έτρεξε, πότε, αν πέρασε, πόσα rows ήταν invalid.
# MAGIC Συμπληρώνει το Delta History με το **γιατί**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   run_id,
# MAGIC   run_timestamp,
# MAGIC   rule_id,
# MAGIC   rule_severity,
# MAGIC   passed,
# MAGIC   invalid_count,
# MAGIC   total_rows
# MAGIC FROM workspace.aade.data_contract_audit
# MAGIC ORDER BY run_timestamp DESC, rule_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 3 — Time Travel: VERSION AS OF
# MAGIC
# MAGIC Query σε **οποιαδήποτε προηγούμενη version** του Silver table σαν να ήταν live.
# MAGIC Killer feature του Delta για debugging + audit.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_at_v0
# MAGIC FROM workspace.aade.tax_declarations_silver VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Πόσα rows έχουμε σε κάθε version
# MAGIC SELECT
# MAGIC   version,
# MAGIC   timestamp,
# MAGIC   operation,
# MAGIC   operationMetrics.numOutputRows AS rows_written
# MAGIC FROM (DESCRIBE HISTORY workspace.aade.tax_declarations_silver)
# MAGIC ORDER BY version

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 4 — Simulate "bad batch" (corrupt write)
# MAGIC
# MAGIC Επίτηδες γράφουμε 2 fake records στο silver — σαν να είχε bug το pipeline.

# COMMAND ----------

# Type-safe approach: φτιάχνουμε bad batch ως SELECT από το ίδιο το silver,
# αλλάζοντας μόνο τα fields που θέλουμε. Αυτό εγγυάται type compatibility.
from pyspark.sql.functions import lit

# Παίρνουμε 2 rows από το silver ως template, τα μετατρέπουμε σε bad records
template = spark.table("workspace.aade.tax_declarations_silver").limit(2).toPandas()
template_rows = template.to_dict("records")

# Mark them as bad: high amount + flag στο Επωνυμία
bad_specs = [
    {"ΔηλωσηID": 99001, "Επωνυμία": "FAKE_RECORD_01", "Ποσό_EUR": 999999.0},
    {"ΔηλωσηID": 99002, "Επωνυμία": "FAKE_RECORD_02", "Ποσό_EUR": 888888.0},
]
for row, override in zip(template_rows, bad_specs):
    row.update(override)

# Convert back to Spark με matching schema
silver_schema = spark.table("workspace.aade.tax_declarations_silver").schema
bad_batch = spark.createDataFrame(template_rows, schema=silver_schema)

# Append → νέα version στο Delta log
bad_batch.write.format("delta").mode("append").saveAsTable("workspace.aade.tax_declarations_silver")

print(f"⚠️  Bad batch γράφτηκε. Νέα version δημιουργήθηκε.")
spark.sql("SELECT COUNT(*) AS total FROM workspace.aade.tax_declarations_silver").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 5 — Detect: τι άλλαξε στην τελευταία version;
# MAGIC
# MAGIC Σύγκρινε **current** vs **previous version** με `EXCEPT` και βρες τα new rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the last 2 versions
# MAGIC SELECT version, timestamp, operation, operationMetrics.numOutputRows AS rows
# MAGIC FROM (DESCRIBE HISTORY workspace.aade.tax_declarations_silver)
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 2

# COMMAND ----------

# Programmatic diff between current και previous version
history = spark.sql("DESCRIBE HISTORY workspace.aade.tax_declarations_silver").collect()
versions = sorted([r["version"] for r in history])
current_v = versions[-1]
previous_v = versions[-2]
print(f"Current version: {current_v}")
print(f"Previous (clean) version: {previous_v}")

new_rows = spark.sql(f"""
  SELECT * FROM workspace.aade.tax_declarations_silver VERSION AS OF {current_v}
  EXCEPT
  SELECT * FROM workspace.aade.tax_declarations_silver VERSION AS OF {previous_v}
""")
print(f"\n🔍 Νέα rows στη version {current_v} (δεν υπήρχαν στη {previous_v}):")
new_rows.select("`ΔηλωσηID`", "`ΑΦΜ`", "`Επωνυμία`", "`Ποσό_EUR`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6 — Unity Catalog Lineage UI
# MAGIC
# MAGIC Στο **Catalog Explorer** βλέπεις γραφικά:
# MAGIC - **Upstream**: από ποιο volume/bronze table γέμισε το silver
# MAGIC - **Downstream**: ποια queries / dashboards / jobs το χρησιμοποιούν
# MAGIC - **Users / notebooks**: ποιοι το διαβάζουν / γράφουν
# MAGIC
# MAGIC ### 🎯 Step-by-step για να δεις το Lineage tab
# MAGIC
# MAGIC 1. Click **Catalog** (αριστερό sidebar)
# MAGIC 2. Πλοήγηση: `workspace` → `aade` → `Tables`
# MAGIC 3. Click στο `tax_declarations_silver`
# MAGIC 4. Πάνω από τα data, click το tab **Lineage**
# MAGIC 5. Δες:
# MAGIC    - **Upstream**: `volume:/Volumes/workspace/aade/aade_data/declarations.csv` → silver
# MAGIC    - **Downstream**: queries, jobs, notebooks
# MAGIC 6. Click σε κάθε node για details
# MAGIC
# MAGIC ⚠️ **Capture lag**: ~15 min για να εμφανιστούν νέα events. Lineage capture χρειάζεται 3-part name (`catalog.schema.table`) στις queries.

# COMMAND ----------

# Print direct URL για το Catalog Explorer
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", default=None)
if workspace_url:
    catalog_url = f"https://{workspace_url}/explore/data/workspace/aade/tax_declarations_silver"
    print(f"🔗 Catalog Explorer URL για το silver:\n   {catalog_url}\n")
    print("   Click → tab 'Lineage'")
else:
    print("⚠️  Workspace URL not auto-detected. Manually: Catalog → workspace → aade → tax_declarations_silver → tab 'Lineage'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 7 — RESTORE: rollback στην clean version
# MAGIC
# MAGIC Τώρα που ξέρουμε ότι η `previous_v` ήταν clean, κάνουμε **point-in-time restore**.
# MAGIC
# MAGIC ⚠️ Το `RESTORE` δημιουργεί **νέα version** που είναι snapshot της προηγούμενης. Δεν διαγράφει history — ο audit trail διατηρείται.

# COMMAND ----------

# Defensive: re-derive previous_v αν δεν υπάρχει στο scope
# (π.χ. όταν τρέχει το cell μεμονωμένα μετά από compute restart)
try:
    previous_v
except NameError:
    print("ℹ️  Re-deriving previous_v από DESCRIBE HISTORY...")
    history = spark.sql("DESCRIBE HISTORY workspace.aade.tax_declarations_silver").collect()
    versions = sorted([r["version"] for r in history])
    if len(versions) < 2:
        raise RuntimeError(
            "Δεν υπάρχουν αρκετές versions για RESTORE. "
            "Τρέξε πρώτα τα Cells 0–5 (Run All) για να δημιουργηθεί η bad-batch version."
        )
    current_v = versions[-1]
    previous_v = versions[-2]
    print(f"   ✅ current={current_v}, previous={previous_v}\n")

spark.sql(f"RESTORE TABLE workspace.aade.tax_declarations_silver TO VERSION AS OF {previous_v}")

after = spark.sql("SELECT COUNT(*) AS total FROM workspace.aade.tax_declarations_silver").collect()[0]["total"]
print(f"✅ RESTORE complete. Silver τώρα έχει {after} rows (όσα είχε στη version {previous_v}).")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Νέα version (RESTORE) στο history
# MAGIC SELECT version, timestamp, operation, operationParameters
# MAGIC FROM (DESCRIBE HISTORY workspace.aade.tax_declarations_silver)
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 8 — Audit forensics: τι rules απέτυχαν τις τελευταίες 24h;
# MAGIC
# MAGIC Σε real production: scheduled query → Slack alert αν `severity = 'critical'` και `times_failed > 0`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   rule_id,
# MAGIC   rule_severity,
# MAGIC   COUNT(*) AS times_failed,
# MAGIC   SUM(invalid_count) AS total_invalid_rows,
# MAGIC   MAX(run_timestamp) AS last_failure
# MAGIC FROM workspace.aade.data_contract_audit
# MAGIC WHERE passed = false
# MAGIC   AND run_timestamp > current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY rule_id, rule_severity
# MAGIC ORDER BY rule_severity, times_failed DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Wrap-up
# MAGIC
# MAGIC | Δυνατότητα | Command | Use case |
# MAGIC |---|---|---|
# MAGIC | **Delta History** | `DESCRIBE HISTORY <table>` | "Τι έγινε σε αυτό το table τις τελευταίες 30 μέρες;" |
# MAGIC | **Time Travel** | `SELECT ... VERSION AS OF n` | "Πώς ήταν τα δεδομένα την Δευτέρα;" |
# MAGIC | **Diff versions** | `EXCEPT` between versions | "Τι rows προστέθηκαν στο τελευταίο batch;" |
# MAGIC | **RESTORE** | `RESTORE TABLE ... TO VERSION AS OF n` | "Bad batch — rollback τώρα." |
# MAGIC | **UC Lineage UI** | Catalog Explorer → Lineage tab | "Από πού ήρθε αυτό; Πού πάει;" |
# MAGIC | **Audit table** | Queries στο `data_contract_audit` | "Ποια rules έσπασαν τις τελευταίες 24h;" |
# MAGIC
# MAGIC ### Κλειστή ερώτηση για τους trainees
# MAGIC
# MAGIC > "Αν αύριο σου πει το business ότι το KPI είναι λάθος εδώ και 3 ημέρες, **πώς ξέρεις από ποιο silver/bronze write ξεκίνησε το λάθος**;"
# MAGIC
# MAGIC **Απάντηση**: συνδυασμός `DESCRIBE HISTORY` (πότε & ποιος) + UC Lineage (ποιο upstream τάισε) + audit table (αν κάποιο rule άλλαξε).
