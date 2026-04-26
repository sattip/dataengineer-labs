# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3 — Data Contract Validation Pipeline
# MAGIC
# MAGIC End-to-end demonstration of contract-driven data quality:
# MAGIC `Volume CSV → Schema check → Quality rules → Valid Silver / Quarantine / Audit`
# MAGIC
# MAGIC **Dataset**: ΑΑΔΕ tax declarations (300 synthetic records + 5 intentionally bad)
# MAGIC **Catalog**: `workspace.aade`
# MAGIC **Volume**: `/Volumes/workspace/aade/aade_data/`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Notebook structure**:
# MAGIC
# MAGIC | # | Step | Time |
# MAGIC |---|------|------|
# MAGIC | 0 | Setup + download από GitHub | 30s |
# MAGIC | 1 | Load YAML contract | 5s |
# MAGIC | 2 | Read declarations CSV | 10s |
# MAGIC | 2.5 | Inject 5 bad records (demo) | 5s |
# MAGIC | 3 | Schema validation | 5s |
# MAGIC | 4 | Run quality rules | 30s |
# MAGIC | 5 | Split valid / invalid | 10s |
# MAGIC | 6 | Write Silver + Quarantine + Audit | 30s |
# MAGIC | 7 | Apply governance tags | 5s |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0 — Setup + Download από GitHub
# MAGIC
# MAGIC Δημιουργεί το schema/volume και κατεβάζει τα 5 αρχεία απευθείας από το public repo.
# MAGIC No manual upload needed — όλα γίνονται προγραμματιστικά.

# COMMAND ----------

# Setup catalog/schema/volume + download Day 3 lab files από GitHub
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
print("✅ workspace.aade.aade_data ready\n")

import urllib.request

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
    try:
        urllib.request.urlretrieve(f"{REPO}/{fname}", f"{VOL}/{fname}")
        print(f"✅ {fname}")
    except Exception as e:
        print(f"❌ {fname}: {e}")

print("\n📂 Files in volume:")
for f in dbutils.fs.ls(VOL):
    print(f"  {f.name:<45s} {f.size/1024:>8.1f} KB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Load YAML Contract από Volume
# MAGIC
# MAGIC Διαβάζουμε το data contract YAML που μόλις κατεβάσαμε.
# MAGIC Σε production setup: το contract κάθεται σε Git repo και sync-άρεται στο workspace.

# COMMAND ----------

import yaml

CONTRACT_PATH = "/Volumes/workspace/aade/aade_data/aade_declarations_data_contract.yaml"

with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
    contract = yaml.safe_load(f)

print(f"📋 Loaded: {contract['contract_id']} v{contract['version']}")
print(f"   Owner:        {contract['owner']}")
print(f"   Domain:       {contract['domain']}")
print(f"   Schema:       {len(contract['schema'])} fields")
print(f"   Quality rules: {len(contract['quality_rules'])} rules")
print(f"   Target table: {contract['dataset']['name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Read Declarations CSV
# MAGIC
# MAGIC Φορτώνουμε τα 300 synthetic ΑΑΔΕ tax declarations από το volume.
# MAGIC `inferSchema=True` τραβάει types από τα data (timestamps, numerics).

# COMMAND ----------

DATA_PATH = "/Volumes/workspace/aade/aade_data/declarations.csv"

raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DATA_PATH)
)

print(f"📥 Loaded {raw_df.count()} rows")
raw_df.printSchema()
raw_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2.5 — Inject 5 Bad Records (για realistic demo)
# MAGIC
# MAGIC Το original CSV είναι clean. Για να δούμε **πώς το contract πιάνει failures**,
# MAGIC εισάγουμε εσκεμμένα 5 bad records — καθένα παραβιάζει διαφορετικό rule.
# MAGIC
# MAGIC | Bad row | Παραβιάζει |
# MAGIC |---------|-----------|
# MAGIC | 9001 | DQ002 — bad ΑΦΜ format (8 digits) |
# MAGIC | 9002 | DQ003 — negative Ποσό_EUR |
# MAGIC | 9003 | DQ006 — invalid Κατάσταση |
# MAGIC | 9004 | DQ005 — MonthNumber=15 |
# MAGIC | NULL | DQ001 — null primary key |

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import to_date, col

bad_rows = [
    # 1) Bad ΑΦΜ format (8 digits instead of 9)
    Row(ΔηλωσηID=9001, Ημερομηνία='2024-03-01', ΑΦΜ='12345678',
        Επωνυμία='Bad AFM Corp', ΔΟΥID=1, Κατηγορία_Φόρου='ΦΠΑ',
        Βάση_Φόρου=1000.0, Συντελεστής_Pct=24.0, Ποσό_EUR=240.0,
        Κατάσταση='Εγκεκριμένη', Περιφέρεια='Αττική', Πόλη='Αθήνα',
        ΥπάλληλοςID=1, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
    # 2) Negative tax amount
    Row(ΔηλωσηID=9002, Ημερομηνία='2024-03-02', ΑΦΜ='999999999',
        Επωνυμία='Negative Amount LLC', ΔΟΥID=2, Κατηγορία_Φόρου='Εισοδήματος',
        Βάση_Φόρου=5000.0, Συντελεστής_Pct=15.0, Ποσό_EUR=-100.0,  # 🚨
        Κατάσταση='Εκκρεμής', Περιφέρεια='Αττική', Πόλη='Πειραιάς',
        ΥπάλληλοςID=2, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
    # 3) Invalid Κατάσταση value
    Row(ΔηλωσηID=9003, Ημερομηνία='2024-03-03', ΑΦΜ='888888888',
        Επωνυμία='Bad Status SA', ΔΟΥID=3, Κατηγορία_Φόρου='Ακινήτων',
        Βάση_Φόρου=2000.0, Συντελεστής_Pct=10.0, Ποσό_EUR=200.0,
        Κατάσταση='ΑΓΝΩΣΤΗ',  # 🚨 not in allowed values
        Περιφέρεια='Αττική', Πόλη='Αθήνα',
        ΥπάλληλοςID=3, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
    # 4) MonthNumber out of range
    Row(ΔηλωσηID=9004, Ημερομηνία='2024-03-04', ΑΦΜ='777777777',
        Επωνυμία='Bad Month IKE', ΔΟΥID=4, Κατηγορία_Φόρου='ΦΠΑ',
        Βάση_Φόρου=3000.0, Συντελεστής_Pct=24.0, Ποσό_EUR=720.0,
        Κατάσταση='Εγκεκριμένη', Περιφέρεια='Αττική', Πόλη='Αθήνα',
        ΥπάλληλοςID=4, Φορ_Ετος=2024, MonthNumber=15,  # 🚨
        MonthName='Άγνωστος'),
    # 5) NULL primary key
    Row(ΔηλωσηID=None,  # 🚨
        Ημερομηνία='2024-03-05', ΑΦΜ='666666666',
        Επωνυμία='Null PK ΟΕ', ΔΟΥID=5, Κατηγορία_Φόρου='Μισθοδοσίας',
        Βάση_Φόρου=4000.0, Συντελεστής_Pct=12.0, Ποσό_EUR=480.0,
        Κατάσταση='Εκκρεμής', Περιφέρεια='Αττική', Πόλη='Αθήνα',
        ΥπάλληλοςID=5, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
]

bad_df = spark.createDataFrame(bad_rows)
bad_df = bad_df.withColumn('Ημερομηνία', to_date(col('Ημερομηνία')))

# Convert raw_df Ημερομηνία to date for schema match, then UNION
raw_df = raw_df.withColumn('Ημερομηνία', to_date(col('Ημερομηνία'))) \
               .unionByName(bad_df, allowMissingColumns=True)

print(f"📥 Total rows: {raw_df.count()} (300 clean + 5 bad)")
print("\nBad records preview:")
raw_df.filter("`ΔηλωσηID` >= 9001 OR `ΔηλωσηID` IS NULL") \
      .select('ΔηλωσηID', 'ΑΦΜ', 'Ποσό_EUR', 'Κατάσταση', 'MonthNumber') \
      .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Schema Validation
# MAGIC
# MAGIC Πρώτος έλεγχος: όλες οι columns που απαιτεί το contract υπάρχουν στα data.
# MAGIC Αυτός ο check πιάνει **breaking schema changes** πριν φτάσουν σε downstream pipelines.

# COMMAND ----------

expected_cols = {f["name"] for f in contract["schema"]}
actual_cols = set(raw_df.columns)

missing = expected_cols - actual_cols
extra = actual_cols - expected_cols

if missing:
    print(f"❌ MISSING required columns: {missing}")
else:
    print(f"✅ All {len(expected_cols)} required columns present")

if extra:
    print(f"ℹ️  Extra columns (not in contract): {extra}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — Run Quality Rules
# MAGIC
# MAGIC Τρέχουμε **όλα τα quality rules** του contract.
# MAGIC Κάθε rule έχει expression σε SQL — εκτελείται διά μέσου Spark `filter()`.
# MAGIC
# MAGIC **Severity levels**:
# MAGIC - `error` → row goes to quarantine (does not enter Silver)
# MAGIC - `warning` → flag for review (still goes to Silver)
# MAGIC
# MAGIC ### ⚠️ Greek column names need backticks
# MAGIC Spark SQL requires non-ASCII identifiers (όπως `ΔηλωσηID`, `ΑΦΜ`) να είναι σε backticks.
# MAGIC Ο `quote_greek_columns` helper το κάνει αυτόματα από τις expressions του YAML.

# COMMAND ----------

import re

def quote_greek_columns(expression: str, schema_fields: list) -> str:
    """Wrap Greek column names with backticks for Spark SQL compatibility.

    Iterates through column names in the contract schema and replaces
    bare references with backticked ones, only outside string literals.
    """
    result = expression
    # Sort by length desc to avoid partial replacements (e.g., 'ΑΦΜ' before 'ΑΦΜ_alt')
    col_names = sorted([f["name"] for f in schema_fields], key=len, reverse=True)

    for col_name in col_names:
        # Skip if column doesn't have non-ASCII chars (no need to quote)
        if col_name.isascii():
            continue
        # Replace col_name with `col_name` using word-boundary-ish regex
        # (?<![\w`]) — not preceded by word char or backtick (avoid double-quoting)
        # (?![\w`])  — not followed by word char or backtick
        pattern = r'(?<![\w`])' + re.escape(col_name) + r'(?![\w`])'
        result = re.sub(pattern, f'`{col_name}`', result)

    return result

results = []
total = raw_df.count()

for rule in contract["quality_rules"]:
    rule_id = rule["id"]
    raw_expr = rule["expression"]
    expr = quote_greek_columns(raw_expr, contract["schema"])
    severity = rule["severity"]

    # Count rows that FAIL the rule (i.e., NOT(expression))
    failed_count = raw_df.filter(f"NOT ({expr})").count()
    pass_pct = 100.0 * (total - failed_count) / total

    icon = "✅" if failed_count == 0 else "🚨"
    results.append({
        "rule_id": rule_id,
        "severity": severity,
        "failed_count": failed_count,
        "pass_pct": pass_pct,
        "expression": expr,         # quoted version (for execution)
        "expression_raw": raw_expr,  # original (for audit/portability)
        "action_on_fail": rule.get("action_on_fail", "quarantine"),
    })
    print(
        f"{icon} {rule_id} [{severity:7s}] "
        f"{failed_count:>3d} failures ({pass_pct:5.1f}% pass)  "
        f"— {raw_expr[:60]}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — Split Valid / Invalid Records
# MAGIC
# MAGIC Με βάση **όλους τους error-severity rules**, διαχωρίζουμε τα valid από τα invalid records.
# MAGIC Το `combined_fail` είναι OR ολων των NOT(expression) — αν παραβιαστεί έστω ένα rule, το row είναι invalid.

# COMMAND ----------

# Use the already-quoted expressions από το results array (contains quoted versions)
error_rules_quoted = [
    r["expression"] for r in results
    if r["severity"] == "error"
]
combined_fail = " OR ".join([f"NOT ({e})" for e in error_rules_quoted])

invalid_df = raw_df.filter(combined_fail)
valid_df = raw_df.filter(f"NOT ({combined_fail})")

print(f"✅ Valid rows:    {valid_df.count():>4d}")
print(f"🚨 Invalid rows:  {invalid_df.count():>4d}")

print("\nSample invalid records:")
invalid_df.select("`ΔηλωσηID`", "`ΑΦΜ`", "`Ποσό_EUR`", "`Κατάσταση`", "MonthNumber") \
          .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 — Write Silver / Quarantine / Audit Tables
# MAGIC
# MAGIC Τρία outputs σε Delta format:
# MAGIC 1. **Silver** — μόνο τα valid records (downstream pipelines)
# MAGIC 2. **Quarantine** — invalid records (manual review, NOT πεταμένα)
# MAGIC 3. **Audit log** — per-rule run history (compliance + debugging)

# COMMAND ----------

from datetime import datetime
from pyspark.sql import Row

# 1. Silver — only valid records
valid_df.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.aade.tax_declarations_silver")
print("✅ Silver written: workspace.aade.tax_declarations_silver")

# 2. Quarantine — invalid records preserved for review
invalid_df.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.aade.tax_declarations_quarantine")
print("🚨 Quarantine written: workspace.aade.tax_declarations_quarantine")

# 3. Audit — log of every contract run
audit_rows = [
    Row(
        contract_id=contract["contract_id"],
        contract_version=contract["version"],
        dataset=contract["dataset"]["name"],
        rule_id=r["rule_id"],
        severity=r["severity"],
        failed_count=int(r["failed_count"]),
        pass_pct=float(r["pass_pct"]),
        action_on_fail=r["action_on_fail"],
        checked_at=datetime.utcnow().isoformat(),
    )
    for r in results
]

audit_df = spark.createDataFrame(audit_rows)
audit_df.write.format("delta").mode("append") \
    .saveAsTable("workspace.aade.data_contract_audit")
print("📋 Audit appended: workspace.aade.data_contract_audit")

# Show audit results
print("\n📊 Audit log preview:")
audit_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7 — Apply Governance Tags
# MAGIC
# MAGIC Σημαίνουμε το Silver table με Unity Catalog tags για discoverability + governance.
# MAGIC Αυτά τα tags είναι **searchable** στο Catalog Explorer και sync-άρονται με Purview
# MAGIC σε enterprise setup.

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE workspace.aade.tax_declarations_silver
    SET TAGS (
        'contract_id'      = '{contract["contract_id"]}',
        'contract_version' = '{contract["version"]}',
        'classification'   = 'confidential',
        'has_pii'          = 'true',
        'pii_columns'      = 'ΑΦΜ,Επωνυμία',
        'owner'            = 'aade-data-engineering',
        'source_system'    = 'TAXIS',
        'retention'        = '7_years',
        'lab'              = 'day3_data_contract'
    )
""")

# Add column comments for self-documentation
spark.sql("""
    ALTER TABLE workspace.aade.tax_declarations_silver
    ALTER COLUMN `ΑΦΜ` COMMENT 'PII: Taxpayer tax ID — apply masking policy for analyst roles'
""")
spark.sql("""
    ALTER TABLE workspace.aade.tax_declarations_silver
    ALTER COLUMN `Επωνυμία` COMMENT 'PII: Taxpayer legal name — first letter only for analysts'
""")

print("🏷️  Tags + comments applied to workspace.aade.tax_declarations_silver")
print("\nVerify στο Catalog Explorer → workspace → aade → tax_declarations_silver → Overview")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Pipeline Complete
# MAGIC
# MAGIC Έχουμε δημιουργήσει 3 Delta tables στο `workspace.aade`:
# MAGIC
# MAGIC | Table | Rows | Purpose |
# MAGIC |-------|------|---------|
# MAGIC | `tax_declarations_silver` | 300 | Production-ready clean data → Gold/BI |
# MAGIC | `tax_declarations_quarantine` | 5 | Failed records για manual review |
# MAGIC | `data_contract_audit` | 8 | Per-rule run history (audit trail) |
# MAGIC
# MAGIC ## Επόμενα βήματα
# MAGIC - **Στο Day 3**: συνέχεια με Bronze/Silver/Gold orchestration
# MAGIC - **Στο Day 5**: monitoring αυτών των rules σαν SLO/alerts
# MAGIC - **Στο Day 6**: capstone — όλα μαζί σε production-grade pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Verification queries (optional, για debugging)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Δες την κατάσταση των 3 tables
# MAGIC SELECT 'silver' AS layer, COUNT(*) AS rows FROM workspace.aade.tax_declarations_silver
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine', COUNT(*) FROM workspace.aade.tax_declarations_quarantine
# MAGIC UNION ALL
# MAGIC SELECT 'audit', COUNT(*) FROM workspace.aade.data_contract_audit;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Audit history — ποιο rule απέτυχε πιο πολύ;
# MAGIC SELECT
# MAGIC   rule_id,
# MAGIC   severity,
# MAGIC   failed_count,
# MAGIC   pass_pct,
# MAGIC   action_on_fail
# MAGIC FROM workspace.aade.data_contract_audit
# MAGIC ORDER BY checked_at DESC, failed_count DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Τα tags που εφαρμόσαμε στο Silver
# MAGIC SELECT *
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'aade'
# MAGIC   AND table_name = 'tax_declarations_silver';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cleanup (optional)
# MAGIC
# MAGIC Αν θέλεις να ξανατρέξεις το lab από την αρχή, αυτά τα cells κάνουν reset:

# COMMAND ----------

# DROP TABLE IF EXISTS workspace.aade.tax_declarations_silver;
# DROP TABLE IF EXISTS workspace.aade.tax_declarations_quarantine;
# DROP TABLE IF EXISTS workspace.aade.data_contract_audit;
# print("🗑️  Tables dropped — uncomment lines above to actually run")
