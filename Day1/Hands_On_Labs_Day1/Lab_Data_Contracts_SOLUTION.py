# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ SOLUTION — Data Contracts: Validating What You Receive
# MAGIC
# MAGIC **Ημέρα 1 · Trainer reference / λύση**
# MAGIC
# MAGIC > Αυτό είναι το **πλήρες** notebook με όλα τα `____` συμπληρωμένα + worked λύσεις στα stretch.
# MAGIC > Δώσε το στους μαθητές **μόνο μετά** την προσπάθεια. Το student version είναι το `Lab_Data_Contracts.py`.
# MAGIC >
# MAGIC > Κάθε σημείο που ήταν κενό στο student version επισημαίνεται με `# ✅ ΛΥΣΗ (TODO N)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📚 ΘΕΩΡΙΑ — δες το student notebook (Lab_Data_Contracts.py)
# MAGIC Η θεωρία είναι ίδια· εδώ κρατάμε μόνο τον κώδικα-λύση για να τρέχει end-to-end.

# COMMAND ----------

# DBTITLE 1,Step 0 — Auto-bootstrap (ίδιο με student)
import csv, io, random

CATALOG      = "gt_lab"
LANDING_PATH = f"/Volumes/{CATALOG}/bronze/landing"
CSV_PATH     = f"{LANDING_PATH}/declarations.csv"
YAML_PATH    = f"{LANDING_PATH}/aade_declarations_data_contract.yaml"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for s in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")

random.seed(42)
MONTHS = {1:"Ιανουάριος",2:"Φεβρουάριος",3:"Μάρτιος",4:"Απρίλιος",5:"Μάιος",6:"Ιούνιος",
          7:"Ιούλιος",8:"Αύγουστος",9:"Σεπτέμβριος",10:"Οκτώβριος",11:"Νοέμβριος",12:"Δεκέμβριος"}
KATHG  = ["ΦΠΑ", "Εισοδήματος", "Ακινήτων", "Μισθοδοσίας"]
KATAST = ["Εγκεκριμένη", "Εκκρεμής", "Απορριφθείσα"]
CITIES = [("Αττική","Αθήνα"),("Αττική","Πειραιάς"),("Κεντρικής Μακεδονίας","Θεσσαλονίκη"),
          ("Θεσσαλίας","Λάρισα"),("Κρήτης","Ηράκλειο"),("Δυτικής Ελλάδας","Πάτρα")]
EPWN   = ["Παπαδόπουλος ΑΕ","Γεωργίου ΟΕ","Δημητρίου ΕΠΕ","Νικολάου ΙΚΕ","Αντωνίου ΑΕ",
          "Βασιλείου ΟΕ","Ιωάννου ΕΠΕ","Κωνσταντίνου ΙΚΕ","Μιχαήλ ΑΕ","Ελευθερίου ΟΕ"]
HEADER = ["ΔηλωσηID","Ημερομηνία","ΑΦΜ","Επωνυμία","ΔΟΥID","Κατηγορία_Φόρου","Βάση_Φόρου",
          "Συντελεστής_Pct","Ποσό_EUR","Κατάσταση","Περιφέρεια","Πόλη","ΥπάλληλοςID",
          "Φορ_Ετος","MonthNumber","MonthName"]

rows = []
for i in range(1, 301):
    month = random.randint(1, 12)
    base  = round(random.uniform(500, 50000), 2)
    rate  = round(random.choice([6.0, 13.0, 15.0, 24.0]), 1)
    amount = round(base * rate / 100, 2)
    perif, poli = random.choice(CITIES)
    rows.append([
        i, f"2024-{month:02d}-{random.randint(1,28):02d}",
        f"{random.randint(100000000, 999999999)}", random.choice(EPWN),
        random.randint(1, 9), random.choice(KATHG), base, rate, amount,
        random.choice(KATAST), perif, poli, random.randint(1, 9),
        2024, month, MONTHS[month],
    ])

if any(f.name == "declarations.csv" for f in dbutils.fs.ls(LANDING_PATH)) is False:
    buf = io.StringIO(); w = csv.writer(buf); w.writerow(HEADER); w.writerows(rows)
    with open(CSV_PATH, "w", encoding="utf-8") as fp:
        fp.write(buf.getvalue())
    print(f"✅ Generated declarations.csv ({len(rows)} καθαρά rows)")
else:
    print("✅ declarations.csv υπάρχει ήδη")

CONTRACT_YAML = """\
contract_id: aade.tax_declarations.v1
version: 1.0.0
owner: AADE Data Engineering Team
domain: Tax Declarations
layer: silver
source_system: TAXIS

dataset:
  name: gt_lab.silver.tax_declarations_silver
  description: Cleaned tax declarations from TAXIS, ready for analytics.
  primary_key:
    - ΔηλωσηID
  retention: 7_years

schema:
  - {name: ΔηλωσηID,        type: integer, nullable: false}
  - {name: Ημερομηνία,      type: date,    nullable: false}
  - {name: ΑΦΜ,             type: string,  nullable: false, pii: true}
  - {name: Επωνυμία,        type: string,  nullable: false, pii: true}
  - {name: ΔΟΥID,           type: integer, nullable: false}
  - {name: Κατηγορία_Φόρου, type: string,  nullable: false}
  - {name: Βάση_Φόρου,      type: double,  nullable: false}
  - {name: Συντελεστής_Pct, type: double,  nullable: false}
  - {name: Ποσό_EUR,        type: double,  nullable: false}
  - {name: Κατάσταση,       type: string,  nullable: false}
  - {name: Περιφέρεια,      type: string,  nullable: false}
  - {name: Πόλη,            type: string,  nullable: false}
  - {name: ΥπάλληλοςID,     type: integer, nullable: true}
  - {name: Φορ_Ετος,        type: integer, nullable: false}
  - {name: MonthNumber,     type: integer, nullable: false}
  - {name: MonthName,       type: string,  nullable: false}

quality_rules:
  - id: DQ001
    name: primary_key_not_null
    severity: error
    expression: ΔηλωσηID IS NOT NULL
    action_on_fail: quarantine
  - id: DQ002
    name: afm_9_digits
    severity: error
    expression: ΑΦΜ IS NOT NULL AND ΑΦΜ RLIKE '^[0-9]{9}$'
    action_on_fail: quarantine
  - id: DQ003
    name: tax_amount_non_negative
    severity: error
    expression: Ποσό_EUR >= 0
    action_on_fail: quarantine
  - id: DQ004
    name: tax_rate_valid_range
    severity: error
    expression: Συντελεστής_Pct BETWEEN 0 AND 100
    action_on_fail: quarantine
  - id: DQ005
    name: month_number_valid
    severity: error
    expression: MonthNumber BETWEEN 1 AND 12
    action_on_fail: quarantine
  - id: DQ006
    name: allowed_tax_status
    severity: error
    expression: Κατάσταση IN ('Εγκεκριμένη', 'Εκκρεμής', 'Απορριφθείσα')
    action_on_fail: quarantine
  - id: DQ007
    name: amount_matches_base_and_rate
    severity: warning
    expression: abs(Ποσό_EUR - (Βάση_Φόρου * Συντελεστής_Pct / 100)) <= 1.0
    action_on_fail: flag_for_review
  - id: DQ008
    name: declaration_date_not_future
    severity: error
    expression: Ημερομηνία <= current_date()
    action_on_fail: quarantine

security:
  classification: confidential
  pii_columns: [ΑΦΜ, Επωνυμία]

publishing:
  if_error_rules_fail: quarantine_and_continue
  write_valid_to: gt_lab.silver.tax_declarations_silver
  write_invalid_to: gt_lab.silver.tax_declarations_quarantine
  write_results_to: gt_lab.silver.data_contract_audit
"""
with open(YAML_PATH, "w", encoding="utf-8") as fp:
    fp.write(CONTRACT_YAML)
print(f"✅ Έγραψα το contract: {YAML_PATH}")

# COMMAND ----------

# DBTITLE 1,Step 1 — Load contract
import yaml

CONTRACT_PATH = "/Volumes/gt_lab/bronze/landing/aade_declarations_data_contract.yaml"
with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
    contract = yaml.safe_load(f)   # ✅ ΛΥΣΗ (TODO 1)

print(f"📋 {contract['contract_id']} v{contract['version']}")
print(f"   Schema fields: {len(contract['schema'])}  ·  Quality rules: {len(contract['quality_rules'])}")
print(f"   Target table:  {contract['dataset']['name']}")

# COMMAND ----------

# DBTITLE 1,Step 2 — Read declarations CSV
from pyspark.sql.functions import to_date, col

DATA_PATH = "/Volumes/gt_lab/bronze/landing/declarations.csv"
raw_df = (
    spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH)
    .withColumn("Ημερομηνία", to_date(col("Ημερομηνία")))
)
print(f"📥 Loaded {raw_df.count()} rows")
display(raw_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Step 2.5 — Inject bad rows
from pyspark.sql import Row

bad_rows = [
    Row(ΔηλωσηID=9001, Ημερομηνία='2024-03-01', ΑΦΜ='12345678',
        Επωνυμία='Bad AFM AE', ΔΟΥID=1, Κατηγορία_Φόρου='ΦΠΑ', Βάση_Φόρου=1000.0,
        Συντελεστής_Pct=24.0, Ποσό_EUR=240.0, Κατάσταση='Εγκεκριμένη',
        Περιφέρεια='Αττική', Πόλη='Αθήνα', ΥπάλληλοςID=1, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
    Row(ΔηλωσηID=9002, Ημερομηνία='2024-03-02', ΑΦΜ='999999999',
        Επωνυμία='Negative OE', ΔΟΥID=2, Κατηγορία_Φόρου='Εισοδήματος', Βάση_Φόρου=5000.0,
        Συντελεστής_Pct=15.0, Ποσό_EUR=-100.0, Κατάσταση='Εκκρεμής',
        Περιφέρεια='Αττική', Πόλη='Πειραιάς', ΥπάλληλοςID=2, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
    Row(ΔηλωσηID=9003, Ημερομηνία='2024-03-03', ΑΦΜ='888888888',
        Επωνυμία='Bad Status EPE', ΔΟΥID=3, Κατηγορία_Φόρου='Ακινήτων', Βάση_Φόρου=2000.0,
        Συντελεστής_Pct=10.0, Ποσό_EUR=200.0, Κατάσταση='ΑΓΝΩΣΤΗ',
        Περιφέρεια='Αττική', Πόλη='Αθήνα', ΥπάλληλοςID=3, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
    Row(ΔηλωσηID=9004, Ημερομηνία='2024-03-04', ΑΦΜ='777777777',
        Επωνυμία='Bad Month IKE', ΔΟΥID=4, Κατηγορία_Φόρου='ΦΠΑ', Βάση_Φόρου=3000.0,
        Συντελεστής_Pct=24.0, Ποσό_EUR=720.0, Κατάσταση='Εγκεκριμένη',
        Περιφέρεια='Αττική', Πόλη='Αθήνα', ΥπάλληλοςID=4, Φορ_Ετος=2024, MonthNumber=15, MonthName='Άγνωστος'),
    Row(ΔηλωσηID=None, Ημερομηνία='2024-03-05', ΑΦΜ='666666666', Επωνυμία='Null PK OE', ΔΟΥID=5,
        Κατηγορία_Φόρου='Μισθοδοσίας', Βάση_Φόρου=4000.0, Συντελεστής_Pct=12.0, Ποσό_EUR=480.0,
        Κατάσταση='Εκκρεμής', Περιφέρεια='Αττική', Πόλη='Αθήνα', ΥπάλληλοςID=5, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
]
bad_df = spark.createDataFrame(bad_rows).withColumn("Ημερομηνία", to_date(col("Ημερομηνία")))
raw_df = raw_df.unionByName(bad_df, allowMissingColumns=True)
print(f"📥 Σύνολο: {raw_df.count()} rows (300 + 5)")

# COMMAND ----------

# DBTITLE 1,Step 3 — Schema validation
expected_cols = {f["name"] for f in contract["schema"]}
actual_cols   = set(raw_df.columns)

missing = expected_cols - actual_cols   # ✅ ΛΥΣΗ (TODO 2)
extra   = actual_cols - expected_cols

if missing:
    raise ValueError(f"❌ SCHEMA DRIFT — λείπουν required στήλες: {missing}")
print(f"✅ Schema OK — και οι {len(expected_cols)} required στήλες υπάρχουν")
if extra:
    print(f"ℹ️  Extra στήλες (εκτός contract): {extra}")

# COMMAND ----------

# DBTITLE 1,Step 4 — Run quality rules
import re

def quote_greek_columns(expression, schema_fields):
    result = expression
    for col_name in sorted([f["name"] for f in schema_fields], key=len, reverse=True):
        if col_name.isascii():
            continue
        result = re.sub(r'(?<![\w`])' + re.escape(col_name) + r'(?![\w`])', f'`{col_name}`', result)
    return result

results = []
for rule in contract["quality_rules"]:
    raw_expr = rule["expression"]
    expr     = quote_greek_columns(raw_expr, contract["schema"])
    failed_count = raw_df.filter(f"NOT ({expr})").count()   # ✅ ΛΥΣΗ (TODO 3)
    results.append({
        "rule_id": rule["id"], "severity": rule["severity"],
        "failed_count": failed_count, "expression": expr, "expression_raw": raw_expr,
        "action_on_fail": rule.get("action_on_fail", "quarantine"),
    })
    icon = "✅" if failed_count == 0 else "🚨"
    print(f"{icon} {rule['id']} [{rule['severity']:7s}] {failed_count:>3d} failures — {raw_expr[:55]}")

# COMMAND ----------

# DBTITLE 1,Step 5 — Split valid / invalid
error_rules   = [r["expression"] for r in results if r["severity"] == "error"]          # ✅ ΛΥΣΗ (TODO 4)
combined_fail = " OR ".join([f"NOT ({e})" for e in error_rules])                        # ✅ ΛΥΣΗ (TODO 5)

invalid_df = raw_df.filter(combined_fail)
valid_df   = raw_df.filter(f"NOT ({combined_fail})")                                    # ✅ ΛΥΣΗ (TODO 6)

print(f"✅ Valid:   {valid_df.count():>4d}")
print(f"🚨 Invalid: {invalid_df.count():>4d}")
display(invalid_df.select("ΔηλωσηID", "ΑΦΜ", "Ποσό_EUR", "Κατάσταση", "MonthNumber"))

# COMMAND ----------

# DBTITLE 1,Step 6 — Write the 3 tables
from datetime import datetime

WRITE_MODE   = "overwrite"                                   # ✅ ΛΥΣΗ (TODO 7a)
SILVER_TABLE = contract["publishing"]["write_valid_to"]      # ✅ ΛΥΣΗ (TODO 7b)
valid_df.write.format("delta").mode(WRITE_MODE).saveAsTable(SILVER_TABLE)
print(f"✅ Silver → {SILVER_TABLE}")

QUARANTINE_TABLE = contract["publishing"]["write_invalid_to"]  # ✅ ΛΥΣΗ (TODO 8)
invalid_df.write.format("delta").mode("overwrite").saveAsTable(QUARANTINE_TABLE)
print(f"🚨 Quarantine → {QUARANTINE_TABLE}")

audit_rows = [
    Row(contract_id=contract["contract_id"], contract_version=contract["version"],
        rule_id=r["rule_id"], severity=r["severity"], failed_count=int(r["failed_count"]),
        action_on_fail=r["action_on_fail"], checked_at=datetime.utcnow().isoformat())
    for r in results
]
spark.createDataFrame(audit_rows).write.format("delta").mode("append") \
     .saveAsTable("gt_lab.silver.data_contract_audit")
print("📋 Audit → gt_lab.silver.data_contract_audit")
display(spark.table("gt_lab.silver.data_contract_audit").orderBy("rule_id"))

# COMMAND ----------

# DBTITLE 1,Step 7 — Apply governance tags
sec = contract["security"]
classification = sec["classification"]            # ✅ ΛΥΣΗ (TODO 9a)
pii_columns    = ",".join(sec["pii_columns"])     # ✅ ΛΥΣΗ (TODO 9b)
has_pii        = "true" if sec.get("pii_columns") else "false"

spark.sql(f"""
    ALTER TABLE {SILVER_TABLE} SET TAGS (
        'contract_id'      = '{contract["contract_id"]}',
        'contract_version' = '{contract["version"]}',
        'classification'   = '{classification}',
        'has_pii'          = '{has_pii}',
        'pii_columns'      = '{pii_columns}',
        'owner'            = 'aade-data-engineering',
        'source_system'    = 'TAXIS',
        'lab'              = 'day1_data_contract'
    )
""")
print(f"🏷️  Tags εφαρμόστηκαν στο {SILVER_TABLE}")

# COMMAND ----------

# DBTITLE 1,Auto-verification
checks = []
for tbl, want, label in [
    ("gt_lab.silver.tax_declarations_silver", 300, "Silver = 300"),
    ("gt_lab.silver.tax_declarations_quarantine", 5, "Quarantine = 5"),
]:
    try:
        n = spark.table(tbl).count(); checks.append((f"{label} (got {n})", n == want))
    except Exception:
        checks.append((f"{tbl} exists", False))
try:
    n = spark.table("gt_lab.silver.data_contract_audit").count()
    checks.append((f"Audit ≥ 8 (got {n})", n >= 8))
except Exception:
    checks.append(("Audit exists", False))

passed = sum(1 for _, ok in checks if ok)
print("=" * 50)
for name, ok in checks:
    print(f"  {'✅' if ok else '❌'} {name}")
print(f"  {passed}/{len(checks)} passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Stretch 1 — Worked: δικό σου rule (Βάση_Φόρου > 0)

# COMMAND ----------

# DBTITLE 1,STRETCH 1 — Solution
# Πρόσθεσε rule + 1 bad row, ξανατρέξε τα Steps 4-6 (εδώ inline mini-version)
contract["quality_rules"].append({
    "id": "DQ009", "name": "tax_base_positive", "severity": "error",
    "expression": "Βάση_Φόρου > 0", "action_on_fail": "quarantine",
})
zero_base = spark.createDataFrame([Row(
    ΔηλωσηID=9005, Ημερομηνία=None, ΑΦΜ='555555555', Επωνυμία='Zero Base AE', ΔΟΥID=1,
    Κατηγορία_Φόρου='ΦΠΑ', Βάση_Φόρου=0.0, Συντελεστής_Pct=24.0, Ποσό_EUR=0.0,
    Κατάσταση='Εκκρεμής', Περιφέρεια='Αττική', Πόλη='Αθήνα', ΥπάλληλοςID=1,
    Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος')]).withColumn("Ημερομηνία", to_date(col("Ημερομηνία")))
test_df = raw_df.unionByName(zero_base, allowMissingColumns=True)
new_rule = quote_greek_columns("Βάση_Φόρου > 0", contract["schema"])
print(f"DQ009 failures: {test_df.filter(f'NOT ({new_rule})').count()}  (περιμένουμε ≥ 1)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Stretch 2 — Worked: publishing gate (> 5% invalid → reject)

# COMMAND ----------

# DBTITLE 1,STRETCH 2 — Solution
bad_pct = 100 * invalid_df.count() / raw_df.count()
THRESHOLD = 5.0
print(f"Invalid: {bad_pct:.2f}%  (threshold {THRESHOLD}%)")
if bad_pct > THRESHOLD:
    raise ValueError(f"🚨 Batch rejected: {bad_pct:.1f}% invalid > {THRESHOLD}% — stop pipeline")
else:
    print("✅ Batch εντός ορίων — publish OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Stretch 3 — Worked: PII comments + masked view

# COMMAND ----------

# DBTITLE 1,STRETCH 3 — Solution
spark.sql("ALTER TABLE gt_lab.silver.tax_declarations_silver "
          "ALTER COLUMN `ΑΦΜ` COMMENT 'PII: ΑΦΜ — masking policy για analyst roles'")
spark.sql("ALTER TABLE gt_lab.silver.tax_declarations_silver "
          "ALTER COLUMN `Επωνυμία` COMMENT 'PII: Επωνυμία — first-letter masking για analysts'")

spark.sql("""
    CREATE OR REPLACE VIEW gt_lab.silver.tax_declarations_masked AS
    SELECT
        `ΔηλωσηID`,
        CONCAT('*****', SUBSTRING(`ΑΦΜ`, 6, 4))      AS ΑΦΜ_masked,
        SUBSTRING(`Επωνυμία`, 1, 1)                  AS Επωνυμία_initial,
        `Κατηγορία_Φόρου`, `Ποσό_EUR`, `Κατάσταση`, `Περιφέρεια`
    FROM gt_lab.silver.tax_declarations_silver
""")
display(spark.table("gt_lab.silver.tax_declarations_masked").limit(5))
