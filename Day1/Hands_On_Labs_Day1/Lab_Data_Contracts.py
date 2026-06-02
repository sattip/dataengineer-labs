# Databricks notebook source
# MAGIC %md
# MAGIC # 🤝 LAB — Data Contracts: Validating What You Receive
# MAGIC
# MAGIC **Ημέρα 1 · Solo ή ζευγάρια · ⏱ 80 λεπτά**
# MAGIC
# MAGIC > Στόχος: Να χτίσεις ένα **contract-driven quality pipeline** — δηλαδή ένα pipeline που **πρώτα ελέγχει** αν τα δεδομένα τηρούν μια συμφωνημένη «συμφωνία» (contract) και **μετά** τα γράφει. Τα καλά πάνε στο Silver, τα κακά στην καραντίνα, και κρατάμε audit trail.
# MAGIC
# MAGIC ## 📑 Δομή Notebook
# MAGIC 1. 📚 **Θεωρία** (20') — Τι είναι data contract, γιατί, και πώς μοιάζει
# MAGIC 2. 🎯 **Εκφώνηση** — Τι θα φτιάξεις
# MAGIC 3. ✍️ **Συμπλήρωσε τα κενά** (45') — Ο κορμός του κώδικα σου δίνεται· εσύ συμπληρώνεις τα `____`
# MAGIC 4. ✅ **Verification** (5') — Auto-check ότι δουλεύει
# MAGIC 5. 🚀 **Stretch** — Για όσους τελειώσουν νωρίς
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✍️ Πώς δουλεύει το «fill-in-the-blanks»
# MAGIC
# MAGIC Σε κάθε code cell θα δεις **σχεδόν ολοκληρωμένο** κώδικα με μερικά κενά γραμμένα ως `____`
# MAGIC (τέσσερις κάτω παύλες). Πάνω από κάθε κενό υπάρχει ένα σχόλιο:
# MAGIC
# MAGIC - **ΤΙ** → τι πρέπει να κάνει η γραμμή
# MAGIC - **ΓΙΑΤΙ** → γιατί το κάνουμε έτσι (η σημαντική γνώση)
# MAGIC - **HINT** → το σχήμα της απάντησης
# MAGIC
# MAGIC > 🔴 **Αν αφήσεις ένα `____`, θα πάρεις `NameError: name '____' is not defined`.**
# MAGIC > Αυτό είναι το σινιάλο σου ότι ένα κενό έμεινε ασυμπλήρωτο — όχι bug. Βρες το και συμπλήρωσέ το.
# MAGIC
# MAGIC Λύση υπάρχει στο `Lab_Data_Contracts_SOLUTION.py` — **κοίτα την μόνο αφού προσπαθήσεις**.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📚 ΜΕΡΟΣ 1 — ΘΕΩΡΙΑ
# MAGIC
# MAGIC ## 1.1 Τι είναι ένα Data Contract;
# MAGIC
# MAGIC Ένα **Data Contract** είναι μια **γραπτή, εκτελέσιμη συμφωνία** ανάμεσα σε αυτόν που **παράγει** τα δεδομένα (source system, π.χ. TAXIS) και σε αυτόν που τα **καταναλώνει** (το pipeline σου, το Silver layer).
# MAGIC
# MAGIC > 🤝 «Εγώ, το source, σου εγγυώμαι ότι θα στείλω δεδομένα με **αυτές** τις στήλες, **αυτούς** τους τύπους και **αυτούς** τους κανόνες ποιότητας. Κι εσύ, το Bronze/Silver, **θα απορρίψεις** ό,τι δεν τηρεί τη συμφωνία.»
# MAGIC
# MAGIC Είναι ταυτόχρονα:
# MAGIC - **Documentation** — λέει σε όλη την ομάδα τι σημαίνει «σωστό data»
# MAGIC - **Code** — εκτελείται αυτόματα σε κάθε run και πιάνει τα προβλήματα
# MAGIC - **SLA** — όροι: freshness (πότε έρχονται), completeness (πόσα rows), schema (ποιες στήλες)
# MAGIC
# MAGIC ## 1.2 Γιατί το χρειαζόμαστε; (Το πρόβλημα)
# MAGIC
# MAGIC **Χωρίς contract:**
# MAGIC - Το source αλλάζει μια στήλη `amount` → `amount_eur` χωρίς να σου πει. Το pipeline σπάει στις 3 το πρωί.
# MAGIC - Μπαίνει ένα αρνητικό ποσό φόρου. Κανείς δεν το πιάνει. Φτάνει στο dashboard του διευθυντή.
# MAGIC - Ένα `NULL` primary key χαλάει όλα τα downstream joins σιωπηλά.
# MAGIC
# MAGIC **Με contract:**
# MAGIC - Breaking schema change → **σταματάμε αμέσως** με καθαρό μήνυμα, πριν μολυνθεί το Silver.
# MAGIC - Τα κακά records πάνε σε **quarantine** για review — δεν χάνονται, αλλά δεν μολύνουν.
# MAGIC - Κάθε run αφήνει **audit trail** (ποιος κανόνας απέτυχε, πόσες φορές) για compliance.
# MAGIC
# MAGIC ## 1.3 Τα 3 outputs ενός contract pipeline
# MAGIC
# MAGIC ```
# MAGIC                          ┌──────────────► ✅ SILVER       (μόνο τα καθαρά → downstream/BI)
# MAGIC  CSV ──► [CONTRACT] ─────┤
# MAGIC                          ├──────────────► 🚨 QUARANTINE   (τα κακά → manual review)
# MAGIC                          └──────────────► 📋 AUDIT        (per-rule ιστορικό → compliance)
# MAGIC ```
# MAGIC
# MAGIC Το «πέταμα» των κακών records είναι **λάθος**. Τα βάζουμε σε καραντίνα γιατί:
# MAGIC - Μπορεί να φταίει το rule, όχι το data.
# MAGIC - Ένας άνθρωπος πρέπει να τα δει (π.χ. φορολογική δήλωση που μπλοκαρίστηκε).
# MAGIC - Compliance: πρέπει να αποδείξεις *τι* απέρριψες και *γιατί*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Πώς μοιάζει ένα contract (YAML)
# MAGIC
# MAGIC Γράφουμε το contract σε **YAML** — human-readable, version-controlled σε Git, ανεξάρτητο από τη γλώσσα του pipeline.
# MAGIC
# MAGIC ```yaml
# MAGIC contract_id: aade.tax_declarations.v1
# MAGIC version: 1.0.0
# MAGIC owner: AADE Data Engineering Team
# MAGIC
# MAGIC dataset:
# MAGIC   name: gt_lab.silver.tax_declarations_silver
# MAGIC   primary_key: [ΔηλωσηID]
# MAGIC
# MAGIC schema:                          # ← ΠΟΙΕΣ στήλες & τύποι (πιάνει schema drift)
# MAGIC   - name: ΔηλωσηID
# MAGIC     type: integer
# MAGIC     nullable: false
# MAGIC   - name: ΑΦΜ
# MAGIC     type: string
# MAGIC     pii: true                    # ← σημαδεύουμε PII για masking/governance
# MAGIC
# MAGIC quality_rules:                   # ← ΚΑΝΟΝΕΣ που πρέπει να ισχύουν (πιάνει bad values)
# MAGIC   - id: DQ002
# MAGIC     severity: error              # error → quarantine | warning → flag μόνο
# MAGIC     expression: ΑΦΜ RLIKE '^[0-9]{9}$'
# MAGIC     action_on_fail: quarantine
# MAGIC ```
# MAGIC
# MAGIC ### Δύο επίπεδα ελέγχου
# MAGIC | Επίπεδο | Τι πιάνει | Παράδειγμα |
# MAGIC |---|---|---|
# MAGIC | **schema** | Λείπει/άλλαξε στήλη | `amount` → `amount_eur` |
# MAGIC | **quality_rules** | Κακή τιμή σε σωστή στήλη | αρνητικό ποσό, `NULL` PK, ΑΦΜ 8 ψηφίων |
# MAGIC
# MAGIC ### Severity → action
# MAGIC - `error` → το row πάει **quarantine** (ΔΕΝ μπαίνει στο Silver)
# MAGIC - `warning` → flag για review, αλλά **μπαίνει** στο Silver (π.χ. «το ποσό δεν ταιριάζει ακριβώς με βάση×συντελεστή»)
# MAGIC
# MAGIC ## 1.5 Production equivalents (πού θα το δεις «στ' αλήθεια»)
# MAGIC | Σήμερα εδώ | Στην παραγωγή |
# MAGIC |---|---|
# MAGIC | YAML contract | dbt schema tests, Soda Checks, Great Expectations suite |
# MAGIC | `validate()` loop | Delta Live Tables `@dlt.expect_or_drop` / `@dlt.expect_or_fail` |
# MAGIC | quarantine table | DLT quarantine / dead-letter table |
# MAGIC | audit table | DLT event log, Soda scan results |
# MAGIC | versioning | Git + semver στο `version:` του YAML |
# MAGIC
# MAGIC > 💡 Δεν εφευρίσκουμε κάτι εξωτικό — χτίζουμε **με το χέρι** το concept που μετά το κάνουν τα tools για σένα. Όταν καταλάβεις *γιατί*, τα tools γίνονται προφανή.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎯 ΜΕΡΟΣ 2 — ΕΚΦΩΝΗΣΗ
# MAGIC
# MAGIC ## Σενάριο
# MAGIC
# MAGIC Είσαι Data Engineer στην **ΑΑΔΕ**. Κάθε πρωί κατεβαίνει ένα CSV με **φορολογικές δηλώσεις** από το TAXIS. Μέχρι σήμερα το φόρτωναν «όπως-όπως» στο Silver — και πέρασαν αρνητικά ποσά, ΑΦΜ με λάθος ψηφία, και μήνες `15`.
# MAGIC
# MAGIC Ο senior σου σου ζητά: **«Βάλε ένα data contract μπροστά. Ό,τι δεν τηρεί τη συμφωνία, καραντίνα.»**
# MAGIC
# MAGIC ## Στόχος
# MAGIC
# MAGIC Να χτίσεις το pipeline:
# MAGIC `CSV → load contract → schema check → quality rules → split → Silver / Quarantine / Audit → governance tags`
# MAGIC
# MAGIC ## Επιτυχία = όλα πράσινα ✅ στο Verification
# MAGIC
# MAGIC - [ ] `gt_lab.silver.tax_declarations_silver` έχει **300** καθαρά rows
# MAGIC - [ ] `gt_lab.silver.tax_declarations_quarantine` έχει **5** bad rows
# MAGIC - [ ] `gt_lab.silver.data_contract_audit` έχει **8** rules καταγεγραμμένα
# MAGIC - [ ] Το Silver έχει governance **tags** (`has_pii`, `classification`)
# MAGIC
# MAGIC ## ✅ Σου δίνω
# MAGIC - **Step 0** (auto-bootstrap) — φτιάχνει catalog/schema/volume, **παράγει** το dataset (300 rows) και **γράφει** το YAML contract. *Μην το αλλάξεις, απλώς τρέξε το.*
# MAGIC - Τον **κορμό** κάθε cell — εσύ συμπληρώνεις μόνο τα `____`.
# MAGIC - Auto-verification στο τέλος.
# MAGIC
# MAGIC ## 🚫 ΔΕΝ σου δίνω
# MAGIC - Τις απαντήσεις στα `____` (είναι στο SOLUTION — κοίτα το **μετά**).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✍️ ΜΕΡΟΣ 3 — Ο ΚΩΔΙΚΑΣ ΣΟΥ
# MAGIC
# MAGIC ## Step 0 — Auto-bootstrap (έτοιμο · μην το αλλάξεις)
# MAGIC
# MAGIC Αυτό το cell είναι **self-contained**: δεν χρειάζεται download από πουθενά.
# MAGIC Φτιάχνει `gt_lab` (catalog) + `bronze/silver/gold` (schemas) + `bronze.landing` (volume),
# MAGIC **παράγει** 300 συνθετικές φορολογικές δηλώσεις, και **γράφει** το YAML contract στο volume.
# MAGIC Είναι **idempotent** — μπορεί να ξανατρέξει χωρίς πρόβλημα.

# COMMAND ----------

# DBTITLE 1,Step 0 — Auto-bootstrap (don't edit)
import csv, io, random

CATALOG      = "gt_lab"
LANDING_PATH = f"/Volumes/{CATALOG}/bronze/landing"
CSV_PATH     = f"{LANDING_PATH}/declarations.csv"
YAML_PATH    = f"{LANDING_PATH}/aade_declarations_data_contract.yaml"

# 1) UC objects (idempotent)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for s in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")

# 2) Generate 300 CLEAN synthetic ΑΑΔΕ declarations
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
    amount = round(base * rate / 100, 2)   # τηρεί τον DQ007 (warning) → καθαρό row
    perif, poli = random.choice(CITIES)
    rows.append([
        i, f"2024-{month:02d}-{random.randint(1,28):02d}",
        f"{random.randint(100000000, 999999999)}", random.choice(EPWN),
        random.randint(1, 9), random.choice(KATHG), base, rate, amount,
        random.choice(KATAST), perif, poli, random.randint(1, 9),
        2024, month, MONTHS[month],
    ])

if any(f.name == "declarations.csv" for f in dbutils.fs.ls(LANDING_PATH)) is False:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(HEADER)
    w.writerows(rows)
    with open(CSV_PATH, "w", encoding="utf-8") as fp:
        fp.write(buf.getvalue())
    print(f"✅ Generated declarations.csv ({len(rows)} καθαρά rows)")
else:
    print("✅ declarations.csv υπάρχει ήδη")

# 3) Write the YAML data contract to the volume
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
print("\n📂 Volume contents:")
for f in dbutils.fs.ls(LANDING_PATH):
    print(f"   {f.name:<45s} {f.size/1024:>7.1f} KB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Φόρτωσε το YAML contract
# MAGIC
# MAGIC Το contract κάθεται σαν αρχείο (σε production: σε Git repo). Το διαβάζουμε σε ένα Python dict
# MAGIC ώστε ο κώδικας να «ξέρει» το schema και τα rules **χωρίς να τα έχουμε hardcode-άρει**.

# COMMAND ----------

# DBTITLE 1,Step 1 — Load contract
import yaml

CONTRACT_PATH = "/Volumes/gt_lab/bronze/landing/aade_declarations_data_contract.yaml"

with open(CONTRACT_PATH, "r", encoding="utf-8") as f:
    # ✍️ TODO 1
    #   ΤΙ:    Parse το YAML αρχείο σε Python dict.
    #   ΓΙΑΤΙ: Το contract είναι data, όχι κώδικας — το διαβάζουμε, δεν το hardcode-άρουμε.
    #          Έτσι, αλλαγή στο YAML αλλάζει τη συμπεριφορά χωρίς να αγγίξεις τον κώδικα.
    #   HINT:  yaml.safe_load(f)
    contract = ____

print(f"📋 {contract['contract_id']} v{contract['version']}")
print(f"   Owner:         {contract['owner']}")
print(f"   Schema fields: {len(contract['schema'])}")
print(f"   Quality rules: {len(contract['quality_rules'])}")
print(f"   Target table:  {contract['dataset']['name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Διάβασε το CSV (το «εισερχόμενο» data)
# MAGIC
# MAGIC Φορτώνουμε τα 300 records από το volume. `inferSchema=True` τραβά types από τα data.

# COMMAND ----------

# DBTITLE 1,Step 2 — Read declarations CSV
from pyspark.sql.functions import to_date, col

DATA_PATH = "/Volumes/gt_lab/bronze/landing/declarations.csv"

raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DATA_PATH)
    .withColumn("Ημερομηνία", to_date(col("Ημερομηνία")))
)

print(f"📥 Loaded {raw_df.count()} rows")
display(raw_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2.5 — Inject 5 «κακά» records (έτοιμο · μην το αλλάξεις)
# MAGIC
# MAGIC Το παραγόμενο CSV είναι καθαρό. Για να **δούμε** το contract να πιάνει failures,
# MAGIC προσθέτουμε επίτηδες 5 χαλασμένα records — το καθένα παραβιάζει διαφορετικό rule.
# MAGIC
# MAGIC | Bad row | Παραβιάζει |
# MAGIC |---|---|
# MAGIC | 9001 | DQ002 — ΑΦΜ 8 ψηφία |
# MAGIC | 9002 | DQ003 — αρνητικό Ποσό_EUR |
# MAGIC | 9003 | DQ006 — άγνωστη Κατάσταση |
# MAGIC | 9004 | DQ005 — MonthNumber=15 |
# MAGIC | NULL | DQ001 — null primary key |

# COMMAND ----------

# DBTITLE 1,Step 2.5 — Inject bad rows (don't edit)
from pyspark.sql import Row

bad_rows = [
    Row(ΔηλωσηID=9001, Ημερομηνία='2024-03-01', ΑΦΜ='12345678',  # 🚨 8 ψηφία
        Επωνυμία='Bad AFM AE', ΔΟΥID=1, Κατηγορία_Φόρου='ΦΠΑ', Βάση_Φόρου=1000.0,
        Συντελεστής_Pct=24.0, Ποσό_EUR=240.0, Κατάσταση='Εγκεκριμένη',
        Περιφέρεια='Αττική', Πόλη='Αθήνα', ΥπάλληλοςID=1, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
    Row(ΔηλωσηID=9002, Ημερομηνία='2024-03-02', ΑΦΜ='999999999',
        Επωνυμία='Negative OE', ΔΟΥID=2, Κατηγορία_Φόρου='Εισοδήματος', Βάση_Φόρου=5000.0,
        Συντελεστής_Pct=15.0, Ποσό_EUR=-100.0,  # 🚨 αρνητικό
        Κατάσταση='Εκκρεμής', Περιφέρεια='Αττική', Πόλη='Πειραιάς', ΥπάλληλοςID=2, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
    Row(ΔηλωσηID=9003, Ημερομηνία='2024-03-03', ΑΦΜ='888888888',
        Επωνυμία='Bad Status EPE', ΔΟΥID=3, Κατηγορία_Φόρου='Ακινήτων', Βάση_Φόρου=2000.0,
        Συντελεστής_Pct=10.0, Ποσό_EUR=200.0, Κατάσταση='ΑΓΝΩΣΤΗ',  # 🚨 εκτός allowed
        Περιφέρεια='Αττική', Πόλη='Αθήνα', ΥπάλληλοςID=3, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
    Row(ΔηλωσηID=9004, Ημερομηνία='2024-03-04', ΑΦΜ='777777777',
        Επωνυμία='Bad Month IKE', ΔΟΥID=4, Κατηγορία_Φόρου='ΦΠΑ', Βάση_Φόρου=3000.0,
        Συντελεστής_Pct=24.0, Ποσό_EUR=720.0, Κατάσταση='Εγκεκριμένη',
        Περιφέρεια='Αττική', Πόλη='Αθήνα', ΥπάλληλοςID=4, Φορ_Ετος=2024, MonthNumber=15, MonthName='Άγνωστος'),  # 🚨
    Row(ΔηλωσηID=None,  # 🚨 null PK
        Ημερομηνία='2024-03-05', ΑΦΜ='666666666', Επωνυμία='Null PK OE', ΔΟΥID=5,
        Κατηγορία_Φόρου='Μισθοδοσίας', Βάση_Φόρου=4000.0, Συντελεστής_Pct=12.0, Ποσό_EUR=480.0,
        Κατάσταση='Εκκρεμής', Περιφέρεια='Αττική', Πόλη='Αθήνα', ΥπάλληλοςID=5, Φορ_Ετος=2024, MonthNumber=3, MonthName='Μάρτιος'),
]

bad_df = spark.createDataFrame(bad_rows).withColumn("Ημερομηνία", to_date(col("Ημερομηνία")))
raw_df = raw_df.unionByName(bad_df, allowMissingColumns=True)

print(f"📥 Σύνολο: {raw_df.count()} rows (300 καθαρά + 5 κακά)")
display(raw_df.filter("`ΔηλωσηID` >= 9001 OR `ΔηλωσηID` IS NULL")
              .select("ΔηλωσηID", "ΑΦΜ", "Ποσό_EUR", "Κατάσταση", "MonthNumber"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Schema validation (πιάσε schema drift)
# MAGIC
# MAGIC Ο **πρώτος** έλεγχος: υπάρχουν όλες οι στήλες που απαιτεί το contract;
# MAGIC Αυτό πιάνει **breaking schema changes** (μετονομασία/διαγραφή στήλης) **πριν** φτάσουν downstream.

# COMMAND ----------

# DBTITLE 1,Step 3 — Schema validation
expected_cols = {f["name"] for f in contract["schema"]}   # στήλες που ΑΠΑΙΤΕΙ το contract
actual_cols   = set(raw_df.columns)                       # στήλες που ΗΡΘΑΝ στα data

# ✍️ TODO 2
#   ΤΙ:    Βρες ποιες required στήλες ΛΕΙΠΟΥΝ από τα data.
#   ΓΙΑΤΙ: Αν λείπει έστω μία → breaking change → σταματάμε ΠΡΙΝ μολύνουμε το Silver.
#   HINT:  set difference — «τι θέλω» μείον «τι έχω». expected_cols - actual_cols
missing = ____

extra = actual_cols - expected_cols   # στήλες-bonus (δεν χαλάνε, αλλά καλό να τις ξέρουμε)

if missing:
    raise ValueError(f"❌ SCHEMA DRIFT — λείπουν required στήλες: {missing}")
print(f"✅ Schema OK — και οι {len(expected_cols)} required στήλες υπάρχουν")
if extra:
    print(f"ℹ️  Extra στήλες (εκτός contract): {extra}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Τρέξε τα quality rules
# MAGIC
# MAGIC Για **κάθε** rule του contract, μετράμε πόσα rows το **παραβιάζουν**.
# MAGIC
# MAGIC ### 💡 Το βασικό insight
# MAGIC Κάθε rule έχει ένα `expression` που είναι **αληθές για τα ΣΩΣΤΑ** rows (π.χ. `Ποσό_EUR >= 0`).
# MAGIC Άρα ένα row **αποτυγχάνει** όταν ισχύει το **αντίθετο**: `NOT (expression)`.
# MAGIC Μετράμε τα failures φιλτράροντας με `NOT (expression)`.
# MAGIC
# MAGIC ### ⚠️ Ελληνικά ονόματα στηλών → backticks
# MAGIC Το Spark SQL θέλει non-ASCII ονόματα (όπως `ΑΦΜ`) μέσα σε backticks.
# MAGIC Ο helper `quote_greek_columns` (έτοιμος) το κάνει αυτόματα — εσύ μην ασχοληθείς μ' αυτό.

# COMMAND ----------

# DBTITLE 1,Step 4 — Run quality rules
import re

def quote_greek_columns(expression, schema_fields):
    """Τυλίγει non-ASCII ονόματα στηλών σε backticks για Spark SQL (έτοιμο helper)."""
    result = expression
    for col_name in sorted([f["name"] for f in schema_fields], key=len, reverse=True):
        if col_name.isascii():
            continue
        result = re.sub(r'(?<![\w`])' + re.escape(col_name) + r'(?![\w`])', f'`{col_name}`', result)
    return result

results = []
for rule in contract["quality_rules"]:
    raw_expr = rule["expression"]
    expr     = quote_greek_columns(raw_expr, contract["schema"])   # έτοιμο: backtick-άρισμα

    # ✍️ TODO 3
    #   ΤΙ:    Μέτρα πόσα rows ΑΠΟΤΥΓΧΑΝΟΥΝ αυτό το rule.
    #   ΓΙΑΤΙ: Το `expr` είναι αληθές για τα ΣΩΣΤΑ rows. Άρα fail = NOT(expr).
    #          Φιλτράρουμε με τη συνθήκη αποτυχίας και μετράμε.
    #   HINT:  raw_df.filter(f"NOT ({expr})").count()
    failed_count = ____

    results.append({
        "rule_id": rule["id"], "severity": rule["severity"],
        "failed_count": failed_count, "expression": expr, "expression_raw": raw_expr,
        "action_on_fail": rule.get("action_on_fail", "quarantine"),
    })
    icon = "✅" if failed_count == 0 else "🚨"
    print(f"{icon} {rule['id']} [{rule['severity']:7s}] {failed_count:>3d} failures — {raw_expr[:55]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Διαχώρισε valid από invalid
# MAGIC
# MAGIC Ένα row είναι **invalid** αν παραβιάζει **έστω ένα** `error`-severity rule.
# MAGIC Τα `warning` rules ΔΕΝ στέλνουν σε καραντίνα — απλώς flag-άρονται.
# MAGIC
# MAGIC ### 💡 Πώς ενώνουμε πολλά rules
# MAGIC Φτιάχνουμε μία μεγάλη συνθήκη: «απέτυχε rule_A **OR** απέτυχε rule_B **OR** ...».
# MAGIC Δηλαδή `NOT(exprA) OR NOT(exprB) OR ...`. Αν *οποιοδήποτε* κομμάτι είναι αληθές → invalid.

# COMMAND ----------

# DBTITLE 1,Step 5 — Split valid / invalid
# ✍️ TODO 4
#   ΤΙ:    Κράτα μόνο τα expressions των rules με severity == "error".
#   ΓΙΑΤΙ: Μόνο τα errors στέλνουν σε καραντίνα. Τα warnings περνούν στο Silver.
#   HINT:  "error"
error_rules = [r["expression"] for r in results if r["severity"] == ____]

# ✍️ TODO 5
#   ΤΙ:    Ένωσε όλα τα "fail conditions" με OR σε μία SQL συνθήκη.
#   ΓΙΑΤΙ: row είναι invalid αν αποτυγχάνει ΕΣΤΩ ΕΝΑ error rule → λογικό OR.
#   HINT:  " OR ".join([f"NOT ({e})" for e in error_rules])
combined_fail = ____

invalid_df = raw_df.filter(combined_fail)

# ✍️ TODO 6
#   ΤΙ:    Κράτα τα VALID rows — αυτά που ΔΕΝ πληρούν τη συνθήκη αποτυχίας.
#   ΓΙΑΤΙ: valid = το λογικό αντίθετο του "απέτυχε κάποιο rule".
#   HINT:  raw_df.filter(f"NOT ({combined_fail})")
valid_df = ____

print(f"✅ Valid:   {valid_df.count():>4d}")
print(f"🚨 Invalid: {invalid_df.count():>4d}")
display(invalid_df.select("ΔηλωσηID", "ΑΦΜ", "Ποσό_EUR", "Κατάσταση", "MonthNumber"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Γράψε Silver + Quarantine + Audit
# MAGIC
# MAGIC Τρία Delta tables:
# MAGIC 1. **Silver** — μόνο τα valid (πάει downstream/Gold/BI)
# MAGIC 2. **Quarantine** — τα invalid (manual review — **όχι** πεταμένα)
# MAGIC 3. **Audit** — per-rule ιστορικό αυτού του run (compliance trail)

# COMMAND ----------

# DBTITLE 1,Step 6 — Write the 3 tables
from datetime import datetime

# --- 1. Silver: μόνο τα valid ---
# ✍️ TODO 7a
#   ΤΙ:    Mode εγγραφής. Θέλουμε να ΑΝΤΙΚΑΘΙΣΤΟΥΜΕ το table σε κάθε run (full refresh).
#   ΓΙΑΤΙ: Το Silver είναι το «καθαρό αντίγραφο» — το ξαναχτίζουμε από το source κάθε φορά.
#   HINT:  "overwrite"
WRITE_MODE = ____

# ✍️ TODO 7b
#   ΤΙ:    Το πλήρες όνομα του Silver table (3-level: catalog.schema.table).
#   ΓΙΑΤΙ: Παίρνουμε το target από το contract — single source of truth.
#   HINT:  contract["publishing"]["write_valid_to"]   (= "gt_lab.silver.tax_declarations_silver")
SILVER_TABLE = ____

valid_df.write.format("delta").mode(WRITE_MODE).saveAsTable(SILVER_TABLE)
print(f"✅ Silver → {SILVER_TABLE}")

# --- 2. Quarantine: τα invalid ---
# ✍️ TODO 8
#   ΤΙ:    Το όνομα του quarantine table (από το contract).
#   ΓΙΑΤΙ: Τα κακά records φυλάσσονται για review — δεν χάνονται ποτέ.
#   HINT:  contract["publishing"]["write_invalid_to"]
QUARANTINE_TABLE = ____

invalid_df.write.format("delta").mode("overwrite").saveAsTable(QUARANTINE_TABLE)
print(f"🚨 Quarantine → {QUARANTINE_TABLE}")

# --- 3. Audit: per-rule ιστορικό (έτοιμο) ---
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

# MAGIC %md
# MAGIC ## Step 7 — Governance tags στο Silver
# MAGIC
# MAGIC Σημαδεύουμε το Silver με Unity Catalog **tags** ώστε να είναι discoverable + governed.
# MAGIC Αυτά τα tags είναι **searchable** στο Catalog Explorer και sync-άρονται με Purview σε enterprise setup.
# MAGIC Παίρνουμε τις τιμές **από το contract** ώστε docs & δεδομένα να μην ξεφεύγουν.

# COMMAND ----------

# DBTITLE 1,Step 7 — Apply governance tags
sec = contract["security"]

# ✍️ TODO 9a — classification (π.χ. confidential/internal/public)
#   ΓΙΑΤΙ: λέει στους χρήστες & στα tools πόσο ευαίσθητο είναι το table.
#   HINT:  sec["classification"]
classification = ____

# ✍️ TODO 9b — λίστα PII στηλών ως comma-string
#   ΓΙΑΤΙ: ώστε masking/access policies να ξέρουν πού να εφαρμοστούν.
#   HINT:  ",".join(sec["pii_columns"])
pii_columns = ____

has_pii = "true" if sec.get("pii_columns") else "false"

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
print("    Δες: Catalog Explorer → gt_lab → silver → tax_declarations_silver → Overview")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✅ ΜΕΡΟΣ 4 — VERIFICATION
# MAGIC
# MAGIC Τρέξε το cell — δεν γράφεις τίποτα. Ελέγχει τη δουλειά σου.

# COMMAND ----------

# DBTITLE 1,Auto-verification
# Τα 3 "hard" checks κρίνουν την επιτυχία. Το tags-check είναι informational:
# το system.information_schema μπορεί να μην είναι ορατό λόγω permissions —
# σ' αυτή την περίπτωση ΔΕΝ μετράει εναντίον σου (εμφανίζεται ως ⚠️ skipped).
checks = []          # (όνομα, ok) — μετράνε στο score
notes  = []          # informational — δεν μετράνε

try:
    n = spark.table("gt_lab.silver.tax_declarations_silver").count()
    checks.append((f"Silver = 300 καθαρά rows (βρέθηκαν {n})", n == 300))
except Exception:
    checks.append(("Silver table υπάρχει", False))

try:
    n = spark.table("gt_lab.silver.tax_declarations_quarantine").count()
    checks.append((f"Quarantine = 5 bad rows (βρέθηκαν {n})", n == 5))
except Exception:
    checks.append(("Quarantine table υπάρχει", False))

try:
    n = spark.table("gt_lab.silver.data_contract_audit").count()
    checks.append((f"Audit ≥ 8 rules (βρέθηκαν {n})", n >= 8))
except Exception:
    checks.append(("Audit table υπάρχει", False))

# Governance tags — informational (δεν επηρεάζει το pass/fail)
try:
    tags = spark.sql("""
        SELECT tag_name FROM system.information_schema.table_tags
        WHERE schema_name='silver' AND table_name='tax_declarations_silver'
    """)
    names = {r["tag_name"] for r in tags.collect()}
    if {"has_pii", "classification"} <= names:
        notes.append(("✅", "Governance tags εφαρμόστηκαν (has_pii, classification)"))
    else:
        notes.append(("⚠️", f"Tags δεν βρέθηκαν — έτρεξες το Step 7; (είδα: {names or 'κανένα'})"))
except Exception:
    notes.append(("⚠️", "Tags: δεν έχω access στο information_schema — skip (όχι λάθος σου)"))

print("=" * 60)
print(" ✅ VERIFICATION RESULTS")
print("=" * 60)
passed = sum(1 for _, ok in checks if ok)
for name, ok in checks:
    print(f"  {'✅' if ok else '❌'} {name}")
for icon, name in notes:
    print(f"  {icon} {name}  (informational)")
print("=" * 60)
print(f"  {passed}/{len(checks)} checks passed")
if passed == len(checks):
    print("\n🎉 ΣΥΓΧΑΡΗΤΗΡΙΑ — έχτισες ένα contract-driven quality pipeline!")
else:
    print("\n⚠️  Δες ποιο check είναι ❌ και γύρνα στο αντίστοιχο Step.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎓 Σκέψεις για συζήτηση
# MAGIC
# MAGIC 1. **Γιατί quarantine** αντί να πετάξουμε τα bad rows; *(audit, ίσως φταίει το rule, ανθρώπινο review)*
# MAGIC 2. **Πότε `error` και πότε `warning`;** Ποια απ' τα 8 rules θα έκανες warning; *(π.χ. DQ007 — soft check)*
# MAGIC 3. **Schema check vs quality rules** — γιατί χρειάζονται **και τα δύο**;
# MAGIC 4. **Versioning**: το source θέλει να προσθέσει στήλη `ΑΜΚΑ`. Πας από `v1.0.0` σε... `v1.1.0` ή `v2.0.0`; *(additive = minor, breaking = major)*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🚀 STRETCH EXERCISES — Για όσους τελειώσουν νωρίς
# MAGIC
# MAGIC Δεν υπάρχουν `____` εδώ — γράψε εσύ από το μηδέν (reference patterns δίνονται).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 Stretch 1 — Πρόσθεσε δικό σου quality rule
# MAGIC
# MAGIC Πρόσθεσε στο contract ένα **9ο rule**: `Βάση_Φόρου > 0` (severity `error`).
# MAGIC Μετά **inject** 1 ακόμη bad row με `Βάση_Φόρου = 0` και ξανατρέξε. Θα το πιάσει;
# MAGIC
# MAGIC **Reference**: το contract είναι dict — `contract["quality_rules"].append({...})` πριν το Step 4.

# COMMAND ----------

# DBTITLE 1,STRETCH 1 — Your own rule
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΟΥ




# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 Stretch 2 — Publishing gate (fail-fast policy)
# MAGIC
# MAGIC Το contract έχει πεδίο `publishing`. Φτιάξε ένα **gate**: αν το ποσοστό invalid rows
# MAGIC ξεπερνά π.χ. **5%**, **σήκωσε exception** ("κακό batch — stop pipeline") αντί να γράψεις Silver.
# MAGIC
# MAGIC **Reference**:
# MAGIC ```python
# MAGIC bad_pct = 100 * invalid_df.count() / raw_df.count()
# MAGIC if bad_pct > 5:
# MAGIC     raise ValueError(f"🚨 Batch rejected: {bad_pct:.1f}% invalid (> 5% threshold)")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,STRETCH 2 — Publishing gate
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΟΥ




# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 Stretch 3 — Column-level PII comment + masking note
# MAGIC
# MAGIC Πρόσθεσε column comments στις PII στήλες ώστε ο επόμενος engineer να ξέρει ότι θέλουν masking.
# MAGIC
# MAGIC **Reference**:
# MAGIC ```python
# MAGIC spark.sql("ALTER TABLE gt_lab.silver.tax_declarations_silver "
# MAGIC           "ALTER COLUMN `ΑΦΜ` COMMENT 'PII: ΑΦΜ — masking policy για analyst roles'")
# MAGIC ```
# MAGIC
# MAGIC **Bonus**: γράψε ένα `CREATE VIEW ..._masked` που δείχνει μόνο τα τελευταία 4 ψηφία του ΑΦΜ.

# COMMAND ----------

# DBTITLE 1,STRETCH 3 — PII comments / masked view
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΟΥ




# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cleanup (optional)
# MAGIC
# MAGIC Για να ξαναρχίσεις από καθαρό state, ξε-σχολίασε και τρέξε:

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS gt_lab.silver.tax_declarations_silver")
# spark.sql("DROP TABLE IF EXISTS gt_lab.silver.tax_declarations_quarantine")
# spark.sql("DROP TABLE IF EXISTS gt_lab.silver.data_contract_audit")
# print("🗑️  Tables dropped")
