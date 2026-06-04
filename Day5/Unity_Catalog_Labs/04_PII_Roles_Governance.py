# Databricks notebook source
# MAGIC %md
# MAGIC # 🔐 Unity Catalog — PII, Ρόλοι & Διακυβέρνηση (Expert Lab)
# MAGIC
# MAGIC **Ημέρα 5 · Security & Governance · ⏱ 60–75'** — ΑΑΔΕ scenario
# MAGIC
# MAGIC > Επέκταση του βασικού `Lab_R3_Unity_Catalog_Admin`. Εκεί τα masks/filters/grants ήταν
# MAGIC > **σχολιασμένα** (προσομοίωση). Εδώ τρέχουν **στ' αλήθεια** σε πλήρες Unity Catalog —
# MAGIC > και υπάρχει **live "switch role" demo** για να δεις τα δεδομένα να αλλάζουν μπροστά σου.
# MAGIC
# MAGIC ## Τι θα δεις να δουλεύει πραγματικά
# MAGIC | Μηχανισμός | UC feature | Σε ποιον/τι |
# MAGIC |---|---|---|
# MAGIC | **GRANT / REVOKE** | object privileges | ποιος βλέπει ποιο schema/table |
# MAGIC | **Column Mask** | `ALTER COLUMN … SET MASK` | κρύβει ΑΦΜ / IBAN / email |
# MAGIC | **Row Filter** | `ALTER TABLE … SET ROW FILTER` | κάθε χρήστης βλέπει μόνο τις περιφέρειές του |
# MAGIC | **Dynamic View** | `current_user()` + mapping | role-based masking (toggle live) |
# MAGIC | **PII Tags** | `SET TAGS` + column comments | classification / discoverability |
# MAGIC | **Audit & Lineage** | system tables, `DESCRIBE HISTORY` | ποιος είδε τι |
# MAGIC | **Time-bound access** | scheduled `REVOKE` (→ Day 6 Jobs) | εξωτερικός ελεγκτής 30 ημερών |
# MAGIC
# MAGIC ## Τα 3 επίπεδα ελέγχου (κρατήστε το)
# MAGIC ```
# MAGIC GRANT        → ΑΝ μπορείς να αγγίξεις το table   (yes/no, ολόκληρο object)
# MAGIC ROW FILTER   → ΠΟΙΕΣ γραμμές βλέπεις             (horizontal)
# MAGIC COLUMN MASK  → ΠΩΣ βλέπεις μια στήλη             (vertical, PII)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 0 — Setup: catalog, schemas, ρόλος του χρήστη
# MAGIC
# MAGIC Φτιάχνουμε 4 schemas (medallion + governance) στο catalog `workspace` (πάντα υπάρχει).
# MAGIC Καταγράφουμε και τον **τρέχοντα χρήστη** (εσένα) στον πίνακα ρόλων.

# COMMAND ----------

# DBTITLE 1,Setup — catalog/schemas + ποιος είσαι
CATALOG = "workspace"   # άλλαξέ το αν έχεις δικό σου catalog (π.χ. gt_lab)

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    print(f"ℹ️  Χρησιμοποιώ υπάρχον catalog '{CATALOG}'.")

spark.sql(f"USE CATALOG {CATALOG}")
for s in ["aade_raw", "aade_secure", "aade_analytics", "aade_governance"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")

ME = spark.sql("SELECT current_user()").collect()[0][0]
print(f"✅ Catalog={CATALOG} · schemas ready")
print(f"👤 current_user() = {ME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 1 — Δεδομένα με ΠΟΛΛΑ PII (ΑΑΔΕ φορολογούμενοι)
# MAGIC
# MAGIC Πέρα από το ΑΦΜ, προσθέτουμε **τηλέφωνο, email, IBAN, εισόδημα** — όλα ευαίσθητα,
# MAGIC ώστε να έχουμε πραγματικούς στόχους για masking.

# COMMAND ----------

# DBTITLE 1,Create taxpayers (PII) + declarations
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, lit, concat, substring

taxpayers = [
    ("100111111","Παπαδόπουλος Νίκος","Αττική","Α' Αθηνών","Τεχνολογία",85000.0,"6944000111","nikos.p@example.gr","GR1601101250000000012300695"),
    ("100111112","Γεωργίου Μαρία","Κεντρικής Μακεδονίας","Α' Θεσσαλονίκης","Υγεία",62000.0,"6970000222","maria.g@example.gr","GR9608100010000001234567890"),
    ("100111113","Δημητρίου Κώστας","Κρήτης","Ηρακλείου","Τουρισμός",45000.0,"6932000333","kostas.d@example.gr","GR4901710020000020123456789"),
    ("100111114","Αντωνίου Ελένη","Αττική","Γλυφάδας","Εκπαίδευση",38000.0,"6945000444","eleni.a@example.gr","GR1102600400000000010123456"),
    ("100111115","Κωνσταντίνου Γιώργος","Δυτικής Ελλάδας","Πατρών","Λιανικό",51000.0,"6988000555","giorgos.k@example.gr","GR7201450030000003012345678"),
    ("100111116","Βασιλείου Αναστασία","Κεντρικής Μακεδονίας","Β' Θεσσαλονίκης","Τεχνολογία",92000.0,"6900000666","anastasia.v@example.gr","GR1301720040000040123456780"),
    ("100111117","Νικολάου Πέτρος","Αττική","Πειραιά","Ναυτιλία",120000.0,"6977000777","petros.n@example.gr","GR0801500050000005012345670"),
    ("100111118","Παπαγεωργίου Σοφία","Ιονίων Νήσων","Κέρκυρας","Τουρισμός",34000.0,"6955000888","sofia.p@example.gr","GR2602600600000006012345678"),
    ("100111119","Αλεξίου Δημήτρης","Κρήτης","Χανίων","Αγροτικός",28000.0,"6911000999","dimitris.a@example.gr","GR9301400070000007012345671"),
    ("100111120","Μιχαηλίδου Κατερίνα","Αττική","Αμαρουσίου","Υγεία",75000.0,"6966001000","katerina.m@example.gr","GR4501100080000008012345672"),
]
schema = StructType([
    StructField("afm",StringType(),False), StructField("name",StringType(),False),
    StructField("region",StringType(),False), StructField("doy",StringType(),False),
    StructField("sector",StringType(),False), StructField("annual_income",DoubleType(),False),
    StructField("phone",StringType(),True), StructField("email",StringType(),True),
    StructField("iban",StringType(),True),
])
spark.createDataFrame(taxpayers, schema).write.mode("overwrite").saveAsTable("aade_raw.taxpayers")

declarations = [
    ("D001","100111111","ΦΠΑ",85000.0,24.0,20400.0,"Εγκεκριμένη","2024"),
    ("D002","100111112","Εισόδημα",62000.0,22.0,13640.0,"Εγκεκριμένη","2024"),
    ("D003","100111113","ΦΠΑ",45000.0,13.0,5850.0,"Εκκρεμής","2024"),
    ("D004","100111114","Εισόδημα",38000.0,22.0,8360.0,"Εγκεκριμένη","2024"),
    ("D005","100111115","Ακίνητα",51000.0,1.5,765.0,"Απορριφθείσα","2024"),
    ("D006","100111116","ΦΠΑ",92000.0,24.0,22080.0,"Εκκρεμής","2024"),
    ("D007","100111117","Εισόδημα",120000.0,44.0,52800.0,"Εγκεκριμένη","2024"),
    ("D008","100111118","ΦΠΑ",34000.0,13.0,4420.0,"Εγκεκριμένη","2024"),
    ("D009","100111119","Εισόδημα",28000.0,22.0,6160.0,"Εκκρεμής","2024"),
    ("D010","100111120","ΦΠΑ",75000.0,24.0,18000.0,"Εγκεκριμένη","2024"),
]
dschema = StructType([
    StructField("declaration_id",StringType(),False), StructField("afm",StringType(),False),
    StructField("tax_category",StringType(),False), StructField("tax_base",DoubleType(),False),
    StructField("tax_rate_pct",DoubleType(),False), StructField("amount_eur",DoubleType(),False),
    StructField("status",StringType(),False), StructField("tax_year",StringType(),False),
])
spark.createDataFrame(declarations, dschema).write.mode("overwrite").saveAsTable("aade_raw.declarations")
print("✅ aade_raw.taxpayers (PII) + aade_raw.declarations έτοιμα")
display(spark.table("aade_raw.taxpayers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 2 — PII classification με Tags & comments
# MAGIC
# MAGIC Πριν προστατέψεις κάτι, το **σημαδεύεις**. Τα tags είναι searchable στο Catalog Explorer
# MAGIC και τροφοδοτούν policies / Purview σε enterprise setup.

# COMMAND ----------

# DBTITLE 1,Tag το table + τις PII στήλες
# MAGIC %sql
# MAGIC ALTER TABLE aade_raw.taxpayers SET TAGS (
# MAGIC   'classification' = 'confidential',
# MAGIC   'has_pii'        = 'true',
# MAGIC   'data_owner'     = 'aade-data-governance',
# MAGIC   'retention'      = '7_years'
# MAGIC );
# MAGIC -- column-level PII tags + comments
# MAGIC ALTER TABLE aade_raw.taxpayers ALTER COLUMN afm   SET TAGS ('pii'='true','pii_type'='national_id');
# MAGIC ALTER TABLE aade_raw.taxpayers ALTER COLUMN iban  SET TAGS ('pii'='true','pii_type'='financial');
# MAGIC ALTER TABLE aade_raw.taxpayers ALTER COLUMN email SET TAGS ('pii'='true','pii_type'='contact');
# MAGIC ALTER TABLE aade_raw.taxpayers ALTER COLUMN phone SET TAGS ('pii'='true','pii_type'='contact');
# MAGIC ALTER TABLE aade_raw.taxpayers ALTER COLUMN afm   COMMENT 'PII: ΑΦΜ — masked για non-privileged roles';

# COMMAND ----------

# DBTITLE 1,Βρες όλες τις PII στήλες (governance query)
# MAGIC %sql
# MAGIC -- "Δείξε μου ΟΛΕΣ τις PII στήλες στο schema" — η ερώτηση που κάνει κάθε DPO
# MAGIC SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value
# MAGIC FROM   system.information_schema.column_tags
# MAGIC WHERE  schema_name = 'aade_raw' AND tag_name = 'pii'
# MAGIC ORDER  BY table_name, column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 3 — Μοντέλο ρόλων (mapping tables)
# MAGIC
# MAGIC Σε production οι ρόλοι είναι **account groups** (SCIM/Entra ID). Για να τρέχει **live** το lab
# MAGIC σε έναν λογαριασμό, χαρτογραφούμε `email → ρόλος` και `email → επιτρεπόμενη περιφέρεια`
# MAGIC σε πίνακες. Τα masks/filters κοιτάνε `current_user()` σ' αυτούς τους πίνακες.
# MAGIC
# MAGIC | Ρόλος | ΑΦΜ/IBAN | Εισόδημα | Γραμμές |
# MAGIC |---|---|---|---|
# MAGIC | `data_engineer` / `admin` | πλήρες | πλήρες | όλες |
# MAGIC | `analyst` | masked | banded | μόνο οι περιφέρειές του |
# MAGIC | `auditor` | masked | πλήρες | όλες |

# COMMAND ----------

# DBTITLE 1,Φτιάξε & γέμισε τα mapping tables (με ΕΣΕΝΑ μέσα)
spark.sql("""CREATE TABLE IF NOT EXISTS aade_governance.user_role
             (email STRING, role STRING) USING DELTA""")
spark.sql("""CREATE TABLE IF NOT EXISTS aade_governance.user_region
             (email STRING, region STRING) USING DELTA""")
spark.sql("DELETE FROM aade_governance.user_role")
spark.sql("DELETE FROM aade_governance.user_region")

# Εσύ ξεκινάς ως data_engineer (βλέπεις τα πάντα) με 2 περιφέρειες
spark.sql(f"""INSERT INTO aade_governance.user_role VALUES
    ('{ME}','data_engineer'),
    ('analyst1@aade.gr','analyst'),
    ('auditor@aade.gr','auditor')""")
spark.sql(f"""INSERT INTO aade_governance.user_region VALUES
    ('{ME}','Αττική'), ('{ME}','Κρήτης'),
    ('analyst1@aade.gr','Κεντρικής Μακεδονίας')""")
print(f"✅ Ρόλος του {ME}: data_engineer · περιφέρειες: Αττική, Κρήτης")
display(spark.table("aade_governance.user_role"))

# COMMAND ----------

# DBTITLE 1,Helper function: ρόλος του τρέχοντος χρήστη
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION aade_governance.my_role()
# MAGIC RETURNS STRING
# MAGIC RETURN COALESCE(
# MAGIC   (SELECT role FROM aade_governance.user_role WHERE email = current_user() LIMIT 1),
# MAGIC   'public');
# MAGIC SELECT current_user() AS me, aade_governance.my_role() AS my_role;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 4 — GRANT / REVOKE (πραγματικά, στα δικά σου objects)
# MAGIC
# MAGIC Είσαι **owner** ό,τι έφτιαξες → μπορείς να κάνεις GRANT. Δίνουμε read στο `account users`
# MAGIC (group που πάντα υπάρχει) για να δούμε τον μηχανισμό· για custom groups δίνουμε το production SQL.

# COMMAND ----------

# DBTITLE 1,GRANT read στο analytics + SHOW GRANTS
# MAGIC %sql
# MAGIC -- ⚠️ UC privilege = USE SCHEMA / USE CATALOG (όχι το legacy 'USAGE')
# MAGIC GRANT USE SCHEMA ON SCHEMA aade_analytics TO `account users`;
# MAGIC -- (τα aggregates μπαίνουν στο Βήμα 8 — εκεί δίνουμε SELECT)
# MAGIC SHOW GRANTS ON SCHEMA aade_analytics;

# COMMAND ----------

# DBTITLE 1,Production GRANTs για custom groups (τρέχει αν υπάρχουν τα groups)
for stmt in [
    "GRANT USE SCHEMA ON SCHEMA aade_secure TO `aade_analysts`",
    "GRANT SELECT    ON SCHEMA aade_secure TO `aade_analysts`",
    "GRANT ALL PRIVILEGES ON SCHEMA aade_raw TO `aade_data_engineers`",
    "GRANT SELECT ON SCHEMA aade_raw       TO `aade_auditors`",
]:
    try:
        spark.sql(stmt); print(f"✅ {stmt}")
    except Exception as e:
        print(f"⏭️  skip (το group δεν υπάρχει ακόμα — φτιάξ' το σε Admin Console/SCIM): {stmt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 5 — 🎭 Column Masks (PII) — ΠΡΑΓΜΑΤΙΚΑ εφαρμοσμένα
# MAGIC
# MAGIC Φτιάχνουμε masking functions που κοιτάνε τον ρόλο σου, και τις **κολλάμε** στις στήλες με
# MAGIC `ALTER COLUMN … SET MASK`. Από εδώ και πέρα, **κάθε** query στο table περνά από το mask.

# COMMAND ----------

# DBTITLE 1,Mask functions (ΑΦΜ, IBAN, email, εισόδημα)
# MAGIC %sql
# MAGIC -- ΑΦΜ: μόνο engineers/admins πλήρες· οι υπόλοιποι τα 4 τελευταία
# MAGIC CREATE OR REPLACE FUNCTION aade_governance.mask_afm(v STRING) RETURNS STRING
# MAGIC RETURN CASE WHEN aade_governance.my_role() IN ('data_engineer','admin') THEN v
# MAGIC             ELSE CONCAT('*****', RIGHT(v,4)) END;
# MAGIC
# MAGIC -- IBAN: δείξε μόνο χώρα + 4 τελευταία
# MAGIC CREATE OR REPLACE FUNCTION aade_governance.mask_iban(v STRING) RETURNS STRING
# MAGIC RETURN CASE WHEN aade_governance.my_role() IN ('data_engineer','admin') THEN v
# MAGIC             ELSE CONCAT(LEFT(v,2), '****', RIGHT(v,4)) END;
# MAGIC
# MAGIC -- email: κράτα μόνο 1ο γράμμα + domain
# MAGIC CREATE OR REPLACE FUNCTION aade_governance.mask_email(v STRING) RETURNS STRING
# MAGIC RETURN CASE WHEN aade_governance.my_role() IN ('data_engineer','admin') THEN v
# MAGIC             ELSE CONCAT(LEFT(v,1), '***@', SPLIT(v,'@')[1]) END;
# MAGIC
# MAGIC -- εισόδημα: analysts βλέπουν "κλίμακα", auditors/engineers το ακριβές
# MAGIC CREATE OR REPLACE FUNCTION aade_governance.mask_income(v DOUBLE) RETURNS STRING
# MAGIC RETURN CASE WHEN aade_governance.my_role() IN ('data_engineer','admin','auditor') THEN CAST(v AS STRING)
# MAGIC             WHEN v < 30000 THEN '< 30k' WHEN v < 70000 THEN '30k–70k' ELSE '70k+' END;

# COMMAND ----------

# DBTITLE 1,Κόλλα τα masks στις στήλες
# MAGIC %sql
# MAGIC ALTER TABLE aade_raw.taxpayers ALTER COLUMN afm           SET MASK aade_governance.mask_afm;
# MAGIC ALTER TABLE aade_raw.taxpayers ALTER COLUMN iban          SET MASK aade_governance.mask_iban;
# MAGIC ALTER TABLE aade_raw.taxpayers ALTER COLUMN email         SET MASK aade_governance.mask_email;
# MAGIC ALTER TABLE aade_raw.taxpayers ALTER COLUMN annual_income SET MASK aade_governance.mask_income;

# COMMAND ----------

# DBTITLE 1,Δες το αποτέλεσμα (ως data_engineer → πλήρες)
# MAGIC %sql
# MAGIC SELECT afm, name, iban, email, annual_income, region FROM aade_raw.taxpayers ORDER BY afm;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6 — 🔀 LIVE DEMO: «Switch role» και ξαναδές
# MAGIC
# MAGIC Δεν χρειάζεται δεύτερος χρήστης. Αλλάζουμε **τον δικό σου ρόλο** στο mapping table και
# MAGIC ξανατρέχουμε το ΙΔΙΟ query — τα masks ενεργοποιούνται αμέσως.

# COMMAND ----------

# DBTITLE 1,Γίνε 'analyst' → ξαναδές (masked!)
# MAGIC %sql
# MAGIC UPDATE aade_governance.user_role SET role = 'analyst' WHERE email = current_user();
# MAGIC SELECT aade_governance.my_role() AS τρέχων_ρόλος;

# COMMAND ----------

# DBTITLE 1,Ίδιο query — τώρα masked + filtered εισόδημα
# MAGIC %sql
# MAGIC SELECT afm, name, iban, email, annual_income, region FROM aade_raw.taxpayers ORDER BY afm;

# COMMAND ----------

# DBTITLE 1,Επανέφερε τον ρόλο σου σε data_engineer
# MAGIC %sql
# MAGIC UPDATE aade_governance.user_role SET role = 'data_engineer' WHERE email = current_user();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 7 — 🧱 Row Filter — κάθε χρήστης μόνο τις περιφέρειές του
# MAGIC
# MAGIC Ένα row filter είναι function που γυρνά `TRUE/FALSE` ανά γραμμή. Κολλάει με
# MAGIC `SET ROW FILTER … ON (στήλη)`. Χρησιμοποιεί lookup στο `user_region`.

# COMMAND ----------

# DBTITLE 1,Row filter function + apply
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION aade_governance.region_filter(region STRING) RETURNS BOOLEAN
# MAGIC RETURN
# MAGIC   aade_governance.my_role() IN ('data_engineer','admin','auditor')   -- αυτοί βλέπουν όλες
# MAGIC   OR EXISTS (SELECT 1 FROM aade_governance.user_region ur
# MAGIC              WHERE ur.email = current_user() AND ur.region = region);
# MAGIC
# MAGIC ALTER TABLE aade_raw.taxpayers SET ROW FILTER aade_governance.region_filter ON (region);

# COMMAND ----------

# DBTITLE 1,Ως analyst → μόνο οι δικές του περιφέρειες
# MAGIC %sql
# MAGIC UPDATE aade_governance.user_role SET role = 'analyst' WHERE email = current_user();
# MAGIC -- εσύ είσαι mapped σε: Αττική, Κρήτης → θα δεις ΜΟΝΟ αυτές
# MAGIC SELECT name, region, doy FROM aade_raw.taxpayers ORDER BY region;

# COMMAND ----------

# DBTITLE 1,Πρόσθεσε μια περιφέρεια στον εαυτό σου → εμφανίζεται αμέσως
# MAGIC %sql
# MAGIC INSERT INTO aade_governance.user_region VALUES (current_user(), 'Αττική');  -- ήδη υπάρχει; ok
# MAGIC INSERT INTO aade_governance.user_region VALUES (current_user(), 'Ιονίων Νήσων');
# MAGIC SELECT name, region FROM aade_raw.taxpayers ORDER BY region;

# COMMAND ----------

# DBTITLE 1,Reset σε data_engineer (βλέπεις πάλι όλες)
# MAGIC %sql
# MAGIC UPDATE aade_governance.user_role SET role = 'data_engineer' WHERE email = current_user();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 8 — Εναλλακτική: Dynamic View (όταν δεν θες masks στο base table)
# MAGIC
# MAGIC Αν δεν θες να αγγίξεις το raw table, χτίζεις **secure view** με τη λογική masking μέσα της.
# MAGIC Οι analysts παίρνουν GRANT μόνο στη view — ποτέ στο raw.

# COMMAND ----------

# DBTITLE 1,Secure view + analytics aggregate
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW aade_secure.taxpayers_v AS
# MAGIC SELECT
# MAGIC   CASE WHEN aade_governance.my_role() IN ('data_engineer','admin') THEN afm
# MAGIC        ELSE CONCAT('*****', RIGHT(afm,4)) END AS afm,
# MAGIC   name, region, doy, sector
# MAGIC FROM aade_raw.taxpayers;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW aade_analytics.regional_summary AS
# MAGIC SELECT t.region, d.tax_category,
# MAGIC        COUNT(*) AS declarations,
# MAGIC        ROUND(SUM(d.amount_eur),2) AS total_amount
# MAGIC FROM aade_raw.declarations d JOIN aade_raw.taxpayers t ON d.afm = t.afm
# MAGIC GROUP BY t.region, d.tax_category;
# MAGIC
# MAGIC GRANT SELECT ON SCHEMA aade_analytics TO `account users`;   -- aggregates = ασφαλή να μοιραστούν
# MAGIC SELECT * FROM aade_analytics.regional_summary ORDER BY total_amount DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 9 — Audit & Lineage (ποιος είδε τι)
# MAGIC
# MAGIC - `DESCRIBE HISTORY` → ιστορικό αλλαγών του πίνακα (πάντα διαθέσιμο).
# MAGIC - `system.access.audit` → ποιος έκανε query σε ποιο PII table (χρειάζεται system tables enabled).
# MAGIC - **Lineage**: Catalog Explorer → `aade_raw.taxpayers` → tab **Lineage** (table & column level).

# COMMAND ----------

# DBTITLE 1,Ιστορικό + (αν υπάρχει) audit log
# MAGIC %sql
# MAGIC DESCRIBE HISTORY aade_raw.taxpayers;

# COMMAND ----------

# DBTITLE 1,System audit — ποιος άγγιξε τα PII tables (try)
try:
    df = spark.sql("""
        SELECT event_time, user_identity.email AS who, action_name,
               request_params.full_name_arg AS object
        FROM system.access.audit
        WHERE action_name IN ('getTable','generateTemporaryTableCredential')
          AND request_params.full_name_arg LIKE '%aade_raw%'
        ORDER BY event_time DESC LIMIT 20
    """)
    display(df)
except Exception as e:
    print("⏭️  system.access.audit δεν είναι προσβάσιμο (χρειάζεται system tables enabled).")
    print("    Enable: Account console → Settings → System tables, ή ρώτα τον metastore admin.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 10 — ✍️ Άσκηση (συμπλήρωσε τα κενά)
# MAGIC
# MAGIC Πρόσθεσε προστασία στη στήλη **`phone`** (PII) και ένα mask «μόνο τα 3 τελευταία ψηφία».
# MAGIC Λύση στο τέλος (Βήμα 12).

# COMMAND ----------

# DBTITLE 1,TODO — mask για το phone
# MAGIC %sql
# MAGIC -- TODO 1: φτιάξε function aade_governance.mask_phone(v STRING)
# MAGIC --         engineers/admins → πλήρες, αλλιώς → CONCAT('*******', RIGHT(v,3))
# MAGIC -- CREATE OR REPLACE FUNCTION aade_governance.mask_phone(v STRING) RETURNS STRING
# MAGIC -- RETURN ____ ;
# MAGIC
# MAGIC -- TODO 2: κόλλα το mask στη στήλη phone
# MAGIC -- ALTER TABLE aade_raw.taxpayers ALTER COLUMN phone SET MASK ____ ;
# MAGIC
# MAGIC -- TODO 3: επιβεβαίωσε ως analyst
# MAGIC -- UPDATE aade_governance.user_role SET role='analyst' WHERE email=current_user();
# MAGIC -- SELECT name, phone FROM aade_raw.taxpayers LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 11 — 🏛️ Capstone: «Εξωτερικός ελεγκτής για 30 ημέρες»
# MAGIC
# MAGIC **Σενάριο:** Ένας external auditor χρειάζεται read-only σε masked δεδομένα **για 30 ημέρες**.
# MAGIC Το UC **δεν** έχει native TTL στα grants → το λύνουμε **διαδικαστικά**:
# MAGIC
# MAGIC 1. **Σήμερα**: `GRANT SELECT ON SCHEMA aade_secure TO `external_auditors``
# MAGIC    (βλέπει μόνο masked views, ποτέ το `aade_raw`).
# MAGIC 2. **Καταγραφή λήξης** σε πίνακα `aade_governance.access_grants(group, object, expires_on)`.
# MAGIC 3. **Σε 30 μέρες**: ένα **scheduled Databricks Job** (Day 6!) τρέχει `REVOKE` για ό,τι έληξε.
# MAGIC
# MAGIC > Έτσι το **time-bound access** γίνεται **κώδικας**, όχι υπενθύμιση στο ημερολόγιο.

# COMMAND ----------

# DBTITLE 1,Capstone — πίνακας λήξης + το REVOKE που θα τρέχει το Job
spark.sql("""CREATE TABLE IF NOT EXISTS aade_governance.access_grants
             (group_name STRING, object STRING, granted_on DATE, expires_on DATE) USING DELTA""")
spark.sql("""INSERT INTO aade_governance.access_grants
             VALUES ('external_auditors','aade_secure', current_date(), date_add(current_date(),30))""")

print("📋 Active time-bound grants:")
display(spark.table("aade_governance.access_grants"))

print("\n🤖 Το nightly Job (Day 6) θα έτρεχε:")
expired = spark.sql("""SELECT group_name, object FROM aade_governance.access_grants
                       WHERE expires_on < current_date()""").collect()
if not expired:
    print("   (κανένα grant δεν έχει λήξει σήμερα — όλα ενεργά)")
for r in expired:
    print(f"   REVOKE ALL PRIVILEGES ON SCHEMA {r['object']} FROM `{r['group_name']}`;")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 12 — 🧹 Cleanup + ✅ Λύση άσκησης
# MAGIC
# MAGIC **Λύση Βήματος 10:**
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION aade_governance.mask_phone(v STRING) RETURNS STRING
# MAGIC RETURN CASE WHEN aade_governance.my_role() IN ('data_engineer','admin') THEN v
# MAGIC             ELSE CONCAT('*******', RIGHT(v,3)) END;
# MAGIC ALTER TABLE aade_raw.taxpayers ALTER COLUMN phone SET MASK aade_governance.mask_phone;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cleanup (ξε-σχολίασε για reset)
# MAGIC %sql
# MAGIC -- ALTER TABLE aade_raw.taxpayers ALTER COLUMN afm           DROP MASK;
# MAGIC -- ALTER TABLE aade_raw.taxpayers ALTER COLUMN iban          DROP MASK;
# MAGIC -- ALTER TABLE aade_raw.taxpayers ALTER COLUMN email         DROP MASK;
# MAGIC -- ALTER TABLE aade_raw.taxpayers ALTER COLUMN annual_income DROP MASK;
# MAGIC -- ALTER TABLE aade_raw.taxpayers DROP ROW FILTER;
# MAGIC -- DROP SCHEMA IF EXISTS aade_governance CASCADE;
# MAGIC -- DROP SCHEMA IF EXISTS aade_analytics  CASCADE;
# MAGIC -- DROP SCHEMA IF EXISTS aade_secure     CASCADE;
# MAGIC -- DROP SCHEMA IF EXISTS aade_raw        CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 Σύνοψη
# MAGIC | Επίπεδο | Εντολή | Πότε το χρησιμοποιείς |
# MAGIC |---|---|---|
# MAGIC | Object access | `GRANT/REVOKE … TO group` | ποιος βλέπει το table καθόλου |
# MAGIC | Row-level | `SET ROW FILTER … ON (col)` | περιορισμός γραμμών ανά χρήστη |
# MAGIC | Column-level | `ALTER COLUMN … SET MASK` | PII (ΑΦΜ/IBAN/email) |
# MAGIC | Soft / sharing | secure **view** + GRANT στη view | analysts χωρίς πρόσβαση στο raw |
# MAGIC | Discovery | `SET TAGS` + `information_schema.column_tags` | «βρες όλα τα PII» |
# MAGIC | Accountability | `system.access.audit`, `DESCRIBE HISTORY`, Lineage | ποιος είδε τι |
# MAGIC | Time-bound | πίνακας λήξης + scheduled `REVOKE` Job | προσωρινή πρόσβαση ελεγκτών |
# MAGIC
# MAGIC **Κανόνας:** *tag → grant → filter → mask → audit.* Πρώτα ξέρεις τι είναι ευαίσθητο,
# MAGIC μετά ποιος το αγγίζει, μετά πόσο βλέπει, και πάντα κρατάς ίχνος.
