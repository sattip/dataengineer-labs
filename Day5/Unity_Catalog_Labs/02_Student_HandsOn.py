# Databricks notebook source
# MAGIC %md
# MAGIC # 👨‍💻 Unity Catalog — Lab 2: Student Hands-On
# MAGIC
# MAGIC **Role:** Student / Trainee — εσείς εκτελείτε ΟΛΑ τα κελιά
# MAGIC **Duration:** ~60 minutes
# MAGIC **Pace:** Step-by-step, αυτόνομα — αν κολλήσετε ρωτάτε
# MAGIC
# MAGIC ## 🎯 Σκοπός
# MAGIC > Να χτίσετε **μόνοι σας** ένα πλήρες UC environment για ένα φανταστικό
# MAGIC > τμήμα της ΑΑΔΕ. Κάθε μαθητής έχει **δικό του schema** για να μην
# MAGIC > συγκρούεται με τους άλλους.
# MAGIC
# MAGIC ## ⚠️ ΠΡΩΤΟ ΒΗΜΑ — Personalize your schema
# MAGIC > Αλλάξτε το `YOUR_NAME` στο επόμενο κελί με το **δικό σας μικρό όνομα**
# MAGIC > (lowercase, χωρίς κενά, χωρίς τόνους). Π.χ. `george`, `maria`, `kostas`.
# MAGIC > Όλα τα παρακάτω cells χρησιμοποιούν αυτή τη variable.
# MAGIC
# MAGIC ## 📋 12 Step-by-step Tasks
# MAGIC | # | Task | Concept |
# MAGIC |---|---|---|
# MAGIC | 1 | Personal Schema Setup | CREATE SCHEMA με σωστά properties |
# MAGIC | 2 | Volume + CSV Upload | File ingestion στο UC |
# MAGIC | 3 | Bronze Delta Table | CREATE TABLE από CSV με metadata |
# MAGIC | 4 | Add Comments & Tags | Documentation σε column-level |
# MAGIC | 5 | Silver Cleaned Table | MERGE pattern με validation |
# MAGIC | 6 | Regular View | Aggregation view για analysts |
# MAGIC | 7 | Dynamic View | User-aware filtering |
# MAGIC | 8 | GRANT Permissions | Cross-schema access patterns |
# MAGIC | 9 | Column Mask Function | SQL function-based PII protection |
# MAGIC | 10 | Apply Mask + Test | ALTER TABLE SET MASK + verification |
# MAGIC | 11 | Row Filter | Region-based row visibility |
# MAGIC | 12 | Audit Verification | SHOW GRANTS + lineage check |
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day5/Unity_Catalog_Labs/02_Student_HandsOn.py
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨 ΑΛΛΑΞΤΕ ΤΟ ONOMA ΣΑΣ ΕΔΩ ⬇️

# COMMAND ----------

# ⚠️ ΑΛΛΑΞΤΕ ΑΥΤΟ — βάλτε το δικό σας όνομα (lowercase, no spaces)
YOUR_NAME = "george"  # ← Αλλάξτε σε `maria`, `kostas`, κλπ.

# Όλα τα παρακάτω cells θα χρησιμοποιούν αυτή τη variable
SCHEMA_NAME = f"aade_{YOUR_NAME}"
CATALOG = "workspace"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"

print(f"✓ Personalized schema: {FULL_SCHEMA}")
print(f"  All cells below will use this schema. Re-run from the top if you change it.")

# Σιγάζουμε noisy logs
import logging
logging.getLogger("pyspark.sql.connect.client.core").setLevel(logging.CRITICAL)
logging.getLogger("py4j").setLevel(logging.CRITICAL)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 1 — Personal Schema Setup
# MAGIC
# MAGIC **Goal:** Φτιάχνετε δικό σας schema με metadata properties.
# MAGIC
# MAGIC **Concepts:**
# MAGIC - Schema-level COMMENT (documentation)
# MAGIC - DBPROPERTIES (key-value metadata)
# MAGIC - Default ownership

# COMMAND ----------

# 1.1 Δημιουργία schema με metadata
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}
    COMMENT 'Personal training schema for UC hands-on lab. Owner: {YOUR_NAME}.'
    WITH DBPROPERTIES (
      'environment'    = 'training',
      'created_by'     = '{YOUR_NAME}',
      'training_day'   = '5',
      'data_owner'     = 'aade'
    )
""")

# 1.2 Verify
display(spark.sql(f"DESCRIBE SCHEMA EXTENDED {FULL_SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verify σας
# MAGIC - [ ] Output δείχνει `Comment` με το δικό σας όνομα
# MAGIC - [ ] DBPROPERTIES περιέχουν `environment=training`
# MAGIC - [ ] Δείτε στο Catalog Explorer (sidebar) το νέο schema

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 2 — Volume + CSV Upload
# MAGIC
# MAGIC **Goal:** Δημιουργήστε volume και γράψτε CSV file ως «source data».
# MAGIC
# MAGIC **Concepts:**
# MAGIC - Volumes είναι UC-managed file storage
# MAGIC - Path format: `/Volumes/catalog/schema/volume/file`

# COMMAND ----------

# 2.1 Δημιουργία volume
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {FULL_SCHEMA}.raw_files
    COMMENT 'Raw CSV/JSON ingestion volume'
""")

# 2.2 Generate mock data — ΑΑΔΕ tax declarations
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(hash(YOUR_NAME) % 1000)  # Διαφορετικά data για κάθε student

data = pd.DataFrame({
    "statement_id": [f"TX{i:05d}" for i in range(1, 101)],
    "afm":          [f"{900000000 + i:09d}" for i in range(1, 101)],
    "fiscal_year":  [2025] * 100,
    "region":       np.random.choice(
                        ["Αττική", "Θεσσαλονίκη", "Κρήτη", "Πάτρα", "Λάρισα"],
                        100, p=[0.4, 0.25, 0.15, 0.1, 0.1]),
    "tax_amount":   np.round(np.random.uniform(800, 25000, 100), 2),
    "status":       np.random.choice(
                        ["Submitted", "Approved", "Rejected"],
                        100, p=[0.3, 0.6, 0.1]),
    "submitted_at": [(datetime.utcnow() - timedelta(days=np.random.randint(0, 30))).isoformat()
                     for _ in range(100)],
})

# 2.3 Write CSV στο volume
volume_path = f"/Volumes/{CATALOG}/{SCHEMA_NAME}/raw_files/tax_declarations_2025.csv"
data.to_csv(volume_path, index=False)
print(f"✓ Wrote {len(data)} rows to:\n  {volume_path}")

# 2.4 Verify file exists
import os
files = os.listdir(f"/Volumes/{CATALOG}/{SCHEMA_NAME}/raw_files")
print(f"\n✓ Files in volume: {files}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verify σας
# MAGIC - [ ] CSV file αναφέρεται σε output (~100 rows)
# MAGIC - [ ] Files list δείχνει `tax_declarations_2025.csv`
# MAGIC - [ ] Δείτε στο Catalog Explorer → schema σας → Volumes → raw_files

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 3 — Bronze Delta Table
# MAGIC
# MAGIC **Goal:** Φτιάχνετε **Bronze** Delta table από το CSV με πλήρες metadata.
# MAGIC
# MAGIC **Concepts:**
# MAGIC - `read_files()` SQL function για file ingestion
# MAGIC - TBLPROPERTIES για governance tags
# MAGIC - Schema inference

# COMMAND ----------

# 3.1 Create Bronze table
spark.sql(f"""
    CREATE OR REPLACE TABLE {FULL_SCHEMA}.bronze_tax_declarations
    USING DELTA
    COMMENT 'Bronze layer — raw tax declarations από TAXIS source'
    TBLPROPERTIES (
      'layer'           = 'bronze',
      'data_owner'      = 'aade',
      'source_system'   = 'taxis',
      'pii_present'     = 'true',
      'retention_years' = '7'
    )
    AS SELECT * FROM read_files(
      '/Volumes/{CATALOG}/{SCHEMA_NAME}/raw_files/tax_declarations_2025.csv',
      format => 'csv',
      header => true,
      inferSchema => true
    )
""")

# 3.2 Verify count + sample
print(f"=== Bronze table created: {FULL_SCHEMA}.bronze_tax_declarations ===\n")
display(spark.sql(f"SELECT COUNT(*) AS row_count FROM {FULL_SCHEMA}.bronze_tax_declarations"))
display(spark.sql(f"SELECT * FROM {FULL_SCHEMA}.bronze_tax_declarations LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verify σας
# MAGIC - [ ] Row count ≈ 100
# MAGIC - [ ] Sample δείχνει στήλες: statement_id, afm, region, tax_amount, κλπ.
# MAGIC - [ ] Δείτε στο Catalog Explorer το table → tab «Details» έχει TBLPROPERTIES

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 4 — Add Comments & Tags σε Columns
# MAGIC
# MAGIC **Goal:** Documentation σε column-level + sensitivity tags.
# MAGIC
# MAGIC **Concepts:**
# MAGIC - Column comments (documentation)
# MAGIC - Column-level tags (key-value metadata)
# MAGIC - Discoverable μέσω `information_schema`

# COMMAND ----------

# 4.1 Add column comments
spark.sql(f"""
    ALTER TABLE {FULL_SCHEMA}.bronze_tax_declarations
    ALTER COLUMN afm COMMENT 'Αριθμός Φορολογικού Μητρώου — 9 ψηφία, PII'
""")
spark.sql(f"""
    ALTER TABLE {FULL_SCHEMA}.bronze_tax_declarations
    ALTER COLUMN tax_amount COMMENT 'Ποσό φόρου σε ευρώ — non-negative'
""")
spark.sql(f"""
    ALTER TABLE {FULL_SCHEMA}.bronze_tax_declarations
    ALTER COLUMN region COMMENT 'Νομός / Περιφέρεια του φορολογούμενου'
""")

# 4.2 Add column-level tags για sensitive data
spark.sql(f"""
    ALTER TABLE {FULL_SCHEMA}.bronze_tax_declarations
    ALTER COLUMN afm SET TAGS (
      'sensitivity' = 'pii',
      'compliance'  = 'gdpr',
      'data_class'  = 'restricted'
    )
""")
spark.sql(f"""
    ALTER TABLE {FULL_SCHEMA}.bronze_tax_declarations
    ALTER COLUMN tax_amount SET TAGS (
      'data_class' = 'confidential'
    )
""")

# 4.3 Verify metadata
print(f"=== Column metadata για {FULL_SCHEMA}.bronze_tax_declarations ===\n")
display(spark.sql(f"DESCRIBE TABLE EXTENDED {FULL_SCHEMA}.bronze_tax_declarations"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verify σας
# MAGIC - [ ] DESCRIBE δείχνει comments σε `afm`, `tax_amount`, `region`
# MAGIC - [ ] Στο Catalog Explorer → click `afm` column → tab «Tags» δείχνει sensitivity=pii

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 5 — Silver Cleaned Table
# MAGIC
# MAGIC **Goal:** Φτιάχνετε Silver με data quality + MERGE pattern.
# MAGIC
# MAGIC **Concepts:**
# MAGIC - Data validation (filter invalid rows)
# MAGIC - Standardization (UPPER, TRIM)
# MAGIC - Deduplication (DISTINCT/ROW_NUMBER)
# MAGIC - MERGE INTO για upserts

# COMMAND ----------

# 5.1 Create Silver table (πρώτη φορά: CREATE)
spark.sql(f"""
    CREATE OR REPLACE TABLE {FULL_SCHEMA}.silver_tax_declarations
    USING DELTA
    COMMENT 'Silver layer — cleaned & validated tax declarations'
    TBLPROPERTIES (
      'layer'      = 'silver',
      'quality'    = 'verified',
      'data_owner' = 'aade'
    )
    AS
    SELECT
      statement_id,
      afm,
      fiscal_year,
      UPPER(TRIM(region))                    AS region,
      tax_amount,
      UPPER(TRIM(status))                    AS status,
      CAST(submitted_at AS TIMESTAMP)        AS submitted_at,
      current_timestamp()                    AS _silver_at
    FROM {FULL_SCHEMA}.bronze_tax_declarations
    WHERE afm IS NOT NULL
      AND LENGTH(afm) = 9
      AND tax_amount >= 0
      AND status IN ('Submitted', 'Approved', 'Rejected')
""")

# 5.2 Verify drop rate
bronze_count = spark.sql(f"SELECT COUNT(*) AS c FROM {FULL_SCHEMA}.bronze_tax_declarations").first()["c"]
silver_count = spark.sql(f"SELECT COUNT(*) AS c FROM {FULL_SCHEMA}.silver_tax_declarations").first()["c"]
print(f"=== Quality summary ===")
print(f"  Bronze rows: {bronze_count}")
print(f"  Silver rows: {silver_count}")
print(f"  Drop rate:   {(bronze_count - silver_count) / max(bronze_count, 1) * 100:.1f}%")

# COMMAND ----------

# 5.3 Demo MERGE pattern (incremental update)
# Φτιάχνουμε ένα mini-batch με 1 UPDATE + 1 NEW row
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW source_updates AS
    SELECT 'TX00001'  AS statement_id, '900000001' AS afm, 2025 AS fiscal_year,
           'ATTIKH'   AS region, 9999.99 AS tax_amount, 'APPROVED' AS status,
           current_timestamp() AS submitted_at
    UNION ALL
    SELECT 'TX99999'  AS statement_id, '999999999' AS afm, 2025 AS fiscal_year,
           'KRHTH'    AS region, 5500.00 AS tax_amount, 'SUBMITTED' AS status,
           current_timestamp() AS submitted_at
""")

# Execute MERGE
spark.sql(f"""
    MERGE INTO {FULL_SCHEMA}.silver_tax_declarations AS t
    USING source_updates AS s
    ON t.statement_id = s.statement_id
    WHEN MATCHED THEN UPDATE SET
      t.tax_amount = s.tax_amount,
      t.status     = s.status,
      t._silver_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
      statement_id, afm, fiscal_year, region, tax_amount, status, submitted_at, _silver_at
    ) VALUES (
      s.statement_id, s.afm, s.fiscal_year, s.region, s.tax_amount, s.status, s.submitted_at, current_timestamp()
    )
""")

# Verify
print("\n=== After MERGE ===")
display(spark.sql(f"""
    SELECT statement_id, afm, region, tax_amount, status, _silver_at
    FROM {FULL_SCHEMA}.silver_tax_declarations
    WHERE statement_id IN ('TX00001', 'TX99999')
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verify σας
# MAGIC - [ ] Silver έχει ~100 rows
# MAGIC - [ ] `TX00001` έχει updated `tax_amount = 9999.99`
# MAGIC - [ ] `TX99999` (νέο) εμφανίστηκε στο πίνακα

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 6 — Regular View
# MAGIC
# MAGIC **Goal:** Aggregation view για analysts — δεν αντιγράφει data.
# MAGIC
# MAGIC **Concepts:**
# MAGIC - Views = saved SQL queries
# MAGIC - Live data (πάντα current)
# MAGIC - Δεν χρειάζονται storage

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {FULL_SCHEMA}.v_regional_tax_summary
    COMMENT 'Aggregated view: tax collection per region'
    AS
    SELECT
      region,
      fiscal_year,
      COUNT(*)                              AS total_declarations,
      ROUND(SUM(tax_amount), 2)             AS total_tax_collected,
      ROUND(AVG(tax_amount), 2)             AS avg_tax,
      SUM(CASE WHEN status='APPROVED' THEN 1 ELSE 0 END) AS approved_count,
      SUM(CASE WHEN status='REJECTED' THEN 1 ELSE 0 END) AS rejected_count
    FROM {FULL_SCHEMA}.silver_tax_declarations
    GROUP BY region, fiscal_year
""")

# Verify
display(spark.sql(f"SELECT * FROM {FULL_SCHEMA}.v_regional_tax_summary ORDER BY total_tax_collected DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 7 — Dynamic View (user-aware)
# MAGIC
# MAGIC **Goal:** View που αλλάζει το output βάσει του current user.
# MAGIC
# MAGIC **Concepts:**
# MAGIC - `current_user()` SQL function
# MAGIC - `is_member('group')` για group-based logic
# MAGIC - Conditional masking χωρίς separate functions

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {FULL_SCHEMA}.v_dynamic_tax_view
    COMMENT 'Dynamic view: auditors βλέπουν full ΑΦΜ, analysts βλέπουν masked'
    AS
    SELECT
      statement_id,
      CASE
        WHEN is_member('account admins')   THEN afm
        WHEN is_member('aade_auditors')    THEN afm
        ELSE CONCAT('*****', RIGHT(afm, 4))
      END                                  AS afm,
      fiscal_year,
      region,
      tax_amount,
      status,
      submitted_at
    FROM {FULL_SCHEMA}.silver_tax_declarations
""")

# Verify (you'll see full or masked depending on your group membership)
print(f"=== Dynamic view output για user: {spark.sql('SELECT current_user()').first()[0]} ===\n")
display(spark.sql(f"SELECT * FROM {FULL_SCHEMA}.v_dynamic_tax_view LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verify σας
# MAGIC - [ ] Αν είστε admin → βλέπετε full ΑΦΜ
# MAGIC - [ ] Αν δεν είστε admin → βλέπετε `*****XXXX`

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 8 — GRANT Permissions
# MAGIC
# MAGIC **Goal:** Δώστε access σε άλλους users / groups.
# MAGIC
# MAGIC **Concepts:**
# MAGIC - USE CATALOG / USE SCHEMA hierarchy
# MAGIC - Object-level GRANTs
# MAGIC - SHOW GRANTS για audit

# COMMAND ----------

# 8.1 GRANT chain: catalog → schema → table
# Στο Free Edition δεν έχουμε actual groups,
# οπότε grant-άρουμε σε `account users` (built-in group όλων των users)

spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `account users`")
spark.sql(f"GRANT USE SCHEMA  ON SCHEMA  {FULL_SCHEMA} TO `account users`")
spark.sql(f"GRANT SELECT      ON TABLE   {FULL_SCHEMA}.silver_tax_declarations TO `account users`")
spark.sql(f"GRANT SELECT      ON VIEW    {FULL_SCHEMA}.v_regional_tax_summary TO `account users`")
spark.sql(f"GRANT SELECT      ON VIEW    {FULL_SCHEMA}.v_dynamic_tax_view TO `account users`")

# 8.2 Inspect grants
print("=== Grants on silver_tax_declarations ===")
display(spark.sql(f"SHOW GRANTS ON TABLE {FULL_SCHEMA}.silver_tax_declarations"))

print("\n=== Grants on schema ===")
display(spark.sql(f"SHOW GRANTS ON SCHEMA {FULL_SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verify σας
# MAGIC - [ ] SHOW GRANTS δείχνει `account users` με SELECT
# MAGIC - [ ] Schema-level grant: USE SCHEMA δίνεται σε account users

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 9 — Column Mask Function
# MAGIC
# MAGIC **Goal:** SQL function που εφαρμόζεται σε ΑΦΜ column.
# MAGIC
# MAGIC **Concepts:**
# MAGIC - CREATE FUNCTION syntax
# MAGIC - Function-based masking (επαναχρησιμοποιείται σε πολλά tables)
# MAGIC - is_member() για conditional logic

# COMMAND ----------

# 9.1 Δημιουργία masking function
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {FULL_SCHEMA}.mask_afm(afm STRING)
    RETURNS STRING
    COMMENT 'Mask AFM εκτός αν είσαι auditor ή admin'
    RETURN
      CASE
        WHEN is_member('account admins')   THEN afm
        WHEN is_member('aade_auditors')    THEN afm
        ELSE CONCAT('*****', RIGHT(afm, 4))
      END
""")

# 9.2 Test the function χωρίς να την attach-άρουμε ακόμα
print("=== Test mask function (πριν την apply-άρουμε στο table) ===\n")
display(spark.sql(f"""
    SELECT
      afm                                       AS original_afm,
      {FULL_SCHEMA}.mask_afm(afm)               AS masked_afm
    FROM {FULL_SCHEMA}.silver_tax_declarations
    LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verify σας
# MAGIC - [ ] Αν είστε admin → masked_afm == original_afm
# MAGIC - [ ] Αν δεν είστε admin → masked_afm είναι `*****XXXX`
# MAGIC - [ ] Function δείχνεται στο Catalog Explorer → schema → Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 10 — Apply Mask + Test
# MAGIC
# MAGIC **Goal:** Apply το mask permanently στο column. Standard SELECT πλέον επιστρέφει masked data.
# MAGIC
# MAGIC **Concepts:**
# MAGIC - ALTER COLUMN SET MASK
# MAGIC - Automatic application σε **κάθε** query
# MAGIC - Propagation σε downstream views

# COMMAND ----------

# 10.1 Apply mask στο column
spark.sql(f"""
    ALTER TABLE {FULL_SCHEMA}.silver_tax_declarations
    ALTER COLUMN afm SET MASK {FULL_SCHEMA}.mask_afm
""")

print(f"✓ Mask εφαρμόστηκε στο column 'afm' του silver_tax_declarations")

# 10.2 Standard SELECT — masking εφαρμόζεται αυτόματα
print(f"\n=== Standard SELECT μετά το mask ===\n")
display(spark.sql(f"""
    SELECT statement_id, afm, region, tax_amount, status
    FROM {FULL_SCHEMA}.silver_tax_declarations
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💡 Key insight
# MAGIC > Δεν αλλάξαμε το query. Το ίδιο `SELECT afm` που τρέχαμε πριν τώρα επιστρέφει
# MAGIC > **masked** δεδομένα αυτόματα. Αυτό σημαίνει ότι:
# MAGIC > - Παλιά reports / dashboards που χρησιμοποιούν αυτό το table → αυτόματα συμβατά με GDPR
# MAGIC > - Νέοι users δεν χρειάζεται να ξέρουν για PII — protection by default
# MAGIC > - Downstream views που τραβάνε από αυτό το table → βλέπουν masked

# COMMAND ----------

# 10.3 Verify ότι το regional summary view επίσης δεν εκθέτει ΑΦΜ
# (Δεν χρειάζεται μάσκα στο view γιατί δεν επιστρέφει ΑΦΜ — αλλά αν επέστρεφε, θα ήταν masked)
display(spark.sql(f"SELECT * FROM {FULL_SCHEMA}.v_regional_tax_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 11 — Row Filter
# MAGIC
# MAGIC **Goal:** Row-level filtering: ένας user βλέπει μόνο δηλώσεις της Περιφέρειάς του.
# MAGIC
# MAGIC **Concepts:**
# MAGIC - CREATE FUNCTION που επιστρέφει BOOLEAN
# MAGIC - ALTER TABLE SET ROW FILTER
# MAGIC - Map από user → permitted regions

# COMMAND ----------

# 11.1 Δημιουργία row filter function
# Σε production: θα είχατε ένα lookup table (user → allowed regions)
# Εδώ: hardcoded για simplicity
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {FULL_SCHEMA}.filter_by_region(region_value STRING)
    RETURNS BOOLEAN
    COMMENT 'Filter rows: admins βλέπουν όλα, άλλοι μόνο Attica'
    RETURN
      is_member('account admins')
      OR is_member('aade_auditors')
      OR UPPER(region_value) = 'ΑΤΤΙΚΗ'
""")

# 11.2 Apply row filter
spark.sql(f"""
    ALTER TABLE {FULL_SCHEMA}.silver_tax_declarations
    SET ROW FILTER {FULL_SCHEMA}.filter_by_region ON (region)
""")

print(f"✓ Row filter εφαρμόστηκε. Standard SELECT τώρα φιλτράρει automatically.")

# 11.3 Verify — μη-admin user βλέπει μόνο Attica
print(f"\n=== After row filter ===")
print("Πόσες γραμμές βλέπω τώρα (αν δεν είμαι admin) — μόνο Αττική:")
display(spark.sql(f"""
    SELECT region, COUNT(*) AS visible_rows
    FROM {FULL_SCHEMA}.silver_tax_declarations
    GROUP BY region
    ORDER BY region
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💡 Σημείωση
# MAGIC > Αν είστε admin (account admin), θα δείτε **όλες** τις περιφέρειες.
# MAGIC > Σε production environment, οι row filters συνδυάζονται με Entra ID attributes
# MAGIC > (π.χ. `department`, `region`) για αυτόματο context-aware filtering.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Step 12 — Audit Verification
# MAGIC
# MAGIC **Goal:** Final review — όλα τα governance objects σας.

# COMMAND ----------

# 12.1 Summary of objects you created
print(f"=== Summary για schema {FULL_SCHEMA} ===\n")

print("📁 Tables:")
display(spark.sql(f"SHOW TABLES IN {FULL_SCHEMA}"))

print("👁️ Views:")
display(spark.sql(f"SHOW VIEWS IN {FULL_SCHEMA}"))

print("📦 Volumes:")
display(spark.sql(f"SHOW VOLUMES IN {FULL_SCHEMA}"))

print("🔧 Functions:")
display(spark.sql(f"SHOW FUNCTIONS IN {FULL_SCHEMA}"))

# COMMAND ----------

# 12.2 All grants applied
print(f"=== Grants σε όλο το schema {FULL_SCHEMA} ===\n")
display(spark.sql(f"SHOW GRANTS ON SCHEMA {FULL_SCHEMA}"))

# COMMAND ----------

# 12.3 Lineage check — πηγαίνετε στο Catalog Explorer
print(f"""
=== 🎯 ΧΕΙΡΟΚΙΝΗΤΟΣ ΕΛΕΓΧΟΣ ΣΤΟ UI ===

1. Sidebar → Catalog Explorer
2. Click: workspace → {SCHEMA_NAME}
3. Click table: silver_tax_declarations
4. Tab «Lineage»:
   - Upstream: bronze_tax_declarations
   - Downstream: v_regional_tax_summary, v_dynamic_tax_view

5. Tab «Permissions»:
   - Βλέπετε `account users` με SELECT

6. Tab «Sample Data»:
   - Παρατηρήστε: ΑΦΜ είναι masked, μόνο Αττική εμφανίζεται

7. Click column `afm`:
   - Tab «Tags»: sensitivity=pii, compliance=gdpr
   - Tab «Column Lineage»: δείχνει downstream uses
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Συγχαρητήρια — Ολοκληρώσατε το Lab 2!
# MAGIC
# MAGIC ### 🎓 Τι κάνατε
# MAGIC | Βήμα | Αντικείμενο | Status |
# MAGIC |---|---|---|
# MAGIC | 1 | Personal Schema | ✅ |
# MAGIC | 2 | Volume + CSV | ✅ |
# MAGIC | 3 | Bronze Table | ✅ |
# MAGIC | 4 | Column Comments + Tags | ✅ |
# MAGIC | 5 | Silver με MERGE | ✅ |
# MAGIC | 6 | Regular View | ✅ |
# MAGIC | 7 | Dynamic View | ✅ |
# MAGIC | 8 | GRANT Chain | ✅ |
# MAGIC | 9 | Mask Function | ✅ |
# MAGIC | 10 | Column Mask Apply | ✅ |
# MAGIC | 11 | Row Filter | ✅ |
# MAGIC | 12 | Audit Review | ✅ |
# MAGIC
# MAGIC ### 🚀 Επόμενο βήμα
# MAGIC Πηγαίνετε στο **Lab 3: Capstone Challenge** — εκεί θα παίξετε ρόλο
# MAGIC **Compliance Officer** της ΑΑΔΕ και θα κάνετε advanced governance tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Optional: Cleanup (αν θέλετε να ξανατρέξετε το lab από την αρχή)

# COMMAND ----------

# Uncomment για cleanup:
# spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.silver_tax_declarations")
# spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.bronze_tax_declarations")
# spark.sql(f"DROP VIEW IF EXISTS {FULL_SCHEMA}.v_regional_tax_summary")
# spark.sql(f"DROP VIEW IF EXISTS {FULL_SCHEMA}.v_dynamic_tax_view")
# spark.sql(f"DROP FUNCTION IF EXISTS {FULL_SCHEMA}.mask_afm")
# spark.sql(f"DROP FUNCTION IF EXISTS {FULL_SCHEMA}.filter_by_region")
# spark.sql(f"DROP VOLUME IF EXISTS {FULL_SCHEMA}.raw_files")
# spark.sql(f"DROP SCHEMA IF EXISTS {FULL_SCHEMA} CASCADE")
print("Cleanup σχόλια — απομονώστε το αν θέλετε reset")
