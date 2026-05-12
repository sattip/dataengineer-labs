# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Unity Catalog — Lab 3: Capstone Challenge
# MAGIC
# MAGIC **Role:** Compliance Officer της ΑΑΔΕ
# MAGIC **Duration:** ~30 minutes
# MAGIC **Difficulty:** ⭐⭐⭐⭐ Expert
# MAGIC
# MAGIC ## 🎯 Σενάριο
# MAGIC > Είστε **Compliance Officer** της ΑΑΔΕ. Το γραφείο GDPR ζητάει urgent
# MAGIC > **audit report** για τα δεδομένα φορολογουμένων. Πρέπει να απαντήσετε σε 7
# MAGIC > συγκεκριμένες ερωτήσεις χρησιμοποιώντας μόνο το Unity Catalog:
# MAGIC >
# MAGIC > 1. Ποια tables περιέχουν PII;
# MAGIC > 2. Ποιοι users έχουν πρόσβαση σε αυτά;
# MAGIC > 3. Ποιος έκανε access τα τελευταία 7 days;
# MAGIC > 4. Ποιες downstream εξαρτήσεις υπάρχουν αν τροποποιήσουμε schema;
# MAGIC > 5. Υπάρχουν tables χωρίς masking σε PII columns;
# MAGIC > 6. Ποιες stale grants υπάρχουν (users που έφυγαν);
# MAGIC > 7. Συντάξτε compliance report για το γραφείο GDPR.
# MAGIC
# MAGIC ## 📋 7 Capstone Tasks
# MAGIC | # | Task | Output |
# MAGIC |---|---|---|
# MAGIC | 1 | PII Discovery | List όλων των PII columns |
# MAGIC | 2 | Permission Audit | Who has access σε PII tables; |
# MAGIC | 3 | Access History | Activity log τελευταίων 7 days |
# MAGIC | 4 | Lineage Impact | Downstream tables αν αλλάξει schema |
# MAGIC | 5 | Coverage Gap | PII columns χωρίς masking |
# MAGIC | 6 | Stale Grants | Cleanup recommendations |
# MAGIC | 7 | Compliance Report | Final summary πίνακας |
# MAGIC
# MAGIC ## ⚠️ Prerequisites
# MAGIC - Έχετε ολοκληρώσει `02_Student_HandsOn` (έχετε `aade_<yourname>` schema)
# MAGIC - Έχετε `account admin` δικαιώματα (αλλιώς system tables δεν είναι queryable)
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day5/Unity_Catalog_Labs/03_Capstone_Challenge.py
# MAGIC > ```

# COMMAND ----------

# ⚠️ Use the same name as Lab 2 για να βρει το schema σας
YOUR_NAME = "george"  # ← Αλλάξτε όπως στο Lab 2

CATALOG = "workspace"
SCHEMA_NAME = f"aade_{YOUR_NAME}"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA_NAME}"

import logging
logging.getLogger("pyspark.sql.connect.client.core").setLevel(logging.CRITICAL)

print(f"✓ Targeting schema: {FULL_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Task 1 — PII Discovery
# MAGIC
# MAGIC **Ερώτηση από GDPR γραφείο:** «Δώστε μου λίστα **όλων** των columns που είναι
# MAGIC sensitivity=pii στο workspace, μαζί με ownership και comments.»

# COMMAND ----------

# 1.1 Query information_schema για columns με tag sensitivity=pii
# Τα tags εκτίθενται μέσω information_schema.column_tags
pii_query = f"""
SELECT
    ct.catalog_name,
    ct.schema_name,
    ct.table_name,
    ct.column_name,
    ct.tag_value AS sensitivity,
    c.comment   AS column_description,
    c.data_type
FROM system.information_schema.column_tags ct
JOIN system.information_schema.columns c
  ON ct.catalog_name = c.table_catalog
  AND ct.schema_name = c.table_schema
  AND ct.table_name  = c.table_name
  AND ct.column_name = c.column_name
WHERE ct.tag_name = 'sensitivity'
  AND ct.tag_value = 'pii'
  AND ct.schema_name LIKE 'aade%'
ORDER BY ct.schema_name, ct.table_name, ct.column_name
"""

print("=== 🔍 ΑΠΟΤΕΛΕΣΜΑ: Όλες οι PII columns στο workspace ===")
try:
    display(spark.sql(pii_query))
except Exception as e:
    print(f"⚠️ system.information_schema δεν είναι enabled. Fallback σε local query:")
    # Fallback: query το δικό σας schema μόνο
    display(spark.sql(f"""
        SELECT
            '{CATALOG}' AS catalog_name,
            '{SCHEMA_NAME}' AS schema_name,
            table_name,
            column_name,
            comment AS column_description,
            data_type
        FROM information_schema.columns
        WHERE table_schema = '{SCHEMA_NAME}'
          AND column_name IN ('afm', 'name', 'email', 'phone')
    """))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📝 Σημείωση report
# MAGIC > Βρήκαμε X PII columns σε Y tables. Όλες πρέπει να έχουν column masking ή
# MAGIC > να είναι αυστηρά restricted σε auditors-only.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Task 2 — Permission Audit
# MAGIC
# MAGIC **Ερώτηση:** «Ποιοι users / groups έχουν πρόσβαση στο PII data;
# MAGIC Δείξτε breakdown ανά privilege type.»

# COMMAND ----------

# 2.1 Συγκέντρωση των grants σε όλα τα tables του schema
all_grants = []

tables = spark.sql(f"SHOW TABLES IN {FULL_SCHEMA}").collect()
views = spark.sql(f"SHOW VIEWS IN {FULL_SCHEMA}").collect()

for t in tables:
    table_name = t["tableName"]
    full_name = f"{FULL_SCHEMA}.{table_name}"
    try:
        grants = spark.sql(f"SHOW GRANTS ON TABLE {full_name}").collect()
        for g in grants:
            all_grants.append({
                "object_type": "TABLE",
                "object_name": full_name,
                "principal":   g.Principal,
                "privilege":   g.ActionType,
            })
    except Exception:
        pass

for v in views:
    view_name = v["viewName"]
    full_name = f"{FULL_SCHEMA}.{view_name}"
    try:
        grants = spark.sql(f"SHOW GRANTS ON VIEW {full_name}").collect()
        for g in grants:
            all_grants.append({
                "object_type": "VIEW",
                "object_name": full_name,
                "principal":   g.Principal,
                "privilege":   g.ActionType,
            })
    except Exception:
        pass

import pandas as pd
df_grants = pd.DataFrame(all_grants)
print(f"=== 🔐 Permission Audit για {FULL_SCHEMA} ===\n")
print(f"Total grants found: {len(df_grants)}")
display(spark.createDataFrame(df_grants) if len(df_grants) > 0 else None)

# COMMAND ----------

# 2.2 Aggregate: ποιοι principals έχουν τι?
if len(df_grants) > 0:
    summary = df_grants.groupby(['principal', 'privilege']).size().reset_index(name='object_count')
    print("=== 📊 Aggregated: principal × privilege ===\n")
    display(spark.createDataFrame(summary))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Task 3 — Access History (τελευταίες 7 ημέρες)
# MAGIC
# MAGIC **Ερώτηση:** «Ποιος έκανε access στο PII data τις τελευταίες 7 ημέρες;»

# COMMAND ----------

# 3.1 Query system.access.audit
audit_query = f"""
SELECT
    event_time,
    user_identity.email                AS user_email,
    action_name,
    request_params.commandText[0]      AS sql_command_preview,
    request_params.full_name_arg[0]    AS target_object,
    response.status_code               AS status
FROM system.access.audit
WHERE event_time >= current_timestamp() - INTERVAL 7 DAYS
  AND service_name = 'unityCatalog'
  AND action_name IN ('getTable', 'createTable', 'updateTable', 'deleteTable')
  AND COALESCE(request_params.full_name_arg[0], '') LIKE '{FULL_SCHEMA}.%'
ORDER BY event_time DESC
LIMIT 50
"""

print(f"=== 📜 Access history τελευταίων 7 ημερών για {FULL_SCHEMA} ===\n")
try:
    display(spark.sql(audit_query))
except Exception as e:
    # Free Edition συχνά δεν έχει system tables active
    print(f"⚠️ system.access.audit δεν διαθέσιμο: {type(e).__name__}")
    print(f"\nΣε production workspace με Premium SKU, αυτό θα επέστρεφε:")
    print(f"  - Κάθε SELECT/CREATE/ALTER ενέργεια")
    print(f"  - User email, timestamp, query preview")
    print(f"  - Status code (success/failure)")
    print(f"\nFallback: δείτε DESCRIBE HISTORY (Delta-level audit):")
    try:
        display(spark.sql(f"DESCRIBE HISTORY {FULL_SCHEMA}.silver_tax_declarations"))
    except Exception:
        print("  (Table δεν υπάρχει — τρέξτε πρώτα το Lab 2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📝 Σημείωση
# MAGIC > Σε Free Edition `system.access.audit` ίσως είναι disabled. Σε production:
# MAGIC > κάθε ενέργεια κρατείται για 7+ έτη (compliance requirement).
# MAGIC > Alternative: `DESCRIBE HISTORY` για Delta-table-level versioning.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Task 4 — Lineage Impact Analysis
# MAGIC
# MAGIC **Ερώτηση:** «Αν αλλάξω τη στήλη `tax_amount` στο silver, πόσα downstream
# MAGIC objects θα σπάσουν;»

# COMMAND ----------

# 4.1 Column-level lineage query
lineage_query = f"""
SELECT
    source_table_full_name      AS source_table,
    source_column_name          AS source_col,
    target_table_full_name      AS impacted_table,
    target_column_name          AS impacted_col,
    entity_type,
    MAX(event_time)             AS last_seen
FROM system.access.column_lineage
WHERE source_table_full_name = '{FULL_SCHEMA}.silver_tax_declarations'
  AND source_column_name = 'tax_amount'
GROUP BY source_table_full_name, source_column_name,
         target_table_full_name, target_column_name, entity_type
ORDER BY last_seen DESC
"""

print(f"=== 🔗 Downstream impact για {FULL_SCHEMA}.silver_tax_declarations.tax_amount ===\n")
try:
    impact = spark.sql(lineage_query)
    cnt = impact.count()
    print(f"Found {cnt} downstream dependencies\n")
    display(impact)
except Exception as e:
    print(f"⚠️ system.access.column_lineage δεν διαθέσιμο σε Free Edition")
    print(f"\nFallback: εμφανίσιμη lineage μέσω Catalog Explorer:")
    print(f"  1. Πηγαίνετε Catalog → {SCHEMA_NAME} → silver_tax_declarations")
    print(f"  2. Tab 'Lineage' — δείτε όλες τις downstream views/tables")
    print(f"  3. Click στη στήλη 'tax_amount' → tab 'Column Lineage'")
    print(f"\nManual lineage check μέσω SHOW CREATE:")
    try:
        display(spark.sql(f"SHOW VIEWS IN {FULL_SCHEMA}"))
        view_ddl = spark.sql(f"SHOW CREATE TABLE {FULL_SCHEMA}.v_regional_tax_summary").first()[0]
        print(f"\nDDL of v_regional_tax_summary (references tax_amount):")
        print(view_ddl)
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📝 Σημείωση
# MAGIC > Σε production: το column-level lineage σου δείχνει **ακριβώς** ποιες views,
# MAGIC > tables, dashboards, και ML pipelines θα σπάσουν αν αλλάξεις τη στήλη.
# MAGIC > Αυτό είναι το killer feature του UC για **change management**.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Task 5 — Coverage Gap (PII without masking)
# MAGIC
# MAGIC **Ερώτηση:** «Υπάρχουν PII columns που **δεν** έχουν masking;»

# COMMAND ----------

# 5.1 Cross-reference: columns με sensitivity=pii ΧΩΡΙΣ mask
gap_query = f"""
WITH pii_columns AS (
    SELECT
        catalog_name,
        schema_name,
        table_name,
        column_name
    FROM system.information_schema.column_tags
    WHERE tag_name = 'sensitivity'
      AND tag_value = 'pii'
      AND schema_name LIKE 'aade%'
),
masked_columns AS (
    SELECT
        catalog_name,
        schema_name,
        table_name,
        column_name
    FROM system.information_schema.column_masks
    WHERE schema_name LIKE 'aade%'
)
SELECT
    p.catalog_name,
    p.schema_name,
    p.table_name,
    p.column_name,
    CASE WHEN m.column_name IS NOT NULL THEN '✅ Masked' ELSE '🚨 NO MASK' END AS mask_status
FROM pii_columns p
LEFT JOIN masked_columns m
  ON p.catalog_name = m.catalog_name
  AND p.schema_name = m.schema_name
  AND p.table_name  = m.table_name
  AND p.column_name = m.column_name
ORDER BY mask_status DESC, p.schema_name, p.table_name
"""

print("=== 🚨 PII Coverage Gap Analysis ===\n")
try:
    display(spark.sql(gap_query))
except Exception:
    # Fallback για Free Edition
    print(f"⚠️ system.information_schema.column_masks δεν διαθέσιμο")
    print(f"\nFallback: manual DESCRIBE TABLE EXTENDED για το δικό σας schema:")
    display(spark.sql(f"DESCRIBE TABLE EXTENDED {FULL_SCHEMA}.silver_tax_declarations"))
    print(f"\nΣτο output ψάξτε για 'Column Mask' section — αν υπάρχει = masked.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📝 Action Item
# MAGIC > Για κάθε PII column χωρίς mask:
# MAGIC > 1. Δημιουργήστε mask function
# MAGIC > 2. `ALTER COLUMN SET MASK`
# MAGIC > 3. Τεκμηριώστε στο compliance report

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Task 6 — Stale Grants Detection
# MAGIC
# MAGIC **Ερώτηση:** «Υπάρχουν grants σε users που **έχουν φύγει** από την εταιρεία;»

# COMMAND ----------

# 6.1 Get all grants + cross-reference με active users
stale_grants_query = f"""
WITH all_grants AS (
    -- Από system.information_schema.table_privileges (account-wide grants)
    SELECT
        grantee     AS principal,
        privilege_type AS privilege,
        table_catalog || '.' || table_schema || '.' || table_name AS object_name
    FROM system.information_schema.table_privileges
    WHERE table_schema LIKE 'aade%'
),
inactive_grants AS (
    SELECT
        principal,
        privilege,
        object_name,
        'check_if_active' AS status
    FROM all_grants
    -- Σε production: JOIN με Entra ID active users table
    -- Εδώ: highlight users με συγκεκριμένα patterns (π.χ. contains 'temp', 'intern', 'guest')
    WHERE LOWER(principal) RLIKE '(temp|intern|guest|test|former)'
)
SELECT * FROM inactive_grants
ORDER BY object_name
"""

print("=== 🧹 Stale Grants Detection ===\n")
try:
    display(spark.sql(stale_grants_query))
except Exception as e:
    print(f"⚠️ system.information_schema.table_privileges fallback:\n")
    print("Σε production τρέχεις:")
    print(f"  SHOW GRANTS ON CATALOG {CATALOG} | FROM users WHERE active=false")
    print("\nΕδώ ένα demo με local grants:")
    try:
        display(spark.sql(f"SHOW GRANTS ON SCHEMA {FULL_SCHEMA}"))
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📝 Action Plan
# MAGIC > 1. Export τη λίστα stale grants
# MAGIC > 2. Cross-check με HR / IT για offboarded employees
# MAGIC > 3. REVOKE access για όλους τους inactive users
# MAGIC > 4. Document σε audit log

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Task 7 — Compliance Report
# MAGIC
# MAGIC **Final deliverable:** Στρώστε όλα τα findings σε ένα executive summary table.

# COMMAND ----------

# 7.1 Create compliance summary table
from datetime import datetime

report_data = [
    ("PII Columns Discovered",        "Count of columns tagged sensitivity=pii",        "Task 1"),
    ("Active Principals",             "Distinct users/groups με access σε PII data",    "Task 2"),
    ("Access Events (7d)",            "SELECT/CREATE/ALTER actions τελευταίες 7 ημέρες", "Task 3"),
    ("Downstream Dependencies",       "Tables/views που εξαρτώνται από PII data",        "Task 4"),
    ("Masking Coverage Gap",          "PII columns χωρίς column mask",                   "Task 5"),
    ("Stale Grants",                  "Grants σε inactive users",                        "Task 6"),
]

# Σε production θα γέμιζες αυτά από τα query results των tasks 1-6
# Εδώ τα γεμίζουμε με sample values από το δικό σας schema
spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.gdpr_compliance_report")
spark.sql(f"""
    CREATE TABLE {FULL_SCHEMA}.gdpr_compliance_report (
        metric STRING,
        description STRING,
        source_task STRING,
        finding_value STRING,
        risk_level STRING,
        action_required STRING,
        reported_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'GDPR Compliance Report — generated από Capstone Lab 3'
""")

# Insert sample findings (σε production: τα συμπληρώνεις από τα queries)
spark.sql(f"""
    INSERT INTO {FULL_SCHEMA}.gdpr_compliance_report VALUES
    ('PII Columns Discovered',  'sensitivity=pii tagged columns',  'Task 1', '2', 'HIGH',   'Verify masking coverage',     current_timestamp()),
    ('Active Principals',       'Users/groups με PII access',       'Task 2', '1 group (account users)', 'MEDIUM', 'Quarterly review',           current_timestamp()),
    ('Access Events (7d)',      'Recent access actions',            'Task 3', 'See system.access.audit',  'INFO',   'Monitor anomalies',           current_timestamp()),
    ('Downstream Dependencies', 'Tables depending on PII',          'Task 4', '2 views + 1 gold table',    'MEDIUM', 'Test impact before changes', current_timestamp()),
    ('Masking Coverage Gap',    'Unmasked PII columns',             'Task 5', '0',                         'LOW',    'Maintain via CI/CD checks',  current_timestamp()),
    ('Stale Grants',            'Inactive users with grants',       'Task 6', '0 detected',                'LOW',    'Quarterly offboarding scan', current_timestamp())
""")

print(f"=== 📋 GDPR COMPLIANCE REPORT για {FULL_SCHEMA} ===\n")
display(spark.sql(f"SELECT * FROM {FULL_SCHEMA}.gdpr_compliance_report"))

# COMMAND ----------

# 7.2 Risk-level breakdown
print("\n=== 🚨 Risk-level summary ===")
display(spark.sql(f"""
    SELECT risk_level, COUNT(*) AS findings_count
    FROM {FULL_SCHEMA}.gdpr_compliance_report
    GROUP BY risk_level
    ORDER BY CASE risk_level
        WHEN 'HIGH' THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'LOW' THEN 3
        WHEN 'INFO' THEN 4
    END
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Capstone Complete!
# MAGIC
# MAGIC ### 🎓 Τι κάνατε ως Compliance Officer
# MAGIC
# MAGIC | Task | Σκοπός | Output |
# MAGIC |---|---|---|
# MAGIC | 1 | PII Discovery | List sensitivity=pii columns |
# MAGIC | 2 | Permission Audit | Who has what access |
# MAGIC | 3 | Access History | Last 7 days activity |
# MAGIC | 4 | Lineage Impact | Downstream dependencies |
# MAGIC | 5 | Coverage Gap | Unmasked PII columns |
# MAGIC | 6 | Stale Grants | Inactive users cleanup |
# MAGIC | 7 | Compliance Report | Executive summary |
# MAGIC
# MAGIC ### 🎯 Key insights
# MAGIC > Με μόνο SQL queries σε system tables, κάνατε **πλήρες audit GDPR**:
# MAGIC > - Δεν χρειάζεστε custom logging
# MAGIC > - Δεν χρειάζεστε external SIEM
# MAGIC > - Όλα built-in στο Unity Catalog
# MAGIC >
# MAGIC > Σε production ΑΑΔΕ: αυτά τρέχουν αυτοματοποιημένα ως **scheduled jobs**
# MAGIC > και τα αποτελέσματα εμφανίζονται σε Dashboard που βλέπει το νομικό τμήμα.
# MAGIC
# MAGIC ### 📚 Συνολικό Glossary (από τα 3 labs)
# MAGIC
# MAGIC | Όρος | Σημασία |
# MAGIC |---|---|
# MAGIC | **Metastore** | Container για catalogs στο Unity Catalog account |
# MAGIC | **Catalog** | Top-level namespace (workspace, prod, dev) |
# MAGIC | **Schema** | Group of objects μέσα στο catalog (όπως database) |
# MAGIC | **Volume** | UC-managed file storage |
# MAGIC | **3-part name** | catalog.schema.object |
# MAGIC | **GRANT/REVOKE** | SQL-based permission management |
# MAGIC | **is_member()** | Function που επιστρέφει BOOLEAN για group membership |
# MAGIC | **Column Mask** | SQL function που εφαρμόζεται σε column-level |
# MAGIC | **Row Filter** | SQL function για row-level visibility |
# MAGIC | **Tag** | Key-value metadata σε column/table/schema/catalog |
# MAGIC | **Dynamic View** | View με user-aware logic μέσω current_user() |
# MAGIC | **Lineage** | Auto-tracked data flow upstream/downstream |
# MAGIC | **System tables** | Built-in audit/lineage/billing tables |
# MAGIC | **information_schema** | SQL standard metadata views |
# MAGIC | **column_tags** | system view με όλα τα tags ανά column |
# MAGIC | **column_masks** | system view με όλα τα applied masks |
# MAGIC | **column_lineage** | system table με column-level data flow |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Final Cleanup (μετά το lab)

# COMMAND ----------

# Πλήρες cleanup του δικού σας schema (uncomment για να τρέξετε):
# spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.gdpr_compliance_report")
# spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.silver_tax_declarations")
# spark.sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.bronze_tax_declarations")
# spark.sql(f"DROP VIEW  IF EXISTS {FULL_SCHEMA}.v_regional_tax_summary")
# spark.sql(f"DROP VIEW  IF EXISTS {FULL_SCHEMA}.v_dynamic_tax_view")
# spark.sql(f"DROP FUNCTION IF EXISTS {FULL_SCHEMA}.mask_afm")
# spark.sql(f"DROP FUNCTION IF EXISTS {FULL_SCHEMA}.filter_by_region")
# spark.sql(f"DROP VOLUME IF EXISTS {FULL_SCHEMA}.raw_files")
# spark.sql(f"DROP SCHEMA IF EXISTS {FULL_SCHEMA} CASCADE")
print(f"Schema {FULL_SCHEMA} αφέθηκε intact. Uncomment cleanup commands για full reset.")
