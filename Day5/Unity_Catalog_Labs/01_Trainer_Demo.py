# Databricks notebook source
# MAGIC %md
# MAGIC # 🎓 Unity Catalog — Lab 1: Trainer Demo
# MAGIC
# MAGIC **Role:** Trainer / Instructor
# MAGIC **Audience:** Students observe — instructor leads
# MAGIC **Duration:** ~30 minutes
# MAGIC **Mode:** Single instructor execution, projector shared
# MAGIC
# MAGIC ## 🎯 Σκοπός
# MAGIC > Να δείξετε **ζωντανά** όλα τα UC concepts σε πραγματικό workspace ΑΑΔΕ context.
# MAGIC > Οι μαθητές βλέπουν, ρωτούν, κρατούν notes — **δεν** εκτελούν τα ίδια κελιά παράλληλα.
# MAGIC > Στο επόμενο lab (`02_Student_HandsOn`) θα τα ξανατρέξουν μόνοι τους.
# MAGIC
# MAGIC ## 📋 7 Demo Steps (~4 min ανά step)
# MAGIC | # | Demo Topic | Concepts |
# MAGIC |---|---|---|
# MAGIC | 1 | Current State Inspection | Workspace exploration, current permissions |
# MAGIC | 2 | Catalog Hierarchy | CREATE CATALOG/SCHEMA/VOLUME, 3-level namespace |
# MAGIC | 3 | Data Ingestion με Governance | Upload data, CREATE TABLE, metadata |
# MAGIC | 4 | RBAC σε Action | GRANT/REVOKE, groups, inheritance |
# MAGIC | 5 | Column Masks (Live Demo) | SQL function masks, is_member() |
# MAGIC | 6 | Auto Lineage | Catalog Explorer lineage tab |
# MAGIC | 7 | Audit Log Queries | system.access.audit walkthrough |
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day5/Unity_Catalog_Labs/01_Trainer_Demo.py
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Demo Step 1 — Current State Inspection
# MAGIC
# MAGIC **Time:** ~4 min
# MAGIC **Show:** Πού είμαστε τώρα στο workspace, τι metastore έχουμε, τι rights έχω εγώ.
# MAGIC
# MAGIC ### 💬 Talking points
# MAGIC > «Πριν φτιάξουμε οτιδήποτε, ας δούμε **πού** είμαστε. Το UC ξεκινάει από **metastore**
# MAGIC > — ένα ανά Databricks account/region. Όχι ανά workspace.»

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1.1 Δείτε τα διαθέσιμα catalogs που έχει το workspace
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1.2 Δείτε ποιος είστε και ποιο workspace
# MAGIC SELECT
# MAGIC   current_user()                AS my_user,
# MAGIC   current_database()            AS current_schema,
# MAGIC   current_catalog()             AS current_catalog,
# MAGIC   current_version().databricks_runtime_version AS dbr_version;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1.3 Schemas που υπάρχουν στο workspace catalog
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1.4 Ποια rights έχω εγώ στο workspace catalog;
# MAGIC SHOW GRANTS `IDENTIFIER`(current_user()) ON CATALOG workspace;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verification checkpoint
# MAGIC - [ ] Οι μαθητές βλέπουν `workspace` catalog
# MAGIC - [ ] Καταλαβαίνουν ότι metastore != workspace
# MAGIC - [ ] Βλέπουν ότι έχω `ALL PRIVILEGES` (γιατί είμαι owner)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Demo Step 2 — Catalog Hierarchy
# MAGIC
# MAGIC **Time:** ~4 min
# MAGIC **Show:** Δημιουργία 3-level namespace: catalog → schema → object (volume + table).
# MAGIC
# MAGIC ### 💬 Talking points
# MAGIC > «Στο UC κάθε αντικείμενο έχει **3-part name**: catalog.schema.object. Σήμερα θα
# MAGIC > φτιάξουμε `workspace.aade_demo.tax_declarations` — δικό μου demo namespace.»

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2.1 Δημιουργία ξεχωριστού schema για το demo
# MAGIC -- (Αποφεύγουμε σύγκρουση με το workspace.aade που χρησιμοποιούν οι μαθητές)
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.aade_demo
# MAGIC COMMENT 'Demo schema για live UC walkthrough. Owner: trainer.'
# MAGIC WITH DBPROPERTIES ('environment' = 'demo', 'created_by' = 'trainer');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2.2 Δημιουργία Volume (file storage) μέσα στο schema
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.aade_demo.raw_files
# MAGIC COMMENT 'Volume για raw CSV/JSON ingestion';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2.3 Confirm hierarchy
# MAGIC SELECT 'CATALOG' AS level, 'workspace' AS name
# MAGIC UNION ALL SELECT 'SCHEMA', 'workspace.aade_demo'
# MAGIC UNION ALL SELECT 'VOLUME', 'workspace.aade_demo.raw_files'
# MAGIC UNION ALL SELECT 'PATH', '/Volumes/workspace/aade_demo/raw_files'

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verification checkpoint
# MAGIC - [ ] Οι μαθητές βλέπουν στο Catalog Explorer (sidebar) το νέο schema
# MAGIC - [ ] Καταλαβαίνουν την ιεραρχία catalog → schema → volume
# MAGIC - [ ] Σχόλιο: «Το ίδιο pattern με folders στο filesystem»

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Demo Step 3 — Data Ingestion με Governance
# MAGIC
# MAGIC **Time:** ~5 min
# MAGIC **Show:** Πώς δεδομένα μπαίνουν στο UC με metadata, comments, tags.

# COMMAND ----------

# 3.1 Δημιουργία mock δεδομένων ΑΑΔΕ
import pandas as pd
import numpy as np

np.random.seed(42)
data = pd.DataFrame({
    "statement_id": [f"TX{i:05d}" for i in range(1, 51)],
    "afm":          [f"{900000000 + i:09d}" for i in range(1, 51)],
    "fiscal_year":  [2025] * 50,
    "region":       np.random.choice(["Αττική", "Θεσσαλονίκη", "Κρήτη", "Πάτρα"], 50),
    "tax_amount":   np.round(np.random.uniform(800, 18000, 50), 2),
    "status":       np.random.choice(["Submitted", "Approved", "Rejected"], 50, p=[0.4, 0.5, 0.1]),
})

# Γράφουμε CSV στο volume
volume_path = "/Volumes/workspace/aade_demo/raw_files/tax_declarations.csv"
data.to_csv(volume_path, index=False)
print(f"✓ Wrote 50 rows to {volume_path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3.2 Δημιουργία Delta table από CSV με ΠΛΗΡΕΣ metadata
# MAGIC CREATE OR REPLACE TABLE workspace.aade_demo.tax_declarations
# MAGIC USING DELTA
# MAGIC COMMENT 'Φορολογικές δηλώσεις 2025 — ΑΑΔΕ TAXIS source'
# MAGIC TBLPROPERTIES (
# MAGIC   'data_owner'  = 'aade',
# MAGIC   'certified'   = 'true',
# MAGIC   'sensitivity' = 'pii',
# MAGIC   'retention_years' = '7'
# MAGIC )
# MAGIC AS SELECT * FROM read_files(
# MAGIC   '/Volumes/workspace/aade_demo/raw_files/tax_declarations.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3.3 Προσθήκη column comments + tags σε κρίσιμες στήλες
# MAGIC ALTER TABLE workspace.aade_demo.tax_declarations
# MAGIC ALTER COLUMN afm
# MAGIC COMMENT 'Αριθμός Φορολογικού Μητρώου — 9 ψηφία, sensitive PII';
# MAGIC
# MAGIC -- 3.4 Set tags σε column-level (απαιτεί UC)
# MAGIC ALTER TABLE workspace.aade_demo.tax_declarations
# MAGIC ALTER COLUMN afm
# MAGIC SET TAGS ('sensitivity' = 'pii', 'compliance' = 'gdpr');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3.5 Show metadata
# MAGIC DESCRIBE EXTENDED workspace.aade_demo.tax_declarations;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verification checkpoint
# MAGIC - [ ] Πίνακας δημιουργήθηκε με comments + tags
# MAGIC - [ ] Δείξτε στο Catalog Explorer → tab «Overview» τα tags
# MAGIC - [ ] Σημείωση: «certified=true tag = αυτό το data είναι production-grade»

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Demo Step 4 — RBAC σε Action
# MAGIC
# MAGIC **Time:** ~4 min
# MAGIC **Show:** GRANT/REVOKE με account groups, inheritance, USE CATALOG concept.
# MAGIC
# MAGIC ### 💬 Talking points
# MAGIC > «Permissions στο UC γίνονται με **standard SQL**. Όχι UI clicks — όλα ως code.
# MAGIC > Best practice: **groups, όχι users**. Όταν φεύγει υπάλληλος → group membership
# MAGIC > αλλάζει, όχι 50 GRANT statements.»

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4.1 Σχήμα ένας virtual analyst (στο Free Edition θα χρησιμοποιήσουμε όνομα ομάδας)
# MAGIC -- Σε production environment, η ομάδα είναι ήδη synced από Entra ID / Okta
# MAGIC -- Εδώ απλώς θα δείξουμε τις εντολές
# MAGIC
# MAGIC -- USE CATALOG: «μου επιτρέπεται να δω αυτό το catalog»
# MAGIC -- USE SCHEMA:  «μου επιτρέπεται να δω αυτό το schema»
# MAGIC -- SELECT:      «μου επιτρέπεται να διαβάσω αυτόν τον πίνακα»
# MAGIC
# MAGIC -- Παράδειγμα GRANT αλυσίδα (commented για να μη σπάσει σε missing group):
# MAGIC
# MAGIC -- GRANT USE CATALOG ON CATALOG workspace TO `aade_analysts`;
# MAGIC -- GRANT USE SCHEMA  ON SCHEMA workspace.aade_demo TO `aade_analysts`;
# MAGIC -- GRANT SELECT      ON TABLE  workspace.aade_demo.tax_declarations TO `aade_analysts`;
# MAGIC
# MAGIC -- Στο Free Edition χωρίς groups, μπορούμε να κάνουμε το ίδιο σε individual user:
# MAGIC GRANT SELECT ON TABLE workspace.aade_demo.tax_declarations TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4.2 Δείτε ποιος έχει τι
# MAGIC SHOW GRANTS ON TABLE workspace.aade_demo.tax_declarations;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4.3 Inheritance check: αν GRANT σε SCHEMA, ισχύει σε όλα τα tables;
# MAGIC -- Εξήγηση: NAI ΟΧΙ - GRANT σε schema ΔΕΝ ισχύει αυτόματα σε tables.
# MAGIC -- Χρειάζεσαι GRANT SELECT ON ALL TABLES IN SCHEMA για explicit cascade.
# MAGIC
# MAGIC GRANT SELECT ON ALL TABLES IN SCHEMA workspace.aade_demo TO `account users`;
# MAGIC GRANT SELECT ON ALL VIEWS  IN SCHEMA workspace.aade_demo TO `account users`;
# MAGIC GRANT SELECT ON ANY FILE                                  TO `account users`;
# MAGIC
# MAGIC -- Future grants pattern (recommended):
# MAGIC ALTER SCHEMA workspace.aade_demo
# MAGIC SET OWNER TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4.4 REVOKE syntax — απαραίτητο όταν φεύγει user
# MAGIC -- REVOKE SELECT ON TABLE workspace.aade_demo.tax_declarations FROM `john.doe@aade.gr`;
# MAGIC
# MAGIC -- Demo only — δεν χρειάζεται actual revoke εδώ
# MAGIC SELECT 'REVOKE pattern shown — not executed in demo' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verification checkpoint
# MAGIC - [ ] Μαθητές κατάλαβαν: GRANT σε schema ≠ GRANT σε tables — χρειάζεται explicit
# MAGIC - [ ] Best practice: groups (`aade_analysts`), όχι individual emails
# MAGIC - [ ] Σημείωση: σε production, group memberships sync αυτόματα από Entra ID

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Demo Step 5 — Column Masks (Live Demo)
# MAGIC
# MAGIC **Time:** ~6 min
# MAGIC **Show:** SQL function-based masking για ΑΦΜ. Live test: «πώς θα δω το ίδιο
# MAGIC table ως admin vs ως analyst;»
# MAGIC
# MAGIC ### 💬 Talking points
# MAGIC > «Το column mask είναι **SQL function** που εφαρμόζεται στη στήλη. Αυτόματα
# MAGIC > εφαρμόζεται σε **κάθε** query — δεν χρειάζεται να θυμάσαι «πρόσεξε τα PII».»

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5.1 Δημιουργία masking function
# MAGIC -- Λογική: αν είσαι auditor → full ΑΦΜ. Αλλιώς → μόνο τα 4 τελευταία ψηφία.
# MAGIC CREATE OR REPLACE FUNCTION workspace.aade_demo.mask_afm(afm STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Επιστρέφει full ΑΦΜ για auditors, masked για όλους τους άλλους'
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN is_member('aade_auditors') THEN afm
# MAGIC     WHEN is_member('account admins') THEN afm
# MAGIC     ELSE CONCAT('*****', RIGHT(afm, 4))
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5.2 Test the function χωρίς το mask ακόμα
# MAGIC SELECT
# MAGIC   afm                                       AS original_afm,
# MAGIC   workspace.aade_demo.mask_afm(afm)         AS masked_afm,
# MAGIC   is_member('aade_auditors')                AS am_i_auditor,
# MAGIC   is_member('account admins')               AS am_i_admin
# MAGIC FROM workspace.aade_demo.tax_declarations
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5.3 Apply mask permanently στη στήλη
# MAGIC ALTER TABLE workspace.aade_demo.tax_declarations
# MAGIC ALTER COLUMN afm SET MASK workspace.aade_demo.mask_afm;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5.4 ΤΩΡΑ ένα standard SELECT — το masking εφαρμόζεται αυτόματα
# MAGIC SELECT statement_id, afm, region, tax_amount, status
# MAGIC FROM workspace.aade_demo.tax_declarations
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💡 Notice για τους μαθητές
# MAGIC > «Παρατηρήστε:
# MAGIC > 1. Δεν αλλάξαμε το query. Το ίδιο `SELECT afm` που τρέχαμε πριν.
# MAGIC > 2. Το mask εφαρμόστηκε **αυτόματα** σε column level.
# MAGIC > 3. Είμαι admin → βλέπω full ΑΦΜ. Αν ήμουν analyst → θα έβλεπα `*****0042`.
# MAGIC > 4. Αν δημιουργήσετε **view** πάνω σε αυτόν τον πίνακα, το mask μεταφέρεται!»

# COMMAND ----------

# MAGIC %md
# MAGIC ### ☑️ Verification checkpoint
# MAGIC - [ ] Function δημιουργήθηκε
# MAGIC - [ ] Mask εφαρμόστηκε στη στήλη `afm`
# MAGIC - [ ] Μαθητές κατάλαβαν τη διαφορά: function-based mask vs hardcoded view

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Demo Step 6 — Auto Lineage
# MAGIC
# MAGIC **Time:** ~4 min
# MAGIC **Show:** Lineage graph στο Catalog Explorer + column-level lineage.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 6.1 Φτιάχνουμε downstream views/tables για να δημιουργηθεί lineage
# MAGIC CREATE OR REPLACE VIEW workspace.aade_demo.v_regional_summary AS
# MAGIC SELECT
# MAGIC   region,
# MAGIC   COUNT(*) AS total_declarations,
# MAGIC   ROUND(SUM(tax_amount), 2) AS total_tax_collected,
# MAGIC   ROUND(AVG(tax_amount), 2) AS avg_tax
# MAGIC FROM workspace.aade_demo.tax_declarations
# MAGIC GROUP BY region;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 6.2 Aggregate ένα ακόμα table downstream
# MAGIC CREATE OR REPLACE TABLE workspace.aade_demo.gold_high_value_taxpayers
# MAGIC USING DELTA
# MAGIC COMMENT 'Top-tier taxpayers — derived από tax_declarations'
# MAGIC AS
# MAGIC SELECT afm, region, tax_amount, status
# MAGIC FROM workspace.aade_demo.tax_declarations
# MAGIC WHERE tax_amount > 10000
# MAGIC   AND status = 'Approved';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 6.3 Verify
# MAGIC SELECT * FROM workspace.aade_demo.v_regional_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 Live UI Demo
# MAGIC
# MAGIC **Πηγαίνετε στο Catalog Explorer** (sidebar → Catalog):
# MAGIC
# MAGIC 1. Click `workspace` → `aade_demo` → `tax_declarations`
# MAGIC 2. Tab **«Lineage»** → δείτε:
# MAGIC    - Upstream: το CSV στο volume
# MAGIC    - Downstream: η view `v_regional_summary` + το table `gold_high_value_taxpayers`
# MAGIC 3. Click **«Column lineage»** σε κάποια στήλη (π.χ. `tax_amount`):
# MAGIC    - Δείτε ότι έχει πάει στο `gold_high_value_taxpayers.tax_amount`
# MAGIC    - Και έχει aggregated σε `v_regional_summary.total_tax_collected`
# MAGIC
# MAGIC ### 💬 Talking points
# MAGIC > «Παρατηρήστε: **δεν** γράψαμε κανένα lineage tracking code. Το UC καταλαβαίνει
# MAGIC > αυτόματα από κάθε `SELECT` ποια columns επηρεάστηκαν. Αυτό είναι **impossible**
# MAGIC > σε classic Hive Metastore.»

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🪜 Demo Step 7 — Audit Log Queries
# MAGIC
# MAGIC **Time:** ~3 min
# MAGIC **Show:** Πώς τα system tables μας απαντάνε σε compliance ερωτήσεις.
# MAGIC
# MAGIC ### 💬 Talking points
# MAGIC > «Κάθε ενέργεια στο UC καταγράφεται. Σε production: 'ποιος έκανε SELECT σε
# MAGIC > sensitive table τους τελευταίους 3 μήνες;' = 1 SQL query.»

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7.1 Διαθέσιμα system schemas (διαθέσιμα μόνο σε account admins / metastore owners)
# MAGIC SHOW SCHEMAS IN system;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7.2 Audit log structure
# MAGIC -- ΣΗΜΕΙΩΣΗ: Σε Free Edition system tables ίσως δεν είναι enabled —
# MAGIC -- σε αυτή την περίπτωση δείξτε τη structure χωρίς να τρέξετε το query.
# MAGIC DESCRIBE TABLE system.access.audit;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7.3 Παράδειγμα ερώτησης: ποιος έκανε αλλαγές στο schema μας τις τελευταίες 24h;
# MAGIC -- (Αν τα system tables δεν είναι enabled, σχολιάστε το query και δείξτε μόνο τη δομή)
# MAGIC
# MAGIC SELECT
# MAGIC   event_time,
# MAGIC   user_identity.email AS who,
# MAGIC   action_name,
# MAGIC   request_params,
# MAGIC   response.status_code
# MAGIC FROM system.access.audit
# MAGIC WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC   AND action_name IN ('createTable', 'updateTable', 'updatePermissions')
# MAGIC   AND service_name = 'unityCatalog'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7.4 Column lineage system table — for impact analysis
# MAGIC SELECT
# MAGIC   source_table_full_name,
# MAGIC   source_column_name,
# MAGIC   target_table_full_name,
# MAGIC   target_column_name,
# MAGIC   event_time
# MAGIC FROM system.access.column_lineage
# MAGIC WHERE source_table_full_name = 'workspace.aade_demo.tax_declarations'
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💡 Closing message για τους μαθητές
# MAGIC
# MAGIC > «Με αυτό κλείνουμε το demo. Είδαμε **όλο** το UC journey:
# MAGIC > 1. Hierarchy (catalog/schema/volume)
# MAGIC > 2. Data ingestion με governance metadata
# MAGIC > 3. RBAC με SQL GRANTs
# MAGIC > 4. Column masking με functions
# MAGIC > 5. Automatic lineage tracking
# MAGIC > 6. Audit logs σε queryable system tables
# MAGIC >
# MAGIC > Τώρα είναι η σειρά σας. Στο επόμενο lab (`02_Student_HandsOn`) θα ξανατρέξετε
# MAGIC > εσείς όλα αυτά — αλλά **με το δικό σας schema** για να μη συγκρουστείτε.»

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Optional Cleanup (τέλος demo)
# MAGIC
# MAGIC Αν θέλετε να ξανατρέξετε το demo από την αρχή σε επόμενη ομάδα:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cleanup commands (μην τα τρέξετε αν θέλετε να αναφέρεστε στο demo αργότερα)
# MAGIC -- DROP TABLE IF EXISTS workspace.aade_demo.gold_high_value_taxpayers;
# MAGIC -- DROP VIEW IF EXISTS workspace.aade_demo.v_regional_summary;
# MAGIC -- DROP FUNCTION IF EXISTS workspace.aade_demo.mask_afm;
# MAGIC -- DROP TABLE IF EXISTS workspace.aade_demo.tax_declarations;
# MAGIC -- DROP VOLUME IF EXISTS workspace.aade_demo.raw_files;
# MAGIC -- DROP SCHEMA IF EXISTS workspace.aade_demo CASCADE;
# MAGIC SELECT 'Cleanup commented out — uncomment to reset demo' AS note;
