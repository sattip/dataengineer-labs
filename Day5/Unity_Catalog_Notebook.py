# Databricks notebook source
# MAGIC %md
# MAGIC # 🔐 Πρακτική Άσκηση — Unity Catalog: Governance, RBAC & Data Masking
# MAGIC
# MAGIC **Ρόλος:** Μηχανικός Δεδομένων (Data Engineer) στην ΑΑΔΕ
# MAGIC **Διάρκεια:** ~30'
# MAGIC **Περιβάλλον:** Databricks Free Edition (Serverless) με Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Στόχος της άσκησης
# MAGIC
# MAGIC > **Να στήσετε ένα ασφαλές, παραγωγικό περιβάλλον Unity Catalog για τα δεδομένα της
# MAGIC > ΑΑΔΕ — με σωστή ιεραρχία, ελεγχόμενες προσβάσεις, masking ευαίσθητων στηλών, και
# MAGIC > πλήρες audit trail για συμμόρφωση με GDPR και EU AI Act.**
# MAGIC
# MAGIC ## 🧭 Πραγματικό σενάριο
# MAGIC
# MAGIC Είστε ο νέος Data Engineer της ΑΑΔΕ και αναλαμβάνετε να στήσετε από το μηδέν τον
# MAGIC χώρο εργασίας για τα δεδομένα φορολογουμένων. Έχετε τρεις διαφορετικές ομάδες που θα
# MAGIC χρειαστούν πρόσβαση:
# MAGIC
# MAGIC - **Data Engineering** — διαβάζει/γράφει σε όλα τα layers
# MAGIC - **Compliance** — βλέπει πλήρη ΑΦΜ για ελεγκτικούς λόγους
# MAGIC - **Analytics** — βλέπει στατιστικά αλλά **όχι** πραγματικά ΑΦΜ
# MAGIC
# MAGIC Ο Διοικητής σας ζήτησε να στήσετε όλο αυτό **σωστά από την πρώτη μέρα** —
# MAGIC γιατί τα δικαιώματα που δίνεις στην αρχή σπάνια αφαιρούνται μετά.
# MAGIC
# MAGIC ## ❓ Γιατί Unity Catalog
# MAGIC
# MAGIC Πριν το Unity Catalog, κάθε workspace είχε τα δικά του permissions, τα δικά του
# MAGIC paths, και κάθε αλλαγή ήταν ad hoc ticket στο IT. Με Unity Catalog:
# MAGIC
# MAGIC - **Κεντρική διακυβέρνηση** για όλη την οργάνωση
# MAGIC - **Three-level namespace**: `catalog.schema.table` (ή `.volume`)
# MAGIC - **Fine-grained access control** μέχρι column level
# MAGIC - **Built-in lineage tracking** — ποιο dataset ήρθε από πού
# MAGIC - **Audit logging** για κάθε read/write
# MAGIC - **Data discovery** — μια σελίδα να βρίσκεις όλα τα assets
# MAGIC
# MAGIC ## 📋 Τα 7 βήματα της άσκησης
# MAGIC
# MAGIC | # | Βήμα | Τι κάνουμε |
# MAGIC |---|---|---|
# MAGIC | 1 | **Setup** | Schema `workspace.aade` + Volume `aade_data` |
# MAGIC | 2 | **Sample Data** | Φτιάχνουμε πίνακα `taxpayers` με ψεύτικους φορολογούμενους |
# MAGIC | 3 | **RBAC: GRANT/REVOKE** | Δίνουμε δικαιώματα σε διαφορετικές ομάδες |
# MAGIC | 4 | **Column Masks** | Μασκάρουμε ΑΦΜ για όσους δεν είναι compliance |
# MAGIC | 5 | **Tags & Metadata** | Σημαίνουμε pii, owner, certified |
# MAGIC | 6 | **Lineage & Audit** | Δούμε ποιος διάβασε τι, πότε |
# MAGIC | 7 | **Cleanup** (προαιρετικό) | Πώς αφαιρούμε όλα όσα φτιάξαμε |
# MAGIC
# MAGIC ## 📦 Παραδοτέα
# MAGIC
# MAGIC - **Schema**: `workspace.aade`
# MAGIC - **Volume**: `workspace.aade.aade_data`
# MAGIC - **Table**: `workspace.aade.taxpayers` με tags + masking
# MAGIC - **Granted permissions**: SELECT στο `account users` group
# MAGIC - **Column mask**: στο `afm` column με dynamic logic

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 1: Setup — Schema & Volume
# MAGIC
# MAGIC Στήνουμε τη βασική ιεραρχία Unity Catalog. Όλες οι εντολές είναι **idempotent**
# MAGIC (χρησιμοποιούν `IF NOT EXISTS`), οπότε μπορείτε να τρέξετε το notebook πολλές φορές
# MAGIC χωρίς προβλήματα.
# MAGIC
# MAGIC ### 1α. Hierarchy που θα δημιουργήσουμε
# MAGIC
# MAGIC ```
# MAGIC workspace            (catalog — προ-υπάρχει στη Databricks Free Edition)
# MAGIC └── aade             (schema — το δικό μας namespace)
# MAGIC     ├── taxpayers    (table — πίνακας φορολογουμένων)
# MAGIC     └── aade_data    (volume — αρχεία, CSVs, models)
# MAGIC ```

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade COMMENT 'ΑΑΔΕ data domain — taxpayers, declarations, audits'")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data COMMENT 'Aρχεία CSV/JSON και model artifacts'")

print("✓ Schema:  workspace.aade")
print("✓ Volume:  workspace.aade.aade_data")
print("✓ Volume path: /Volumes/workspace/aade/aade_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1β. Επιβεβαίωση δημιουργίας
# MAGIC
# MAGIC Ας δούμε τι υπάρχει στο schema μας. Το `DESCRIBE SCHEMA EXTENDED` δείχνει
# MAGIC τα metadata: owner, created_at, comment.

# COMMAND ----------

print("=== Schema metadata ===")
spark.sql("DESCRIBE SCHEMA EXTENDED workspace.aade").show(truncate=False)

print("=== Volumes στο schema ===")
spark.sql("SHOW VOLUMES IN workspace.aade").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 2: Sample Data — Πίνακας `taxpayers`
# MAGIC
# MAGIC Φτιάχνουμε έναν μικρό πίνακα με **ψεύτικους** φορολογούμενους ΑΑΔΕ. Όλα τα ΑΦΜ
# MAGIC είναι placeholder (`090000xxx`) — ποτέ δεν χρησιμοποιούμε πραγματικά δεδομένα σε
# MAGIC training environments.
# MAGIC
# MAGIC > **🛡️ Κανόνας:** Σε κάθε training/dev workspace, **ποτέ** πραγματικά δεδομένα
# MAGIC > φορολογουμένων. GDPR + ν. 4624/2019 + κοινή λογική.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("afm", StringType(), False),
    StructField("name", StringType(), False),
    StructField("region", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("income", DoubleType(), True),
    StructField("status", StringType(), False),
])

data = [
    ("090000001", "Παπαδόπουλος Γιώργος", "Αττική",       2025, 45000.0, "Approved"),
    ("090000002", "Ιωάννου Μαρία",         "Αττική",       2025, 38000.0, "Approved"),
    ("090000003", "Νικολάου Κώστας",       "Θεσσαλονίκη",  2025, 72000.0, "Pending"),
    ("090000004", "Δημητρίου Ελένη",       "Κρήτη",        2025, 28000.0, "Approved"),
    ("090000005", "Αθανασίου Δημήτρης",    "Αττική",       2025, 95000.0, "Flagged"),
    ("090000006", "Γεωργίου Άννα",         "Πάτρα",        2025, 33000.0, "Approved"),
    ("090000007", "Παπαϊωάννου Νίκος",     "Θεσσαλονίκη",  2025, 41000.0, "Approved"),
    ("090000008", "Καλογήρου Σοφία",       "Αττική",       2025, 56000.0, "Pending"),
]

df = spark.createDataFrame(data, schema)

# Save ως managed Delta table στο Unity Catalog
df.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.taxpayers")

print("✓ Πίνακας workspace.aade.taxpayers δημιουργήθηκε")
print(f"  Γραμμές: {spark.table('workspace.aade.taxpayers').count()}")

# COMMAND ----------

print("=== Δείγμα δεδομένων ===")
spark.table("workspace.aade.taxpayers").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 3: RBAC — GRANT & REVOKE
# MAGIC
# MAGIC ### 3α. Η αρχή του Least Privilege
# MAGIC
# MAGIC Στον δημόσιο τομέα, η αρχή είναι **«όσα λιγότερα δικαιώματα γίνεται»**. Δίνετε
# MAGIC σε κάθε ομάδα **ακριβώς** ό,τι χρειάζεται και τίποτα παραπάνω. Ποτέ Owner για
# MAGIC ευκολία.
# MAGIC
# MAGIC ### 3β. Συνηθέστερα δικαιώματα
# MAGIC
# MAGIC | Privilege | Τι κάνει |
# MAGIC |---|---|
# MAGIC | `USE CATALOG` | Μπορείς να βλέπεις ότι υπάρχει ο catalog |
# MAGIC | `USE SCHEMA` | Μπορείς να βλέπεις τα schemas μέσα του |
# MAGIC | `SELECT` | Μπορείς να διαβάσεις τα data |
# MAGIC | `MODIFY` | Μπορείς να γράψεις (INSERT/UPDATE/DELETE) |
# MAGIC | `CREATE TABLE` | Μπορείς να δημιουργήσεις νέους πίνακες |
# MAGIC | `ALL PRIVILEGES` | Όλα τα παραπάνω (επικίνδυνο — να το αποφεύγετε) |
# MAGIC
# MAGIC ### 3γ. Το `account users` group
# MAGIC
# MAGIC Στο Databricks υπάρχει το προ-υπάρχον group `account users` που περιλαμβάνει
# MAGIC κάθε χρήστη του workspace. Αντί να δίνετε permissions σε κάθε email ξεχωριστά,
# MAGIC τα δίνετε στο group και αυτόματα όλοι το έχουν.

# COMMAND ----------

# Δίνουμε read-only access σε όλους τους χρήστες του workspace
spark.sql("GRANT USE CATALOG ON CATALOG workspace TO `account users`")
spark.sql("GRANT USE SCHEMA  ON SCHEMA  workspace.aade TO `account users`")
spark.sql("GRANT SELECT      ON TABLE   workspace.aade.taxpayers TO `account users`")

print("✓ Granted USE CATALOG + USE SCHEMA + SELECT στο account users group")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3δ. Επιβεβαίωση των grants
# MAGIC
# MAGIC Με `SHOW GRANTS` δούμε ποιος έχει τι. Αυτό είναι το πρώτο πράγμα που θα κοιτάξει
# MAGIC ένας auditor.

# COMMAND ----------

print("=== Grants στον πίνακα taxpayers ===")
spark.sql("SHOW GRANTS ON TABLE workspace.aade.taxpayers").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3ε. REVOKE — αφαίρεση δικαιωμάτων
# MAGIC
# MAGIC Για να δείξουμε πώς αφαιρείς πρόσβαση. Αν θέλετε να την κρατήσετε, αφήστε αυτό
# MAGIC το cell σχολιασμένο.

# COMMAND ----------

# Παράδειγμα REVOKE — αφήνουμε σχόλιο για να μη σπάσει η ροή της άσκησης
# spark.sql("REVOKE SELECT ON TABLE workspace.aade.taxpayers FROM `account users`")
# print("✓ Revoked SELECT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 4: Column Masks — Δυναμική απόκρυψη ΑΦΜ
# MAGIC
# MAGIC ### 4α. Η ιδέα
# MAGIC
# MAGIC Ίδιο dataset, ίδιο SELECT — διαφορετική προβολή ανάλογα με τον χρήστη. Έτσι:
# MAGIC
# MAGIC - Ο **compliance officer** βλέπει `123456789` ολόκληρο
# MAGIC - Ο **analyst** βλέπει `XXXXXX789` (μόνο τα τρία τελευταία)
# MAGIC - Ο **εξωτερικός σύμβουλος** βλέπει `XXXXXXXXX` (πλήρως masked)
# MAGIC
# MAGIC Αυτό λέγεται **Dynamic Data Masking (DDM)**. Τα data στη βάση μένουν ίδια — αλλάζει
# MAGIC μόνο η προβολή στο SELECT.
# MAGIC
# MAGIC ### 4β. Η function
# MAGIC
# MAGIC Φτιάχνουμε μια SQL function που δέχεται ένα ΑΦΜ και επιστρέφει είτε το ΑΦΜ
# MAGIC ολόκληρο είτε masked, ανάλογα με το αν ο χρήστης ανήκει στο group `compliance`.
# MAGIC
# MAGIC > **Σημείωση Free Edition:** Στην Free Edition το `is_account_group_member`
# MAGIC > λειτουργεί με τα account-level groups. Αν δεν υπάρχει group `compliance`, η
# MAGIC > function θα μασκάρει για όλους — το οποίο είναι ασφαλές default.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE FUNCTION workspace.aade.mask_afm(afm STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('compliance') THEN afm
    ELSE CONCAT('XXXXXX', RIGHT(afm, 3))
  END
""")

print("✓ Function workspace.aade.mask_afm δημιουργήθηκε")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4γ. Εφαρμογή του mask στον πίνακα
# MAGIC
# MAGIC Με `ALTER TABLE ... ALTER COLUMN ... SET MASK` λέμε στο Unity Catalog ότι
# MAGIC κάθε φορά που κάποιος επιλέγει την στήλη `afm`, να περάσει πρώτα από τη function
# MAGIC `mask_afm`.

# COMMAND ----------

spark.sql("""
ALTER TABLE workspace.aade.taxpayers
ALTER COLUMN afm SET MASK workspace.aade.mask_afm
""")

print("✓ Column mask εφαρμόστηκε στη στήλη afm")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4δ. Έλεγχος: τι βλέπει ο χρήστης
# MAGIC
# MAGIC Τρέξτε τώρα ένα SELECT. Αν **δεν είστε** στο group `compliance` (που είναι το
# MAGIC default), θα δείτε masked ΑΦΜ της μορφής `XXXXXX001`. Πραγματικά πρόκειται για
# MAGIC dynamic masking — τα data στη βάση παραμένουν ίδια.

# COMMAND ----------

print("=== SELECT μετά το masking ===")
spark.sql("SELECT afm, name, region, status FROM workspace.aade.taxpayers").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **🔍 Τι μάθαμε:**
# MAGIC
# MAGIC - Όλοι οι ΑΦΜ φαίνονται ως `XXXXXX001`, `XXXXXX002`, κ.λπ.
# MAGIC - Τα **πραγματικά** δεδομένα στη βάση είναι αμετάβλητα
# MAGIC - Αν ο admin σας προσθέσει στο group `compliance`, ξαφνικά θα βλέπατε τα πλήρη ΑΦΜ
# MAGIC - Αυτό είναι **GDPR-compliant** by default — οι περισσότεροι χρήστες δεν μπορούν να
# MAGIC   εξάγουν προσωπικά δεδομένα

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 5: Tags & Metadata — Σήμανση Πινάκων
# MAGIC
# MAGIC ### 5α. Γιατί Tags
# MAGIC
# MAGIC Τα tags είναι **key-value pairs** που σημαίνετε σε πίνακες/στήλες/schemas. Σας δίνουν:
# MAGIC
# MAGIC - **Discoverability** — οι data scientists ψάχνουν "tables with tag pii=true"
# MAGIC - **Compliance** — αυτόματο handling των PII columns από downstream tools
# MAGIC - **Endorsement** — `certified=true` σημαίνει «επίσημα εγκεκριμένος πίνακας»
# MAGIC - **Ownership** — ξέρετε ποιος είναι υπεύθυνος αν κάτι σπάσει

# COMMAND ----------

spark.sql("""
ALTER TABLE workspace.aade.taxpayers
SET TAGS (
  'certified'  = 'true',
  'owner'      = 'data-engineering@aade.gov.gr',
  'pii'        = 'true',
  'data_class' = 'highly_confidential',
  'domain'     = 'taxpayers'
)
""")

# Επίσης σημαίνουμε τη στήλη afm ως PII για να την βρίσκει το Purview
spark.sql("""
ALTER TABLE workspace.aade.taxpayers
ALTER COLUMN afm SET TAGS ('classification' = 'pii_critical')
""")

print("✓ Tags εφαρμόστηκαν σε table + column")

# COMMAND ----------

print("=== Tags του πίνακα ===")
spark.sql("""
SELECT tag_name, tag_value
FROM   system.information_schema.table_tags
WHERE  catalog_name = 'workspace'
  AND  schema_name  = 'aade'
  AND  table_name   = 'taxpayers'
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 6: Lineage & Audit — Ποιος έκανε τι
# MAGIC
# MAGIC ### 6α. Lineage
# MAGIC
# MAGIC Το Unity Catalog καταγράφει **αυτόματα** ποιο dataset ήρθε από πού. Αν διαβάσετε
# MAGIC από `workspace.aade.taxpayers` και γράψετε σε νέο πίνακα, η σχέση καταγράφεται.
# MAGIC
# MAGIC Ας δούμε αυτό σε δράση — δημιουργούμε έναν derived table και ψάχνουμε το lineage.

# COMMAND ----------

# Φτιάχνουμε derived table — μέσος όρος εισοδήματος ανά region
spark.sql("""
CREATE OR REPLACE TABLE workspace.aade.region_stats AS
SELECT region,
       COUNT(*) AS taxpayers_count,
       AVG(income) AS avg_income
FROM   workspace.aade.taxpayers
GROUP BY region
""")

print("✓ Derived table workspace.aade.region_stats δημιουργήθηκε")
spark.table("workspace.aade.region_stats").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6β. Lineage queries
# MAGIC
# MAGIC Οι παρακάτω queries δείχνουν τη σχέση μεταξύ tables. Μπορεί να χρειαστούν λίγα
# MAGIC λεπτά για να γεμίσουν τα system tables με νέα events.

# COMMAND ----------

print("=== Table lineage (upstream sources για region_stats) ===")
spark.sql("""
SELECT entity_type,
       source_table_full_name,
       target_table_full_name,
       event_time
FROM   system.access.table_lineage
WHERE  target_table_full_name = 'workspace.aade.region_stats'
ORDER BY event_time DESC
LIMIT  10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6γ. Audit logs
# MAGIC
# MAGIC Κάθε access σε ένα table καταγράφεται στο `system.access.audit`. Παράδειγμα query:
# MAGIC ποιος διάβασε τον πίνακα `taxpayers` τις τελευταίες ώρες.

# COMMAND ----------

print("=== Audit events στον πίνακα taxpayers (24h) ===")
spark.sql("""
SELECT user_identity.email AS user,
       service_name,
       action_name,
       event_time
FROM   system.access.audit
WHERE  request_params.full_name_arg = 'workspace.aade.taxpayers'
  AND  event_time >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY event_time DESC
LIMIT  20
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC > **🔍 Σημείωση:** Σε production ΑΑΔΕ, τα audit logs κρατιούνται για **7 χρόνια**
# MAGIC > σε dedicated Log Analytics workspace. Είναι νομική απαίτηση για συμμόρφωση με
# MAGIC > GDPR audit trails και EU AI Act άρθρο 12 (logging για high-risk συστήματα).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 7: Cleanup (προαιρετικό)
# MAGIC
# MAGIC Αν θέλετε να καθαρίσετε όλα όσα φτιάξαμε, αφαιρέστε τα σχόλια στο επόμενο cell.
# MAGIC **Προσοχή:** Σε production ΠΟΤΕ μη τρέχετε `DROP` χωρίς approval και backup.

# COMMAND ----------

# Cleanup — uncomment αν θες να σβήσεις
# spark.sql("DROP TABLE IF EXISTS workspace.aade.region_stats")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.taxpayers")
# spark.sql("DROP FUNCTION IF EXISTS workspace.aade.mask_afm")
# spark.sql("DROP VOLUME IF EXISTS workspace.aade.aade_data")
# spark.sql("DROP SCHEMA IF EXISTS workspace.aade CASCADE")
# print("✓ Όλα διαγράφηκαν")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση
# MAGIC
# MAGIC ### 🎓 Τι μάθατε σε αυτή την άσκηση
# MAGIC
# MAGIC 1. **Three-level namespace** του Unity Catalog: `catalog.schema.table`
# MAGIC 2. **Volumes** για file storage με governance και audit
# MAGIC 3. **GRANT/REVOKE** για RBAC σε επίπεδο catalog/schema/table
# MAGIC 4. **Column Masks** για dynamic data masking (DDM) — GDPR-ready
# MAGIC 5. **Tags & Metadata** για discoverability και compliance
# MAGIC 6. **Lineage tracking** — αυτόματη καταγραφή σχέσεων μεταξύ tables
# MAGIC 7. **Audit logs** — ποιος διάβασε τι, πότε
# MAGIC
# MAGIC ### 🎯 Πρακτικό checklist για το γραφείο σας
# MAGIC
# MAGIC | Action | Πότε |
# MAGIC |---|---|
# MAGIC | Φτιάξε schema ανά domain (taxpayers, declarations, audits) | Day 1 |
# MAGIC | Δώσε `USE CATALOG` + `USE SCHEMA` + `SELECT` στα groups | Day 1 |
# MAGIC | Σήμανε pii columns με tags | Πριν γράψεις πρώτο SELECT |
# MAGIC | Φτιάξε column masks για ΑΦΜ, ΑΔΤ, ΙΒΑΝ | Πριν δώσεις access σε αναλυτές |
# MAGIC | Καθιέρωσε quarterly access review | Setup once, run forever |
# MAGIC | Συνδέσε system.access.audit με Log Analytics | Για 7-year retention |
# MAGIC
# MAGIC ### 💡 Take-home message
# MAGIC
# MAGIC > **Σε δημόσιο φορέα όπως η ΑΑΔΕ, δεν αρκεί να δουλεύει το pipeline.
# MAGIC > Πρέπει να μπορεί να αποδειχθεί κάθε access και κάθε αλλαγή. Το Unity Catalog
# MAGIC > σας δίνει το framework — εσείς πρέπει να το χρησιμοποιήσετε σωστά από την πρώτη μέρα.**
