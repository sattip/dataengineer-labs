# Databricks notebook source
# MAGIC %md
# MAGIC # Άσκηση #4 — Lineage & Time Travel (Ημέρα 3)
# MAGIC
# MAGIC **Σενάριο**: Tax Steward της ΑΑΔΕ. Χθες ο automated job έτρεξε ένα bad batch και "μόλυνε" το `tax_declarations_silver`. Σήμερα πρέπει να:
# MAGIC
# MAGIC 1. **Δεις τι άλλαξε** — `DESCRIBE HISTORY`
# MAGIC 2. **Συγκρίνεις versions** — `VERSION AS OF`
# MAGIC 3. **Εντοπίσεις τι έσπασε** — query στο audit table
# MAGIC 4. **Δεις από πού ήρθε το data** — Unity Catalog Lineage UI
# MAGIC 5. **Κάνεις rollback** — `RESTORE TABLE`
# MAGIC
# MAGIC **Διάρκεια**: ~20 min
# MAGIC **Prerequisite**: έχει τρέξει το `data_contract_validation_notebook` (Άσκηση 3) — υπάρχουν τα `tax_declarations_silver`, `tax_declarations_quarantine`, `data_contract_audit` στο `workspace.aade`.
# MAGIC
# MAGIC **Catalog/Schema**: `workspace.aade`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 0 — Prerequisites check
# MAGIC
# MAGIC Επιβεβαίωσε ότι τα 3 tables από την Άσκηση 3 υπάρχουν.

# COMMAND ----------

required_tables = ["tax_declarations_silver", "tax_declarations_quarantine", "data_contract_audit"]
for t in required_tables:
    full = f"workspace.aade.{t}"
    try:
        cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {full}").collect()[0]["n"]
        print(f"✅ {full}: {cnt} rows")
    except Exception as e:
        print(f"❌ {full} MISSING — τρέξε πρώτα την Άσκηση 3 (data_contract_validation_notebook)")
        raise

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
# MAGIC ### History στο audit table — βλέπεις τα contract runs
# MAGIC
# MAGIC Το `data_contract_audit` είναι ο **business-level** lineage: δείχνει ποιο rule έτρεξε, πότε, αν πέρασε, πόσα rows ήταν invalid.

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
# MAGIC ## Βήμα 2 — Time Travel: VERSION AS OF
# MAGIC
# MAGIC Μπορείς να query-άρεις **οποιαδήποτε προηγούμενη version** του Silver table σαν να ήταν live. Αυτό είναι το killer feature του Delta για debugging + audit.
# MAGIC
# MAGIC Δες τη version 0 (initial bronze→silver write):

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_at_v0
# MAGIC FROM workspace.aade.tax_declarations_silver VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Πόσα rows έχουμε σε κάθε version (όλες τις versions του history)
# MAGIC SELECT
# MAGIC   version,
# MAGIC   timestamp,
# MAGIC   operation,
# MAGIC   operationMetrics.numOutputRows AS rows_written
# MAGIC FROM (DESCRIBE HISTORY workspace.aade.tax_declarations_silver)
# MAGIC ORDER BY version

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 3 — Simulate "bad batch" (corrupt write)
# MAGIC
# MAGIC Για να δείξουμε τη χρησιμότητα του history, θα **βάλουμε επίτηδες** μερικές κακές εγγραφές στο silver — π.χ. ΑΦΜ με wrong checksum που πέρασε από bug στο pipeline.

# COMMAND ----------

from pyspark.sql import Row

bad_batch = spark.createDataFrame([
    Row(**{"ΔηλωσηID": 99001, "ΑΦΜ": "BAD_AFM_01",  "ΕτοςΔηλωσης": 2024, "ΕισοδημαΑπoΕργασια": 999999.0,
           "ΕισοδημαΑπoΑκινητα": 0.0, "ΛοιπαΕισοδηματα": 0.0, "ΣυνολικοΕισοδημα": 999999.0,
           "ΦοροςΠροςΠληρωμη": 100000.0, "ΑριθμοςΔΟΥ": 1, "ΗμερομηνιαΥποβολης": "2024-12-31",
           "Επωνυμία": "FAKE_RECORD"}),
    Row(**{"ΔηλωσηID": 99002, "ΑΦΜ": "BAD_AFM_02",  "ΕτοςΔηλωσης": 2024, "ΕισοδημαΑπoΕργασια": 888888.0,
           "ΕισοδημαΑπoΑκινητα": 0.0, "ΛοιπαΕισοδηματα": 0.0, "ΣυνολικοΕισοδημα": 888888.0,
           "ΦοροςΠροςΠληρωμη": 90000.0, "ΑριθμοςΔΟΥ": 1, "ΗμερομηνιαΥποβολης": "2024-12-31",
           "Επωνυμία": "FAKE_RECORD"}),
])

# Ευθυγραμμίζουμε τύπους με τον silver schema
silver_cols = spark.table("workspace.aade.tax_declarations_silver").columns
bad_batch = bad_batch.select(*[c for c in silver_cols if c in bad_batch.columns])

# WRITE (mode=append) — δημιουργείται νέα version στο Delta log
bad_batch.write.format("delta").mode("append").saveAsTable("workspace.aade.tax_declarations_silver")

print(f"⚠️  Bad batch γράφτηκε. Νέα version δημιουργήθηκε.")
spark.sql("SELECT COUNT(*) AS total FROM workspace.aade.tax_declarations_silver").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 4 — Detect: τι άλλαξε στην τελευταία version;
# MAGIC
# MAGIC Σύγκρινε **current** vs **previous version** και βρες τα new rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the last 2 versions
# MAGIC SELECT version, timestamp, operation, operationMetrics.numOutputRows AS rows
# MAGIC FROM (DESCRIBE HISTORY workspace.aade.tax_declarations_silver)
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 2

# COMMAND ----------

# Βρίσκουμε programmatically την προηγούμενη version (πριν το bad batch)
history = spark.sql("DESCRIBE HISTORY workspace.aade.tax_declarations_silver").collect()
versions = sorted([r["version"] for r in history])
current_v = versions[-1]
previous_v = versions[-2]
print(f"Current version: {current_v}")
print(f"Previous (clean) version: {previous_v}")

# Diff: rows που υπάρχουν στο current αλλά όχι στο previous
new_rows = spark.sql(f"""
  SELECT * FROM workspace.aade.tax_declarations_silver VERSION AS OF {current_v}
  EXCEPT
  SELECT * FROM workspace.aade.tax_declarations_silver VERSION AS OF {previous_v}
""")
print(f"\n🔍 Νέα rows στη version {current_v} (που δεν υπήρχαν στη {previous_v}):")
new_rows.select("ΔηλωσηID", "ΑΦΜ", "Επωνυμία", "ΣυνολικοΕισοδημα").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 5 — Unity Catalog Lineage UI
# MAGIC
# MAGIC Στο **Catalog Explorer** μπορείς να δεις γραφικά:
# MAGIC - Από ποιο **upstream table** ήρθε το silver (το bronze + reference tables)
# MAGIC - Σε ποια **downstream tables / queries / dashboards** χρησιμοποιείται
# MAGIC - Ποιοι **users / jobs / notebooks** το διαβάζουν / γράφουν
# MAGIC
# MAGIC ### 🎯 Steps για να δεις το Lineage tab
# MAGIC
# MAGIC 1. Click **Catalog** (αριστερό sidebar)
# MAGIC 2. Πλοήγηση: `workspace` → `aade` → `Tables`
# MAGIC 3. Click στο `tax_declarations_silver`
# MAGIC 4. Click το tab **Lineage** (πάνω, δίπλα στο "Overview", "Sample Data", "Permissions")
# MAGIC 5. Δες το διάγραμμα: **upstream** (bronze.declarations_csv ή volume file) → silver → **downstream** (queries, jobs)
# MAGIC 6. Click σε κάθε node για να δεις details
# MAGIC
# MAGIC **Note**: Lineage capture χρειάζεται να έχει τρέξει το pipeline ΜΕ Unity Catalog enabled. Εάν δείξει empty, ξανατρέξε το `data_contract_validation_notebook`.

# COMMAND ----------

# Print direct URL για το Catalog Explorer (copy-paste σε browser)
import os
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", default=None)
if workspace_url:
    catalog_url = f"https://{workspace_url}/explore/data/workspace/aade/tax_declarations_silver"
    print(f"🔗 Catalog Explorer URL για το silver table:\n   {catalog_url}\n")
    print("   Click → tab 'Lineage' για το διάγραμμα.")
else:
    print("⚠️  Workspace URL not auto-detected. Πήγαινε manually: Catalog → workspace → aade → tax_declarations_silver → tab 'Lineage'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6 — RESTORE: rollback στην clean version
# MAGIC
# MAGIC Τώρα που ξέρουμε ότι η `previous_v` ήταν clean, κάνουμε **point-in-time restore**.
# MAGIC
# MAGIC ⚠️ Το `RESTORE` δημιουργεί **νέα version** που είναι snapshot της προηγούμενης. Δεν διαγράφει history — ο audit trail διατηρείται.

# COMMAND ----------

spark.sql(f"RESTORE TABLE workspace.aade.tax_declarations_silver TO VERSION AS OF {previous_v}")

after = spark.sql("SELECT COUNT(*) AS total FROM workspace.aade.tax_declarations_silver").collect()[0]["total"]
print(f"✅ RESTORE complete. Silver table τώρα έχει {after} rows (όσα είχε στη version {previous_v}).")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Δες ότι έχουμε νέα version (RESTORE) στο history
# MAGIC SELECT version, timestamp, operation, operationParameters
# MAGIC FROM (DESCRIBE HISTORY workspace.aade.tax_declarations_silver)
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 7 — Audit query: τι rules απέτυχαν τις τελευταίες 24h;
# MAGIC
# MAGIC Σε real production, ένα Slack alert / dashboard θα έτρεχε αυτό το query κάθε ώρα.

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
# MAGIC Σε αυτή την άσκηση είδες:
# MAGIC
# MAGIC | Δυνατότητα | Command | Use case |
# MAGIC |---|---|---|
# MAGIC | **Delta History** | `DESCRIBE HISTORY <table>` | "Τι έγινε σε αυτό το table τις τελευταίες 30 μέρες;" |
# MAGIC | **Time Travel** | `SELECT ... VERSION AS OF n` | "Πώς ήταν τα δεδομένα την Δευτέρα;" |
# MAGIC | **Diff versions** | `EXCEPT` between versions | "Τι rows προστέθηκαν στο τελευταίο batch;" |
# MAGIC | **RESTORE** | `RESTORE TABLE ... TO VERSION AS OF n` | "Κάνε rollback τώρα — bad batch έσπασε production." |
# MAGIC | **UC Lineage UI** | Catalog Explorer → Lineage tab | "Από πού ήρθε αυτό το data; Πού πάει;" |
# MAGIC | **Audit table** | `data_contract_audit` queries | "Ποια rules έσπασαν τις τελευταίες 24h;" |
# MAGIC
# MAGIC ### Τι μάθαμε
# MAGIC
# MAGIC - Delta = **versioned filesystem** για tables. Κάθε write → νέα version, παλιές γίνονται queryable.
# MAGIC - **Lineage** στο Unity Catalog = αυτόματο. Δεν χρειάζεται να γράψεις metadata — το capture-άρει αυτόματα από queries / writes.
# MAGIC - **Audit table** = business-level lineage. Συμπληρώνει το technical lineage με "γιατί έσπασε" + "ποιο rule".
# MAGIC - **RESTORE** = production lifesaver. Recovery από bug σε λεπτά αντί ωρών.
# MAGIC
# MAGIC ### Κλειστή ερώτηση για τους trainees
# MAGIC
# MAGIC > "Αν αύριο σου πει το business ότι το KPI του dashboard είναι λάθος εδώ και 3 ημέρες, **πώς ξέρεις από ποιο silver/bronze write ξεκίνησε το λάθος**;"
# MAGIC
# MAGIC **Απάντηση**: συνδυασμός `DESCRIBE HISTORY` (πότε & ποιος) + UC Lineage (ποιο upstream το τάισε) + audit table (αν κάποιο rule άλλαξε).
