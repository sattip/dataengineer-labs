# Databricks notebook source
# MAGIC %md
# MAGIC # 🅱️ Scenario B — flaky_pipeline (Ημέρα 5)
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC
# MAGIC > **GitHub** (share this στο Zoom chat):
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day5/flaky_pipeline.py
# MAGIC > ```
# MAGIC
# MAGIC **Companion οδηγός**: `Day5/labs_walkthrough/Scenario_B_Job_Notifications.md`
# MAGIC **Διάρκεια**: μέρος του 15-min Σεναρίου Β
# MAGIC **Catalog/Schema**: `aade_lab.public`
# MAGIC
# MAGIC ## 🎯 Τι κάνει αυτό το notebook
# MAGIC
# MAGIC Demo «flaky» pipeline που:
# MAGIC
# MAGIC 1. Φτιάχνει το catalog/schema/audit table (idempotent)
# MAGIC 2. **Με 60% πιθανότητα αποτυγχάνει** σκόπιμα — για να δοκιμάσουμε email notifications + retry logic
# MAGIC 3. **ΠΑΝΤΑ logάρει** στο `pipeline_runs` audit table πριν αποτύχει ή πετύχει
# MAGIC
# MAGIC ## 📋 Πώς το χρησιμοποιείς
# MAGIC
# MAGIC 1. **Import** στο Databricks Workspace ως notebook με όνομα `flaky_pipeline`
# MAGIC 2. Δημιούργησε **Job** που τρέχει αυτό το notebook (δες Scenario_B_Job_Notifications.md Βήμα 3)
# MAGIC 3. Πρόσθεσε **email notification on failure** στο Job (Βήμα 4)
# MAGIC 4. Πρόσθεσε **retry policy** (Βήμα 5)
# MAGIC 5. **Run now** πολλές φορές — μερικά runs θα αποτύχουν, θα πάρεις emails
# MAGIC
# MAGIC ## 🚨 Production pattern που διδάσκει
# MAGIC
# MAGIC > **Πάντα logάρουμε** στο audit table **πριν** σηκώσουμε exception. Έτσι ακόμα κι αν το job crashάρει, υπάρχει record. Αυτό είναι **sacred** σε production — ποτέ μην χάνεις log entry.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Setup catalog, schema, audit table

# COMMAND ----------

import random
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit

print(f"Pipeline run started at {datetime.now()}")

# Idempotent setup — μπορεί να ξανατρέξει χωρίς πρόβλημα
spark.sql("CREATE CATALOG IF NOT EXISTS aade_lab")
spark.sql("CREATE SCHEMA IF NOT EXISTS aade_lab.public")
spark.sql("""
  CREATE TABLE IF NOT EXISTS aade_lab.public.pipeline_runs (
    run_id     STRING,
    status     STRING,
    run_ts     TIMESTAMP,
    error_msg  STRING
  ) USING DELTA
""")

print("✅ Setup complete — aade_lab.public.pipeline_runs ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Simulate flaky behavior (60% chance of failure)
# MAGIC
# MAGIC Χρησιμοποιούμε `random.random() < 0.6` για να αναπαράγουμε πραγματικότητα: production pipelines **όντως** αποτυγχάνουν συχνά λόγω network glitches, upstream data quality issues, transient errors. Το σύστημα notifications + retries πρέπει να δουλεύει.

# COMMAND ----------

should_fail = random.random() < 0.6
run_id = f"run_{int(datetime.now().timestamp())}"

print(f"🎲 Decision for {run_id}: {'FAIL' if should_fail else 'SUCCESS'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Log και exit
# MAGIC
# MAGIC **Critical pattern**: ΠΑΝΤΑ logάρουμε ΠΡΙΝ σηκώσουμε exception. Αλλιώς, αν crashάρει το cluster μεταξύ raise και log, θα χάσουμε το record.

# COMMAND ----------

if should_fail:
    # Log failure FIRST, then raise
    spark.sql(f"""
      INSERT INTO aade_lab.public.pipeline_runs
      VALUES ('{run_id}', 'FAILED', current_timestamp(),
              'Simulated: data quality check failed')
    """)
    print(f"📝 Logged failure for {run_id}")
    raise Exception("Simulated pipeline failure: data quality check failed")

# Success path
spark.sql(f"""
  INSERT INTO aade_lab.public.pipeline_runs
  VALUES ('{run_id}', 'SUCCESS', current_timestamp(), NULL)
""")
print(f"✅ Pipeline {run_id} succeeded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — Verify (optional) — δες τα τελευταία 10 runs

# COMMAND ----------

display(spark.sql("""
  SELECT run_id, status, run_ts, error_msg
  FROM aade_lab.public.pipeline_runs
  ORDER BY run_ts DESC
  LIMIT 10
"""))
