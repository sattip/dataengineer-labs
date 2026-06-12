# Databricks notebook source
# MAGIC %md
# MAGIC # 🏛️ Άσκηση Ημέρα 5 — Μέρος 4/4: Governance & Audit
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~60' · **Δυσκολία:** ⭐⭐⭐ Hard · **~11 TODOs**
# MAGIC > Self-contained · UC.
# MAGIC
# MAGIC ## 📖 Πού είμαστε
# MAGIC
# MAGIC Κρύψαμε τα PII (Μέρος 3). Τώρα: **ποιος έχει πρόσβαση** (GRANT/REVOKE), **πώς το ελέγχουμε**
# MAGIC (information_schema), πώς **βρίσκουμε PII** αυτόματα, και **ποιος έκανε τι** (audit / system tables).
# MAGIC Αυτά είναι η καθημερινή δουλειά **governance** σε δημόσιο φορέα (GDPR/compliance).
# MAGIC
# MAGIC ## 🎯 Τι θα μάθεις
# MAGIC
# MAGIC - **RBAC matrix** & least privilege.
# MAGIC - **`GRANT` / `REVOKE`** (`USE SCHEMA` + `SELECT`) και `SHOW GRANTS`.
# MAGIC - **`information_schema`** — αυτόματο inventory tables/columns/grants.
# MAGIC - **PII discovery** — εντοπισμός ευαίσθητων στηλών με query στο metadata.
# MAGIC - **Audit** μέσω **system tables** (ποιος έκανε query, πότε).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + Gold table να governάρουμε (έτοιμο)

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col, count, sum as spark_sum, lower

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve("https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/declarations.csv",
                               f"{VOLUME}/declarations.csv")

GOLD = "workspace.aade.gov_revenue_by_region"
(spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
 .select(col("ΑΦΜ").cast("string").alias("afm"), col("Ποσό_EUR").cast("double").alias("tax_amount_eur"),
         col("Περιφέρεια").alias("region"))
 .groupBy("region").agg(count("*").alias("n"), spark_sum("tax_amount_eur").alias("total_eur"))
 .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(GOLD))
print(f"✓ Gold: {spark.table(GOLD).count()} περιφέρειες")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — RBAC matrix (least privilege)
# MAGIC
# MAGIC Συμπλήρωσε τα access για τον **Data Analyst**: όχι Bronze, μόνο READ σε Silver & Gold.

# COMMAND ----------

roles = spark.createDataFrame([
    ("Data Engineer","READ/WRITE","READ/WRITE","READ/WRITE","Πλήρες control"),
    ("Data Steward", "READ",      "READ/WRITE","READ",      "Quality + curation"),
    ("Data Analyst", "______",    "______",    "______",    "BI + ad-hoc"),   # TODO 1: "—","READ","READ"
    ("Executive",    "—",         "—",         "READ",      "Dashboards"),
    ("Auditor (DPO)","READ-meta", "READ-meta", "READ-meta", "Audit logs"),
    ("Citizen (GDPR)","—",        "—",         "—",         "Right to erasure"),
], ["role","bronze","silver","gold","notes"])
display(roles)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — GRANT / REVOKE
# MAGIC
# MAGIC Για ανάγνωση table χρειάζονται **δύο** grants: `USE SCHEMA` (να «δει» το schema) + `SELECT`
# MAGIC (να διαβάσει). ⚠️ Είναι `USE SCHEMA`, **όχι** το legacy `USAGE`. (try/except για Free Edition.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — GRANT read access στο Gold

# COMMAND ----------

GROUP = "account users"
try:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA workspace.aade TO `{GROUP}`")
    spark.sql(f"GRANT ______ ON TABLE {GOLD} TO `{GROUP}`")            # TODO 2: SELECT
    print(f"✅ Granted SELECT on {GOLD} to `{GROUP}`")
    grants_ok = True
    display(spark.sql(f"SHOW GRANTS ON TABLE {GOLD}"))
except Exception as e:
    grants_ok = None; print(f"⚠️  GRANT skipped (edition/δικαιώματα): {str(e)[:140]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — REVOKE (αφαίρεση πρόσβασης)

# COMMAND ----------

try:
    spark.sql(f"______ SELECT ON TABLE {GOLD} FROM `{GROUP}`")          # TODO 3: REVOKE
    print(f"✅ Revoked SELECT from `{GROUP}`")
except Exception as e:
    print(f"⚠️  REVOKE skipped: {str(e)[:140]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — `information_schema`: αυτόματο inventory
# MAGIC
# MAGIC Κάθε catalog στο UC έχει `information_schema` με metadata για tables/columns/grants. Είναι ο
# MAGIC τρόπος να **απαντήσεις προγραμματιστικά** σε ερωτήσεις governance: «πόσα tables;», «ποιες στήλες;»,
# MAGIC «πού υπάρχει PII;».

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Πόσα tables/views στο schema aade;

# COMMAND ----------

n_tables = spark.sql(f"""
    SELECT count(*) AS c FROM workspace.information_schema.________            -- TODO 4a: tables
    WHERE table_schema = '______'                                             -- TODO 4b: aade
""").collect()[0]["c"]
print(f"Αντικείμενα στο workspace.aade: {n_tables}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — PII discovery (compliance!)
# MAGIC
# MAGIC Για GDPR πρέπει να ξέρεις **πού** βρίσκεται το PII. Αντί να ψάχνεις στο χέρι, κάνεις query στο
# MAGIC `information_schema.columns` για ονόματα στηλών που υποδηλώνουν ευαίσθητα δεδομένα (afm, amka, email…).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — Βρες όλες τις PII στήλες στο schema

# COMMAND ----------

SENSITIVE = ["afm", "amka", "email", "tax_amount_eur", "income", "phone", "iban"]
pii = spark.sql(f"""
    SELECT table_name, column_name, data_type
    FROM workspace.information_schema.columns
    WHERE table_schema = 'aade'
      AND lower(column_name) ____ ('afm','amka','email','tax_amount_eur','income','phone','iban')  -- TODO 5: IN
    ORDER BY table_name, column_name
""")
print(f"🔎 Βρέθηκαν {pii.count()} PII στήλες:")
pii.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — Audit μέσω system tables
# MAGIC
# MAGIC Το UC καταγράφει **κάθε** ενέργεια στα **system tables** (`system.access.audit`): ποιος, πότε,
# MAGIC τι query, σε ποιο table. Έτσι απαντάς «ποιος είδε τα εισοδήματα τον Μάρτιο;». (Σε Free Edition
# MAGIC τα system tables ίσως δεν είναι ενεργά — try/except.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 6 — Audit query (ποιος πείραξε τα δεδομένα μας)

# COMMAND ----------

try:
    audit = spark.sql(f"""
        SELECT event_time, user_identity.email AS user, action_name
        FROM system.access.audit
        WHERE action_name ____ ('getTable','generateTemporaryTableCredential','commandSubmit')  -- TODO 6: IN
        ORDER BY event_time DESC
        LIMIT 10
    """)
    print("Πρόσφατα audit events:")
    audit.show(truncate=False)
    audit_ok = True
except Exception as e:
    audit_ok = None
    print(f"⚠️  system.access.audit δεν είναι διαθέσιμο (Free Edition): {str(e)[:120]}")
    print("   → Σε production UC, αυτό το query δίνει πλήρες audit trail.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 4

# COMMAND ----------

results = {
    "RBAC matrix = 6 ρόλοι":              roles.count() == 6,
    "information_schema: ≥ 1 table":      n_tables >= 1,
    "PII discovery βρήκε ≥ 1 στήλη":      pii.count() >= 1,
    "PII discovery βρήκε το 'afm'":       pii.filter(col("column_name") == "afm").count() >= 1,
}
print("=" * 55)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
for label, flag in [("GRANT/REVOKE (production)", grants_ok), ("Audit system tables (production)", audit_ok)]:
    print(f"  {'✅ OK  ' if flag else 'ℹ️  SKIP'} — {label}")
print("=" * 55)
print("🎉🎉 ΟΛΟΚΛΗΡΩΣΑΤΕ ΟΛΗ ΤΗΝ ΑΣΚΗΣΗ DAY 5!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Σύνοψη Ημέρας 5 — Performance & Security
# MAGIC
# MAGIC ```
# MAGIC Μέρος 1: Partitioning · broadcast join · caching · explain plans
# MAGIC Μέρος 2: Data skew detection + salting (correctness-verified)
# MAGIC Μέρος 3: Column masking + Row-level security (PII) — views & UC policies
# MAGIC Μέρος 4: GRANT/REVOKE · information_schema · PII discovery · audit (system tables)
# MAGIC ```
# MAGIC **Cleanup (προαιρετικό):**
# MAGIC ```python
# MAGIC # for t in ["perf_requests_fact","perf_regions_dim","perf_requests_partitioned","perf_log",
# MAGIC #           "skew_fact","pii_declarations","pii_declarations_masked","pii_declarations_myregion",
# MAGIC #           "gov_revenue_by_region"]:
# MAGIC #     spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")
# MAGIC ```
