# Databricks notebook source
# MAGIC %md
# MAGIC # 🔐 Άσκηση Ημέρα 1 — Μέρος 3/3: Governance + Data Contracts
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~50' · **Δυσκολία:** ⭐⭐⭐ Medium-Hard
# MAGIC > **Προαπαιτούμενο:** Έχετε τρέξει Μέρη 1 & 2 (υπάρχει το Silver `declarations_clean`).
# MAGIC
# MAGIC ## 📖 Πού είμαστε
# MAGIC
# MAGIC Το pipeline δουλεύει. Τώρα το κάνουμε **production-grade** με δύο πυλώνες:
# MAGIC 1. **Governance** — ποιος βλέπει τι (ρόλοι, least privilege, GRANT).
# MAGIC 2. **Data Contracts** — μια «πύλη ποιότητας» που **αποτρέπει** βρώμικα δεδομένα από το Silver.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - **Least privilege** & RBAC matrix για δημόσιο φορέα.
# MAGIC - `GRANT` σε UC (`USE SCHEMA`, `SELECT`) — η σωστή σύνταξη.
# MAGIC - Τι είναι **Data Contract** και γιατί είναι η ασπίδα του Silver.
# MAGIC - Χτίσιμο ενός `validate_contract()` με 5 ελέγχους (το flagship Day-1 pattern).

# COMMAND ----------

# DBTITLE 1,Config (έτοιμο)
from pyspark.sql.functions import col, count, when, lit

CATALOG       = "workspace"
SCHEMA_SILVER = "aade_silver"
SCHEMA_GOLD   = "aade_gold"
SILVER_TBL = f"{CATALOG}.{SCHEMA_SILVER}.declarations_clean"
GOLD_TBL   = f"{CATALOG}.{SCHEMA_GOLD}.declarations_by_category_region"

silver = spark.table(SILVER_TBL)
print(f"Silver: {silver.count()} γραμμές")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Least Privilege & RBAC
# MAGIC
# MAGIC **Αρχή ελάχιστου προνομίου:** δίνουμε **μόνο** τα δικαιώματα που χρειάζεται κάθε ρόλος —
# MAGIC ποτέ «για κάθε ενδεχόμενο». Όσο λιγότερα δικαιώματα, τόσο μικρότερο το **blast radius**
# MAGIC ενός compromise. Σε δημόσιο φορέα με PII (ΑΦΜ, εισοδήματα) αυτό είναι **υποχρεωτικό**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Φτιάξτε τον πίνακα ρόλων (RBAC matrix)
# MAGIC
# MAGIC Συμπληρώστε τα access για τον **Data Analyst**: δεν βλέπει Bronze (`—`), βλέπει Silver & Gold
# MAGIC μόνο για ανάγνωση (`READ`).

# COMMAND ----------

roles = spark.createDataFrame([
    ("Data Engineer", "READ/WRITE", "READ/WRITE", "READ/WRITE", "Πλήρες control"),
    ("Data Steward",  "READ",       "READ/WRITE", "READ",       "Quality + curation"),
    ("Data Analyst",  "______",     "______",     "______",     "BI + ad-hoc"),   # TODO 1: "—", "READ", "READ"
    ("Executive",     "—",          "—",          "READ",       "Curated dashboards μόνο"),
    ("Auditor (DPO)", "READ-meta",  "READ-meta",  "READ-meta",  "Audit logs"),
    ("Citizen (GDPR)","—",          "—",          "—",          "Right to erasure μόνο"),
], ["role", "bronze", "silver", "gold", "notes"])
display(roles)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — GRANT σε Unity Catalog
# MAGIC
# MAGIC Για να διαβάσει κάποιος ένα table χρειάζεται **δύο** grants:
# MAGIC - `GRANT USE SCHEMA ON SCHEMA ...` (να «δει» το schema), και
# MAGIC - `GRANT SELECT ON TABLE ...` (να διαβάσει το table).
# MAGIC
# MAGIC ⚠️ Είναι **`USE SCHEMA`**, όχι το παλιό/legacy `USAGE`. Σε Free Edition ίσως δεν έχετε
# MAGIC δικαίωμα GRANT — γι' αυτό το τυλίγουμε σε `try/except` (μαθαίνετε τη σύνταξη ούτως ή άλλως).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — GRANT read access στο Gold (για analysts)
# MAGIC
# MAGIC Συμπληρώστε το privilege keyword.

# COMMAND ----------

GROUP = "account users"   # built-in group· σε production: π.χ. "aade_analysts"
try:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA_GOLD} TO `{GROUP}`")
    spark.sql(f"GRANT ______ ON TABLE {GOLD_TBL} TO `{GROUP}`")   # TODO 2: privilege ανάγνωσης → SELECT
    print(f"✅ Granted SELECT on {GOLD_TBL} to `{GROUP}`")
    display(spark.sql(f"SHOW GRANTS ON TABLE {GOLD_TBL}"))
except Exception as e:
    print(f"⚠️  GRANT δεν εφαρμόστηκε (πιθανόν Free Edition χωρίς δικαιώματα): {str(e)[:140]}")
    print("   → Η σύνταξη παραμένει σωστή· σε production cluster θα δουλέψει.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — Τι είναι Data Contract
# MAGIC
# MAGIC Ένα **Data Contract** είναι μια ρητή «συμφωνία» για το πώς πρέπει να μοιάζουν τα δεδομένα:
# MAGIC πόσες γραμμές, ποιες στήλες, ποια δεν επιτρέπεται να είναι NULL, ποια μορφή/τιμές.
# MAGIC Λειτουργεί ως **πύλη ποιότητας** μεταξύ Bronze και Silver: ό,τι δεν περνά το contract
# MAGIC **δεν** προχωράει — ειδοποιεί αντί να μολύνει το Gold.
# MAGIC
# MAGIC ⚠️ Δεν είναι built-in: το `validate_contract()` το **γράφουμε εμείς**. Σε production το αντικαθιστούν
# MAGIC DLT `@dlt.expect_or_fail` ή Great Expectations.

# COMMAND ----------

# DBTITLE 1,Το contract spec (έτοιμο)
CONTRACT = {
    "version": "1.0",
    "min_rows": 100,
    "max_rows": 100000,
    "required_columns": ["declaration_id", "afm", "amount_eur", "status", "tax_category"],
    "non_nullable": ["declaration_id", "afm"],
    "afm_regex": r"^\d{9}$",
    "allowed_status": ["Εγκεκριμένη", "Εκκρεμής", "Απορριφθείσα"],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Χτίστε το `validate_contract()` (5 έλεγχοι)
# MAGIC
# MAGIC Συμπληρώστε τα κενά. Επιστρέφει `(df_valid, df_invalid, report)`. Κάθε `# →` εξηγεί το βήμα.

# COMMAND ----------

def validate_contract(df, contract):
    report = {"checks": {}, "n_total": df.count(), "n_invalid": 0, "violations": []}
    n = report["n_total"]

    # → Check 1: Row-count bounds
    if n < contract["min_rows"] or n > contract["max_rows"]:
        report["violations"].append(f"row count {n} εκτός ορίων")
    report["checks"]["row_bounds"] = "pass" if not report["violations"] else "fail"

    # → Check 2: Required columns exist
    missing = [c for c in contract["required_columns"] if c not in df.columns]
    if missing:
        report["violations"].append(f"λείπουν στήλες: {missing}")
    report["checks"]["columns_exist"] = "pass" if not missing else f"fail: {missing}"

    # Χτίζουμε σωρευτικό invalid_mask (γραμμή-γραμμή)
    invalid_mask = lit(False)

    # → Check 3: Non-nullable columns (το κλειδί δεν επιτρέπεται NULL)
    for c in contract["non_nullable"]:
        if c in df.columns:
            invalid_mask = invalid_mask | col(c)._________            # TODO 3a: «είναι NULL» → .isNull()

    # → Check 4: AFM regex (ακριβώς 9 ψηφία)
    invalid_mask = invalid_mask | (
        col("afm").isNotNull() & (~col("afm").cast("string").______(contract["afm_regex"]))  # TODO 3b: rlike
    )

    # → Check 5: Allowed status values
    invalid_mask = invalid_mask | (~col("status").______(contract["allowed_status"]))         # TODO 3c: isin

    # Split valid / invalid
    df_with = df.withColumn("_is_invalid", invalid_mask)
    df_valid   = df_with.filter(~col("_is_invalid")).drop("_is_invalid")
    df_invalid = df_with.filter(col("_is_invalid")).drop("_is_invalid")     # (έτοιμο)
    report["n_invalid"] = df_invalid.count()
    return df_valid, df_invalid, report

print("✅ validate_contract() ορίστηκε")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Τρέξτε το contract πάνω στο Silver
# MAGIC
# MAGIC Τα δεδομένα μας είναι καθαρά → περιμένουμε **0 invalid**.

# COMMAND ----------

valid_df, invalid_df, report = validate_contract(spark.table(SILVER_TBL), ________)   # TODO 4: το CONTRACT
print("=== DATA CONTRACT REPORT ===")
print(f"  Total:    {report['n_total']}")
print(f"  Valid:    {valid_df.count()}")
print(f"  Invalid:  {report['n_invalid']}")
print(f"  Checks:   {report['checks']}")
print(f"  Violations: {report['violations'] or 'καμία ✅'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 TODO 5 — Δοκιμάστε ότι «πιάνει» τα κακά (inject 3 bad rows)
# MAGIC
# MAGIC Φτιάχνουμε 3 σκόπιμα προβληματικές γραμμές. Το contract πρέπει να βρει **3 invalid**.

# COMMAND ----------

from pyspark.sql import Row
bad = spark.createDataFrame([
    Row(declaration_id=9001, afm=None,        amount_eur=10.0, status="Εγκεκριμένη", tax_category="ΦΠΑ"),  # null afm
    Row(declaration_id=9002, afm="12345",     amount_eur=10.0, status="Εγκεκριμένη", tax_category="ΦΠΑ"),  # bad afm
    Row(declaration_id=9003, afm="100111111", amount_eur=10.0, status="ΑΓΝΩΣΤΟ",     tax_category="ΦΠΑ"),  # bad status
])
_, bad_invalid, bad_report = validate_contract(bad, CONTRACT)
print(f"Injected 3 bad rows → invalid = {bad_report['n_invalid']}")   # πρέπει 3
bad_invalid.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 3 (τελικό Ημέρας 1)

# COMMAND ----------

results = {
    "RBAC matrix έχει 6 ρόλους":      roles.count() == 6,
    "Contract: valid+invalid=total":  (valid_df.count() + report["n_invalid"]) == report["n_total"],
    "Καθαρό Silver → 0 invalid":      report["n_invalid"] == 0,
    "Contract πιάνει τα bad rows = 3": bad_report["n_invalid"] == 3,
    "Όλοι οι checks ορίστηκαν":        set(["row_bounds","columns_exist"]).issubset(set(report["checks"].keys())),
}
print("=" * 50)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 50)
print("🎉🎉 ΟΛΟΚΛΗΡΩΣΑΤΕ ΟΛΗ ΤΗΝ ΑΣΚΗΣΗ DAY 1!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Τι χτίσατε (Ημέρα 1)
# MAGIC
# MAGIC ```
# MAGIC declarations.csv (TAXIS, 300 δηλώσεις)
# MAGIC      │  UC: catalog/schema/volume (Μέρος 1)
# MAGIC      ▼
# MAGIC 🥉 aade_bronze.declarations_raw     (raw + audit)
# MAGIC      │  cast types · ΑΦΜ→string · rename (Μέρος 2)
# MAGIC      ▼
# MAGIC 🥈 aade_silver.declarations_clean   ← 🔐 Data Contract gate (Μέρος 3)
# MAGIC      │  groupBy/agg
# MAGIC      ▼
# MAGIC 🥇 aade_gold.declarations_by_category_region  →  Power BI
# MAGIC ```
# MAGIC
# MAGIC **Cleanup (προαιρετικό):**
# MAGIC ```python
# MAGIC # for t in ["aade_bronze.declarations_raw","aade_silver.declarations_clean","aade_gold.declarations_by_category_region"]:
# MAGIC #     spark.sql(f"DROP TABLE IF EXISTS workspace.{t}")
# MAGIC ```
