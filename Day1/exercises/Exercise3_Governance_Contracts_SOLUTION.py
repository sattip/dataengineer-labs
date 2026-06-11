# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 3/3: Governance + Data Contracts

# COMMAND ----------

from pyspark.sql.functions import col, count, when, lit
from pyspark.sql import Row

CATALOG       = "workspace"
SCHEMA_SILVER = "aade_silver"
SCHEMA_GOLD   = "aade_gold"
SILVER_TBL = f"{CATALOG}.{SCHEMA_SILVER}.declarations_clean"
GOLD_TBL   = f"{CATALOG}.{SCHEMA_GOLD}.declarations_by_category_region"

# COMMAND ----------

# TODO 1 — RBAC matrix
roles = spark.createDataFrame([
    ("Data Engineer", "READ/WRITE", "READ/WRITE", "READ/WRITE", "Πλήρες control"),
    ("Data Steward",  "READ",       "READ/WRITE", "READ",       "Quality + curation"),
    ("Data Analyst",  "—",          "READ",       "READ",       "BI + ad-hoc"),
    ("Executive",     "—",          "—",          "READ",       "Curated dashboards μόνο"),
    ("Auditor (DPO)", "READ-meta",  "READ-meta",  "READ-meta",  "Audit logs"),
    ("Citizen (GDPR)","—",          "—",          "—",          "Right to erasure μόνο"),
], ["role", "bronze", "silver", "gold", "notes"])
display(roles)

# COMMAND ----------

# TODO 2 — GRANT
GROUP = "account users"
try:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA_GOLD} TO `{GROUP}`")
    spark.sql(f"GRANT SELECT ON TABLE {GOLD_TBL} TO `{GROUP}`")
    print("✅ Granted")
    display(spark.sql(f"SHOW GRANTS ON TABLE {GOLD_TBL}"))
except Exception as e:
    print(f"⚠️  GRANT skipped (Free Edition?): {str(e)[:120]}")

# COMMAND ----------

# TODO 3 — validate_contract
CONTRACT = {
    "version": "1.0", "min_rows": 100, "max_rows": 100000,
    "required_columns": ["declaration_id", "afm", "amount_eur", "status", "tax_category"],
    "non_nullable": ["declaration_id", "afm"],
    "afm_regex": r"^\d{9}$",
    "allowed_status": ["Εγκεκριμένη", "Εκκρεμής", "Απορριφθείσα"],
}

def validate_contract(df, contract):
    report = {"checks": {}, "n_total": df.count(), "n_invalid": 0, "violations": []}
    n = report["n_total"]
    if n < contract["min_rows"] or n > contract["max_rows"]:
        report["violations"].append(f"row count {n} εκτός ορίων")
    report["checks"]["row_bounds"] = "pass" if not report["violations"] else "fail"
    missing = [c for c in contract["required_columns"] if c not in df.columns]
    if missing:
        report["violations"].append(f"λείπουν στήλες: {missing}")
    report["checks"]["columns_exist"] = "pass" if not missing else f"fail: {missing}"

    invalid_mask = lit(False)
    for c in contract["non_nullable"]:
        if c in df.columns:
            invalid_mask = invalid_mask | col(c).isNull()
    invalid_mask = invalid_mask | (
        col("afm").isNotNull() & (~col("afm").cast("string").rlike(contract["afm_regex"]))
    )
    invalid_mask = invalid_mask | (~col("status").isin(contract["allowed_status"]))

    df_with = df.withColumn("_is_invalid", invalid_mask)
    df_valid   = df_with.filter(~col("_is_invalid")).drop("_is_invalid")
    df_invalid = df_with.filter(col("_is_invalid")).drop("_is_invalid")
    report["n_invalid"] = df_invalid.count()
    return df_valid, df_invalid, report

print("✅ validate_contract() defined")

# COMMAND ----------

# TODO 4 — run on Silver
valid_df, invalid_df, report = validate_contract(spark.table(SILVER_TBL), CONTRACT)
print(f"Total {report['n_total']} · Valid {valid_df.count()} · Invalid {report['n_invalid']}")
print("Checks:", report["checks"], "Violations:", report["violations"] or "καμία ✅")

# COMMAND ----------

# TODO 5 — inject bad rows
bad = spark.createDataFrame([
    Row(declaration_id=9001, afm=None,        amount_eur=10.0, status="Εγκεκριμένη", tax_category="ΦΠΑ"),
    Row(declaration_id=9002, afm="12345",     amount_eur=10.0, status="Εγκεκριμένη", tax_category="ΦΠΑ"),
    Row(declaration_id=9003, afm="100111111", amount_eur=10.0, status="ΑΓΝΩΣΤΟ",     tax_category="ΦΠΑ"),
])
_, bad_invalid, bad_report = validate_contract(bad, CONTRACT)
print(f"Injected bad rows → invalid = {bad_report['n_invalid']} (αναμένεται 3)")
bad_invalid.show(truncate=False)

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 3 ολοκληρώθηκε — Ημέρα 1 complete")
