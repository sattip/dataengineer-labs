# Databricks notebook source
# MAGIC %md
# MAGIC # 🔐 Άσκηση Ημέρα 5 — Μέρος 3/4: Column Masking + Row-Level Security
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~70' · **Δυσκολία:** ⭐⭐⭐ Hard · **~12 TODOs**
# MAGIC > Self-contained · τρέχει σε **Serverless** (Unity Catalog).
# MAGIC
# MAGIC ## 📖 Το πρόβλημα
# MAGIC
# MAGIC Τα δεδομένα της ΑΑΔΕ έχουν **PII**: ΑΦΜ, επωνυμίες, **εισοδήματα/ποσά φόρου**. Δεν επιτρέπεται
# MAGIC να τα βλέπουν **όλοι**. Ένας απλός analyst πρέπει να βλέπει **μασκαρισμένο** ΑΦΜ και **κρυμμένο**
# MAGIC ποσό, και **μόνο** τις περιφέρειες αρμοδιότητάς του. Αυτό είναι **column masking** + **row-level security**.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθεις
# MAGIC
# MAGIC - **`is_account_group_member()`** — η συνάρτηση που ελέγχει ρόλο/group.
# MAGIC - **Dynamic view** masking (CASE) — μασκάρισμα ΑΦΜ & απόκρυψη ποσού.
# MAGIC - **Row-level security** μέσω view (φίλτρο περιφέρειας).
# MAGIC - **UC Column Mask** (`ALTER COLUMN ... SET MASK`) — ο production τρόπος.
# MAGIC - **UC Row Filter** (`SET ROW FILTER`) — production row-level security.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + Silver με PII (έτοιμο)

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
# declarations.csv από το repo (περιέχει PII: ΑΦΜ, επωνυμία, ποσά)
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve("https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/declarations.csv",
                               f"{VOLUME}/declarations.csv")

SILVER        = "workspace.aade.pii_declarations"
VIEW_MASKED   = "workspace.aade.pii_declarations_masked"
VIEW_MYREGION = "workspace.aade.pii_declarations_myregion"

(spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
 .select(
     col("ΑΦΜ").cast("string").alias("afm"),
     col("Επωνυμία").alias("business_name"),
     col("Ποσό_EUR").cast("double").alias("tax_amount_eur"),   # ευαίσθητο
     col("Περιφέρεια").alias("region"),
     col("Κατάσταση").alias("status"))
 .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(SILVER))
print(f"✓ Silver (PII): {spark.table(SILVER).count()} γραμμές")
spark.table(SILVER).show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — `is_account_group_member()` & least privilege
# MAGIC
# MAGIC Η `is_account_group_member('<group>')` επιστρέφει `true` αν ο **τρέχων χρήστης** ανήκει στο group.
# MAGIC Τη χρησιμοποιούμε για να αποφασίσουμε *τι βλέπει ποιος*. Θα δουλέψουμε με ένα group
# MAGIC `aade_pii_unmasked` (PII-cleared). Αφού **δεν** είστε μέλος του, θα δείτε τα δεδομένα **μασκαρισμένα**
# MAGIC — ακριβώς όπως ένας απλός analyst.

# COMMAND ----------

print("Είστε μέλος του 'aade_pii_unmasked';",
      spark.sql("SELECT is_account_group_member('aade_pii_unmasked') AS m").collect()[0]["m"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Column masking μέσω Dynamic View
# MAGIC
# MAGIC Φτιάχνουμε ένα **view** που: δείχνει το πλήρες ΑΦΜ/ποσό **μόνο** σε PII-cleared χρήστες,
# MAGIC αλλιώς **μασκάρει**. Pattern: `CASE WHEN is_account_group_member('...') THEN <raw> ELSE <masked> END`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Δημιούργησε το masked view

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {VIEW_MASKED} AS
    SELECT
        CASE WHEN ____________________________('aade_pii_unmasked')    -- TODO 1a: is_account_group_member
             THEN afm
             ELSE concat('***', ____________)                          -- TODO 1b: substr(afm, 7, 3) (3 τελευταία)
        END AS afm,
        business_name,
        CASE WHEN ____________________________('aade_pii_unmasked')    -- TODO 1c: is_account_group_member (ξανά)
             THEN tax_amount_eur
             ELSE ______                                               -- TODO 1d: NULL (κρύψε το ποσό)
        END AS tax_amount_eur,
        region,
        status
    FROM {SILVER}
""")
print("=== Masked view (όπως το βλέπει απλός analyst) ===")
spark.table(VIEW_MASKED).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — Row-Level Security μέσω View
# MAGIC
# MAGIC Ένας analyst της Αττικής πρέπει να βλέπει **μόνο** γραμμές Αττικής. Φτιάχνουμε view με `WHERE`
# MAGIC που επιτρέπει: ή είσαι σε group «όλες οι περιφέρειες», ή η γραμμή είναι της περιφέρειάς σου.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Δημιούργησε το row-filtered view (μόνο Αττική)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {VIEW_MYREGION} AS
    SELECT * FROM {SILVER}
    WHERE is_account_group_member('aade_all_regions')
       ___ region = 'Αττική'                                          -- TODO 2: OR
""")
myregion_count = spark.table(VIEW_MYREGION).count()
attica_count   = spark.table(SILVER).filter(col("region") == "Αττική").count()
print(f"Row-filtered view: {myregion_count} γραμμές (Αττική στο silver: {attica_count})")
spark.table(VIEW_MYREGION).select("region").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — Ο production τρόπος: UC Column Mask
# MAGIC
# MAGIC Αντί για view, στο Unity Catalog ορίζεις μια **masking function** και την «κολλάς» στη στήλη με
# MAGIC `ALTER TABLE ... ALTER COLUMN ... SET MASK`. Τότε **κάθε** query στο table (όχι μόνο σε view)
# MAGIC εφαρμόζει τη μάσκα αυτόματα. (Σε Free Edition ίσως χρειάζεται δικαίωμα — wrapped σε try/except.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — UC column mask (production)

# COMMAND ----------

try:
    # 3a: masking function — επιστρέφει raw αν PII-cleared, αλλιώς μασκαρισμένο
    spark.sql("""
        CREATE OR REPLACE FUNCTION workspace.aade.mask_afm(a STRING)
        RETURN CASE WHEN ____________________________('aade_pii_unmasked') THEN a
                    ELSE concat('***', substr(a, 7, 3)) END
    """)                                                                  -- TODO 3a: is_account_group_member
    # 3b: κόλλησε τη μάσκα στη στήλη afm
    spark.sql(f"ALTER TABLE {SILVER} ALTER COLUMN afm SET ________ workspace.aade.mask_afm")   # TODO 3b: MASK
    print("✅ Column mask εφαρμόστηκε. Query απευθείας στο table:")
    spark.table(SILVER).select("afm","business_name").show(3, truncate=False)
    uc_mask_ok = spark.sql(f"SELECT afm FROM {SILVER} LIMIT 1").collect()[0]["afm"].startswith("***")
except Exception as e:
    uc_mask_ok = None
    print(f"⚠️  UC column mask δεν εφαρμόστηκε (δικαιώματα/edition): {str(e)[:140]}")
    print("   → Η σύνταξη είναι σωστή· σε production cluster με UC δουλεύει.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — UC Row Filter (production)
# MAGIC
# MAGIC Ομοίως, ορίζεις function που επιστρέφει boolean και την κολλάς ως **ROW FILTER**.

# COMMAND ----------

try:
    spark.sql("""
        CREATE OR REPLACE FUNCTION workspace.aade.region_filter(r STRING)
        RETURN is_account_group_member('aade_all_regions') ___ r = 'Αττική'
    """)                                                                  -- TODO 4a: OR
    # 4b: κόλλησε το row filter στη στήλη region
    spark.sql(f"ALTER TABLE {SILVER} SET ROW ______ workspace.aade.region_filter ON (region)")   # TODO 4b: FILTER
    filtered = spark.table(SILVER).count()
    print(f"✅ Row filter εφαρμόστηκε. Ορατές γραμμές τώρα: {filtered}")
    # καθάρισε τα policies για να μην επηρεάσουν το Μέρος 4
    spark.sql(f"ALTER TABLE {SILVER} DROP ROW FILTER")
    spark.sql(f"ALTER TABLE {SILVER} ALTER COLUMN afm DROP MASK")
    print("   (policies καθαρίστηκαν για το Μέρος 4)")
except Exception as e:
    print(f"⚠️  UC row filter δεν εφαρμόστηκε: {str(e)[:140]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 3

# COMMAND ----------

import re
masked_afms = [r["afm"] for r in spark.table(VIEW_MASKED).select("afm").limit(20).collect()]
all_masked  = all(a.startswith("***") for a in masked_afms)
income_hidden = spark.table(VIEW_MASKED).filter(col("tax_amount_eur").isNotNull()).count() == 0

results = {
    "Masked view: ΑΦΜ μασκαρισμένα (***)":   all_masked,
    "Masked view: ποσό κρυμμένο (NULL)":     income_hidden,
    "Row view: μόνο Αττική":                  spark.table(VIEW_MYREGION).select("region").distinct().count() == 1,
    "Row view count == Αττική count":         myregion_count == attica_count,
    "is_account_group_member δουλεύει":       isinstance(spark.sql("SELECT is_account_group_member('x') m").collect()[0]["m"], bool),
}
print("=" * 55)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
if uc_mask_ok is True:
    print("  ✅ OK   — UC column mask (production) εφαρμόστηκε")
elif uc_mask_ok is None:
    print("  ℹ️  SKIP — UC column mask (δες σημείωση· δεν επηρεάζει το pass)")
print("=" * 55)
print("🎉 Τέλος Μέρους 3!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise4_Governance_Audit_STARTER`
# MAGIC
# MAGIC Ξέρουμε να **κρύβουμε** δεδομένα. Στο Μέρος 4: **GRANT/REVOKE**, ποιος έχει access (information_schema),
# MAGIC και **audit** — ποιος έκανε τι (system tables).
