# Databricks notebook source
# MAGIC %md
# MAGIC # 🔏 Άσκηση Ημέρα 5 — Μέρος 6/6 (Advanced): PII Tokenization & ABAC
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~70' · **Δυσκολία:** ⭐⭐⭐⭐ Advanced · **~9 TODOs**
# MAGIC > Self-contained · τρέχει σε **Serverless** (UC).
# MAGIC
# MAGIC ## 📖 Πού πάμε πιο πέρα
# MAGIC
# MAGIC Στο Μέρος 3 μασκάραμε το ΑΦΜ (`***`). Αλλά τι γίνεται όταν θες να **μοιραστείς** δεδομένα έξω
# MAGIC (π.χ. με ερευνητικό φορέα) χωρίς να αποκαλύψεις ΑΦΜ, αλλά **να μπορείς ακόμα να κάνεις join**;
# MAGIC → **Pseudonymization** (hashing). Και πώς δίνεις σε **κάθε χρήστη** πρόσβαση **μόνο** στις δικές
# MAGIC του περιφέρειες **δυναμικά**, χωρίς hardcoded κανόνες; → **ABAC** με πίνακα εξουσιοδοτήσεων.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθεις
# MAGIC
# MAGIC - **Pseudonymization** με `sha2()` — ντετερμινιστικό token, joinable, μη-αναστρέψιμο (GDPR).
# MAGIC - **Salted hashing** — γιατί το salt εμποδίζει rainbow-table & cross-dataset linkage.
# MAGIC - **ABAC / mapping-table row-level security** με `current_user()` (realistic, όχι hardcoded).
# MAGIC - **Sensitivity tagging** στηλών (governance metadata).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + Silver με PII (έτοιμο)

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col, sha2, concat, lit, length

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve("https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/declarations.csv",
                               f"{VOLUME}/declarations.csv")

SILVER       = "workspace.aade.tok_declarations"
SHARED       = "workspace.aade.tok_declarations_shared"   # view (pseudonymized)
ENTITLEMENTS = "workspace.aade.entitlements"
ABAC_VIEW    = "workspace.aade.tok_declarations_abac"     # view (ABAC RLS)

(spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
 .select(col("ΑΦΜ").cast("string").alias("afm"), col("Επωνυμία").alias("business_name"),
         col("Ποσό_EUR").cast("double").alias("tax_amount_eur"), col("Περιφέρεια").alias("region"))
 .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(SILVER))
print(f"✓ Silver (PII): {spark.table(SILVER).count()} γραμμές")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Pseudonymization με `sha2()`
# MAGIC
# MAGIC `sha2(afm, 256)` παράγει ένα **ντετερμινιστικό** 64-χαρακτήρων hex token: ίδιο ΑΦΜ → ίδιο token
# MAGIC (άρα **joinable** μεταξύ datasets), αλλά **μη-αναστρέψιμο** (δεν βγάζεις πίσω το ΑΦΜ).
# MAGIC Ιδανικό για να μοιραστείς δεδομένα χωρίς να εκθέσεις PII.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Φτιάξε pseudonymized «shared» view

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {SHARED} AS
    SELECT
        ________(afm, ___) AS afm_token,        -- TODO 1a: sha2 · TODO 1b: 256
        business_name,
        tax_amount_eur,
        region
    FROM {SILVER}
""")
print("=== Shared (pseudonymized) ===")
spark.table(SHARED).show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Απόδειξε: ντετερμινιστικό & 1:1 (joinable)
# MAGIC
# MAGIC Ο αριθμός distinct ΑΦΜ πρέπει να **ισούται** με τον αριθμό distinct tokens (καμία σύγκρουση,
# MAGIC ίδιο ΑΦΜ → πάντα ίδιο token).

# COMMAND ----------

n_afm   = spark.table(SILVER).select("afm").distinct().count()
n_token = spark.table(SHARED).select("________").distinct().count()   # TODO 2a: afm_token
print(f"distinct ΑΦΜ: {n_afm} · distinct tokens: {n_token} · 1:1; → {n_afm == n_token}")
token_len = spark.table(SHARED).select(length("afm_token").alias("L")).first()["L"]
print(f"Μήκος token: {token_len} (αναμένεται 64 hex)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Salted hashing
# MAGIC
# MAGIC Σκέτο `sha2(afm)` είναι ευάλωτο: κάποιος με λίστα ΑΦΜ μπορεί να φτιάξει rainbow table και να
# MAGIC τα αντιστοιχίσει. Με **salt** (μυστικό prefix) `sha2(concat(salt, afm))`, τα tokens γίνονται
# MAGIC αδύνατο να αντιστραφούν χωρίς το salt — και **διαφορετικά** ανά dataset (αποτρέπει linkage).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Salted token

# COMMAND ----------

SALT = "aade_2026_secret"   # σε production: από secret scope, ΟΧΙ στον κώδικα!
salted = spark.table(SILVER).withColumn(
    "afm_token_salted",
    sha2(concat(lit(SALT), col("____")), 256)        # TODO 3a: afm
)
salted.select("afm", "afm_token_salted").show(3, truncate=False)

# Σύγκριση για το ΙΔΙΟ ΑΦΜ: unsalted vs salted token (πρέπει να διαφέρουν)
sample_afm = spark.table(SILVER).select("afm").orderBy("afm").first()["afm"]
unsalted_t = spark.sql(f"SELECT sha2('{sample_afm}', 256) AS t").first()["t"]
salted_t   = salted.filter(col("afm") == sample_afm).select("afm_token_salted").first()["afm_token_salted"]
print(f"ΑΦΜ {sample_afm}:")
print(f"  unsalted = {unsalted_t}")
print(f"  salted   = {salted_t}")
print(f"Unsalted == Salted (ίδιο ΑΦΜ); → {unsalted_t == salted_t} (πρέπει False)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — ABAC: row-level security με πίνακα εξουσιοδοτήσεων
# MAGIC
# MAGIC Αντί για hardcoded `region = 'Αττική'`, φτιάχνουμε πίνακα **`entitlements`** (`user_email → allowed_region`).
# MAGIC Το view φιλτράρει με `current_user()`: **κάθε** χρήστης βλέπει **μόνο** τις περιφέρειες που του
# MAGIC έχουν ανατεθεί. Προσθέτουμε εγγραφή για **εσένα** (current_user) → `Αττική`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Entitlements table + ABAC view

# COMMAND ----------

# 4a: φτιάξε τον πίνακα εξουσιοδοτήσεων
spark.sql(f"CREATE OR REPLACE TABLE {ENTITLEMENTS} (user_email STRING, allowed_region STRING) USING delta")

# 4b: δώσε στον ΤΡΕΧΟΝΤΑ χρήστη πρόσβαση μόνο στην Αττική (current_user χωρίς hardcode email)
spark.sql(f"INSERT INTO {ENTITLEMENTS} SELECT ____________(), 'Αττική'")   # TODO 4a: current_user
display(spark.table(ENTITLEMENTS))

# 4c: ABAC view — φιλτράρει με βάση τα entitlements του current_user()
spark.sql(f"""
    CREATE OR REPLACE VIEW {ABAC_VIEW} AS
    SELECT * FROM {SILVER} s
    WHERE s.region ___ (                                  -- TODO 4b: IN
        SELECT allowed_region FROM {ENTITLEMENTS}
        WHERE user_email = ____________()                -- TODO 4c: current_user
    )
""")
abac_regions = [r["region"] for r in spark.table(ABAC_VIEW).select("region").distinct().collect()]
print(f"Ο current_user βλέπει περιφέρειες: {abac_regions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — Sensitivity tagging (governance metadata)
# MAGIC
# MAGIC Με **tags** μαρκάρουμε ευαίσθητες στήλες (`sensitivity = PII`) ώστε εργαλεία governance/discovery
# MAGIC να τις βρίσκουν αυτόματα. (Σε Free Edition ίσως χρειάζεται δικαίωμα — try/except.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — Tag τη στήλη afm ως PII

# COMMAND ----------

try:
    spark.sql(f"ALTER TABLE {SILVER} ALTER COLUMN afm SET ________ ('sensitivity' = 'PII')")   # TODO 5: TAGS
    print("✅ Tag εφαρμόστηκε στη στήλη afm")
    display(spark.sql(f"SELECT * FROM workspace.information_schema.column_tags WHERE schema_name='aade' AND table_name='tok_declarations'"))
    tag_ok = True
except Exception as e:
    tag_ok = None
    print(f"⚠️  Column tags δεν εφαρμόστηκαν (edition/δικαιώματα): {str(e)[:140]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 6

# COMMAND ----------

attica = spark.table(SILVER).filter(col("region") == "Αττική").count()
results = {
    "Token = 64 hex chars":               token_len == 64,
    "Pseudonymization 1:1 (joinable)":     n_afm == n_token,
    "Salted ≠ unsalted (ίδιο ΑΦΜ)":        unsalted_t != salted_t,
    "Shared view ΔΕΝ έχει raw 'afm'":      "afm" not in spark.table(SHARED).columns,
    "ABAC: ο current_user βλέπει μόνο Αττική": abac_regions == ["Αττική"],
    "ABAC count == Αττική count":          spark.table(ABAC_VIEW).count() == attica,
    "Entitlements driven (όχι hardcoded)": spark.table(ENTITLEMENTS).count() >= 1,
}
print("=" * 56)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print(f"  {'✅ OK  ' if tag_ok else 'ℹ️  SKIP'} — Sensitivity tag (production)")
print("=" * 56)
print("🏆 ΟΛΟΚΛΗΡΩΣΑΤΕ ΤΑ ADVANCED ΜΕΡΗ (5 & 6) ΤΗΣ ΗΜΕΡΑΣ 5!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Σύνοψη advanced security
# MAGIC
# MAGIC ```
# MAGIC Masking (Μέρος 3)  → κρύβει στον non-cleared χρήστη (***), ΟΧΙ joinable
# MAGIC Pseudonymization   → sha2 token: μη-αναστρέψιμο ΑΛΛΑ joinable (share δεδομένων)
# MAGIC Salted hashing     → + αποτρέπει rainbow-table & cross-dataset linkage
# MAGIC ABAC (entitlements)→ κάθε χρήστης βλέπει μόνο τα δικά του rows, δυναμικά
# MAGIC Tags               → αυτόματο PII discovery/governance
# MAGIC ```
# MAGIC **Cleanup (προαιρετικό):**
# MAGIC ```python
# MAGIC # for t in ["tok_declarations","tok_declarations_shared","entitlements","tok_declarations_abac",
# MAGIC #           "lc_source","lc_clustered"]:
# MAGIC #     spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")
# MAGIC ```
