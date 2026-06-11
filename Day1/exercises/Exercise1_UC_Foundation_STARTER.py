# Databricks notebook source
# MAGIC %md
# MAGIC # 🏗️ Άσκηση Ημέρα 1 — Μέρος 1/3: Unity Catalog Foundation
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~55' · **Δυσκολία:** ⭐ Easy-Medium
# MAGIC > **Στυλ:** Συμπληρώνετε τα `_____` σε κάθε `# TODO`. Πάνω από κάθε TODO υπάρχει ένα κελί
# MAGIC > 🧠 ΕΝΝΟΙΑ που εξηγεί *τι / γιατί / πώς*. Διαβάστε το πρώτα.
# MAGIC
# MAGIC ## 📖 Το Σενάριο
# MAGIC
# MAGIC Είστε νέοι Data Engineers στην **ΑΑΔΕ**. Πριν γράψετε γραμμή κώδικα ανάλυσης, πρέπει να
# MAGIC στήσετε το **θεμέλιο**: την ιεραρχία του **Unity Catalog** (catalog → schema → table/volume)
# MAGIC που θα χρησιμοποιείτε όλη την εβδομάδα, και να φορτώσετε το πρώτο σας dataset:
# MAGIC τις **φορολογικές δηλώσεις TAXIS** (`declarations.csv`, 300 δηλώσεις).
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε στο Μέρος 1
# MAGIC
# MAGIC - Το **3-level namespace** (`catalog.schema.table`) και γιατί αντικατέστησε το παλιό 2-level.
# MAGIC - **Volumes** vs DBFS — πού αποθηκεύουμε αρχεία (CSV/JSON) με governance.
# MAGIC - **Idempotency** (`IF NOT EXISTS`) — γιατί κάθε setup script πρέπει να ξαναtρέχει χωρίς error.
# MAGIC - Η παγίδα του **`inferSchema`** με το ΑΦΜ (το ξαναβλέπουμε «ζωντανά»).
# MAGIC - `SHOW SCHEMAS` / `SHOW VOLUMES` — πώς επιβεβαιώνουμε ότι όλα στήθηκαν.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 0 — Το 3-Level Namespace
# MAGIC
# MAGIC ```
# MAGIC catalog              ← λογικό container (π.χ. workspace, prod, sandbox)
# MAGIC   └── schema         ← ομάδα σχετικών tables (π.χ. bronze, silver, gold)
# MAGIC         ├── table    ← Delta table (π.χ. declarations_raw)
# MAGIC         └── volume   ← managed path για αρχεία (CSV/JSON/images)
# MAGIC ```
# MAGIC
# MAGIC Πλήρες path: `workspace.aade_bronze.declarations_raw` (table) ·
# MAGIC `/Volumes/workspace/aade_bronze/landing` (volume).
# MAGIC
# MAGIC Στο lab χρησιμοποιούμε τον **υπάρχοντα** catalog `workspace` (δουλεύει σε Free Edition) και
# MAGIC φτιάχνουμε 3 schemas — ένα ανά medallion layer.

# COMMAND ----------

# DBTITLE 1,Configuration (έτοιμο)
CATALOG        = "workspace"
SCHEMA_BRONZE  = "aade_bronze"
SCHEMA_SILVER  = "aade_silver"
SCHEMA_GOLD    = "aade_gold"
VOLUME_FQN     = f"{CATALOG}.{SCHEMA_BRONZE}.landing"
LANDING_PATH   = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/landing"

print(f"Catalog: {CATALOG}")
print(f"Landing: {LANDING_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Idempotency: `IF NOT EXISTS`
# MAGIC
# MAGIC Κάθε DDL (`CREATE SCHEMA`, `CREATE VOLUME`…) πρέπει να ξαναtρέχει **χωρίς error**:
# MAGIC ```sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ...   -- ✅ re-run safe
# MAGIC CREATE SCHEMA ...                 -- ❌ error στο 2ο run
# MAGIC ```
# MAGIC Αυτό είναι production rule: τα setup scripts τρέχουν πολλές φορές (CI/CD, retries).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Δημιουργία 3 schemas (bronze / silver / gold)
# MAGIC
# MAGIC Συμπληρώστε το DDL keyword ώστε να είναι **idempotent**.
# MAGIC *Hint:* `CREATE SCHEMA IF NOT EXISTS <fqn> COMMENT '...'`.

# COMMAND ----------

for schema, desc in [
    (SCHEMA_BRONZE, "Raw ingested data — immutable, full history"),
    (SCHEMA_SILVER, "Cleaned, typed, validated, deduplicated"),
    (SCHEMA_GOLD,   "Aggregated, business-ready για BI"),
]:
    spark.sql(f"_________________________ {CATALOG}.{schema} COMMENT '{desc}'")   # TODO 1: CREATE SCHEMA IF NOT EXISTS
    print(f"✅ Schema: {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Volumes vs DBFS
# MAGIC
# MAGIC **DBFS** = παλιό global filesystem, χωρίς governance (όλοι έβλεπαν τα πάντα).
# MAGIC **Volume** = UC-managed path για **αρχεία** (όχι tables). Έχει owner, permissions, audit,
# MAGIC και εμφανίζεται στο Catalog Explorer. Εδώ αποθηκεύουμε τα raw CSV πριν τα κάνουμε Delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Δημιουργία Volume `landing`
# MAGIC
# MAGIC *Hint:* `CREATE VOLUME IF NOT EXISTS <catalog.schema.volume>`.

# COMMAND ----------

spark.sql(f"________________________ {VOLUME_FQN} COMMENT 'Landing zone για raw CSVs'")   # TODO 2: CREATE VOLUME IF NOT EXISTS
print(f"✅ Volume ready: {LANDING_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell — Download declarations.csv (έτοιμο, μην το αλλάξετε)
# MAGIC
# MAGIC Κατεβάζει τα ΑΑΔΕ CSV στο Volume σας. Idempotent.

# COMMAND ----------

import urllib.request, os
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day1"
for fname in ["declarations.csv", "doy.csv", "employees.csv", "taxpayers.csv"]:
    target = f"{LANDING_PATH}/{fname}"
    if not os.path.exists(target):
        urllib.request.urlretrieve(f"{REPO}/{fname}", target); print(f"✅ {fname}")
    else:
        print(f"⏭️  {fname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — `inferSchema` και το ΑΦΜ (ξανά!)
# MAGIC
# MAGIC Διαβάζουμε με `inferSchema=True` για **εξερεύνηση**. Παρατηρήστε τι τύπο δίνει ο Spark
# MAGIC στο `ΑΦΜ` — θα είναι `integer`/`long`, που είναι **λάθος** για identifier (χάνει αρχικά
# MAGIC μηδενικά, δεν επιτρέπει regex). Στο Μέρος 2 θα το διορθώσουμε με explicit schema.
# MAGIC Επίσης: οι στήλες είναι στα **Ελληνικά** — κι αυτό το «καθαρίζουμε» στο Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Read το CSV
# MAGIC
# MAGIC Συμπληρώστε τις δύο options.

# COMMAND ----------

df = (
    spark.read
         .option("__________", "true")   # TODO 3a: header
         .option("__________", "true")   # TODO 3b: inferSchema
         .csv(f"{LANDING_PATH}/declarations.csv")
)
print(f"Δηλώσεις: {df.count()} γραμμές")   # αναμένεται 300
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **🔎 Παρατηρήστε:** τι τύπο έδωσε ο Spark στο `ΑΦΜ`; (κρατήστε το στο μυαλό για το Μέρος 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Επιβεβαίωση: SHOW SCHEMAS & SHOW VOLUMES
# MAGIC
# MAGIC Συμπληρώστε τις εντολές που **εμφανίζουν** schemas & volumes.

# COMMAND ----------

print("=== SCHEMAS στο catalog ===")
display(spark.sql(f"____ SCHEMAS IN {CATALOG}"))            # TODO 4a: SHOW

print("=== VOLUMES στο bronze schema ===")
display(spark.sql(f"____ VOLUMES IN {CATALOG}.{SCHEMA_BRONZE}"))   # TODO 4b: SHOW

print("=== Αρχεία στο Volume ===")
display(dbutils.fs.ls(LANDING_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 1

# COMMAND ----------

existing_schemas = [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()]
files = [f.name for f in dbutils.fs.ls(LANDING_PATH)]
results = {
    "Schema aade_bronze υπάρχει": SCHEMA_BRONZE in existing_schemas,
    "Schema aade_silver υπάρχει": SCHEMA_SILVER in existing_schemas,
    "Schema aade_gold υπάρχει":   SCHEMA_GOLD in existing_schemas,
    "Volume landing υπάρχει":     spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA_BRONZE}").filter("volume_name = 'landing'").count() == 1,
    "declarations.csv στο volume": "declarations.csv" in files,
    "DataFrame = 300 γραμμές":    df.count() == 300,
}
print("=" * 50)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 50)
print("🎉 Τέλος Μέρους 1!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise2_Medallion_STARTER`
# MAGIC
# MAGIC Έχουμε το θεμέλιο (catalog/schemas/volume) και το raw CSV. Στο Μέρος 2 χτίζουμε το
# MAGIC **Medallion**: Bronze → Silver (με σωστούς τύπους — εδώ διορθώνουμε το ΑΦΜ!) → Gold.
