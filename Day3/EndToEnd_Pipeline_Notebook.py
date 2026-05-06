# Databricks notebook source
# MAGIC %md
# MAGIC # Άσκηση #8 — End-to-End Production Pipeline (Capstone) (Ημέρα 3)
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC
# MAGIC > **GitHub** (share this στο Zoom chat):
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/EndToEnd_Pipeline_Notebook.py
# MAGIC > ```
# MAGIC
# MAGIC **Catalog/Schema**: `workspace.aade`
# MAGIC **Volume**: `/Volumes/workspace/aade/aade_data/`
# MAGIC **Διάρκεια**: ~60–75 min (capstone hands-on demo)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Σενάριο
# MAGIC
# MAGIC Είσαι **Data Engineer στην ΑΑΔΕ**. Κάθε βράδυ στις **03:00**, το παλιό mainframe ξεφορτώνει στο object storage **ένα CSV ανά ημέρα** με τις φορολογικές δηλώσεις. Πρέπει να φτιάξεις ένα **production pipeline** που:
# MAGIC
# MAGIC 1. Τρέχει **incremental** — μόνο νέα files, ποτέ δεν ξαναδιαβάζει τα χθεσινά
# MAGIC 2. Αντέχει σε **bad data** (DQ + quarantine)
# MAGIC 3. Διατηρεί **πλήρες ιστορικό αλλαγών** ανά δήλωση (SCD Type 2)
# MAGIC 4. Παρέχει **Gold KPIs** για το dashboard του Υπουργού
# MAGIC 5. Είναι **γρήγορο** σε ad-hoc queries (Liquid Clustering)
# MAGIC 6. Είναι **auditable** (history + lineage) και **operable** (ένα Job, ένα schedule)
# MAGIC
# MAGIC > 💡 Αυτό είναι το capstone της Ημέρας 3 — κάθε concept που είδαμε ως ξεχωριστή άσκηση μπαίνει στη θέση του στο production pipeline.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Key concepts (νέα + γνωστά)
# MAGIC
# MAGIC | # | Concept | Νέο/Γνωστό | Άσκηση όπου το είδαμε |
# MAGIC |---|---------|-----------|------------------------|
# MAGIC | 1 | Auto Loader (`cloudFiles`) — daily file batches | Γνωστό | Άσκηση 7 |
# MAGIC | 2 | Schema evolution + `_rescued_data` | Γνωστό | Άσκηση 7 |
# MAGIC | 3 | DQ split (CHECK + filter → quarantine) | Γνωστό | Άσκηση 3 |
# MAGIC | 4 | **SCD Type 2 με MERGE** (history of changes) | 🆕 **ΝΕΟ** | — |
# MAGIC | 5 | **Liquid Clustering** (vs Z-ORDER) | 🆕 **ΝΕΟ** | — |
# MAGIC | 6 | Gold aggregation + materialized-view pattern | Μερικώς | Άσκηση 6 |
# MAGIC | 7 | `DESCRIBE HISTORY` + UC Lineage | Γνωστό | Άσκηση 4 |
# MAGIC | 8 | OPTIMIZE / VACUUM operating cadence | Γνωστό | Άσκηση 5 |
# MAGIC | 9 | **Job/Workflow orchestration mapping** | 🆕 **ΝΕΟ** | — |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ Architecture (Medallion + governance)
# MAGIC
# MAGIC ```
# MAGIC  ┌───────────────────────────────────────────────────────────────────┐
# MAGIC  │  LANDING ZONE (Volume)  /Volumes/workspace/aade/aade_data/landing │
# MAGIC  │  declarations_2026-05-04.csv → declarations_2026-05-05.csv → ... │
# MAGIC  └────────────────────┬──────────────────────────────────────────────┘
# MAGIC                       │  Auto Loader (cloudFiles, availableNow=True)
# MAGIC                       ▼
# MAGIC      ┌─────────────────────────────┐
# MAGIC      │ BRONZE  declarations_bronze │  raw + _ingestion_ts + _src_file
# MAGIC      └─────────────┬───────────────┘
# MAGIC                    │  DQ split (CHECK + filter)
# MAGIC                    ▼
# MAGIC      ┌─────────────┴───────────────┐
# MAGIC      │       OK rows               │  ─── bad rows ───►  declarations_quarantine
# MAGIC      ▼
# MAGIC  staging  ──────────►  SILVER (SCD Type 2) declarations_silver_scd2
# MAGIC                          │
# MAGIC                          │  Liquid Clustering on (afm, status)
# MAGIC                          │  is_current = true  → "current view"
# MAGIC                          ▼
# MAGIC                 GOLD  kpi_doy_monthly_gold  (aggregations)
# MAGIC                          │
# MAGIC                          ▼
# MAGIC                  Dashboard / API consumers
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Notebook structure
# MAGIC
# MAGIC | Cell | Step | Time |
# MAGIC |---|------|------|
# MAGIC | 0 | Setup + download seed CSV | 1 min |
# MAGIC | 1 | Simulate landing-zone daily drops (3 batches) | 2 min |
# MAGIC | 2 | Auto Loader → Bronze (incremental) | 6 min |
# MAGIC | 3 | DQ split → Silver staging + Quarantine | 6 min |
# MAGIC | 4 | Initial Silver (SCD Type 2) build | 8 min |
# MAGIC | 5 | Simulate corrections → SCD Type 2 MERGE | 10 min |
# MAGIC | 6 | Liquid Clustering στο Silver | 5 min |
# MAGIC | 7 | Gold KPIs (DOY × month) | 5 min |
# MAGIC | 8 | Materialized-view pattern για dashboard | 4 min |
# MAGIC | 9 | Audit: HISTORY + Lineage walkthrough | 5 min |
# MAGIC | 10 | Production hardening (OPTIMIZE/VACUUM cadence) | 5 min |
# MAGIC | 11 | Job orchestration mapping (Databricks Workflows) | 5 min |
# MAGIC | 12 | Cleanup (optional) | 2 min |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0 — Setup + download seed CSV
# MAGIC
# MAGIC Φτιάχνουμε catalog/schema/volume και κατεβάζουμε τη βάση `declarations.csv` από GitHub. Είναι το ίδιο dataset (300 rows ΑΑΔΕ) που χρησιμοποιήσαμε στην Άσκηση 3 — εδώ θα το «κόψουμε» σε ημερήσιες παρτίδες για να προσομοιώσουμε landing zone.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

import urllib.request, os
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3"
VOL = "/Volumes/workspace/aade/aade_data"

# Working folders inside the volume
LANDING = f"{VOL}/landing"               # daily file drops
SCHEMA_LOC = f"{VOL}/_autoloader/schema" # Auto Loader schema location
CHECKPOINT = f"{VOL}/_autoloader/checkpoint"

for p in [LANDING, SCHEMA_LOC, CHECKPOINT]:
    os.makedirs(p, exist_ok=True)

# Seed CSV (single source we'll split into 3 daily batches)
seed_target = f"{VOL}/declarations.csv"
if not os.path.exists(seed_target):
    urllib.request.urlretrieve(f"{REPO}/declarations.csv", seed_target)
    print(f"✅ {seed_target}")
else:
    print(f"⏭️  {seed_target}")

print(f"\nLanding zone:    {LANDING}")
print(f"Schema location: {SCHEMA_LOC}")
print(f"Checkpoint:      {CHECKPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Simulate landing-zone daily drops
# MAGIC
# MAGIC Σπάμε το `declarations.csv` σε **3 ημερήσιες παρτίδες** (Day 1, Day 2, Day 3). Στην πραγματικότητα αυτά θα έρχονταν από κάποιο SFTP/Blob upload από το mainframe — εμείς τα γράφουμε εδώ:
# MAGIC
# MAGIC - `declarations_2026-05-04.csv` — ~100 πρώτες
# MAGIC - `declarations_2026-05-05.csv` — επόμενες ~100
# MAGIC - `declarations_2026-05-06.csv` — τελευταίες ~100
# MAGIC
# MAGIC > 💡 Στο live class θα τρέξουμε αρχικά **μόνο τα 2 πρώτα batches**, μετά θα ξανατρέξουμε το pipeline με το 3ο για να δείξουμε ότι το Auto Loader παίρνει **μόνο τα νέα files** (incremental).

# COMMAND ----------

import shutil

# Wipe landing for a clean demo (idempotent re-run)
if os.path.exists(LANDING):
    shutil.rmtree(LANDING)
os.makedirs(LANDING, exist_ok=True)

import pandas as pd
df_full = pd.read_csv(seed_target)

# Splits — change to 50/50/50 if your seed has 150 rows
n = len(df_full)
splits = [
    ("2026-05-04", df_full.iloc[: n // 3]),
    ("2026-05-05", df_full.iloc[n // 3 : 2 * n // 3]),
    ("2026-05-06", df_full.iloc[2 * n // 3 :]),
]

# We only drop the first 2 batches now. The 3rd we keep aside on disk
# but write it as a "later arrival" so we can demo incremental in Step 2.5
for batch_date, chunk in splits[:2]:
    fname = f"{LANDING}/declarations_{batch_date}.csv"
    chunk.to_csv(fname, index=False)
    print(f"🟢 dropped {fname}  ({len(chunk)} rows)")

# Stash batch 3 outside landing for now — we'll move it later
STAGED_BATCH3 = f"{VOL}/_staging_batch3.csv"
splits[2][1].to_csv(STAGED_BATCH3, index=False)
print(f"\n📦 batch 3 staged (NOT yet in landing) → {STAGED_BATCH3}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Auto Loader → Bronze (incremental ingest)
# MAGIC
# MAGIC Αυτή είναι η **καρδιά** του Bronze layer. Το Auto Loader (`cloudFiles`):
# MAGIC
# MAGIC - **ανιχνεύει νέα files** στο landing path (file-notification ή directory-listing mode)
# MAGIC - **κρατάει state** στο checkpoint — ξέρει ποια files έχει ήδη επεξεργαστεί
# MAGIC - **schema inference + evolution** — αν αύριο προστεθεί στήλη, την παίρνει
# MAGIC - **`_rescued_data`** — ότι δεν ταίριαξε με το schema πάει σε ένα JSON column για forensics (αντί να σκάει το pipeline)
# MAGIC
# MAGIC ### Trigger: `availableNow=True`
# MAGIC
# MAGIC Δεν θέλουμε continuous streaming για ημερήσιο job. Το `availableNow=True` λέει: «**δες όλα τα νέα files, γράψ' τα, σταμάτα**». Είναι το ιδανικό trigger για **batch-on-streaming-engine** pattern — ίδιο API με streaming, αλλά τρέχει σαν batch job.

# COMMAND ----------

BRONZE = "workspace.aade.declarations_bronze"

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .load(LANDING)
)

# Add lineage columns at ingest — απαραίτητα για audit
from pyspark.sql.functions import current_timestamp, input_file_name, to_date

bronze_stream = (
    bronze_stream
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_src_file", input_file_name())
    .withColumn("_ingestion_date", to_date(current_timestamp()))
)

(
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(BRONZE)
).awaitTermination()

print(f"✅ Bronze ingest finished")
spark.sql(f"SELECT COUNT(*) AS bronze_rows FROM {BRONZE}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Σημαντικό**: το `_rescued_data` πρέπει να είναι **ως επί το πλείστον NULL**. Αν δεις πολλά non-NULL → σημαίνει ότι το schema του CSV άλλαξε χωρίς να το ξέρεις. Production rule: αν το `_rescued_data` ξεπεράσει το 1% του batch, στείλε alert.

# COMMAND ----------

spark.sql(f"""
SELECT
  COUNT(*)                                                         AS total,
  SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END)       AS rescued_rows,
  COUNT(DISTINCT _src_file)                                        AS files_seen
FROM {BRONZE}
""").show()

display(spark.sql(f"SELECT * FROM {BRONZE} LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — DQ split → Silver staging + Quarantine
# MAGIC
# MAGIC Πριν προχωρήσουμε σε SCD Type 2, **καθαρίζουμε** το bronze. Οι κανόνες είναι ίδιοι με Άσκηση 3:
# MAGIC
# MAGIC | Κανόνας | Συνέπεια |
# MAGIC |---------|----------|
# MAGIC | `afm` IS NOT NULL AND length 9 | Drop → quarantine |
# MAGIC | `tax_amount` >= 0 | Drop → quarantine |
# MAGIC | `submission_date` parseable | Drop → quarantine |
# MAGIC | `status` ∈ {Υποβληθέν, Ακυρωμένο, Εκκρεμές} | Map → "UNKNOWN" + flag |
# MAGIC
# MAGIC Όλοι οι rejected πάνε στο `declarations_quarantine` με `_quarantine_reason`. Σε production: alerts όταν quarantine_pct > X%.

# COMMAND ----------

from pyspark.sql.functions import col, expr, when, lit, length, trim

QUARANTINE = "workspace.aade.declarations_quarantine"
SILVER_STAGING = "workspace.aade.declarations_silver_staging"

bronze_df = spark.table(BRONZE)

# Build a single _quarantine_reason column (first failing rule wins)
checked = bronze_df.withColumn(
    "_quarantine_reason",
    when(col("afm").isNull() | (length(trim(col("afm"))) != 9), lit("BAD_AFM"))
    .when(col("tax_amount").cast("double") < 0, lit("NEGATIVE_AMOUNT"))
    .when(expr("try_to_date(submission_date, 'yyyy-MM-dd')").isNull(), lit("BAD_DATE"))
    .otherwise(lit(None))
)

# Bad rows → quarantine (overwrite for idempotent demo)
bad = checked.filter(col("_quarantine_reason").isNotNull())
bad.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(QUARANTINE)
print(f"🚫 quarantined: {bad.count()} rows → {QUARANTINE}")

# Good rows → silver staging (with normalized status)
good = (
    checked.filter(col("_quarantine_reason").isNull())
    .drop("_quarantine_reason")
    .withColumn(
        "status_norm",
        when(col("status").isin("Υποβληθέν", "Ακυρωμένο", "Εκκρεμές"), col("status"))
        .otherwise(lit("UNKNOWN")),
    )
)
good.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(SILVER_STAGING)
print(f"✅ staging: {good.count()} rows → {SILVER_STAGING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — Initial Silver (SCD Type 2) build
# MAGIC
# MAGIC ### Τι είναι το SCD Type 2;
# MAGIC
# MAGIC Η **τυπική** silver table θα κρατούσε ένα row ανά δήλωση και θα έκανε UPDATE όταν αλλάζει κάτι. Πρόβλημα: **χάνεις την ιστορία**. Δεν μπορείς να απαντήσεις «τι εμφανιζόταν στο dashboard στις 4/5/2026 για τον ΑΦΜ X;».
# MAGIC
# MAGIC Το **Slowly Changing Dimension Type 2** λύνει αυτό:
# MAGIC
# MAGIC - Κάθε row έχει `effective_from` / `effective_to` / `is_current`
# MAGIC - Όταν αλλάζει κάτι: **κλείνεις** το παλιό row (`is_current=false`, `effective_to=today`) και **εισάγεις** καινούριο row με `effective_from=today, is_current=true`
# MAGIC - Έτσι έχεις **πλήρες ιστορικό** ανά business key (`declaration_id`)
# MAGIC
# MAGIC ### Initial load
# MAGIC
# MAGIC Στο πρώτο τρέξιμο όλα είναι «νέα»: `is_current=true`, `effective_from=today`, `effective_to=NULL`.

# COMMAND ----------

SILVER = "workspace.aade.declarations_silver_scd2"

# Drop and recreate for an idempotent demo run
spark.sql(f"DROP TABLE IF EXISTS {SILVER}")

spark.sql(f"""
CREATE TABLE {SILVER} (
  declaration_id  STRING NOT NULL,
  afm             STRING NOT NULL,
  doy_code        STRING,
  tax_amount      DOUBLE,
  status_norm     STRING,
  submission_date DATE,
  effective_from  DATE NOT NULL,
  effective_to    DATE,
  is_current      BOOLEAN NOT NULL,
  _hash           STRING
) USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
)
""")

from pyspark.sql.functions import sha2, concat_ws, current_date

initial = (
    spark.table(SILVER_STAGING)
    .selectExpr(
        "declaration_id",
        "afm",
        "doy_code",
        "CAST(tax_amount AS DOUBLE) AS tax_amount",
        "status_norm",
        "try_to_date(submission_date, 'yyyy-MM-dd') AS submission_date",
    )
    # _hash = fingerprint of attributes that we track for changes
    .withColumn(
        "_hash",
        sha2(concat_ws("||", col("afm"), col("doy_code"),
                       col("tax_amount").cast("string"), col("status_norm"),
                       col("submission_date").cast("string")), 256),
    )
    .withColumn("effective_from", current_date())
    .withColumn("effective_to", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

initial.write.format("delta").mode("append").saveAsTable(SILVER)

print(f"✅ Initial silver SCD2 built")
spark.sql(f"""
SELECT is_current, COUNT(*) AS n FROM {SILVER} GROUP BY is_current
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — Simulate corrections → SCD Type 2 MERGE
# MAGIC
# MAGIC Το mainframe ξυπνάει την επόμενη μέρα και στέλνει το **batch 3**. Μέσα έχει:
# MAGIC - **νέες** δηλώσεις (insert)
# MAGIC - **διορθώσεις** σε υπάρχουσες δηλώσεις (νέο `tax_amount` ή νέο `status`) — αυτές πρέπει να γίνουν **νέες versions** στο silver, χωρίς να χαθεί η παλιά
# MAGIC
# MAGIC Θα κάνουμε:
# MAGIC 1. Drop batch 3 στο landing (Auto Loader θα το πάρει incremental)
# MAGIC 2. Re-run Bronze ingest (μόνο το νέο file θα διαβαστεί)
# MAGIC 3. Re-run DQ split (full overwrite του staging — απλούστερο για demo)
# MAGIC 4. **Inject 2 corrections** χειροκίνητα στο staging για να δείξουμε SCD2 update
# MAGIC 5. Τρέξε **SCD Type 2 MERGE**

# COMMAND ----------

# 1) move batch 3 into landing
shutil.copy(STAGED_BATCH3, f"{LANDING}/declarations_2026-05-06.csv")
print(f"🟢 dropped declarations_2026-05-06.csv into landing")

# 2) re-run Auto Loader (incremental — picks ONLY the new file)
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .load(LANDING)
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_src_file", input_file_name())
    .withColumn("_ingestion_date", to_date(current_timestamp()))
    .writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(BRONZE)
).awaitTermination()

spark.sql(f"SELECT COUNT(*) AS bronze_rows, COUNT(DISTINCT _src_file) AS files FROM {BRONZE}").show()

# COMMAND ----------

# 3) re-run DQ split (overwrite staging from full bronze)
bronze_df = spark.table(BRONZE)
checked = bronze_df.withColumn(
    "_quarantine_reason",
    when(col("afm").isNull() | (length(trim(col("afm"))) != 9), lit("BAD_AFM"))
    .when(col("tax_amount").cast("double") < 0, lit("NEGATIVE_AMOUNT"))
    .when(expr("try_to_date(submission_date, 'yyyy-MM-dd')").isNull(), lit("BAD_DATE"))
    .otherwise(lit(None))
)
good = (
    checked.filter(col("_quarantine_reason").isNull())
    .drop("_quarantine_reason")
    .withColumn(
        "status_norm",
        when(col("status").isin("Υποβληθέν", "Ακυρωμένο", "Εκκρεμές"), col("status"))
        .otherwise(lit("UNKNOWN")),
    )
)
good.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(SILVER_STAGING)
print(f"✅ staging refreshed: {good.count()} rows")

# COMMAND ----------

# 4) Inject 2 fake corrections so the SCD2 MERGE has something visible to do.
#    Pick 2 declaration_ids that exist in current silver and bump tax_amount.
sample_ids = [
    r["declaration_id"]
    for r in spark.sql(
        f"SELECT declaration_id FROM {SILVER} WHERE is_current = true LIMIT 2"
    ).collect()
]

if sample_ids:
    spark.sql(f"""
        UPDATE {SILVER_STAGING}
        SET tax_amount = tax_amount * 1.10,
            status_norm = 'Ακυρωμένο'
        WHERE declaration_id IN ({",".join([f"'{i}'" for i in sample_ids])})
    """)
    print(f"💉 injected corrections for: {sample_ids}")
else:
    print("⚠️  no current silver rows to correct — skipping injection")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5) SCD Type 2 MERGE
# MAGIC
# MAGIC Δύο-βήματη προσέγγιση (πιο readable από το single-MERGE-with-UNION trick):
# MAGIC
# MAGIC **Βήμα Α**: για κάθε `declaration_id` του staging, αν το hash διαφέρει από το current silver row → **κλείσε** το current row.
# MAGIC
# MAGIC **Βήμα Β**: **εισήγαγε** το νέο row με `is_current=true`, `effective_from=today`. Αν είναι ολόκαινούρια δήλωση (ποτέ δεν εμφανίστηκε), απλά μπαίνει.

# COMMAND ----------

# Build the staging-with-hash CTE-like view
staging_hashed = (
    spark.table(SILVER_STAGING)
    .selectExpr(
        "declaration_id",
        "afm",
        "doy_code",
        "CAST(tax_amount AS DOUBLE) AS tax_amount",
        "status_norm",
        "try_to_date(submission_date, 'yyyy-MM-dd') AS submission_date",
    )
    .withColumn(
        "_hash",
        sha2(concat_ws("||", col("afm"), col("doy_code"),
                       col("tax_amount").cast("string"), col("status_norm"),
                       col("submission_date").cast("string")), 256),
    )
)
staging_hashed.createOrReplaceTempView("staging_hashed")

# --- Βήμα Α: close current rows where hash changed ---
spark.sql(f"""
MERGE INTO {SILVER} t
USING staging_hashed s
ON  t.declaration_id = s.declaration_id
AND t.is_current = true
AND t._hash <> s._hash
WHEN MATCHED THEN UPDATE SET
  t.is_current   = false,
  t.effective_to = current_date()
""")

# --- Βήμα Β: insert new versions (changed) + brand new rows ---
spark.sql(f"""
INSERT INTO {SILVER}
SELECT
  s.declaration_id, s.afm, s.doy_code, s.tax_amount, s.status_norm, s.submission_date,
  current_date() AS effective_from,
  CAST(NULL AS DATE) AS effective_to,
  true AS is_current,
  s._hash
FROM staging_hashed s
LEFT JOIN (SELECT declaration_id, _hash FROM {SILVER} WHERE is_current = true) c
  ON s.declaration_id = c.declaration_id
WHERE c.declaration_id IS NULL          -- brand-new declarations
   OR c._hash <> s._hash                -- corrected declarations (new version)
""")

print("✅ SCD Type 2 MERGE complete")
spark.sql(f"""
SELECT is_current, COUNT(*) AS n FROM {SILVER} GROUP BY is_current
""").show()

# Inspect SCD2 history for one of the corrected declarations
if sample_ids:
    spark.sql(f"""
        SELECT declaration_id, tax_amount, status_norm,
               effective_from, effective_to, is_current
        FROM {SILVER}
        WHERE declaration_id = '{sample_ids[0]}'
        ORDER BY effective_from
    """).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 — Liquid Clustering στο Silver
# MAGIC
# MAGIC ### Z-ORDER vs Liquid Clustering
# MAGIC
# MAGIC Στην Άσκηση 5 είδαμε `OPTIMIZE ... ZORDER BY (col)`. Μειονέκτημα: είναι **statically defined** — αν αλλάξει το query pattern, πρέπει να ξανακάνεις OPTIMIZE με νέο ZORDER, και τα παλιά files μένουν με την παλιά διάταξη.
# MAGIC
# MAGIC Το **Liquid Clustering** (DBR 13.3+):
# MAGIC - Δηλώνεται με `CLUSTER BY` στο TBL ή `ALTER TABLE ... CLUSTER BY`
# MAGIC - **Incrementally** clustering — δεν χρειάζεται full rewrite
# MAGIC - Μπορείς να **αλλάξεις keys** χωρίς να ξανακάνεις OPTIMIZE από την αρχή
# MAGIC - Λειτουργεί καλύτερα για **streaming writes** (που είναι ακριβώς το pattern μας με Auto Loader)
# MAGIC
# MAGIC > 💡 Vendor recommendation: **σε νέους πίνακες χρησιμοποίησε Liquid Clustering. Z-ORDER κράτα το για legacy tables.**

# COMMAND ----------

# Add liquid clustering keys based on actual query patterns:
#  - WHERE afm = ?               (lookup)
#  - WHERE status_norm = ?       (filter για dashboards)
spark.sql(f"ALTER TABLE {SILVER} CLUSTER BY (afm, status_norm)")
spark.sql(f"OPTIMIZE {SILVER}")

# Verify clustering metadata is now in DESCRIBE DETAIL
display(spark.sql(f"DESCRIBE DETAIL {SILVER}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7 — Gold KPIs (DOY × month)
# MAGIC
# MAGIC Ώρα να φτιάξουμε το Gold table που θα τροφοδοτήσει το dashboard του Υπουργού. Σημαντικό: **το Gold διαβάζει ΜΟΝΟ τα current rows του silver** (`is_current=true`) — αυτή είναι η «ζωντανή φωτογραφία» των δηλώσεων.

# COMMAND ----------

GOLD = "workspace.aade.kpi_doy_monthly_gold"

spark.sql(f"DROP TABLE IF EXISTS {GOLD}")
spark.sql(f"""
CREATE TABLE {GOLD} USING DELTA AS
SELECT
  doy_code,
  date_trunc('MONTH', submission_date)        AS month,
  COUNT(*)                                    AS declarations,
  SUM(tax_amount)                             AS total_tax,
  AVG(tax_amount)                             AS avg_tax,
  SUM(CASE WHEN status_norm = 'Ακυρωμένο' THEN 1 ELSE 0 END) AS cancelled,
  SUM(CASE WHEN status_norm = 'UNKNOWN'   THEN 1 ELSE 0 END) AS unknown_status
FROM {SILVER}
WHERE is_current = true
  AND submission_date IS NOT NULL
GROUP BY doy_code, date_trunc('MONTH', submission_date)
ORDER BY month DESC, doy_code
""")

display(spark.sql(f"SELECT * FROM {GOLD} LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8 — Materialized-view pattern για dashboard
# MAGIC
# MAGIC Σε **DBSQL** μπορούμε να δηλώσουμε `CREATE MATERIALIZED VIEW` που το Databricks μόνο του ξέρει πώς να κάνει incremental refresh. Σε **Free Edition all-purpose compute** το συγκεκριμένο SQL δεν τρέχει — οπότε χρησιμοποιούμε το **«manual MV pattern»**:
# MAGIC
# MAGIC ```python
# MAGIC spark.sql("CREATE OR REPLACE TABLE dashboard_doy_summary AS SELECT ...")
# MAGIC ```
# MAGIC
# MAGIC ή πιο σωστά, **MERGE-based incremental refresh** που τρέχει σε ένα Job βήμα ΜΕΤΑ το silver. Στο Workflow αυτό μπαίνει ως ξεχωριστό task με dependency.

# COMMAND ----------

DASH = "workspace.aade.dashboard_doy_summary"

spark.sql(f"""
CREATE OR REPLACE TABLE {DASH} USING DELTA AS
SELECT
  doy_code,
  COUNT(*)         AS active_declarations,
  SUM(tax_amount)  AS total_tax_collected,
  MAX(submission_date) AS latest_submission
FROM {SILVER}
WHERE is_current = true
GROUP BY doy_code
ORDER BY total_tax_collected DESC
""")

display(spark.sql(f"SELECT * FROM {DASH}"))

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 Σε **DBSQL Pro** το ίδιο γίνεται:
# MAGIC > ```sql
# MAGIC > CREATE MATERIALIZED VIEW dashboard_doy_summary
# MAGIC > AS SELECT ... FROM declarations_silver_scd2 WHERE is_current=true ...;
# MAGIC > REFRESH MATERIALIZED VIEW dashboard_doy_summary;
# MAGIC > ```
# MAGIC > Το Databricks χειρίζεται το incremental refresh αυτόματα — χρησιμοποιεί το CDF του silver. Ίδια λογική με αυτό που είδαμε στην Άσκηση 6.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9 — Audit: HISTORY + Lineage
# MAGIC
# MAGIC Δείχνουμε στους εκπαιδευόμενους τρία πράγματα που κάνει «πληρωμένη» δουλειά αυτό το pipeline:
# MAGIC
# MAGIC 1. **`DESCRIBE HISTORY`** σε κάθε layer → ποιος, πότε, πόσα rows. Auditable by design.
# MAGIC 2. **Unity Catalog Lineage** στο Catalog Explorer → δες ποιοι πίνακες τροφοδοτούν τον dashboard table.
# MAGIC 3. **Per-row provenance** μέσω `_src_file` στο bronze → «ποιο CSV file έβαλε αυτό το row;»

# COMMAND ----------

print(">>> Bronze history (last 5)")
display(spark.sql(f"DESCRIBE HISTORY {BRONZE} LIMIT 5"))

print("\n>>> Silver SCD2 history (last 10)")
display(spark.sql(f"DESCRIBE HISTORY {SILVER} LIMIT 10"))

print("\n>>> Gold history")
display(spark.sql(f"DESCRIBE HISTORY {GOLD} LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Manual lineage check** (μέχρι να ανοίξεις την UC Lineage tab):
# MAGIC
# MAGIC - landing files  →  `declarations_bronze`  (Auto Loader)
# MAGIC - `declarations_bronze`  →  `declarations_silver_staging` + `declarations_quarantine` (DQ split)
# MAGIC - `declarations_silver_staging`  →  `declarations_silver_scd2` (SCD2 MERGE)
# MAGIC - `declarations_silver_scd2`  →  `kpi_doy_monthly_gold` + `dashboard_doy_summary`
# MAGIC
# MAGIC Στο **Catalog Explorer → workspace.aade.kpi_doy_monthly_gold → Lineage tab** θα δεις ολόκληρο το γράφο πάνω-κάτω, **αυτόματα** χωρίς να γράψεις τίποτα.

# COMMAND ----------

# Per-row provenance: ποιο file έφερε ποιο declaration_id
display(spark.sql(f"""
SELECT declaration_id, _src_file, _ingestion_ts
FROM {BRONZE}
ORDER BY _ingestion_ts DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10 — Production hardening: OPTIMIZE / VACUUM cadence
# MAGIC
# MAGIC Το pipeline τρέχει κάθε νύχτα. Μετά από εβδομάδες ο πίνακας έχει **μικρά Parquet files** (ένα ανά batch) και **παλιά file versions** (από SCD2 closures). Production cadence:
# MAGIC
# MAGIC | Operation | Cadence | Σκοπός |
# MAGIC |-----------|---------|--------|
# MAGIC | `OPTIMIZE` (auto via TBLPROPERTIES `autoOptimize`) | κάθε write | compaction |
# MAGIC | `OPTIMIZE` ad-hoc | weekly | επιβεβαίωση + Liquid re-layout |
# MAGIC | `VACUUM` με `RETAIN 168 HOURS` (7 days) | weekly | καθαρισμός orphan files |
# MAGIC | `ANALYZE TABLE ... COMPUTE STATISTICS` | μετά από big batches | καλύτερα query plans |

# COMMAND ----------

# Ad-hoc weekly maintenance demo (συνήθως σε ξεχωριστό Job task)
spark.sql(f"OPTIMIZE {SILVER}")
spark.sql(f"OPTIMIZE {GOLD}")
spark.sql(f"OPTIMIZE {DASH}")

# Δεν τρέχουμε VACUUM εδώ γιατί το default 7-day retention δεν θα διαγράψει τίποτα
# σε ένα fresh demo. Σε production:
#   spark.sql(f"VACUUM {SILVER} RETAIN 168 HOURS")

# Stats για κάθε layer
for t in [BRONZE, SILVER, GOLD, DASH]:
    spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS FOR ALL COLUMNS")
    print(f"📊 stats updated for {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 11 — Job orchestration mapping (Databricks Workflows)
# MAGIC
# MAGIC Όλα όσα κάναμε σε ένα notebook → **σε production** σπάνε σε ξεχωριστά **Workflow tasks** με dependencies:
# MAGIC
# MAGIC ```yaml
# MAGIC # databricks.yml (asset bundle)
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     daily_declarations_pipeline:
# MAGIC       schedule:
# MAGIC         quartz_cron_expression: "0 0 4 * * ?"   # daily 04:00 UTC
# MAGIC         timezone_id: "Europe/Athens"
# MAGIC       tasks:
# MAGIC         - task_key: ingest_bronze
# MAGIC           notebook_task: { notebook_path: "/.../01_bronze_autoloader" }
# MAGIC
# MAGIC         - task_key: dq_split
# MAGIC           depends_on: [{ task_key: ingest_bronze }]
# MAGIC           notebook_task: { notebook_path: "/.../02_dq_split" }
# MAGIC
# MAGIC         - task_key: silver_scd2
# MAGIC           depends_on: [{ task_key: dq_split }]
# MAGIC           notebook_task: { notebook_path: "/.../03_silver_scd2" }
# MAGIC
# MAGIC         - task_key: gold_aggregations
# MAGIC           depends_on: [{ task_key: silver_scd2 }]
# MAGIC           notebook_task: { notebook_path: "/.../04_gold_kpis" }
# MAGIC
# MAGIC         - task_key: refresh_dashboard
# MAGIC           depends_on: [{ task_key: gold_aggregations }]
# MAGIC           notebook_task: { notebook_path: "/.../05_dashboard_mv" }
# MAGIC
# MAGIC         - task_key: maintenance
# MAGIC           depends_on: [{ task_key: refresh_dashboard }]
# MAGIC           run_if: ALL_DONE   # τρέξε ακόμα κι αν αποτύχει το dashboard
# MAGIC           notebook_task: { notebook_path: "/.../06_optimize_vacuum" }
# MAGIC
# MAGIC       email_notifications:
# MAGIC         on_failure: ["data-eng-oncall@aade.gr"]
# MAGIC ```
# MAGIC
# MAGIC ### Γιατί ξεχωριστά tasks και όχι 1 mega-notebook;
# MAGIC
# MAGIC | Reason | Πώς βοηθάει |
# MAGIC |--------|--------------|
# MAGIC | **Retries** ανά task | αν αποτύχει το `gold_aggregations`, retry-άρει μόνο αυτό |
# MAGIC | **Conditional execution** | maintenance τρέχει ακόμα κι αν αποτύχουν προηγούμενα (`ALL_DONE`) |
# MAGIC | **Different cluster sizes** | bronze ingest = small, silver MERGE = larger |
# MAGIC | **Observability** | per-task duration metrics, alerts |
# MAGIC | **Parallelism** | ανεξάρτητα tasks τρέχουν ταυτόχρονα |
# MAGIC
# MAGIC ### Trigger types — τι να διαλέξεις
# MAGIC
# MAGIC - **Scheduled** (`cron`) — daily/hourly batches. Το νυχτερινό μας job πέφτει εδώ.
# MAGIC - **File arrival** (`file_arrival`) — τρέξε όταν εμφανιστεί νέο file στο landing path. Αν το mainframe είναι unpredictable, αυτό είναι καλύτερο από cron.
# MAGIC - **Continuous** — for streaming pipelines που γράφουν συνεχώς (όχι το δικό μας).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 12 — Cleanup (optional)
# MAGIC
# MAGIC Αν θες να τρέξεις πάλι από την αρχή με καθαρό state, ξε-σχολίασε και τρέξε:

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS workspace.aade.declarations_bronze")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.declarations_quarantine")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.declarations_silver_staging")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.declarations_silver_scd2")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.kpi_doy_monthly_gold")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.dashboard_doy_summary")
# import shutil
# shutil.rmtree(f"{VOL}/landing", ignore_errors=True)
# shutil.rmtree(f"{VOL}/_autoloader", ignore_errors=True)
# print("🧹 cleaned up")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Τι κερδίσαμε
# MAGIC
# MAGIC Φύγαμε από αυτό το notebook με ένα **production-grade pipeline pattern** που:
# MAGIC
# MAGIC - **Auto Loader** για incremental ingest από landing zone (file detection αυτόματο)
# MAGIC - **Bronze** με full provenance (`_src_file`, `_ingestion_ts`)
# MAGIC - **DQ split** με quarantine για forensics
# MAGIC - **Silver SCD Type 2** που κρατάει πλήρες ιστορικό αλλαγών — auditable for tax court
# MAGIC - **Liquid Clustering** για γρήγορα ad-hoc queries
# MAGIC - **Gold + dashboard MV pattern** για downstream consumers
# MAGIC - **Audit & Lineage** out of the box με Unity Catalog
# MAGIC - **Workflow orchestration** mapping — από notebook σε scheduled production job
# MAGIC
# MAGIC ### 🎯 Production checklist (πάρτε κρατήσεις)
# MAGIC
# MAGIC - [ ] Alert αν `_rescued_data` count > 1% του batch
# MAGIC - [ ] Alert αν quarantine count > 5% του batch
# MAGIC - [ ] Daily OPTIMIZE σε Workflow task
# MAGIC - [ ] Weekly VACUUM με `RETAIN 168 HOURS`
# MAGIC - [ ] Email notifications on failure
# MAGIC - [ ] UC Permissions: `data_engineers` GRANT MODIFY, `analysts` GRANT SELECT στο Gold μόνο
# MAGIC - [ ] Row filter στο silver: `is_current = true` view για analysts (κρύβουμε historical versions)
