# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 LAB 2 — Multi-Source Bronze Ingestion
# MAGIC
# MAGIC **Ημέρα 1 · Groups of 2-3 · ⏱ 110 λεπτά**
# MAGIC
# MAGIC > Στόχος: Δημιουργία 5 Bronze Delta tables (μία ανά πηγή) με production-grade ποιότητα — audit columns, type casting, idempotency, partitioning. **Παρουσίαση** στο τέλος.
# MAGIC
# MAGIC ## 📑 Δομή Notebook
# MAGIC 1. 📚 **Θεωρία** (20') — Bronze layer + best practices
# MAGIC 2. 🎯 **Εκφώνηση** — Τι θα φτιάξεις (groups)
# MAGIC 3. ✍️ **Ο κώδικας σου** — Γράψτε εσείς (75')
# MAGIC 4. 🎤 **Παρουσίαση** — 2 ομάδες παρουσιάζουν (10' η κάθε μία)
# MAGIC 5. ✅ **Verification** + discussion (5')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📚 ΜΕΡΟΣ 1 — ΘΕΩΡΙΑ
# MAGIC
# MAGIC ## 2.1 Τι είναι το Bronze Layer;
# MAGIC
# MAGIC Στη **Medallion Architecture**, το Bronze είναι το **πρώτο σημείο επαφής** των δεδομένων με την πλατφόρμα. Κάθε record που μπαίνει στο σύστημα **πάντα** πρώτα φτάνει στο Bronze.
# MAGIC
# MAGIC **Φιλοσοφία Bronze:**
# MAGIC - **Immutable** (read-only after write)
# MAGIC - **Full history retention** (κρατάμε ΟΛΑ τα versions)
# MAGIC - **Source format copy** (όπως ήρθαν τα δεδομένα)
# MAGIC - **Καμία επιχειρησιακή λογική** (αυτό είναι Silver/Gold δουλειά)
# MAGIC
# MAGIC ## 2.2 Γιατί χωριστά tables ανά πηγή;
# MAGIC
# MAGIC ❌ **Αντι-pattern**: ένα μεγάλο `bronze_all_data` table με πεδίο `source_type`.
# MAGIC ✅ **Σωστό**: ένα table ανά πηγή — `taxis_declarations_raw`, `efka_contributions_raw`, κλπ.
# MAGIC
# MAGIC **Γιατί:** Κάθε πηγή έχει διαφορετικό schema, refresh cadence, owner, retention policy. Πιο εύκολο governance, πιο γρήγορα queries, πιο καθαρό lineage.
# MAGIC
# MAGIC ## 2.3 Audit Columns — Πάντα ΥΠΟΧΡΕΩΤΙΚΑ
# MAGIC
# MAGIC Κάθε Bronze table πρέπει να έχει **minimum 2 audit columns**:
# MAGIC
# MAGIC | Column | Σκοπός | Πώς γεμίζει |
# MAGIC |---|---|---|
# MAGIC | `_ingestion_ts` | Πότε μπήκε στο Bronze | `current_timestamp()` |
# MAGIC | `_source_file` | Από ποιο file/πηγή | `input_file_name()` |
# MAGIC
# MAGIC **Production extra:**
# MAGIC - `_pipeline_run_id` (ποιο job run το γέννησε)
# MAGIC - `_ingestion_user` (ποιος service principal)
# MAGIC - `_source_system_ts` (timestamp από το source system, αν διαθέσιμο)
# MAGIC
# MAGIC **Γιατί:** Όταν 6 μήνες μετά μυρίσει κάποιο record παράξενα, θέλεις να ξέρεις **πότε** και **από πού** ήρθε.
# MAGIC
# MAGIC ## 2.4 Type Casting — Γιατί ΟΧΙ `inferSchema`;
# MAGIC
# MAGIC `inferSchema=true` σε production = πρόβλημα. Παραδείγματα:
# MAGIC
# MAGIC | Source | inferSchema παράγει | Πραγματικό issue |
# MAGIC |---|---|---|
# MAGIC | `afm = "0001234567"` | `integer 1234567` | **Leading zero χάθηκε** — δεν matchάρει πια στο dim_citizen |
# MAGIC | `amount = "1.000,50"` | `string "1.000,50"` | Δεν γίνονται aggregations |
# MAGIC | `date = "2026-05-22"` | `string` | Δεν δουλεύει date arithmetic |
# MAGIC
# MAGIC **Production rule**: Στο Bronze ingestion, **κρατάς ως string** OR **κάνεις explicit cast** με γνωστό schema.
# MAGIC
# MAGIC ## 2.5 Idempotency — Re-run δεν διπλασιάζει
# MAGIC
# MAGIC ❌ **Anti-pattern**:
# MAGIC ```python
# MAGIC df.write.mode("append").saveAsTable("bronze.x")  # Rerun → duplicates!
# MAGIC ```
# MAGIC
# MAGIC ✅ **Σωστό patterns**:
# MAGIC ```python
# MAGIC # Option 1: Overwrite (αν θες full reload)
# MAGIC df.write.mode("overwrite").saveAsTable("bronze.x")
# MAGIC
# MAGIC # Option 2: MERGE (production για incremental)
# MAGIC MERGE INTO bronze.x AS t
# MAGIC USING staging AS s ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC Σήμερα θα χρησιμοποιήσετε **overwrite** για απλότητα. MERGE θα δούμε σε Day 3.
# MAGIC
# MAGIC ## 2.6 Partitioning Strategy
# MAGIC
# MAGIC **Κανόνας:** Partition by **low-cardinality column** που χρησιμοποιείται συχνά σε φίλτρα.
# MAGIC
# MAGIC ✅ **Καλά partition keys**: `year=2026`, `month=05`, `region`, `source_system`
# MAGIC ❌ **Κακά partition keys**: `afm` (millions of distinct values → millions of tiny files), `timestamp` (too granular)
# MAGIC
# MAGIC Για Bronze: συνηθέστερα `partitionBy("_ingestion_date")` — γρήγορα κάνεις time-based queries.
# MAGIC
# MAGIC ## 2.7 Naming Conventions
# MAGIC
# MAGIC Pattern που θα ακολουθήσετε:
# MAGIC ```
# MAGIC bronze.{source}_{entity}_raw
# MAGIC ```
# MAGIC
# MAGIC Παραδείγματα:
# MAGIC - `bronze.taxis_declarations_raw`
# MAGIC - `bronze.efka_contributions_raw`
# MAGIC - `bronze.kep_events_raw`
# MAGIC - `bronze.mydata_invoices_raw`
# MAGIC - `bronze.citizen_registry_raw`
# MAGIC
# MAGIC `_raw` suffix δείχνει ότι είναι Bronze (raw, immutable).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎯 ΜΕΡΟΣ 2 — ΕΚΦΩΝΗΣΗ
# MAGIC
# MAGIC ## Σενάριο
# MAGIC
# MAGIC Ο φορέας σας λαμβάνει καθημερινά CSV files από 5 διαφορετικά συστήματα:
# MAGIC
# MAGIC | # | Πηγή | File | Description |
# MAGIC |---|---|---|---|
# MAGIC | 1 | TAXIS | `taxis_declarations.csv` | Φορολογικές δηλώσεις |
# MAGIC | 2 | EFKA  | `efka_contributions.csv` | Ασφαλιστικές εισφορές |
# MAGIC | 3 | KEP   | `kep_events.csv` | Events ΚΕΠ services |
# MAGIC | 4 | myDATA | `mydata_invoices.csv` | Τιμολόγια myDATA |
# MAGIC | 5 | Registry | `citizen_registry.csv` | Μητρώο Πολιτών |
# MAGIC
# MAGIC ## Στόχος (ως ομάδα)
# MAGIC
# MAGIC Δημιουργήστε **5 Bronze Delta tables** που να πληρούν **όλες** τις παρακάτω απαιτήσεις:
# MAGIC
# MAGIC ### 📋 Απαιτήσεις
# MAGIC
# MAGIC 1. **Naming**: Ακολουθήστε pattern `bronze.{source}_{entity}_raw`
# MAGIC 2. **Audit columns**: Κάθε table να έχει `_ingestion_ts` + `_source_file`
# MAGIC 3. **Type casting**: ΟΧΙ inferSchema. Καθόρισε εσύ τα types (ή κράτα string)
# MAGIC 4. **Partitioning**: By `_ingestion_date` (extracted από `_ingestion_ts`)
# MAGIC 5. **Idempotency**: Rerun δεν πρέπει να διπλασιάζει records
# MAGIC 6. **DRY**: Χρησιμοποιήστε **ένα reusable function** για όλες τις 5 πηγές (όχι copy-paste 5 φορές)
# MAGIC 7. **Verification**: Στο τέλος, count rows σε κάθε table + ένα `SHOW TABLES`
# MAGIC
# MAGIC ## 🎤 Παρουσίαση (10' ανά ομάδα)
# MAGIC
# MAGIC Στις 14:35 (75' μετά την έναρξη του lab), **2 ομάδες** παρουσιάζουν τη λύση τους. Πρέπει να εξηγήσετε:
# MAGIC
# MAGIC | # | Question | Why we ask |
# MAGIC |---|---|---|
# MAGIC | 1 | **Naming convention** που επιλέξατε — γιατί έτσι; | Δεν υπάρχει ένα σωστό, αξίζει η αιτιολόγηση |
# MAGIC | 2 | **Type casting strategy** — γιατί ένα type vs άλλο; | Trade-offs casting vs string |
# MAGIC | 3 | **Audit columns** που προσθέσατε επιπλέον; | Production thinking |
# MAGIC | 4 | **Idempotency test** — πώς το επιβεβαιώσατε; | Show, don't tell |
# MAGIC | 5 | **Όποια challenge** σας έκανε να σκεφτείτε; | Learning highlights |
# MAGIC
# MAGIC ## ⚠️ Επιτυχία = όλα τα παρακάτω πράσινα ✅
# MAGIC
# MAGIC - [ ] 5 tables στο `gt_lab.bronze.*_raw`
# MAGIC - [ ] Κάθε table έχει `_ingestion_ts` + `_source_file`
# MAGIC - [ ] Counts: citizen=9, taxis=10, efka=7, kep=10, mydata=10
# MAGIC - [ ] Idempotency: 2ο run δίνει ίδια counts
# MAGIC - [ ] Code DRY (μία function που χρησιμοποιείται 5 φορές)
# MAGIC
# MAGIC ## 🚫 ΔΕΝ σου δίνω
# MAGIC
# MAGIC - Skeleton κώδικα. Πρέπει η ομάδα να σχεδιάσει.
# MAGIC - Schema definitions. Εσείς αποφασίζετε types.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✍️ ΜΕΡΟΣ 3 — Ο ΚΩΔΙΚΑΣ ΣΑΣ
# MAGIC
# MAGIC ## Step 0 — Auto-bootstrap (έτοιμο, μην το αλλάξεις)

# COMMAND ----------

# DBTITLE 1,Auto-bootstrap (don't edit)
CATALOG       = "gt_lab"
LANDING_PATH  = f"/Volumes/{CATALOG}/bronze/landing"
GITHUB_BASE   = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day6/data_for_students"
REQUIRED_CSVS = ["citizen_registry.csv", "taxis_declarations.csv",
                 "efka_contributions.csv", "kep_events.csv", "mydata_invoices.csv"]

def _bootstrap():
    """Serverless-safe: writes directly to /Volumes path (no /tmp/ access needed)."""
    import urllib.request
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    for s in ["bronze", "silver", "gold"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")
    try:
        existing = {f.name for f in dbutils.fs.ls(LANDING_PATH)}
    except: existing = set()
    missing = set(REQUIRED_CSVS) - existing
    if not missing:
        print(f"✅ All {len(REQUIRED_CSVS)} CSVs already in volume")
        return
    print(f"⬇️  Downloading {len(missing)} CSVs από GitHub...")
    for f in missing:
        url = f"{GITHUB_BASE}/{f}"
        target = f"{LANDING_PATH}/{f}"
        # Direct stream from URL → volume (no /tmp/, serverless-safe)
        with urllib.request.urlopen(url, timeout=30) as response:
            content = response.read()
        with open(target, "wb") as fp:
            fp.write(content)
        print(f"   ✅ {f}")

_bootstrap()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Σχεδιασμός σε χαρτί (10' ομαδική συζήτηση)
# MAGIC
# MAGIC **ΠΡΙΝ** ξεκινήσετε να γράφετε κώδικα, συζητήστε ως ομάδα:
# MAGIC
# MAGIC 1. **Naming pattern** — βάλτε σε χαρτί τα 5 τελικά table names
# MAGIC 2. **Types** — για κάθε CSV column, ποιο type θα κρατήσετε;
# MAGIC 3. **Audit columns** — μόνο τα 2 minimum ή θα προσθέσετε extras;
# MAGIC 4. **Function signature** — `def load_to_bronze(source_name, entity_name, csv_file): ...`
# MAGIC 5. **Partitioning** — `_ingestion_date` ή κάτι άλλο;
# MAGIC
# MAGIC Όταν συμφωνήσετε, ξεκινήστε κώδικα.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Define your function
# MAGIC
# MAGIC Φτιάξτε **μία** reusable function που:
# MAGIC - Παίρνει: source_name, entity_name, csv_filename
# MAGIC - Διαβάζει το CSV από `/Volumes/gt_lab/bronze/landing/`
# MAGIC - Προσθέτει audit columns (`_ingestion_ts`, `_source_file`, optionally `_ingestion_date`)
# MAGIC - Γράφει σε `gt_lab.bronze.{source}_{entity}_raw` με partition + overwrite

# COMMAND ----------

# DBTITLE 1,STEP 2 — Reusable loader function
# 👇 ΓΡΑΨΤΕ ΤΗ FUNCTION ΣΑΣ ΕΔΩ
# Hint imports που θα χρειαστείτε:
# from pyspark.sql.functions import current_timestamp, input_file_name, to_date

def load_to_bronze(source: str, entity: str, csv_filename: str):
    """Load a CSV from landing volume into Bronze Delta table.

    Args:
        source: source system name (e.g. 'taxis')
        entity: entity name (e.g. 'declarations')
        csv_filename: filename in landing volume (e.g. 'taxis_declarations.csv')
    """
    # 👇 Implement σας εδώ
    pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Καλέστε τη function για όλες τις 5 πηγές
# MAGIC
# MAGIC **Tip:** Use a list of tuples + loop. Όχι copy-paste 5 φορές.

# COMMAND ----------

# DBTITLE 1,STEP 3 — Load all 5 sources
# 👇 ΓΡΑΨΤΕ ΤΟΝ LOOP ΕΔΩ





# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Επιβεβαίωση: SHOW TABLES + counts
# MAGIC
# MAGIC Verify ότι όλα τα 5 tables υπάρχουν και έχουν τα expected counts.

# COMMAND ----------

# DBTITLE 1,STEP 4 — Verify tables + counts
# 👇 ΓΡΑΨΤΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ ΕΔΩ
# - SHOW TABLES IN gt_lab.bronze
# - Counts ανά table





# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Test idempotency
# MAGIC
# MAGIC **Κρίσιμο test:** Τρέξτε **ξανά** τη loader function για ένα table. Counts πρέπει να μείνουν ΙΔΙΑ (όχι να διπλασιαστούν).
# MAGIC
# MAGIC **Hint:** `mode("overwrite")` είναι idempotent. `mode("append")` ΔΕΝ είναι.

# COMMAND ----------

# DBTITLE 1,STEP 5 — Idempotency test
# 👇 ΓΡΑΨΤΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ ΕΔΩ
# 1. Πάρε το count ενός table (π.χ. taxis_declarations_raw)
# 2. Τρέξε ξανά τη function για το ίδιο source
# 3. Πάρε το count ξανά
# 4. assert count_before == count_after





# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Inspect ένα table
# MAGIC
# MAGIC Διάλεξε ΕΝΑ από τα 5 Bronze tables και:
# MAGIC - `DESCRIBE EXTENDED` για να δεις properties + location
# MAGIC - `display()` τα πρώτα 10 records
# MAGIC - Επιβεβαίωση: βλέπεις τα audit columns (`_ingestion_ts`, `_source_file`);

# COMMAND ----------

# DBTITLE 1,STEP 6 — Deep inspect
# 👇 ΓΡΑΨΤΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ ΕΔΩ





# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎤 ΜΕΡΟΣ 4 — ΠΑΡΟΥΣΙΑΣΗ
# MAGIC
# MAGIC Στις 14:35, **2 ομάδες** θα παρουσιάσουν live μέσα από το δικό τους notebook.
# MAGIC
# MAGIC **Δομή παρουσίασης (10' ανά ομάδα):**
# MAGIC 1. **2'** — Approach: ποιο naming, ποια types, γιατί;
# MAGIC 2. **5'** — Live walkthrough κώδικα (scroll από function definition → loop → verification)
# MAGIC 3. **2'** — Challenge that surprised you
# MAGIC 4. **1'** — Q&A από άλλες ομάδες

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✅ ΜΕΡΟΣ 5 — AUTO VERIFICATION
# MAGIC
# MAGIC Τρέξε αυτό το cell για auto-check (μην το αλλάξεις).

# COMMAND ----------

# DBTITLE 1,Auto-verification
EXPECTED = {
    "bronze.citizen_registry_raw":    9,
    "bronze.taxis_declarations_raw":  10,
    "bronze.efka_contributions_raw":  7,
    "bronze.kep_events_raw":          10,
    "bronze.mydata_invoices_raw":     10,
}

checks = []
for table, expected_count in EXPECTED.items():
    full_name = f"gt_lab.{table}"
    try:
        actual = spark.table(full_name).count()
        ok = actual == expected_count
        checks.append((f"{table}: {actual} rows (expected {expected_count})", ok))
        # Check audit columns
        cols = spark.table(full_name).columns
        has_ts   = "_ingestion_ts" in cols
        has_file = "_source_file" in cols
        if not has_ts:
            checks.append((f"  ⚠️ {table}: missing _ingestion_ts", False))
        if not has_file:
            checks.append((f"  ⚠️ {table}: missing _source_file", False))
    except Exception as e:
        checks.append((f"{table}: ❌ NOT FOUND", False))

# Idempotency check: history should show overwrite operations
try:
    hist = spark.sql("DESCRIBE HISTORY gt_lab.bronze.taxis_declarations_raw")
    n_ops = hist.count()
    ok = n_ops >= 1
    checks.append((f"DESCRIBE HISTORY works ({n_ops} versions)", ok))
except Exception as e:
    checks.append(("DESCRIBE HISTORY", False))

# Report
print("=" * 65)
print(" ✅ LAB 2 — VERIFICATION RESULTS")
print("=" * 65)
passed = sum(1 for _, ok in checks if ok)
total = len(checks)
for name, ok in checks:
    icon = "✅" if ok else "❌"
    print(f"  {icon} {name}")
print("=" * 65)
print(f"  Score: {passed}/{total}")
if passed == total:
    print("\n🎉 ΣΥΓΧΑΡΗΤΗΡΙΑ — Lab 2 ολοκληρώθηκε!")
    print("    Είστε έτοιμοι για παρουσίαση 🎤")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎓 Bonus — Discussion Questions
# MAGIC
# MAGIC Όταν τελειώσετε ως ομάδα, σκεφτείτε:
# MAGIC
# MAGIC 1. **Stretch goal**: Πώς θα επεκτείνατε τη loader function για να υποστηρίζει διαφορετικά file formats (JSON, Parquet) εκτός CSV;
# MAGIC
# MAGIC 2. **Schema evolution**: Αν αύριο το TAXIS προσθέσει νέο column, τι θα συμβεί στο σημερινό pipeline; Πώς θα το κάνατε resilient;
# MAGIC
# MAGIC 3. **Production scale**: Αν τα CSVs ήταν 50 GB το καθένα αντί για 10 KB, ποιες αλλαγές θα κάνατε;
# MAGIC    *(Hints: Auto Loader, file format change, cluster size)*
# MAGIC
# MAGIC 4. **Concurrent runs**: 2 pipelines τρέχουν ταυτόχρονα και γράφουν στο ίδιο Bronze table. Τι συμβαίνει; Πώς το χειρίζεστε;
# MAGIC    *(Hint: Delta optimistic concurrency — να ψάξετε αυτό σε Day 3)*
# MAGIC
# MAGIC 5. **Cost**: Πώς θα μετρούσατε **πόσο κοστίζει** σε DBUs αυτή η pipeline ένα τυπικό run;
# MAGIC
# MAGIC ## 🔗 Επόμενο
# MAGIC
# MAGIC Day 2: **Bronze → Silver Transformations** — quality checks, deduplication, type enforcement.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📖 REFERENCE EXAMPLE — Πλήρες worked example
# MAGIC
# MAGIC Αν θες να δεις **ένα ολοκληρωμένο παράδειγμα** πριν γράψεις τον δικό σου κώδικα, παρακάτω είναι το citizen_registry implemented end-to-end.
# MAGIC
# MAGIC ⚠️ **ΠΡΟΣΟΧΗ**: Μη το αντιγράψεις απευθείας. Δες το pattern, μετά εφάρμοσε ΔΙΚΗ σου λύση για όλες τις 5 πηγές.

# COMMAND ----------

# DBTITLE 1,REFERENCE — citizen_registry end-to-end (one source only)
# Δες το pattern. Στον δικό σου κώδικα, κάνε το loop για 5 sources.

from pyspark.sql.functions import current_timestamp, input_file_name, to_date
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# Step 1 — Define explicit schema (όχι inferSchema)
citizen_schema = StructType([
    StructField("afm",        StringType(),    False),
    StructField("full_name",  StringType(),    True),
    StructField("region",     StringType(),    True),
    StructField("birth_year", IntegerType(),   True),
    StructField("is_active",  BooleanType(),   True),
    StructField("updated_at", TimestampType(), True),
])

# Step 2 — Read CSV with schema
df_raw = (spark.read
            .schema(citizen_schema)
            .option("header", "true")
            .csv("/Volumes/gt_lab/bronze/landing/citizen_registry.csv"))

# Step 3 — Add audit columns
df_with_audit = (df_raw
    .withColumn("_ingestion_ts",   current_timestamp())
    .withColumn("_source_file",    input_file_name())
    .withColumn("_ingestion_date", to_date(current_timestamp()))
)

# Step 4 — Write Bronze Delta με partition + overwrite (idempotent)
target_table = "gt_lab.bronze.citizen_registry_raw"
(df_with_audit.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("_ingestion_date")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table))

# Step 5 — Add table metadata (production polish)
spark.sql(f"""
    ALTER TABLE {target_table}
    SET TBLPROPERTIES (
        'comment' = 'Raw citizen registry from CRM source',
        'owner' = 'data-platform-team',
        'pii_classification' = 'high'
    )
""")

# Step 6 — Verify
n = spark.table(target_table).count()
print(f"✅ {target_table}: {n} rows")
display(spark.table(target_table).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤔 Σκέψεις πάνω στο reference
# MAGIC
# MAGIC Πριν γράψεις τον δικό σου κώδικα στο Step 2 παραπάνω, σκέψου:
# MAGIC
# MAGIC 1. **Πώς θα γενικεύσεις** αυτό το pattern σε function για 5 πηγές;
# MAGIC    *(Hint: τα schemas διαφέρουν ανά source. Πώς θα τα παραμετροποιήσεις;)*
# MAGIC
# MAGIC 2. **Τι θα έβαζες σαν πρόσθετο audit column;**
# MAGIC    *(Hint: `_pipeline_run_id` με `uuid.uuid4()`; `_ingestion_user` με `current_user()`;)*
# MAGIC
# MAGIC 3. **Πώς θα testάρεις idempotency;**
# MAGIC    *(Δες το Step 5 — `assert count_before == count_after`)*
# MAGIC
# MAGIC 4. **Τι αν το CSV έχει malformed row;**
# MAGIC    *(Hint: `.option("mode", "PERMISSIVE")` ή `"FAILFAST"` ή `"DROPMALFORMED"`)*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🚀 STRETCH EXERCISES — Για όσους τελειώσουν νωρίς

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 Stretch 1 — Schema evolution simulation
# MAGIC
# MAGIC **Σενάριο:** Το TAXIS upgraded και πρόσθεσε νέο column `tax_year_period` στο CSV. Πώς θα το χειριστείς χωρίς να σπάσει το pipeline;
# MAGIC
# MAGIC ### 📚 Theory
# MAGIC
# MAGIC Delta υποστηρίζει **schema evolution** με flag:
# MAGIC
# MAGIC ```python
# MAGIC # Με mergeSchema=true, νέα columns προστίθενται αυτόματα
# MAGIC df_new.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("append") \
# MAGIC     .option("mergeSchema", "true") \
# MAGIC     .saveAsTable("gt_lab.bronze.taxis_declarations_raw")
# MAGIC ```
# MAGIC
# MAGIC ### ✍️ Δοκίμασε
# MAGIC
# MAGIC 1. Διάβασε το `taxis_declarations.csv` ΞΑΝΑ
# MAGIC 2. Πρόσθεσε artificial column: `df.withColumn("tax_year_period", lit("Q1-2026"))`
# MAGIC 3. Γράψε στο Bronze με `mergeSchema=true`
# MAGIC 4. Verify με `DESCRIBE` ότι το schema πλέον έχει την νέα στήλη

# COMMAND ----------

# DBTITLE 1,STRETCH 1 — Schema evolution
# 👇 ΓΡΑΨΤΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ ΕΔΩ





# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 Stretch 2 — Auto Loader pattern (production preview)
# MAGIC
# MAGIC Σε production δεν διαβάζεις CSVs με `spark.read.csv()` — χρησιμοποιείς **Auto Loader** που:
# MAGIC - Παρακολουθεί directory για νέα files
# MAGIC - Διαβάζει incremental (μόνο νέα)
# MAGIC - Schema inference + evolution
# MAGIC - Checkpoint state
# MAGIC
# MAGIC ### 📚 Reference pattern
# MAGIC
# MAGIC ```python
# MAGIC checkpoint = "/Volumes/gt_lab/bronze/_checkpoints/citizen_autoloader"
# MAGIC schema_loc = "/Volumes/gt_lab/bronze/_schemas/citizen"
# MAGIC
# MAGIC # Read stream με Auto Loader
# MAGIC df_stream = (spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "csv")
# MAGIC     .option("cloudFiles.schemaLocation", schema_loc)
# MAGIC     .option("cloudFiles.inferColumnTypes", "true")
# MAGIC     .option("header", "true")
# MAGIC     .load("/Volumes/gt_lab/bronze/landing/citizen_registry*.csv"))
# MAGIC
# MAGIC # Write stream στο Bronze Delta
# MAGIC query = (df_stream
# MAGIC     .withColumn("_ingestion_ts", current_timestamp())
# MAGIC     .withColumn("_source_file", input_file_name())
# MAGIC     .writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", checkpoint)
# MAGIC     .trigger(availableNow=True)   # σαν batch — runs once + stops
# MAGIC     .toTable("gt_lab.bronze.citizen_registry_autoloader"))
# MAGIC
# MAGIC query.awaitTermination()
# MAGIC ```
# MAGIC
# MAGIC ### ✍️ Δοκίμασε
# MAGIC
# MAGIC Φτιάξε ένα 6ο table με Auto Loader pattern (αντί για τη δική σας function).
# MAGIC Verify ότι **δεύτερη φορά που τρέχει**, ΔΕΝ διπλασιάζει — Auto Loader ξέρει ποια files ήδη διαβάστηκαν.

# COMMAND ----------

# DBTITLE 1,STRETCH 2 — Auto Loader
# 👇 ΓΡΑΨΤΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ ΕΔΩ





# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 Stretch 3 — Data Quality preview
# MAGIC
# MAGIC Στο Day 2 θα δούμε quality checks. Ας ξεκινήσουμε με ένα απλό:
# MAGIC
# MAGIC ### 📚 Theory
# MAGIC
# MAGIC Πριν γράψεις σε Bronze, **assert** ότι το input έχει βασικά properties:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col, count, when
# MAGIC
# MAGIC def quality_check(df, table_name):
# MAGIC     """Run basic quality checks. Raises if critical fail."""
# MAGIC     n_total = df.count()
# MAGIC     if n_total == 0:
# MAGIC         raise ValueError(f"❌ {table_name}: empty DataFrame")
# MAGIC
# MAGIC     # Check 1 — Required columns
# MAGIC     required = ["afm"] if "afm" in df.columns else []
# MAGIC     for c in required:
# MAGIC         nulls = df.filter(col(c).isNull()).count()
# MAGIC         null_pct = 100 * nulls / n_total
# MAGIC         if null_pct > 5:
# MAGIC             raise ValueError(f"❌ {table_name}.{c}: {null_pct:.1f}% nulls (>5% threshold)")
# MAGIC         print(f"  ✅ {table_name}.{c}: {null_pct:.1f}% nulls")
# MAGIC
# MAGIC     print(f"✅ {table_name}: {n_total} rows passed quality")
# MAGIC     return df
# MAGIC ```
# MAGIC
# MAGIC ### ✍️ Δοκίμασε
# MAGIC
# MAGIC Πρόσθεσε `quality_check()` call στη loader function σας. Trigger ένα FAIL εσκεμμένα (π.χ. set null rate threshold σε 0%) και δες τι γίνεται.

# COMMAND ----------

# DBTITLE 1,STRETCH 3 — Quality check
# 👇 ΓΡΑΨΤΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ ΕΔΩ





# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 SUPER STRETCH — DESCRIBE HISTORY + Time Travel
# MAGIC
# MAGIC Delta tables έχουν **built-in version history**. Production gold για audit/debug.
# MAGIC
# MAGIC ### 📚 Reference
# MAGIC
# MAGIC ```python
# MAGIC # See all versions
# MAGIC display(spark.sql("DESCRIBE HISTORY gt_lab.bronze.taxis_declarations_raw"))
# MAGIC
# MAGIC # Read previous version
# MAGIC df_old = (spark.read
# MAGIC             .format("delta")
# MAGIC             .option("versionAsOf", 0)  # ή timestampAsOf
# MAGIC             .table("gt_lab.bronze.taxis_declarations_raw"))
# MAGIC
# MAGIC # Diff: τι άλλαξε μεταξύ versions
# MAGIC v0 = spark.read.format("delta").option("versionAsOf", 0).table("gt_lab.bronze.taxis_declarations_raw")
# MAGIC v1 = spark.read.format("delta").option("versionAsOf", 1).table("gt_lab.bronze.taxis_declarations_raw")
# MAGIC diff = v1.exceptAll(v0)
# MAGIC display(diff)
# MAGIC ```
# MAGIC
# MAGIC ### ✍️ Δοκίμασε
# MAGIC
# MAGIC Τρέξε τη loader function **2 φορές** (πρώτη φορά κανονικά, δεύτερη φορά με `mode("overwrite")`). Μετά:
# MAGIC 1. `DESCRIBE HISTORY` → πόσες versions έχεις;
# MAGIC 2. Διάβασε `versionAsOf=0` και count
# MAGIC 3. Διάβασε current και count
# MAGIC 4. Πρέπει να είναι ίδιο count (idempotency confirmed via Delta history!)

# COMMAND ----------

# DBTITLE 1,SUPER STRETCH — Delta time travel
# 👇 ΓΡΑΨΤΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ ΕΔΩ


