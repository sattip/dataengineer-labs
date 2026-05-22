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
# MAGIC
# MAGIC ## 2.8 Data Contracts — Validating What You Receive
# MAGIC
# MAGIC Ένα **Data Contract** είναι μια **εγγύηση** μεταξύ source system και Bronze layer:
# MAGIC
# MAGIC > «Το TAXIS εγγυάται ότι κάθε CSV θα έχει column `afm` με 9 ψηφία, μη NULL.»
# MAGIC > «Το Bronze εγγυάται ότι θα reject-άρει οποιοδήποτε record δεν τηρεί το contract.»
# MAGIC
# MAGIC ### Γιατί;
# MAGIC
# MAGIC Χωρίς contract:
# MAGIC - Σιωπηλά μπαίνουν bad data σε Bronze
# MAGIC - Silver/Gold "ξυπνάει" 3 μήνες μετά με broken queries
# MAGIC - Κανείς δεν ξέρει αν η source άλλαξε
# MAGIC
# MAGIC Με contract:
# MAGIC - **Fail fast**: αν schema αλλάξει, pipeline σπάει με σαφές error
# MAGIC - **Documentation**: το contract είναι τόσο docs όσο και code
# MAGIC - **Trust boundary**: clear ευθύνες producer vs consumer
# MAGIC
# MAGIC ### Σχήμα data contract (απλό)
# MAGIC
# MAGIC ```python
# MAGIC CONTRACTS = {
# MAGIC     "citizen_registry": {
# MAGIC         "version": "1.0",
# MAGIC         "owner": "registry-team@gov.gr",
# MAGIC         "primary_key": "afm",
# MAGIC         "columns": {
# MAGIC             "afm":        {"type": "string", "nullable": False, "regex": r"^\d{9}$"},
# MAGIC             "full_name":  {"type": "string", "nullable": True,  "max_length": 200},
# MAGIC             "region":     {"type": "string", "nullable": True,  "allowed": ["ATTICA", "MACEDONIA", "CRETE", "EPIRUS", "THESSALY", "PELOPONNESE"]},
# MAGIC             "birth_year": {"type": "integer","nullable": True,  "min": 1900, "max": 2026},
# MAGIC             "is_active":  {"type": "boolean","nullable": False},
# MAGIC             "updated_at": {"type": "timestamp", "nullable": True},
# MAGIC         },
# MAGIC         "quality": {
# MAGIC             "max_null_pct": {"afm": 0.0, "is_active": 0.0},
# MAGIC             "min_rows": 1,
# MAGIC             "max_rows": 100_000_000,
# MAGIC         }
# MAGIC     },
# MAGIC     "taxis_declarations": { ... },
# MAGIC     # ... ένα ανά source
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Validation function (skeleton)
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col, count, when, length
# MAGIC
# MAGIC def validate_contract(df, contract):
# MAGIC     """Raises αν το df δεν τηρεί το contract. Returns df αν OK."""
# MAGIC     n = df.count()
# MAGIC
# MAGIC     # Check 1 — Row count bounds
# MAGIC     q = contract.get("quality", {})
# MAGIC     if n < q.get("min_rows", 0):
# MAGIC         raise ValueError(f"❌ Contract violation: {n} rows < min {q['min_rows']}")
# MAGIC     if n > q.get("max_rows", float("inf")):
# MAGIC         raise ValueError(f"❌ Contract violation: {n} rows > max {q['max_rows']}")
# MAGIC
# MAGIC     # Check 2 — Schema (columns exist + nullable rules)
# MAGIC     for col_name, spec in contract["columns"].items():
# MAGIC         if col_name not in df.columns:
# MAGIC             raise ValueError(f"❌ Missing column: {col_name}")
# MAGIC         if not spec["nullable"]:
# MAGIC             nulls = df.filter(col(col_name).isNull()).count()
# MAGIC             if nulls > 0:
# MAGIC                 raise ValueError(f"❌ {col_name}: {nulls} nulls (non-nullable)")
# MAGIC
# MAGIC     # Check 3 — Per-column quality thresholds
# MAGIC     for col_name, max_null_pct in q.get("max_null_pct", {}).items():
# MAGIC         nulls = df.filter(col(col_name).isNull()).count()
# MAGIC         null_pct = 100 * nulls / n if n > 0 else 0
# MAGIC         if null_pct > max_null_pct:
# MAGIC             raise ValueError(
# MAGIC                 f"❌ {col_name}: {null_pct:.2f}% nulls > {max_null_pct}% threshold"
# MAGIC             )
# MAGIC
# MAGIC     # Check 4 — Regex patterns
# MAGIC     for col_name, spec in contract["columns"].items():
# MAGIC         if "regex" in spec and col_name in df.columns:
# MAGIC             bad = df.filter(
# MAGIC                 col(col_name).isNotNull() &
# MAGIC                 ~col(col_name).cast("string").rlike(spec["regex"])
# MAGIC             ).count()
# MAGIC             if bad > 0:
# MAGIC                 raise ValueError(f"❌ {col_name}: {bad} rows fail regex {spec['regex']}")
# MAGIC
# MAGIC     # Check 5 — Allowed values
# MAGIC     for col_name, spec in contract["columns"].items():
# MAGIC         if "allowed" in spec and col_name in df.columns:
# MAGIC             bad = df.filter(
# MAGIC                 col(col_name).isNotNull() &
# MAGIC                 ~col(col_name).isin(spec["allowed"])
# MAGIC             ).count()
# MAGIC             if bad > 0:
# MAGIC                 raise ValueError(f"❌ {col_name}: {bad} rows outside allowed values")
# MAGIC
# MAGIC     print(f"✅ Contract validated: {n} rows pass")
# MAGIC     return df
# MAGIC ```
# MAGIC
# MAGIC ### Πού μπαίνει στο pipeline;
# MAGIC
# MAGIC ```python
# MAGIC def load_to_bronze(source, entity, csv_file):
# MAGIC     df = spark.read.option("header", "true").csv(...)
# MAGIC     df = validate_contract(df, CONTRACTS[source])   # ← ΕΔΩ — πριν το write
# MAGIC     # ... audit cols, write Delta ...
# MAGIC ```
# MAGIC
# MAGIC **Fail-fast pattern**: αν data δεν τηρούν contract → exception → pipeline stops → alert.
# MAGIC
# MAGIC ### Production patterns
# MAGIC
# MAGIC | Pattern | Tool |
# MAGIC |---|---|
# MAGIC | YAML-based contracts | dbt schema tests, Great Expectations, Soda |
# MAGIC | Runtime validation | DLT expectations (`@dlt.expect_or_fail`) |
# MAGIC | Schema registry | Confluent Schema Registry για Kafka |
# MAGIC | Versioning | Git + semver στο contract YAML |
# MAGIC
# MAGIC Σήμερα στο stretch exercise, **θα γράψεις τον δικό σου mini contract validator**.

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

    Returns:
        int: row count of resulting Bronze table
    """
    # ═══════════════════════════════════════════════════════════════════
    # 👇 IMPLEMENT BELOW — pseudocode hints:
    # ═══════════════════════════════════════════════════════════════════
    #
    # STEP A — Build paths
    #   target_table = f"gt_lab.bronze.{source}_{entity}_raw"
    #   csv_path     = f"{LANDING_PATH}/{csv_filename}"
    #
    # STEP B — Read CSV (decide: inferSchema or explicit?)
    #   df = spark.read.option("header", "true") \
    #             .option("inferSchema", "true") \
    #             .csv(csv_path)
    #
    # STEP C — (Optional) Validate against contract — δες Stretch 4
    #   df = validate_contract(df, CONTRACTS[source])
    #
    # STEP D — Add audit columns
    #   from pyspark.sql.functions import current_timestamp, input_file_name, to_date
    #   df_audited = (df
    #       .withColumn("_ingestion_ts",   current_timestamp())
    #       .withColumn("_source_file",    input_file_name())
    #       .withColumn("_ingestion_date", to_date(current_timestamp()))
    #   )
    #
    # STEP E — Write Bronze Delta (idempotent + partitioned)
    #   df_audited.write \
    #       .format("delta") \
    #       .mode("overwrite") \
    #       .partitionBy("_ingestion_date") \
    #       .option("overwriteSchema", "true") \
    #       .saveAsTable(target_table)
    #
    # STEP F — Return count για verification
    #   return spark.table(target_table).count()
    # ═══════════════════════════════════════════════════════════════════

    pass  # 👈 Replace με τη δική σου implementation


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
# MAGIC %md
# MAGIC ---
# MAGIC # 🧪 STRETCH 4 — Data Contract Validator (Mini-Lab · ⏱ 60 λεπτά)
# MAGIC
# MAGIC > **Standalone mini-lab** μέσα στο Lab 2. Αν τελειώσατε νωρίς τα core steps, δουλεύετε αυτό σε groups.
# MAGIC
# MAGIC ## 🎯 Goal
# MAGIC
# MAGIC Να υλοποιήσετε **production-grade contract validation system** για το Bronze ingestion σας:
# MAGIC - Define contracts για **5 sources** (όχι μόνο 2)
# MAGIC - Build **comprehensive validator** με 7 check types
# MAGIC - **Quarantine pattern**: bad records → quarantine table αντί για pipeline crash
# MAGIC - **Metrics report**: ποιοι έλεγχοι έσπασαν, σε πόσα records
# MAGIC - **Integrate** στο existing `load_to_bronze()` σας
# MAGIC
# MAGIC ## 📋 Pacing (60' breakdown)
# MAGIC
# MAGIC | Time | Step | Deliverable |
# MAGIC |---|---|---|
# MAGIC | 0-10' | **A** — Design 5 contracts | CONTRACTS dict για όλες τις πηγές |
# MAGIC | 10-25' | **B** — Build validator | `validate_contract()` με 7 checks |
# MAGIC | 25-35' | **C** — Quarantine pattern | bad records → separate table |
# MAGIC | 35-45' | **D** — Integrate + test happy path | `load_with_contracts()` |
# MAGIC | 45-55' | **E** — Break tests + metrics | inject bad data, see what catches |
# MAGIC | 55-60' | **F** — Discussion | trade-offs με ομάδα |
# MAGIC
# MAGIC ## 🎤 Παρουσίαση (αν χρόνος)
# MAGIC
# MAGIC Στο τέλος, 1 ομάδα παρουσιάζει 5':
# MAGIC - Ποιους checks διάλεξαν;
# MAGIC - Πώς χειρίστηκαν trade-off **fail-fast** vs **quarantine**;
# MAGIC - Τι metric ποιότητας θα έβαζαν σε production dashboard;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 Background: Why 1 hour for this?
# MAGIC
# MAGIC Στη δουλειά σας, **data quality θα κλέψει το μεγαλύτερο μέρος του χρόνου σας**. Senior DEs ξοδεύουν 60-80% του χρόνου τους σε:
# MAGIC - Defining what "good data" means (contracts)
# MAGIC - Catching violations early (validation)
# MAGIC - Routing bad data σωστά (quarantine, alert, fix-at-source)
# MAGIC
# MAGIC Σήμερα θα χτίσετε **mini-version του ίδιου system** που θα δουλέψετε σε production.
# MAGIC
# MAGIC ### Production analogue
# MAGIC
# MAGIC | What you'll build | Production tool που κάνει το ίδιο |
# MAGIC |---|---|
# MAGIC | CONTRACTS dict | dbt YAML schemas, Great Expectations suite, Soda checks |
# MAGIC | validate_contract() | DLT `@dlt.expect_or_fail`, GE `validate_run()` |
# MAGIC | quarantine table | DLT bad records, DLQ patterns, Soda failed rows |
# MAGIC | metrics report | Soda Cloud, Monte Carlo, Datadog DQ dashboards |
# MAGIC
# MAGIC ### 7 Validation Check Types
# MAGIC
# MAGIC | # | Check | Example | Severity |
# MAGIC |---|---|---|---|
# MAGIC | 1 | **Row count bounds** | min=1, max=10M | Critical |
# MAGIC | 2 | **Required columns exist** | `afm` MUST be present | Critical |
# MAGIC | 3 | **Non-nullable constraints** | `afm` NOT NULL | Critical |
# MAGIC | 4 | **Null rate thresholds** | < 2% nulls in `email` | Warning |
# MAGIC | 5 | **Regex patterns** | AFM = 9 digits | Critical |
# MAGIC | 6 | **Allowed values (enum)** | region ∈ {ATTICA, ...} | Critical |
# MAGIC | 7 | **Numeric ranges** | tax_amount ≥ 0 | Critical |
# MAGIC
# MAGIC Severity matters: **Critical** → block pipeline. **Warning** → log + alert αλλά continue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP A (10') — Design 5 Contracts
# MAGIC
# MAGIC Define `CONTRACTS` dict για ΟΛΕΣ τις 5 πηγές. Hints:
# MAGIC
# MAGIC ### Citizen Registry
# MAGIC - `afm`: string, NOT NULL, regex `^\d{9}$`
# MAGIC - `region`: enum [ATTICA, MACEDONIA, CRETE, EPIRUS, THESSALY, PELOPONNESE]
# MAGIC - `birth_year`: int, range 1900-2026
# MAGIC - `is_active`: bool, NOT NULL
# MAGIC - `min_rows`: 1, `max_rows`: 100M
# MAGIC
# MAGIC ### TAXIS Declarations
# MAGIC - `statement_id`: string, NOT NULL (primary key)
# MAGIC - `afm`: string, NOT NULL, regex 9 digits
# MAGIC - `fiscal_year`: int, range 2000-2030
# MAGIC - `tax_amount`: decimal, min ≥ 0
# MAGIC - `status`: enum [Submitted, Approved, Rejected, Pending]
# MAGIC
# MAGIC ### EFKA Contributions
# MAGIC - `contribution_id`: string, NOT NULL
# MAGIC - `afm`, `employer_afm`: strings, regex 9 digits, NOT NULL
# MAGIC - `gross_salary`: decimal, min ≥ 0
# MAGIC - `employee_contribution`: decimal, min ≥ 0
# MAGIC - `period`: regex `^\d{4}-\d{2}$` (YYYY-MM)
# MAGIC
# MAGIC ### KEP Events
# MAGIC - `event_id`: string, NOT NULL
# MAGIC - `citizen_afm`: string, NOT NULL, regex 9 digits
# MAGIC - `event_type`: enum [REQUEST_CREATED, REQUEST_COMPLETED, REQUEST_FAILED]
# MAGIC - `wait_minutes`: int, range 0-1440 (max 1 day)
# MAGIC
# MAGIC ### myDATA Invoices
# MAGIC - `invoice_id`: string, NOT NULL
# MAGIC - `issuer_afm`, `receiver_afm`: regex 9 digits
# MAGIC - `total_amount`: decimal, NULL allowed (credit notes — δες discussion)
# MAGIC - `transmission_status`: enum [Accepted, Rejected, Pending]

# COMMAND ----------

# DBTITLE 1,STEP A — CONTRACTS dict (5 sources)
# 👇 ΓΡΑΨΤΕ ΤΟ ΔΙΚΟ ΣΑΣ CONTRACTS DICT ΕΔΩ
#
# Pseudocode skeleton (1 source — extend για 5):
#
# CONTRACTS = {
#     "citizen": {
#         "version": "1.0",
#         "owner": "registry-team@gov.gr",
#         "primary_key": "afm",
#         "columns": {
#             "afm":        {"type": "string", "nullable": False,
#                            "regex": r"^\d{9}$"},
#             "region":     {"type": "string", "nullable": True,
#                            "allowed": ["ATTICA", "MACEDONIA", ...]},
#             "birth_year": {"type": "integer", "nullable": True,
#                            "min": 1900, "max": 2026},
#             "is_active":  {"type": "boolean", "nullable": False},
#         },
#         "quality": {
#             "min_rows": 1,
#             "max_rows": 100_000_000,
#             "max_null_pct": {"afm": 0.0, "is_active": 0.0,
#                              "region": 5.0}  # tolerated up to 5% region nulls
#         }
#     },
#     # ... 4 more sources
# }

CONTRACTS = {
    # Fill in σύμφωνα με το spec
}





# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP B (15') — Build `validate_contract()` με 7 checks
# MAGIC
# MAGIC ### Function signature
# MAGIC
# MAGIC ```python
# MAGIC def validate_contract(df, contract, fail_fast=True):
# MAGIC     """
# MAGIC     Validate DataFrame against contract.
# MAGIC
# MAGIC     Args:
# MAGIC         df: input DataFrame
# MAGIC         contract: dict με spec
# MAGIC         fail_fast: αν True, raise σε πρώτο failure.
# MAGIC                    αν False, accumulate violations σε report.
# MAGIC
# MAGIC     Returns:
# MAGIC         (df_valid, df_invalid, report)
# MAGIC         - df_valid:   records that pass all checks
# MAGIC         - df_invalid: records that fail (με extra _failure_reason column)
# MAGIC         - report:     dict με metrics ανά check
# MAGIC     """
# MAGIC ```
# MAGIC
# MAGIC ### Tips
# MAGIC
# MAGIC - Build conditions ως PySpark expressions (`col(x).isNull()`, `col(x).rlike(pat)`)
# MAGIC - Combine με `&` (AND) και `|` (OR) — όχι Python `and`/`or`
# MAGIC - Για quarantine, χρησιμοποίησε `when().otherwise()` για να φτιάξεις `_failure_reason`
# MAGIC - Metrics report = ένα dict με counts ανά check

# COMMAND ----------

# DBTITLE 1,STEP B — validate_contract() implementation
# 👇 ΓΡΑΨΤΕ ΤΗ FUNCTION ΣΑΣ ΕΔΩ
#
# from pyspark.sql.functions import col, when, lit, count
#
# def validate_contract(df, contract, fail_fast=True):
#     report = {"checks": {}, "n_total": df.count(), "n_failures": 0}
#
#     # Build invalid_mask ως cumulative OR of all violations
#     invalid_mask = lit(False)
#     failure_reasons = []
#
#     # Check 1: Row count bounds
#     # ...
#
#     # Check 2: Required columns
#     for c in contract["columns"]:
#         if c not in df.columns:
#             if fail_fast: raise ValueError(...)
#             else: report["checks"][f"missing_{c}"] = -1
#
#     # Check 3: Non-nullable
#     for c, spec in contract["columns"].items():
#         if not spec.get("nullable", True):
#             condition = col(c).isNull()
#             invalid_mask = invalid_mask | condition
#             # ... build _failure_reason column
#
#     # Check 4: Null rate thresholds
#     # ...
#
#     # Check 5: Regex
#     for c, spec in contract["columns"].items():
#         if "regex" in spec:
#             condition = col(c).isNotNull() & ~col(c).cast("string").rlike(spec["regex"])
#             invalid_mask = invalid_mask | condition
#
#     # Check 6: Allowed values
#     # ...
#
#     # Check 7: Numeric ranges
#     for c, spec in contract["columns"].items():
#         if "min" in spec or "max" in spec:
#             condition = lit(False)
#             if "min" in spec:
#                 condition = condition | (col(c) < spec["min"])
#             if "max" in spec:
#                 condition = condition | (col(c) > spec["max"])
#             invalid_mask = invalid_mask | (col(c).isNotNull() & condition)
#
#     # Split
#     df_valid   = df.filter(~invalid_mask)
#     df_invalid = df.filter(invalid_mask).withColumn("_failure_reason", lit("see report"))
#
#     report["n_failures"] = df_invalid.count()
#     return df_valid, df_invalid, report





# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP C (10') — Quarantine Pattern
# MAGIC
# MAGIC ### Concept
# MAGIC
# MAGIC Αντί για **fail-and-crash**, πιο production-σωστό είναι **fail-and-quarantine**:
# MAGIC
# MAGIC ```
# MAGIC Raw CSV → validate → ┬→ Bronze (clean records)
# MAGIC                       └→ Quarantine (bad records + _failure_reason)
# MAGIC ```
# MAGIC
# MAGIC ### Quarantine table schema
# MAGIC
# MAGIC ```
# MAGIC gt_lab.bronze.quarantine_{source}_{entity}
# MAGIC
# MAGIC Columns:
# MAGIC   - Όλες οι original columns
# MAGIC   - _failure_reason  (string, why it failed)
# MAGIC   - _quarantine_ts   (timestamp)
# MAGIC   - _pipeline_run_id (για lineage)
# MAGIC ```
# MAGIC
# MAGIC ### Step C — Write quarantine
# MAGIC
# MAGIC Φτιάξτε function `write_quarantine(df_invalid, source, entity)` που:
# MAGIC 1. Adds `_quarantine_ts` + `_pipeline_run_id`
# MAGIC 2. Writes σε `gt_lab.bronze.quarantine_{source}_{entity}` (append mode — δες γιατί παρακάτω)
# MAGIC 3. Updates table metadata με last quarantine timestamp

# COMMAND ----------

# DBTITLE 1,STEP C — write_quarantine() implementation
# 👇 ΓΡΑΨΤΕ ΤΗ FUNCTION ΣΑΣ ΕΔΩ
#
# def write_quarantine(df_invalid, source, entity):
#     if df_invalid.count() == 0:
#         return  # Nothing to quarantine — clean source
#
#     target = f"gt_lab.bronze.quarantine_{source}_{entity}"
#     df_q = (df_invalid
#         .withColumn("_quarantine_ts", current_timestamp())
#         .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
#     )
#     (df_q.write
#         .format("delta")
#         .mode("append")  # APPEND γιατί κρατάμε ιστορικό όλων των violations
#         .option("mergeSchema", "true")
#         .saveAsTable(target))
#
#     n = df_q.count()
#     print(f"  ⚠️  Quarantined {n} rows → {target}")





# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP D (10') — Integrate σε `load_with_contracts()`
# MAGIC
# MAGIC Φτιάξτε νέα wrapper function που συνδυάζει όλα:
# MAGIC
# MAGIC ```python
# MAGIC def load_with_contracts(source, entity, csv_filename):
# MAGIC     """End-to-end loader with contract validation + quarantine."""
# MAGIC     # 1. Read CSV
# MAGIC     # 2. Validate (fail_fast=False για quarantine mode)
# MAGIC     # 3. Write valid → Bronze
# MAGIC     # 4. Write invalid → Quarantine
# MAGIC     # 5. Return report
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,STEP D — load_with_contracts() pipeline
# 👇 ΓΡΑΨΤΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ ΕΔΩ
#
# def load_with_contracts(source, entity, csv_filename):
#     # Read
#     csv_path = f"{LANDING_PATH}/{csv_filename}"
#     df = spark.read.option("header","true").csv(csv_path)
#
#     # Validate (quarantine mode)
#     contract = CONTRACTS.get(source)
#     if not contract:
#         raise ValueError(f"No contract για {source}")
#     df_valid, df_invalid, report = validate_contract(df, contract, fail_fast=False)
#
#     # Bronze write (valid only)
#     # ... add audit cols + write
#
#     # Quarantine write
#     write_quarantine(df_invalid, source, entity)
#
#     # Report
#     print(f"
#     📊 {source}/{entity} report:")
#     print(f"  Total:        {report['n_total']}")
#     print(f"  Valid:        {report['n_total'] - report['n_failures']}")
#     print(f"  Quarantined:  {report['n_failures']}")
#     return report
#
#
# # Test happy path — όλες οι πηγές
# for source, entity, csv in [
#     ("citizen", "registry", "citizen_registry.csv"),
#     ("taxis",   "declarations", "taxis_declarations.csv"),
#     ("efka",    "contributions", "efka_contributions.csv"),
#     ("kep",     "events", "kep_events.csv"),
#     ("mydata",  "invoices", "mydata_invoices.csv"),
# ]:
#     load_with_contracts(source, entity, csv)





# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP E (10') — Break Tests + Metrics Report
# MAGIC
# MAGIC ### Failure scenarios να test-άρετε
# MAGIC
# MAGIC 1. **Inject bad AFM**: στο CSV path φτιάξτε artificial record με 8-digit AFM
# MAGIC 2. **Out-of-range value**: tax_amount = -500
# MAGIC 3. **Invalid enum**: status = "PENDING_REVIEW" (not in allowed)
# MAGIC 4. **Schema break**: αφαιρέστε required column
# MAGIC
# MAGIC ### Hints για bad data injection
# MAGIC
# MAGIC Δεν χρειάζεται να αλλάξετε CSV. Φτιάξτε artificial DataFrame με `spark.createDataFrame()`:
# MAGIC
# MAGIC ```python
# MAGIC bad_records = spark.createDataFrame([
# MAGIC     ("BAD1", "12345", 2025, "VAT", 100, -50, "Approved", None, None),
# MAGIC     ("BAD2", "999999999", 1800, "VAT", 100, 50, "UNKNOWN_STATUS", None, None),
# MAGIC ], schema=taxis_schema)
# MAGIC validate_contract(bad_records, CONTRACTS["taxis"], fail_fast=False)
# MAGIC ```
# MAGIC
# MAGIC ### Quarantine table inspection
# MAGIC
# MAGIC Μετά τα break tests, query το quarantine για να δεις τι έπιασε:
# MAGIC
# MAGIC ```python
# MAGIC display(spark.table("gt_lab.bronze.quarantine_taxis_declarations"))
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,STEP E — Break tests
# 👇 ΓΡΑΨΤΕ TESTS ΕΔΩ
#
# Test 1 — Bad AFM (8 digits)
# bad_afm = spark.createDataFrame([("TX_BAD1", "12345", ...)], schema=...)
# valid, invalid, report = validate_contract(bad_afm, CONTRACTS["taxis"], fail_fast=False)
# assert invalid.count() == 1
#
# Test 2 — Negative amount
# ...
#
# Test 3 — Invalid enum
# ...
#
# Test 4 — Missing column
# ...





# COMMAND ----------

# DBTITLE 1,STEP E — Build metrics report για όλο το pipeline
# 👇 ΓΡΑΨΤΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ ΕΔΩ
#
# Aggregate quarantine tables → metrics dashboard
#
# from pyspark.sql.functions import lit
#
# quarantine_tables = [
#     ("citizen", "gt_lab.bronze.quarantine_citizen_registry"),
#     ("taxis",   "gt_lab.bronze.quarantine_taxis_declarations"),
#     # ... 5 total
# ]
#
# metrics = []
# for source, table in quarantine_tables:
#     try:
#         n = spark.table(table).count()
#         metrics.append((source, n))
#     except:
#         metrics.append((source, 0))
#
# df_metrics = spark.createDataFrame(metrics, ["source", "quarantined_count"])
# display(df_metrics)





# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP F (5') — Group Discussion
# MAGIC
# MAGIC ### Συζητήστε ως ομάδα και ετοιμαστείτε να παρουσιάσετε:
# MAGIC
# MAGIC 1. **Fail-fast vs Quarantine**: Πότε επιλέγετε ποιο;
# MAGIC    - Fail-fast = block pipeline όταν schema break (cardinal)
# MAGIC    - Quarantine = κρατάμε τα καλά, sideline τα κακά (συνηθέστερο)
# MAGIC
# MAGIC 2. **Severity levels**: Όλα critical ή κάποια warning;
# MAGIC    - π.χ. "tax_amount missing" → critical (block)
# MAGIC    - π.χ. "full_name has trailing spaces" → warning (log, don't block)
# MAGIC
# MAGIC 3. **Contract evolution**: Πώς αλλάζετε contract χωρίς downtime;
# MAGIC    - Versioning (1.0 → 1.1 backwards-compat, 2.0 breaking)
# MAGIC    - Migration period (παλιό + νέο parallel)
# MAGIC
# MAGIC 4. **Performance**: Έχετε 7 separate column scans. Πώς το βελτιστοποιείτε;
# MAGIC    - **Single aggregation pass**: όλα τα checks σε ένα `df.agg(...)` query
# MAGIC    - Spark Catalyst θα τα συνδυάσει σε ένα plan
# MAGIC
# MAGIC 5. **Quarantine review process**: Πώς τα bad records ξανα-μπαίνουν στο pipeline;
# MAGIC    - Manual review από data steward
# MAGIC    - Automated re-ingestion όταν fixed-at-source
# MAGIC    - "DLQ replay" pattern (Kafka semantics)
# MAGIC
# MAGIC 6. **Alerting**: Ποιες metrics στέλνετε σε production dashboard;
# MAGIC    - Quarantine rate (% bad records)
# MAGIC    - Per-check violation counts (which rules trigger most;)
# MAGIC    - Source-level health score
# MAGIC    - Trend over time (έχει η ποιότητα του TAXIS βελτιωθεί;)
# MAGIC
# MAGIC ### 🎤 Πιθανές ερωτήσεις παρουσίασης
# MAGIC
# MAGIC - "Γιατί έβαλες fail_fast=False σε όλα;" — πρέπει να εξηγήσεις tradeoff
# MAGIC - "Τι γίνεται αν 100% records αποτύχουν;" — alert immediately, source broke
# MAGIC - "Πώς θα ξέρω αν source άλλαξε schema;" — required columns check + schema_version field
# MAGIC - "Πόσο σου κοστίζει αυτή η validation;" — extra full scan per source

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ STEP G — Auto-verification για Stretch 4
# MAGIC
# MAGIC Τρέξε αυτό για auto-check:

# COMMAND ----------

# DBTITLE 1,Verification: contract validator
checks_s4 = []

# Check 1: CONTRACTS dict has 5 entries
try:
    n_contracts = len(CONTRACTS)
    checks_s4.append((f"CONTRACTS dict: {n_contracts} sources", n_contracts >= 5))
except NameError:
    checks_s4.append(("CONTRACTS dict defined", False))

# Check 2: validate_contract callable
try:
    callable_ok = callable(validate_contract)
    checks_s4.append(("validate_contract() defined", callable_ok))
except NameError:
    checks_s4.append(("validate_contract() defined", False))

# Check 3: Quarantine tables exist
try:
    quarantine_tables = [
        "gt_lab.bronze.quarantine_citizen_registry",
        "gt_lab.bronze.quarantine_taxis_declarations",
    ]
    found = 0
    for t in quarantine_tables:
        try:
            spark.table(t)
            found += 1
        except:
            pass
    checks_s4.append((f"Quarantine tables: {found}/{len(quarantine_tables)}", found >= 1))
except:
    checks_s4.append(("Quarantine tables", False))

print("=" * 60)
print(" 🧪 STRETCH 4 — Verification")
print("=" * 60)
passed = sum(1 for _, ok in checks_s4 if ok)
for n, ok in checks_s4:
    print(f"  {'✅' if ok else '❌'} {n}")
print(f"  {passed}/{len(checks_s4)} passed")
if passed == len(checks_s4):
    print("🎉 Stretch 4 ολοκληρώθηκε! Είσαι έτοιμος για παρουσίαση.")

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


