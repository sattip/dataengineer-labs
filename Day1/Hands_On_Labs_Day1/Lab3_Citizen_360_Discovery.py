# Databricks notebook source
# MAGIC %md
# MAGIC # 🌐 LAB 3 — Citizen 360 Discovery
# MAGIC
# MAGIC **Ημέρα 1 · Groups of 2-3 · ⏱ 90 λεπτά**
# MAGIC
# MAGIC > Στόχος: Να συνδέσεις τα 5 Bronze tables που έφτιαξες στο Lab 2 σε **μία ενοποιημένη εικόνα ανά πολίτη** — αυτό είναι το **Citizen 360**, ο πιο πολύτιμος data asset ενός δημόσιου φορέα.
# MAGIC
# MAGIC ## 📑 Δομή Notebook
# MAGIC 1. 📚 **Θεωρία** (15') — JOINs, dim_citizen pattern, Gold layer, data quality
# MAGIC 2. 🎯 **Εκφώνηση** — Τι θα φτιάξεις (groups)
# MAGIC 3. ✍️ **Ο κώδικας σου** — Γράψε εσύ (55')
# MAGIC 4. 🎤 **Παρουσίαση** — 2 ομάδες (10' η κάθε μία)
# MAGIC 5. ✅ **Verification** + discussion (10')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📚 ΜΕΡΟΣ 1 — ΘΕΩΡΙΑ
# MAGIC
# MAGIC ## 3.1 Τι είναι το Citizen 360;
# MAGIC
# MAGIC Το **Citizen 360** (γνωστό και ως Customer 360 σε private sector) είναι το «**Holy Grail**» κάθε data platform:
# MAGIC
# MAGIC > Μία **ενοποιημένη εικόνα ανά πολίτη**, με όλη του την αλληλεπίδραση με τον φορέα σε ένα μέρος.
# MAGIC
# MAGIC ### Γιατί το θέλουμε;
# MAGIC
# MAGIC Σήμερα, σε έναν τυπικό δημόσιο φορέα:
# MAGIC - Ο πολίτης Α είναι σε **TAXIS** ως φορολογούμενος
# MAGIC - Στο **EFKA** ως εργαζόμενος/συνταξιούχος
# MAGIC - Στο **myDATA** ως εκδότης/λήπτης τιμολογίων
# MAGIC - Στο **ΚΕΠ** ως αιτών υπηρεσίας
# MAGIC - Στο **Μητρώο Πολιτών** ως natural person
# MAGIC
# MAGIC Αν μπορούσαμε να τα δούμε **όλα σε ένα μέρος**, θα είχαμε:
# MAGIC - **Καλύτερη εξυπηρέτηση**: «Ξέρω τον πολίτη όταν με καλεί»
# MAGIC - **Fraud detection**: «Γιατί κάποιος δηλώνει €5K εισόδημα αλλά εκδίδει τιμολόγια €500K;»
# MAGIC - **Service prediction**: «Πολίτες με αυτό το profile τείνουν να ζητήσουν Υπηρεσία Χ»
# MAGIC - **Compliance**: «Όλα τα δεδομένα ενός πολίτη σε ένα audit query»
# MAGIC
# MAGIC ## 3.2 Ο AFM είναι το join key
# MAGIC
# MAGIC Σε ελληνικό δημόσιο, ο **ΑΦΜ** είναι ο natural key που ενώνει συστήματα.
# MAGIC
# MAGIC ```
# MAGIC                  ┌─────────────┐
# MAGIC                  │ citizen_AFM │  ← Universal join key
# MAGIC                  └─────────────┘
# MAGIC                        │
# MAGIC      ┌─────────┬───────┴───────┬─────────┬─────────┐
# MAGIC      │         │               │         │         │
# MAGIC   TAXIS      EFKA            KEP     myDATA  Registry
# MAGIC   (afm)      (afm)       (citizen_afm) (issuer_afm)  (afm)
# MAGIC ```
# MAGIC
# MAGIC **Προσοχή:** Το AFM column **δεν έχει το ίδιο όνομα παντού**:
# MAGIC - TAXIS: `afm`
# MAGIC - EFKA: `afm` + `employer_afm`
# MAGIC - KEP: `citizen_afm`
# MAGIC - myDATA: `issuer_afm` + `receiver_afm`
# MAGIC - Registry: `afm`
# MAGIC
# MAGIC Production rule: **πάντα κάνε rename σε `afm`** πριν join, για clarity.
# MAGIC
# MAGIC ## 3.3 JOIN Types — Πότε ποιο;
# MAGIC
# MAGIC | JOIN | Behavior | Use case για Citizen 360 |
# MAGIC |---|---|---|
# MAGIC | **INNER** | Μόνο matching σε ΟΛΑ τα tables | «Πολίτες που υπάρχουν σε ΟΛΑ τα συστήματα» |
# MAGIC | **LEFT** | Όλοι από αριστερά + matches | «Όλοι πολίτες στο Registry + ό,τι έχουν αλλού» ✅ συχνότερο |
# MAGIC | **FULL OUTER** | Όλοι από παντού | «Discovery — ποιοι λείπουν από πού;» |
# MAGIC | **ANTI** | Στο αριστερά αλλά ΟΧΙ στο δεξιά | «Πολίτες σε Registry χωρίς ΚΕΠ history» |
# MAGIC
# MAGIC Για Citizen 360 → **LEFT JOIN από Registry** (Registry = dimension spine).
# MAGIC
# MAGIC ## 3.4 Aggregation πριν Join — Γιατί;
# MAGIC
# MAGIC ❌ **Anti-pattern**: Direct join με detail tables (1 πολίτης × 10 KEP events = duplicate rows)
# MAGIC
# MAGIC ```python
# MAGIC # BAD: registry × kep_events → row explosion
# MAGIC registry.join(kep_events, ...)  # 9 citizens × 10 events = up to 90 rows
# MAGIC ```
# MAGIC
# MAGIC ✅ **Σωστό**: **Aggregate πρώτα**, μετά join
# MAGIC
# MAGIC ```python
# MAGIC # Aggregate KEP events ανά πολίτη
# MAGIC kep_per_citizen = kep_events.groupBy("citizen_afm").agg(
# MAGIC     count("*").alias("kep_event_count"),
# MAGIC     avg("wait_minutes").alias("avg_wait_min")
# MAGIC )
# MAGIC # Τώρα 1 row ανά πολίτη — safe to join
# MAGIC registry.join(kep_per_citizen, registry.afm == kep_per_citizen.citizen_afm, "left")
# MAGIC ```
# MAGIC
# MAGIC ## 3.5 Gold Layer Intro
# MAGIC
# MAGIC Το αποτέλεσμα Citizen 360 πάει στο **Gold layer** — business-ready data για:
# MAGIC - **Power BI dashboards** (executive views)
# MAGIC - **ML features** (πρόβλεψη churn, fraud)
# MAGIC - **API exposure** (citizen lookup service)
# MAGIC
# MAGIC Naming: `gt_lab.gold.citizen_360`
# MAGIC
# MAGIC ## 3.6 Data Quality Issues που θα ανακαλύψεις
# MAGIC
# MAGIC Όταν join-άρεις real-world data, θα δεις:
# MAGIC
# MAGIC | Issue | Cause | Detection |
# MAGIC |---|---|---|
# MAGIC | Citizens σε TAXIS αλλά **όχι** Registry | Outdated Registry | LEFT ANTI JOIN |
# MAGIC | Same AFM σε πολλούς δήμους | Mobile citizen ή error | GROUP BY + count distinct |
# MAGIC | AFM = NULL σε child tables | Bad input data | NULL count check |
# MAGIC | Duplicate AFM στο Registry | Data entry error | GROUP BY afm HAVING count > 1 |
# MAGIC
# MAGIC ## 3.7 Type Consistency
# MAGIC
# MAGIC ⚠️ **Critical:** AFM πρέπει να είναι **ίδιος τύπος** σε όλες τις tables πριν join.
# MAGIC
# MAGIC Αν Bronze του TAXIS έχει AFM = `string` και Registry έχει AFM = `integer`:
# MAGIC ```python
# MAGIC # Implicit cast → silent slow join
# MAGIC registry.join(taxis, on="afm")  # works αλλά slow
# MAGIC
# MAGIC # Explicit cast — production rule
# MAGIC registry = registry.withColumn("afm", col("afm").cast("string"))
# MAGIC taxis    = taxis.withColumn("afm", col("afm").cast("string"))
# MAGIC registry.join(taxis, on="afm")  # fast + safe
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎯 ΜΕΡΟΣ 2 — ΕΚΦΩΝΗΣΗ
# MAGIC
# MAGIC ## Σενάριο
# MAGIC
# MAGIC Ο γενικός γραμματέας του φορέα ζητάει: **«Δώστε μου ένα screen που να βλέπω για κάθε πολίτη ΟΛΗ του την εικόνα».**
# MAGIC
# MAGIC Έχεις τα 5 Bronze tables (από Lab 2). Τώρα φτιάχνεις το **πρώτο Gold table** του φορέα.
# MAGIC
# MAGIC ## Στόχος (ως ομάδα)
# MAGIC
# MAGIC Δημιουργήστε **`gt_lab.gold.citizen_360`** που πληροί:
# MAGIC
# MAGIC ### 📋 Απαιτήσεις
# MAGIC
# MAGIC 1. **Schema**: Μία row ανά πολίτη (μοναδικό AFM)
# MAGIC 2. **Spine**: Ξεκινήστε από `citizen_registry_raw` (LEFT JOIN από εκεί)
# MAGIC 3. **TAXIS aggregations**:
# MAGIC    - `total_tax_paid` (SUM)
# MAGIC    - `tax_declarations_count` (COUNT)
# MAGIC    - `last_tax_year` (MAX(fiscal_year))
# MAGIC 4. **EFKA aggregations**:
# MAGIC    - `total_efka_paid` (SUM employee_contribution)
# MAGIC    - `efka_periods_count`
# MAGIC 5. **KEP aggregations**:
# MAGIC    - `kep_events_total`
# MAGIC    - `avg_wait_minutes`
# MAGIC 6. **myDATA aggregations**:
# MAGIC    - `invoices_issued` (count όπου citizen = issuer)
# MAGIC    - `invoices_received` (count όπου citizen = receiver)
# MAGIC 7. **NULL handling**: Πολίτες χωρίς activity σε source → 0 ή NULL (decide + defend)
# MAGIC 8. **Save**: Delta table στο `gt_lab.gold.citizen_360`
# MAGIC
# MAGIC ### ⚠️ Επιπλέον: Discovery analysis
# MAGIC
# MAGIC Πέρα από το main table, παράγετε **3 quick analyses**:
# MAGIC - **A**: Ποιοι πολίτες έχουν tax declarations αλλά δεν είναι στο Registry; *(orphan records)*
# MAGIC - **B**: Top 5 πολίτες κατά `total_tax_paid`
# MAGIC - **C**: Distribution των `kep_events_total` ανά region
# MAGIC
# MAGIC ## 🎤 Παρουσίαση (10' ανά ομάδα)
# MAGIC
# MAGIC Στις 75' μετά την έναρξη, **2 ομάδες** παρουσιάζουν. Πρέπει να εξηγήσετε:
# MAGIC
# MAGIC 1. **Join strategy**: LEFT vs INNER vs FULL OUTER — γιατί επιλέξατε αυτό;
# MAGIC 2. **NULL handling**: 0 ή NULL για πολίτες χωρίς activity — γιατί;
# MAGIC 3. **Type alignment**: Πώς χειριστήκατε AFM types;
# MAGIC 4. **Data quality findings**: Τι ανακάλυψες με το discovery analysis;
# MAGIC 5. **Performance**: Aggregate-then-join ή join-then-aggregate;
# MAGIC
# MAGIC ## 🚫 ΔΕΝ σου δίνω
# MAGIC
# MAGIC - Skeleton του query. Πρέπει εσείς να σχεδιάσετε.
# MAGIC - Schema του gold table. Καθόρισε αυτά εσύ.
# MAGIC
# MAGIC ## ✅ Σου δίνω
# MAGIC
# MAGIC - Auto-bootstrap (Step 0) που θυμίζει catalog/schema/volume + CSV upload
# MAGIC - Helper hint cells για το πρώτο join
# MAGIC - Verification στο τέλος

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✍️ ΜΕΡΟΣ 3 — Ο ΚΩΔΙΚΑΣ ΣΑΣ
# MAGIC
# MAGIC ## Step 0 — Auto-bootstrap

# COMMAND ----------

# DBTITLE 1,Auto-bootstrap (don't edit)
CATALOG       = "gt_lab"
LANDING_PATH  = f"/Volumes/{CATALOG}/bronze/landing"
GITHUB_BASE   = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day6/data_for_students"
REQUIRED_CSVS = ["citizen_registry.csv", "taxis_declarations.csv",
                 "efka_contributions.csv", "kep_events.csv", "mydata_invoices.csv"]

def _bootstrap():
    import urllib.request, os
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    for s in ["bronze", "silver", "gold"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")
    try:
        existing = {f.name for f in dbutils.fs.ls(LANDING_PATH)}
    except: existing = set()
    missing = set(REQUIRED_CSVS) - existing
    if not missing:
        print(f"✅ Όλα ήδη στο volume"); return
    for f in missing:
        urllib.request.urlretrieve(f"{GITHUB_BASE}/{f}", f"/tmp/{f}")
        dbutils.fs.cp(f"file:/tmp/{f}", f"{LANDING_PATH}/{f}")
        os.remove(f"/tmp/{f}")
        print(f"   ✅ {f}")
_bootstrap()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Prerequisite check
# MAGIC
# MAGIC Τα 5 Bronze tables από Lab 2 πρέπει να υπάρχουν. Επιβεβαίωσε.

# COMMAND ----------

# DBTITLE 1,STEP 1 — Check Bronze tables exist
# 👇 ΓΡΑΨΕ ΕΝΑ QUERY να επιβεβαιώσεις ότι υπάρχουν 5 tables στο bronze schema
# (Αν δεν υπάρχουν, τρέξε πρώτα το Lab 2 ή χρησιμοποίησε direct CSV reads)





# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Inspect each Bronze table
# MAGIC
# MAGIC Πριν join, **κοίτα τα δεδομένα**. Senior engineer rule.

# COMMAND ----------

# DBTITLE 1,STEP 2 — Inspect schemas + sample data
# 👇 Για κάθε bronze table:
#   - printSchema()
#   - display(spark.table(name).limit(3))
# Δώσε attention σε:
#   - AFM column type (string vs int)
#   - AFM column name (afm vs citizen_afm vs issuer_afm)
#   - Cardinality (πόσοι unique AFMs;)





# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Aggregate TAXIS ανά AFM
# MAGIC
# MAGIC **Στόχος:** Από `bronze.taxis_declarations_raw` → 1 row ανά AFM με:
# MAGIC - `total_tax_paid`
# MAGIC - `tax_declarations_count`
# MAGIC - `last_tax_year`
# MAGIC
# MAGIC **Hint:** `groupBy("afm").agg(sum(...), count(...), max(...))`

# COMMAND ----------

# DBTITLE 1,STEP 3 — Aggregate TAXIS
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ
# from pyspark.sql.functions import sum as _sum, count, max as _max





# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Aggregate EFKA ανά AFM

# COMMAND ----------

# DBTITLE 1,STEP 4 — Aggregate EFKA
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ
# total_efka_paid + efka_periods_count





# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Aggregate KEP ανά AFM (citizen_afm!)
# MAGIC
# MAGIC **Προσοχή:** Στο KEP το column λέγεται `citizen_afm` όχι `afm`. Rename πριν aggregate.

# COMMAND ----------

# DBTITLE 1,STEP 5 — Aggregate KEP
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ
# Rename: df.withColumnRenamed("citizen_afm", "afm")
# Aggregate: kep_events_total + avg_wait_minutes





# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Aggregate myDATA (issued + received)
# MAGIC
# MAGIC **Πιο tricky:** Στο myDATA, κάθε invoice έχει `issuer_afm` ΚΑΙ `receiver_afm`. Ένας πολίτης είναι:
# MAGIC - **Issuer** σε κάποια invoices (τιμολόγια που κόβει)
# MAGIC - **Receiver** σε άλλες (τιμολόγια που λαμβάνει)
# MAGIC
# MAGIC Φτιάξε **2 ξεχωριστά aggregations** (issued_count, received_count) και μετά join τα πάνω σε AFM.

# COMMAND ----------

# DBTITLE 1,STEP 6 — Aggregate myDATA (διπλό aggregation)
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ
# - issued    = mydata.groupBy("issuer_afm").agg(count("*").alias("invoices_issued"))
# - received  = mydata.groupBy("receiver_afm").agg(count("*").alias("invoices_received"))
# - rename και join τα δύο (full outer ή ξεχωριστά)





# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Build citizen_360 με LEFT JOINs από Registry
# MAGIC
# MAGIC **Spine** = `bronze.citizen_registry_raw`. LEFT JOIN τα 4 aggregations.

# COMMAND ----------

# DBTITLE 1,STEP 7 — Final JOIN
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ
# citizen_360 = (registry
#     .join(taxis_agg,  on="afm", how="left")
#     .join(efka_agg,   on="afm", how="left")
#     .join(kep_agg,    on="afm", how="left")
#     .join(mydata_agg, on="afm", how="left")
# )
# Hint: AFM types πρέπει να ταιριάζουν!





# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Save στο Gold

# COMMAND ----------

# DBTITLE 1,STEP 8 — Write Gold Delta
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ
# citizen_360.write.format("delta").mode("overwrite").saveAsTable("gt_lab.gold.citizen_360")
# Optionally: ALTER TABLE με metadata
# Verify: spark.table("gt_lab.gold.citizen_360").count()





# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Discovery Analyses
# MAGIC
# MAGIC **A — Orphan tax records:** Ποιοι AFMs σε `bronze.taxis_declarations_raw` δεν υπάρχουν στο `citizen_registry_raw`;

# COMMAND ----------

# DBTITLE 1,STEP 9A — Orphan tax records
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ
# Hint: LEFT ANTI JOIN
# taxis.join(registry, "afm", "left_anti")





# COMMAND ----------

# MAGIC %md
# MAGIC **B — Top 5 taxpayers:** Top 5 πολίτες κατά `total_tax_paid`.

# COMMAND ----------

# DBTITLE 1,STEP 9B — Top 5 taxpayers
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ
# Query το citizen_360 → orderBy desc(total_tax_paid) → limit 5





# COMMAND ----------

# MAGIC %md
# MAGIC **C — KEP events ανά region:** Distribution της δραστηριότητας ΚΕΠ ανά περιοχή.

# COMMAND ----------

# DBTITLE 1,STEP 9C — KEP activity by region
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ
# Query το citizen_360 → groupBy region → agg(sum(kep_events_total))





# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎤 ΜΕΡΟΣ 4 — ΠΑΡΟΥΣΙΑΣΗ
# MAGIC
# MAGIC 2 ομάδες παρουσιάζουν (10' η κάθε μία). Δομή:
# MAGIC
# MAGIC 1. **2'** — Approach: ποιο join strategy + NULL handling
# MAGIC 2. **4'** — Live walkthrough: aggregations → final join → discovery findings
# MAGIC 3. **2'** — Surprises: data quality issue που ανακάλυψες
# MAGIC 4. **2'** — Q&A

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✅ ΜΕΡΟΣ 5 — AUTO VERIFICATION

# COMMAND ----------

# DBTITLE 1,Auto-verification
checks = []
gold = "gt_lab.gold.citizen_360"

try:
    # Check 1: Table exists
    df = spark.table(gold)
    checks.append((f"{gold} υπάρχει", True))

    # Check 2: One row per AFM (no duplicates)
    n_total = df.count()
    n_unique = df.select("afm").distinct().count()
    checks.append((f"Unique AFMs: {n_unique}/{n_total} (no dups)", n_total == n_unique))

    # Check 3: Required columns
    required = ["afm", "total_tax_paid", "tax_declarations_count",
                "total_efka_paid", "kep_events_total",
                "invoices_issued", "invoices_received"]
    cols = df.columns
    missing = [c for c in required if c not in cols]
    checks.append((f"Required columns ({len(required)} needed)", not missing))
    if missing:
        checks.append((f"  ⚠️ Missing: {missing}", False))

    # Check 4: At least one record has tax data
    n_with_tax = df.filter("total_tax_paid > 0").count()
    checks.append((f"Citizens with tax > 0: {n_with_tax}", n_with_tax >= 1))

    # Check 5: At least one record has KEP activity
    n_with_kep = df.filter("kep_events_total > 0").count()
    checks.append((f"Citizens with KEP activity: {n_with_kep}", n_with_kep >= 1))

except Exception as e:
    checks.append((f"❌ {gold} NOT FOUND or broken: {e}", False))

# Report
print("=" * 65)
print(" ✅ LAB 3 — VERIFICATION RESULTS")
print("=" * 65)
passed = sum(1 for _, ok in checks if ok)
for name, ok in checks:
    print(f"  {'✅' if ok else '❌'} {name}")
print("=" * 65)
print(f"  Score: {passed}/{len(checks)}")
if passed == len(checks):
    print("\n🎉 ΣΥΓΧΑΡΗΤΗΡΙΑ — Lab 3 ολοκληρώθηκε!")
    print("    You just built your first Gold table 🏆")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🚀 STRETCH EXERCISES

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 Stretch 1 — Citizen Risk Score
# MAGIC
# MAGIC Φτιάξε ένα απλό `citizen_risk_score` column based on:
# MAGIC - High tax + low EFKA contributions = suspicious
# MAGIC - Many KEP requests + low tax = potential issue
# MAGIC - Define your own formula!
# MAGIC
# MAGIC ### Reference
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import when, col, coalesce, lit
# MAGIC
# MAGIC df_with_risk = citizen_360.withColumn(
# MAGIC     "risk_score",
# MAGIC     when(coalesce(col("total_tax_paid"), lit(0)) > 10000, 1).otherwise(0) +
# MAGIC     when(coalesce(col("total_efka_paid"), lit(0)) == 0, 1).otherwise(0) +
# MAGIC     when(coalesce(col("kep_events_total"), lit(0)) > 5, 1).otherwise(0)
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,STRETCH 1 — Risk score
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 Stretch 2 — citizen_active_in_systems
# MAGIC
# MAGIC Πρόσθεσε ένα ARRAY column με τα συστήματα στα οποία ο πολίτης είναι active:
# MAGIC - `["TAXIS", "EFKA", "KEP", "myDATA"]` αν έχει activity σε όλα
# MAGIC
# MAGIC ### Reference
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import array_remove, array
# MAGIC
# MAGIC df_active = citizen_360.withColumn(
# MAGIC     "active_in_systems",
# MAGIC     array_remove(array(
# MAGIC         when(col("total_tax_paid") > 0, "TAXIS"),
# MAGIC         when(col("total_efka_paid") > 0, "EFKA"),
# MAGIC         when(col("kep_events_total") > 0, "KEP"),
# MAGIC         when(col("invoices_issued") + col("invoices_received") > 0, "myDATA")
# MAGIC     ), None)
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,STRETCH 2 — Active systems array
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌟 Stretch 3 — Power BI-ready view
# MAGIC
# MAGIC Φτιάξε μία **VIEW** που μασκάρει το AFM (PII):
# MAGIC ```sql
# MAGIC CREATE OR REPLACE VIEW gt_lab.gold.citizen_360_masked AS
# MAGIC SELECT
# MAGIC     CONCAT('***', RIGHT(afm, 4)) AS afm_masked,
# MAGIC     region,
# MAGIC     birth_year,
# MAGIC     -- όλες οι aggregations χωρίς PII
# MAGIC     total_tax_paid, kep_events_total, ...
# MAGIC FROM gt_lab.gold.citizen_360;
# MAGIC ```
# MAGIC
# MAGIC Analysts βλέπουν το masked view, όχι το raw table.

# COMMAND ----------

# DBTITLE 1,STRETCH 3 — Masked view
# 👇 ΓΡΑΨΕ ΤΟΝ ΚΩΔΙΚΑ ΣΑΣ


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 SUPER STRETCH — citizen_360 ως streaming Gold
# MAGIC
# MAGIC Σε production, το citizen_360 πρέπει να **ανανεώνεται live** καθώς έρχονται νέα data.
# MAGIC
# MAGIC ### Concept
# MAGIC
# MAGIC Με Delta Live Tables (DLT) ή streaming:
# MAGIC ```python
# MAGIC # Read streams from all 5 Bronze
# MAGIC # Apply same aggregations
# MAGIC # MERGE INTO gold.citizen_360 incrementally
# MAGIC ```
# MAGIC
# MAGIC Δοκίμασε να σχεδιάσεις σε χαρτί το pipeline (δεν χρειάζεται να το υλοποιήσεις σήμερα — Day 4 streaming material).

# COMMAND ----------

# DBTITLE 1,SUPER STRETCH — Design notes (σε comments)
# 👇 ΓΡΑΨΕ TO PSEUDOCODE σε comments





# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎓 Bonus Discussion Questions
# MAGIC
# MAGIC Όταν ολοκληρώσετε ως ομάδα, σκεφτείτε:
# MAGIC
# MAGIC 1. **Aggregate vs join order**: Γιατί πρώτα aggregate και μετά join; Τι θα γινόταν αντίθετα;
# MAGIC
# MAGIC 2. **NULL semantics**: Citizen χωρίς tax_declarations: `total_tax_paid = NULL` ή `0`; Trade-offs;
# MAGIC
# MAGIC 3. **Schema evolution**: Αν αύριο προστεθεί ΕΛΑΣ data, πώς θα τα ενσωματώσεις;
# MAGIC
# MAGIC 4. **Performance**: Αν citizens είναι 10M αντί για 9, ποιες αλλαγές; *(broadcast, partitioning, etc.)*
# MAGIC
# MAGIC 5. **Privacy**: Citizen 360 = high PII concentration. Πώς το προστατεύεις;
# MAGIC
# MAGIC ## 🔗 Επόμενο
# MAGIC
# MAGIC Day 2: Bronze → Silver με quality checks. Day 6: Capstone — Citizen 360 ως end-to-end DLT pipeline + Job με monitoring.
