# Databricks notebook source
# MAGIC %md
# MAGIC # Πρακτική Άσκηση — Train/Test Split & Data Leakage Detection
# MAGIC **Ρόλος: Μηχανικοί Δεδομένων**
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4/TrainTest_Leakage_Notebook.py
# MAGIC > ```
# MAGIC
# MAGIC **Σενάριο**: σας δίνεται ιστορικό αιτήσεων ΚΕΠ (10K rows, 12 μήνες). Πρέπει να
# MAGIC ετοιμάσετε **σωστό train/test split χωρίς leakage** και να εντοπίσετε ποια
# MAGIC features περιέχουν *future information* πριν παραδώσετε το dataset στον DS.
# MAGIC
# MAGIC **Τι θα μάθετε:**
# MAGIC - Quick profiling (row count, schema, date range)
# MAGIC - **Temporal split** (όχι random!) για time-series ML
# MAGIC - **Correlation analysis**: ποια features έχουν ύποπτα υψηλή συσχέτιση με target
# MAGIC - Function `check_temporal_leakage()` που εντοπίζει features με future timestamps
# MAGIC - **Leakage report**: features προς αφαίρεση + αιτιολογία
# MAGIC
# MAGIC **Διάρκεια:** ~25'
# MAGIC **Περιβάλλον:** Databricks Free Edition (Serverless)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 1: Download Dataset & Quick Profiling
# MAGIC
# MAGIC Κατεβάζουμε το `kep_requests.csv` (10.000 αιτήσεις από ΚΕΠ, 12 μήνες δεδομένων).

# COMMAND ----------

import urllib.request
import os

# Unity Catalog Volume path (αντί για /tmp).
# Προαπαίτηση: catalog `aade` με schema `default` και volume `lab_data` (managed).
# Αν δεν υπάρχει, δημιουργείστε το με:
#   CREATE CATALOG IF NOT EXISTS aade;
#   CREATE SCHEMA  IF NOT EXISTS aade.default;
#   CREATE VOLUME  IF NOT EXISTS aade.default.lab_data;
volume_dir = "/Volumes/aade/default/lab_data"
os.makedirs(volume_dir, exist_ok=True)

url = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4/kep_requests.csv"
local = f"{volume_dir}/kep_requests.csv"
urllib.request.urlretrieve(url, local)
print(f"✓ Downloaded to Volume: {local}")

# COMMAND ----------

from pyspark.sql.functions import col, count, when, min as spark_min, max as spark_max, datediff, lit

df = spark.read.option("header", "true").option("inferSchema", "true").csv(local)

print("=== Profiling ===")
print(f"Rows:    {df.count():,}")
print(f"Columns: {len(df.columns)}")
print(f"\nSchema:")
df.printSchema()

# Date range
print("=== Date range ===")
df.select(
    spark_min("request_timestamp").alias("min_date"),
    spark_max("request_timestamp").alias("max_date"),
).show(truncate=False)

print("=== Sample rows ===")
df.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 2: Target & Initial Look
# MAGIC
# MAGIC **Target** = `audit_outcome` (passed / failed / pending).
# MAGIC Σκοπός μοντέλου: predict το audit_outcome **πριν** ολοκληρωθεί ο έλεγχος.

# COMMAND ----------

print("=== Target distribution ===")
df.groupBy("audit_outcome").count().show()

print("=== Service types ===")
df.groupBy("service_type").count().orderBy(col("count").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 3: Temporal Split (ΟΧΙ Random!)
# MAGIC
# MAGIC **Κανόνας**: σε time-series ML, **ΠΟΤΕ random split**. Ο λόγος:
# MAGIC με random split το μοντέλο «βλέπει» δεδομένα του μέλλοντος στο training,
# MAGIC κάτι που δεν θα συμβαίνει σε production.
# MAGIC
# MAGIC **Σωστή προσέγγιση**: cutoff date.
# MAGIC - **Training**: αιτήσεις ΠΡΙΝ 2024-01-01
# MAGIC - **Test**: αιτήσεις ΑΠΟ 2024-01-01

# COMMAND ----------

cutoff = "2024-01-01"
train = df.filter(col("request_timestamp") < cutoff)
test = df.filter(col("request_timestamp") >= cutoff)

print(f"=== Split στο cutoff={cutoff} ===")
print(f"Train rows: {train.count():,}")
print(f"Test rows:  {test.count():,}")
print(f"\nTrain date range:")
train.select(spark_min("request_timestamp"), spark_max("request_timestamp")).show(truncate=False)
print(f"Test date range:")
test.select(spark_min("request_timestamp"), spark_max("request_timestamp")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 4: Correlation Analysis — Detection #1
# MAGIC
# MAGIC Υπολογίζουμε correlation κάθε numerical feature με το target.
# MAGIC
# MAGIC **Κανόνας**: αν \|corr\| > 0.95 → **ύποπτο για leakage**.
# MAGIC
# MAGIC Λογική: στατιστικά είναι σχεδόν αδύνατο ένα legitimate feature να έχει
# MAGIC τόσο τέλεια σχέση με το target. Συνήθως σημαίνει ότι το feature **είναι** το
# MAGIC target ή derives από αυτό.

# COMMAND ----------

# Convert audit_outcome σε numeric για correlation analysis
from pyspark.sql.functions import when as F_when

df_corr = df.withColumn(
    "target_numeric",
    F_when(col("audit_outcome") == "passed", 1)
    .when(col("audit_outcome") == "failed", 0)
    .otherwise(None)
).filter(col("target_numeric").isNotNull())

# Numerical features (συμπεριλαμβανομένων ύποπτων)
numeric_features = ["documents_submitted", "wait_time_minutes", "final_decision_amount"]

print("=== Correlation με target (audit_outcome) ===\n")
for feat in numeric_features:
    df_valid = df_corr.filter(col(feat).isNotNull())
    if df_valid.count() == 0:
        print(f"  {feat:30s}: no valid values")
        continue
    corr_value = df_valid.stat.corr(feat, "target_numeric")
    flag = "  ⚠️ ΥΠΟΠΤΟ" if abs(corr_value) > 0.95 else ""
    print(f"  {feat:30s}: {corr_value:+.4f}{flag}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 5: Temporal Leakage Check — Detection #2
# MAGIC
# MAGIC Λειτουργία `check_temporal_leakage(df)` που επιστρέφει features όπου:
# MAGIC
# MAGIC `max(feature_timestamp) > target_timestamp`
# MAGIC
# MAGIC Δηλαδή features που είναι «**στο μέλλον**» σε σχέση με το event που προβλέπουμε.
# MAGIC
# MAGIC Στην περίπτωσή μας:
# MAGIC - **Event time**: `request_timestamp` (πότε υποβλήθηκε η αίτηση)
# MAGIC - **Suspicious feature time**: `audit_completion_date` (πότε ολοκληρώθηκε ο έλεγχος)
# MAGIC
# MAGIC Φυσικά το `audit_completion_date` είναι **πάντα μετά** το `request_timestamp` →
# MAGIC είναι **temporal leakage**.

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

def check_temporal_leakage(df, event_time_col, candidate_time_cols):
    """
    Επιστρέφει features όπου max(feature_timestamp) > event_timestamp.
    Αυτά είναι temporal leakage candidates.
    """
    leakage_features = []
    for c in candidate_time_cols:
        # Πόσες γραμμές έχουν feature timestamp > event timestamp;
        df_check = df.filter(
            col(c).isNotNull() & col(event_time_col).isNotNull()
        )
        future_count = df_check.filter(
            to_timestamp(col(c)) > to_timestamp(col(event_time_col))
        ).count()
        total = df_check.count()
        if future_count > 0:
            pct = (future_count / total) * 100 if total > 0 else 0
            leakage_features.append({
                "feature": c,
                "rows_in_future": future_count,
                "pct_in_future": round(pct, 2),
            })
    return leakage_features


# Έλεγχος
candidate_time_cols = ["audit_completion_date"]
result = check_temporal_leakage(df, "request_timestamp", candidate_time_cols)

print("=== Temporal Leakage Detection ===\n")
if result:
    for r in result:
        print(f"  ⚠️ {r['feature']}: {r['rows_in_future']:,} rows ({r['pct_in_future']}%) είναι ΣΤΟ ΜΕΛΛΟΝ vs request_timestamp")
        print(f"     → TEMPORAL LEAKAGE")
else:
    print("  ✓ Καμία temporal leakage feature")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6: Leakage Report
# MAGIC
# MAGIC Τελικό report με features προς **αφαίρεση** + αιτιολογία.
# MAGIC
# MAGIC **Κλασικά leakage patterns** (από το slide):
# MAGIC - `final_decision_amount` (= final_decision)
# MAGIC - `audit_completion_date`
# MAGIC - `closure_reason`
# MAGIC - οποιοδήποτε `audit_*` field
# MAGIC
# MAGIC Αν εμφανίζονται στο training, **το μοντέλο "βλέπει το μέλλον"**.

# COMMAND ----------

# Final report
print("=" * 70)
print("LEAKAGE REPORT — Features προς ΑΦΑΙΡΕΣΗ")
print("=" * 70)

leakage_findings = [
    {
        "feature": "audit_completion_date",
        "reason": "Timestamp ΜΕΤΑ το request — δεν είναι διαθέσιμο τη στιγμή της πρόβλεψης",
        "type": "TEMPORAL LEAKAGE",
    },
    {
        "feature": "final_decision_amount",
        "reason": "Παράγεται ΜΕΤΑ τον έλεγχο — derives από το audit_outcome",
        "type": "TARGET LEAKAGE",
    },
    {
        "feature": "closure_reason",
        "reason": "Διαθέσιμο μόνο μετά το κλείσιμο της αίτησης — future info",
        "type": "TEMPORAL + TARGET LEAKAGE",
    },
]

for f in leakage_findings:
    print(f"\n❌ {f['feature']}")
    print(f"   Type:  {f['type']}")
    print(f"   Why:   {f['reason']}")

print("\n" + "=" * 70)
print("✓ ΑΣΦΑΛΗ FEATURES (legitimate):")
print("=" * 70)
safe_features = [
    "request_timestamp (event time)",
    "service_type",
    "documents_submitted (γνωστό κατά την υποβολή)",
    "wait_time_minutes (γνωστό όταν ξεκινάει service)",
    "citizen_id",
]
for s in safe_features:
    print(f"   ✓ {s}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 7: Καθαρό Dataset για παράδοση στον DS
# MAGIC
# MAGIC Φτιάχνουμε **clean version** χωρίς τις leakage features και κάνουμε
# MAGIC **temporal split** για να την παραδώσουμε στον Data Scientist.

# COMMAND ----------

leakage_cols = ["audit_completion_date", "final_decision_amount", "closure_reason"]

df_clean = df.drop(*leakage_cols)
print("=== Clean dataset schema ===")
df_clean.printSchema()

# Temporal split
train_clean = df_clean.filter(col("request_timestamp") < cutoff)
test_clean = df_clean.filter(col("request_timestamp") >= cutoff)

print(f"\n=== Final splits για παράδοση στον DS ===")
print(f"Train rows: {train_clean.count():,}")
print(f"Test rows:  {test_clean.count():,}")

# Save στο Unity Catalog (catalog `aade`, schema `default`)
# Σχόλιο: αν τρέχετε σε Free Edition / preview χωρίς δικαιώματα δημιουργίας tables,
# αφήστε τα παρακάτω σχολιασμένα και χρησιμοποιήστε τα DataFrames in-memory.
# train_clean.write.format("delta").mode("overwrite").saveAsTable("aade.default.kep_train_clean")
# test_clean.write.format("delta").mode("overwrite").saveAsTable("aade.default.kep_test_clean")

print("\n✓ Dataset έτοιμο για ML training (χωρίς leakage)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση
# MAGIC
# MAGIC ### Τι μάθατε:
# MAGIC 1. **Quick profiling** πριν κάνετε οτιδήποτε άλλο (row count, schema, date range)
# MAGIC 2. **Temporal split** (όχι random) για time-series ML
# MAGIC 3. **Correlation analysis**: |corr| > 0.95 = ύποπτο για leakage
# MAGIC 4. **`check_temporal_leakage()`**: εντοπισμός features με future timestamps
# MAGIC 5. **Leakage report** με features προς αφαίρεση + αιτιολογία
# MAGIC
# MAGIC ### Τα 3 leakage features που εντοπίσαμε:
# MAGIC | Feature | Type | Γιατί |
# MAGIC |---|---|---|
# MAGIC | `audit_completion_date` | Temporal | Timestamp μετά το event |
# MAGIC | `final_decision_amount` | Target | Παράγεται μετά τον έλεγχο |
# MAGIC | `closure_reason` | Temporal + Target | Future info |
# MAGIC
# MAGIC ### Bridge:
# MAGIC Με αυτό το καθαρό dataset, ο **Data Scientist** μπορεί να εκπαιδεύσει μοντέλο
# MAGIC που δεν θα «τσιμπήσει» από data leakage. Επόμενο βήμα: feature engineering +
# MAGIC training (βλ. `Feature_Engineering_Notebook.py` και `ML_Model_Notebook.py`).
