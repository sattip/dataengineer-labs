# Databricks notebook source
# MAGIC %md
# MAGIC # 🛠️ Πρακτική Άσκηση — Train/Test Split & Data Leakage Detection
# MAGIC
# MAGIC **Ρόλος:** Μηχανικός Δεδομένων (Data Engineer) στην ΑΑΔΕ
# MAGIC **Διάρκεια:** ~25'
# MAGIC **Περιβάλλον:** Databricks Free Edition (Serverless) με Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Στόχος της άσκησης
# MAGIC
# MAGIC Σε αυτή την άσκηση **μπαίνετε στον ρόλο του Data Engineer** που ετοιμάζει ένα dataset
# MAGIC για να το παραδώσει στον Data Scientist της ομάδας. Ο **βασικός σας στόχος** είναι ένας
# MAGIC και πολύ συγκεκριμένος:
# MAGIC
# MAGIC > **Να εντοπίσετε και να αφαιρέσετε όλα τα features που "βλέπουν το μέλλον" (data leakage),
# MAGIC > και να φτιάξετε ένα σωστό temporal train/test split — ώστε το μοντέλο που θα εκπαιδευτεί
# MAGIC > να δουλεύει σωστά και στην παραγωγή, όχι μόνο στο laptop του Data Scientist.**
# MAGIC
# MAGIC ## 🧭 Πραγματικό σενάριο
# MAGIC
# MAGIC Σας δίνεται **ιστορικό αιτήσεων ΚΕΠ** (`kep_requests.csv`, **10.000 αιτήσεις, 12 μήνες**
# MAGIC δεδομένων). Η ομάδα Data Science θέλει να φτιάξει μοντέλο που, **τη στιγμή που έρχεται μια
# MAGIC νέα αίτηση**, να προβλέπει αν θα περάσει τον έλεγχο ή όχι (`audit_outcome`).
# MAGIC
# MAGIC Το πρόβλημα είναι ότι το dataset έχει **κρυμμένες παγίδες**: στήλες που γέμισαν *μετά*
# MAGIC από τον έλεγχο. Αν αυτές μπουν στο μοντέλο, στο test θα δείτε εντυπωσιακή ακρίβεια — και
# MAGIC στο production θα γκρεμιστεί. **Δουλειά σας:** να μην αφήσετε αυτό να συμβεί.
# MAGIC
# MAGIC ## ❓ Γιατί έχει σημασία (το πιο σημαντικό slide της ημέρας)
# MAGIC
# MAGIC Το **data leakage είναι ο νούμερο ένα δολοφόνος** των ML μοντέλων στην παραγωγή.
# MAGIC Συμβαίνει σχεδόν σε όλους όσους ξεκινούν με ML, και δυστυχώς το παρατηρεί κανείς πολύ
# MAGIC αργά — όταν η Διοίκηση ρωτάει «μα γιατί δεν δουλεύει το μοντέλο που μας δείξατε στο pilot;».
# MAGIC Σε δημόσιο φορέα όπως η ΑΑΔΕ, αυτό μπορεί να σημαίνει:
# MAGIC
# MAGIC - Άδικους ελέγχους σε φορολογούμενους (false positives)
# MAGIC - Χαμένα πραγματικά κρούσματα (false negatives)
# MAGIC - Νομικές προσφυγές που δεν μπορείτε να υπερασπιστείτε
# MAGIC - Πτώση εμπιστοσύνης στην ίδια την έννοια του AI στο Δημόσιο
# MAGIC
# MAGIC ## 📋 Τα 5 βήματα της άσκησης
# MAGIC
# MAGIC | # | Βήμα | Τι κάνουμε |
# MAGIC |---|---|---|
# MAGIC | 1 | **Profiling** | Φορτώνουμε το dataset σε PySpark DataFrame και κάνουμε quick profiling: row count, schema, date range. *Μάντεψε σωστά πριν επιλύσεις.* |
# MAGIC | 2 | **Temporal Split** | Training = αιτήσεις πριν την 2024-01-01, test = μετά. **ΠΟΤΕ random split** σε time-series ML. |
# MAGIC | 3 | **Correlation Analysis** | Υπολογίζουμε correlation κάθε feature με το target (`audit_outcome`). Όποιο έχει `\|corr\| > 0.95` → **ύποπτο για leakage**. |
# MAGIC | 4 | **`check_temporal_leakage()`** | Γράφουμε function που επιστρέφει features όπου `max(feature_timestamp) > target_timestamp`. |
# MAGIC | 5 | **Leakage Report** | Παράγουμε αναφορά: features προς αφαίρεση + αιτιολογία. Φτιάχνουμε καθαρά Delta tables για παράδοση στον DS. |
# MAGIC
# MAGIC > **💡 Tip από το slide:** Τα κλασικά leakage patterns που θα συναντήσετε είναι
# MAGIC > `final_decision`, `fine_amount`, `audit_completion_date`, `closure_reason`. Αν εμφανίζονται
# MAGIC > στο training set, **το μοντέλο "βλέπει το μέλλον"**.
# MAGIC
# MAGIC ## 📦 Παραδοτέα στο τέλος
# MAGIC
# MAGIC - **Volume**: `/Volumes/workspace/aade/aade_data/kep_requests.csv` (το raw dataset)
# MAGIC - **Delta Table**: `workspace.aade.kep_train_clean` (training set, χωρίς leakage)
# MAGIC - **Delta Table**: `workspace.aade.kep_test_clean` (test set, χωρίς leakage)
# MAGIC - **Leakage Report** (printed στο notebook output)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 0: Setup — Unity Catalog Volume
# MAGIC
# MAGIC Πριν κατεβάσουμε το dataset, χρειαζόμαστε ένα **Unity Catalog Volume** για να το
# MAGIC αποθηκεύσουμε. Στο Databricks το Volume είναι το προτεινόμενο σημείο για αρχεία
# MAGIC (αντί για το παλιό `/tmp` ή `/dbfs`), γιατί:
# MAGIC
# MAGIC - **Persists** ανάμεσα σε cluster restarts
# MAGIC - **Audit-loggable** μέσω Unity Catalog (ποιος διάβασε τι, πότε)
# MAGIC - **Permission-controlled** με GRANT statements
# MAGIC - **Καθαρό path** (`/Volumes/<catalog>/<schema>/<volume>/...`)
# MAGIC
# MAGIC Οι παρακάτω εντολές είναι **idempotent** — αν τα objects υπάρχουν ήδη, δεν σπάνε.

# COMMAND ----------

import urllib.request
import os

# Unity Catalog Volume setup (αντί για /tmp).
# Δημιουργία schema/volume στο default `workspace` catalog (idempotent).
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

volume_dir = "/Volumes/workspace/aade/aade_data"
os.makedirs(volume_dir, exist_ok=True)

print(f"✓ Schema:  workspace.aade")
print(f"✓ Volume:  workspace.aade.aade_data")
print(f"✓ Path:    {volume_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 1: Download Dataset & Quick Profiling
# MAGIC
# MAGIC ### 1α. Κατεβάζουμε το CSV στο Volume
# MAGIC
# MAGIC Κατεβάζουμε το `kep_requests.csv` (10.000 αιτήσεις από ΚΕΠ, 12 μήνες δεδομένων)
# MAGIC απευθείας από το GitHub στο Unity Catalog Volume.

# COMMAND ----------

url = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4/kep_requests.csv"
local = f"{volume_dir}/kep_requests.csv"
urllib.request.urlretrieve(url, local)
print(f"✓ Downloaded to Volume: {local}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1β. Φόρτωση σε PySpark DataFrame & Profiling
# MAGIC
# MAGIC **Πριν αγγίξουμε οτιδήποτε άλλο**, κάνουμε *quick profiling*. Αυτό είναι ο
# MAGIC πιο σημαντικός κανόνας του Data Engineering, και είναι αυτός που χωρίζει τους
# MAGIC junior από τους senior:
# MAGIC
# MAGIC > **Πάντα profiling πριν αναλύσεις. Μάθε με τι δουλεύεις πριν αρχίσεις να αποφασίζεις.**
# MAGIC
# MAGIC Τι κοιτάμε:
# MAGIC - **Row count**: είναι όσες περιμέναμε; (10.000)
# MAGIC - **Schema**: τι στήλες έχουμε, τι τύπους; Υπάρχει κάτι περίεργο;
# MAGIC - **Date range**: ποια περίοδο καλύπτει; (αυτό θα μας οδηγήσει στο cutoff date στο Βήμα 2)
# MAGIC - **Sample rows**: τι μοιάζουν τα δεδομένα στην πραγματικότητα;

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
# MAGIC **🔍 Παρατηρήσεις από το profiling:**
# MAGIC
# MAGIC - Έχουμε **10.000 αιτήσεις** σε 12 μήνες δεδομένα (από Ιαν 2023 έως Δεκ 2024)
# MAGIC - Υπάρχουν δύο timestamp στήλες: `request_timestamp` (πότε ήρθε η αίτηση) και
# MAGIC   `audit_completion_date` (πότε ολοκληρώθηκε ο έλεγχος). Σημειώστε αυτή τη διαφορά
# MAGIC   — θα γίνει κρίσιμη στο Βήμα 5.
# MAGIC - Υπάρχουν στήλες όπως `final_decision_amount` και `closure_reason` που "**μυρίζουν**"
# MAGIC   ύποπτες από τώρα. Θα τις εξετάσουμε με προσοχή παρακάτω.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 2: Target & Initial Look
# MAGIC
# MAGIC Πριν φτιάξουμε split, πρέπει να καταλάβουμε **τι προβλέπουμε**.
# MAGIC
# MAGIC - **Target column** = `audit_outcome` με τιμές `passed` / `flagged` / `rejected`
# MAGIC   - `passed`: η αίτηση πέρασε ομαλά τον έλεγχο
# MAGIC   - `flagged`: σημάνθηκε για επιπλέον έλεγχο (μπορεί τελικά να εγκριθεί ή όχι)
# MAGIC   - `rejected`: απορρίφθηκε
# MAGIC - **Στόχος του ML μοντέλου**: τη στιγμή που έρχεται μια νέα αίτηση, να προβλέψει
# MAGIC   το audit_outcome **πριν** ολοκληρωθεί ο έλεγχος
# MAGIC - **Class balance**: είναι σημαντικό να δούμε αν το dataset είναι balanced ή imbalanced
# MAGIC - **Binary target**: για το correlation analysis θα ομαδοποιήσουμε σε
# MAGIC   `passed` (=1, OK) έναντι `flagged ή rejected` (=0, problematic)

# COMMAND ----------

print("=== Target distribution ===")
df.groupBy("audit_outcome").count().show()

print("=== Service types ===")
df.groupBy("service_type").count().orderBy(col("count").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **🔍 Τι μάθαμε:**
# MAGIC
# MAGIC - Έχουμε τρεις κλάσεις: `passed`, `failed`, `pending`. Οι `pending` αιτήσεις δεν
# MAGIC   έχουν ολοκληρωμένο έλεγχο ακόμα — θα τις χειριστούμε ξεχωριστά (πιθανώς θα τις
# MAGIC   πετάξουμε από το training set).
# MAGIC - Οι **service types** μας λένε τι είδους αιτήσεις έχουμε (διαβατήρια, πιστοποιητικά,
# MAGIC   κ.λπ.) — αυτό μπορεί να γίνει χρήσιμο feature αργότερα.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 3: Temporal Split (ΟΧΙ Random!)
# MAGIC
# MAGIC ### 3α. Ο χρυσός κανόνας
# MAGIC
# MAGIC > **Σε time-series ML, ΠΟΤΕ random split. Πάντα temporal split.**
# MAGIC
# MAGIC ### 3β. Γιατί;
# MAGIC
# MAGIC Φανταστείτε ότι κάνετε random 80/20 split. Το μοντέλο εκπαιδεύεται σε δεδομένα
# MAGIC από όλο το έτος — και μετά το τεστάρετε σε άλλα δεδομένα από το **ίδιο έτος**.
# MAGIC Δηλαδή το μοντέλο "βλέπει" δεδομένα του Δεκεμβρίου ενώ προβλέπει για τον Μάιο.
# MAGIC Αυτό **δεν συμβαίνει στην παραγωγή** — εκεί το μοντέλο πάντα προβλέπει το μέλλον,
# MAGIC χωρίς να ξέρει τι θα γίνει.
# MAGIC
# MAGIC Το αποτέλεσμα: στο test βλέπετε 95% accuracy, στο production 60%. Ο Data Scientist
# MAGIC σας θα κατηγορηθεί άδικα.
# MAGIC
# MAGIC ### 3γ. Σωστή προσέγγιση: cutoff date
# MAGIC
# MAGIC Διαλέγουμε μια ημερομηνία (π.χ. **2024-01-01**) και:
# MAGIC - **Training**: ότι ήρθε ΠΡΙΝ από αυτή την ημερομηνία
# MAGIC - **Test**: ότι ήρθε ΑΠΟ αυτή την ημερομηνία και μετά
# MAGIC
# MAGIC Έτσι μιμούμαστε ακριβώς αυτό που γίνεται στην παραγωγή: το μοντέλο μαθαίνει από
# MAGIC το παρελθόν και προβλέπει το μέλλον.

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
# MAGIC **🔍 Παρατηρήστε:**
# MAGIC
# MAGIC - Το train set έχει αιτήσεις **μόνο** από Ιαν–Δεκ 2023
# MAGIC - Το test set έχει αιτήσεις **μόνο** από Ιαν 2024 και μετά
# MAGIC - **Καμία επικάλυψη** στο χρόνο. Όπως ακριβώς θα είναι στην παραγωγή.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 4: Correlation Analysis — Detection Mechanism #1
# MAGIC
# MAGIC ### 4α. Η λογική
# MAGIC
# MAGIC Υπολογίζουμε correlation κάθε numerical feature με το binary target
# MAGIC (1 = passed, 0 = flagged/rejected).
# MAGIC
# MAGIC ### 4β. Πρακτικά thresholds (χρησιμοποιήστε και τα τρία επίπεδα)
# MAGIC
# MAGIC | `|corr|` | Ερμηνεία | Action |
# MAGIC |---|---|---|
# MAGIC | > 0.95 | **Σχεδόν σίγουρα leakage** — το feature είναι το target μεταμφιεσμένο | Drop |
# MAGIC | 0.5 – 0.95 | **Ισχυρή υποψία** — ελέγξτε με domain knowledge και temporal check | Investigate |
# MAGIC | < 0.5 | Πιθανώς legitimate | Keep (αλλά πάντα verify) |
# MAGIC
# MAGIC ### 4γ. Γιατί δουλεύει
# MAGIC
# MAGIC Στατιστικά είναι **σχεδόν αδύνατο** ένα legitimate feature να έχει πολύ τέλεια
# MAGIC σχέση με το target. Όταν βλέπετε corr 0.99, υπάρχουν δύο σενάρια:
# MAGIC
# MAGIC 1. Το feature **είναι** το target μεταμφιεσμένο (target leakage)
# MAGIC 2. Το feature **derives από το target** (π.χ. υπολογίστηκε *μετά* τον έλεγχο)
# MAGIC
# MAGIC Και τα δύο είναι κακά. Σε production, αυτό το feature **δεν θα υπάρχει**.
# MAGIC
# MAGIC ### 4γ. Σημαντικό caveat
# MAGIC
# MAGIC Η correlation ανίχνευση πιάνει μόνο **numerical** leakage. Δεν θα πιάσει string
# MAGIC features όπως το `closure_reason` (που είναι κι αυτό leakage). Γι' αυτό χρειάζεται
# MAGIC και το temporal check στο Βήμα 5.

# COMMAND ----------

# Convert audit_outcome σε binary numeric (1 = passed, 0 = flagged/rejected) για correlation
from pyspark.sql.functions import when as F_when

df_corr = df.withColumn(
    "target_numeric",
    F_when(col("audit_outcome") == "passed", 1.0)
    .when(col("audit_outcome").isin("flagged", "rejected"), 0.0)
    .otherwise(None)
).filter(col("target_numeric").isNotNull())

# Cast features σε double ρητά (αποφυγή inferSchema surprises)
# και ελέγχουμε διακύμανση πριν το correlation
numeric_features = ["documents_submitted", "wait_time_minutes", "final_decision_amount"]

# Sanity check: δείχνουμε class balance και non-null counts
print("=== Pre-correlation sanity check ===")
print(f"Total rows με valid target: {df_corr.count():,}")
df_corr.groupBy("target_numeric").count().show()

print("=== Correlation με target (1=passed, 0=flagged/rejected) ===\n")
for feat in numeric_features:
    df_valid = df_corr.select(
        col(feat).cast("double").alias(feat),
        col("target_numeric"),
    ).filter(col(feat).isNotNull())

    n = df_valid.count()
    if n == 0:
        print(f"  {feat:30s}: no valid values")
        continue

    # Έλεγχος ότι υπάρχει διακύμανση (αλλιώς το corr είναι NaN)
    distinct_feat = df_valid.select(feat).distinct().count()
    if distinct_feat < 2:
        print(f"  {feat:30s}: σταθερή τιμή (no variance) — corr undefined")
        continue

    corr_value = df_valid.stat.corr(feat, "target_numeric")
    abs_c = abs(corr_value)
    if abs_c > 0.95:
        flag = "  ⚠️ ΣΧΕΔΟΝ ΣΙΓΟΥΡΑ LEAKAGE"
    elif abs_c > 0.5:
        flag = "  ⚠️ ΙΣΧΥΡΗ ΥΠΟΨΙΑ — investigate"
    else:
        flag = ""
    print(f"  {feat:30s}: corr={corr_value:+.4f}  (n={n:,}){flag}")

# COMMAND ----------

# MAGIC %md
# MAGIC **🔍 Ερμηνεία αποτελεσμάτων:**
# MAGIC
# MAGIC - `documents_submitted` και `wait_time_minutes` έχουν **σχεδόν μηδενική** correlation
# MAGIC   (~0.00). Αυτά είναι legitimate features — τα γνωρίζουμε τη στιγμή της υποβολής της αίτησης.
# MAGIC - `final_decision_amount` έχει **ισχυρή αρνητική** correlation (γύρω στο −0.70). Πέφτει
# MAGIC   στη δεύτερη ζώνη ("ισχυρή υποψία") και απαιτεί διερεύνηση. Στην πραγματικότητα είναι
# MAGIC   το ποσό προστίμου / τέλους που υπολογίζεται **μετά** τον έλεγχο. Δεν θα είναι διαθέσιμο
# MAGIC   τη στιγμή της πρόβλεψης. **Πετάμε.**
# MAGIC
# MAGIC > **💡 Σημείωση:** Στην πραγματικότητα τα leakage features δεν χτυπάνε πάντα 0.99.
# MAGIC > Συχνά κρύβονται γύρω στο 0.5–0.85, επειδή ο θόρυβος και τα null τα αραιώνουν. Γι'
# MAGIC > αυτό **το correlation από μόνο του δεν φτάνει** — το συνδυάζουμε πάντα με temporal check
# MAGIC > και domain knowledge (Βήμα 5).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 5: Temporal Leakage Check — Detection Mechanism #2
# MAGIC
# MAGIC ### 5α. Η λογική
# MAGIC
# MAGIC Γράφουμε function `check_temporal_leakage(df)` που επιστρέφει features όπου:
# MAGIC
# MAGIC ```
# MAGIC max(feature_timestamp) > target_timestamp
# MAGIC ```
# MAGIC
# MAGIC Δηλαδή features που έχουν τιμές **"στο μέλλον"** σε σχέση με το event που προβλέπουμε.
# MAGIC
# MAGIC ### 5β. Στην περίπτωσή μας
# MAGIC
# MAGIC - **Event time**: `request_timestamp` (πότε υποβλήθηκε η αίτηση — η στιγμή της πρόβλεψης)
# MAGIC - **Suspicious feature time**: `audit_completion_date` (πότε ολοκληρώθηκε ο έλεγχος)
# MAGIC
# MAGIC Φυσικά το `audit_completion_date` είναι **πάντα μετά** το `request_timestamp` —
# MAGIC δεν γίνεται να ολοκληρωθεί ο έλεγχος πριν υποβληθεί η αίτηση. Άρα:
# MAGIC
# MAGIC > **`audit_completion_date` είναι temporal leakage και πρέπει να φύγει.**
# MAGIC
# MAGIC ### 5γ. Γιατί η function είναι reusable
# MAGIC
# MAGIC Σε πραγματικά project θα έχετε **δεκάδες** timestamp columns. Η function τις
# MAGIC ελέγχει όλες αυτόματα και επιστρέφει αναφορά. Αντί να το κάνετε χειροκίνητα.

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

def check_temporal_leakage(df, event_time_col, candidate_time_cols):
    """
    Επιστρέφει features όπου max(feature_timestamp) > event_timestamp.
    Αυτά είναι temporal leakage candidates.

    Args:
        df: PySpark DataFrame
        event_time_col: Η στήλη που περιέχει το event timestamp (π.χ. "request_timestamp")
        candidate_time_cols: Λίστα με στήλες προς έλεγχο
    Returns:
        Λίστα από dicts με {feature, rows_in_future, pct_in_future}
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
# MAGIC **🔍 Αποτέλεσμα:**
# MAGIC
# MAGIC Όπως ήταν αναμενόμενο, **το 100% των γραμμών** του `audit_completion_date` είναι στο
# MAGIC μέλλον σε σχέση με το `request_timestamp`. Αυτό αποδεικνύει ότι είναι temporal leakage
# MAGIC και πρέπει να αφαιρεθεί.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 6: Leakage Report
# MAGIC
# MAGIC ### 6α. Σύνοψη ευρημάτων
# MAGIC
# MAGIC Συγκεντρώνουμε όλα τα ευρήματά μας σε ένα δομημένο **report** που μπορούμε να
# MAGIC παραδώσουμε στον Data Scientist και στη Διοίκηση.
# MAGIC
# MAGIC ### 6β. Κλασικά leakage patterns
# MAGIC
# MAGIC Από το slide:
# MAGIC - `final_decision_amount` (= final_decision)
# MAGIC - `audit_completion_date`
# MAGIC - `closure_reason`
# MAGIC - οποιοδήποτε `audit_*` field
# MAGIC
# MAGIC Αν εμφανίζονται στο training, **το μοντέλο "βλέπει το μέλλον"**. Σε production
# MAGIC αυτές οι στήλες δεν θα υπάρχουν τη στιγμή της πρόβλεψης.

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
# MAGIC ## 🪜 Βήμα 7: Καθαρό Dataset για παράδοση στον DS
# MAGIC
# MAGIC ### 7α. Drop τα leakage features
# MAGIC
# MAGIC Αφαιρούμε τις τρεις προβληματικές στήλες και κρατάμε μόνο legitimate features.
# MAGIC
# MAGIC ### 7β. Temporal split στο καθαρό dataset
# MAGIC
# MAGIC Ξανακάνουμε temporal split — αυτή τη φορά πάνω στο **καθαρό** dataset.
# MAGIC
# MAGIC ### 7γ. Persist σε Delta tables
# MAGIC
# MAGIC Σώζουμε ως **Delta tables** στο Unity Catalog, ώστε ο Data Scientist να μπορεί
# MAGIC να τα φορτώσει με ένα απλό `spark.table("workspace.aade.kep_train_clean")`.
# MAGIC
# MAGIC **Γιατί Delta και όχι Parquet:**
# MAGIC - ACID transactions (concurrent writes)
# MAGIC - Time travel (γυρίζετε σε προηγούμενες versions)
# MAGIC - Schema enforcement
# MAGIC - Performance optimizations (Z-Ordering, OPTIMIZE)

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

# Save στο Unity Catalog (catalog `workspace`, schema `aade`)
train_clean.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.kep_train_clean")
test_clean.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.kep_test_clean")

print("\n✓ Dataset έτοιμο για ML training (χωρίς leakage)")
print(f"  → workspace.aade.kep_train_clean")
print(f"  → workspace.aade.kep_test_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση
# MAGIC
# MAGIC ### 🎓 Τι μάθατε σε αυτή την άσκηση
# MAGIC
# MAGIC 1. **Quick profiling** πριν κάνετε οτιδήποτε άλλο (row count, schema, date range)
# MAGIC 2. **Temporal split** (όχι random) για time-series ML — γιατί random split μιμείται την παραγωγή λάθος
# MAGIC 3. **Correlation analysis** ως πρώτη γραμμή άμυνας: `|corr| > 0.95` = ύποπτο για leakage
# MAGIC 4. **Function `check_temporal_leakage()`** για systematic εντοπισμό features με future timestamps
# MAGIC 5. **Leakage report** με features προς αφαίρεση + αιτιολογία (ώστε να είστε in line με EU AI Act audit trail)
# MAGIC 6. **Persist σε Delta tables** στο Unity Catalog για παράδοση στον DS
# MAGIC
# MAGIC ### 📊 Τα 3 leakage features που εντοπίσαμε
# MAGIC
# MAGIC | Feature | Type | Γιατί |
# MAGIC |---|---|---|
# MAGIC | `audit_completion_date` | Temporal | Timestamp μετά το event |
# MAGIC | `final_decision_amount` | Target | Παράγεται μετά τον έλεγχο |
# MAGIC | `closure_reason` | Temporal + Target | Future info |
# MAGIC
# MAGIC ### 🚀 Επόμενα βήματα
# MAGIC
# MAGIC Με αυτό το καθαρό dataset, ο **Data Scientist** μπορεί να εκπαιδεύσει μοντέλο
# MAGIC που δεν θα «τσιμπήσει» από data leakage. Επόμενα στάδια στη ροή:
# MAGIC
# MAGIC - **Lab 3** (`Feature_Engineering_Notebook.py`): φτιάχνουμε features από raw data
# MAGIC - **Lab 4** (`ML_Model_Notebook.py`): training Random Forest με sklearn + MLflow tracking
# MAGIC
# MAGIC ### 💡 Take-home message
# MAGIC
# MAGIC > **Το data leakage δεν φαίνεται με γυμνό μάτι. Φαίνεται μόνο όταν το ψάξεις.
# MAGIC > Σε δημόσιο φορέα όπως η ΑΑΔΕ, αυτό το βήμα δεν είναι nice-to-have — είναι το νόμιμο και ηθικό σας καθήκον.**
