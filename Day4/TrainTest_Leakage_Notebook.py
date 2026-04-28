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
# MAGIC ## 🔍 Ερμηνεία αποτελεσμάτων — Αναλυτικά
# MAGIC
# MAGIC ### Τι σημαίνει «correlation»;
# MAGIC
# MAGIC Η correlation είναι ένας αριθμός από **−1 έως +1** που μετράει πόσο συγχρονισμένα
# MAGIC κινούνται δύο μεταβλητές:
# MAGIC
# MAGIC | Τιμή | Σημασία |
# MAGIC |---|---|
# MAGIC | **+1.00** | Τέλεια θετική σχέση — όταν το ένα ανεβαίνει, το άλλο πάντα ανεβαίνει |
# MAGIC | **0.00** | Τυχαία σχέση — η μία δεν μας λέει τίποτα για την άλλη |
# MAGIC | **−1.00** | Τέλεια αρνητική σχέση — όταν το ένα ανεβαίνει, το άλλο πάντα κατεβαίνει |
# MAGIC
# MAGIC Το target μας είναι binary: **1 = passed**, **0 = flagged/rejected**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1️⃣ `documents_submitted: +0.0070`
# MAGIC
# MAGIC Σχεδόν **μηδέν**. Δηλαδή ο αριθμός των εγγράφων που υποβάλλει ο πολίτης
# MAGIC **δεν προβλέπει** αν η αίτηση θα περάσει ή θα απορριφθεί. Είναι θόρυβος.
# MAGIC
# MAGIC > **🔍 Στην πράξη:** Όποιος υποβάλλει 5 ή 25 έγγραφα έχει σχεδόν την ίδια
# MAGIC > πιθανότητα να περάσει. Αυτό βγάζει νόημα — το πλήθος εγγράφων εξαρτάται
# MAGIC > από τον τύπο της υπηρεσίας, όχι από την ποιότητά τους.
# MAGIC
# MAGIC ✅ **Verdict:** Legitimate feature. **Κρατάμε.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ `wait_time_minutes: +0.0017`
# MAGIC
# MAGIC Επίσης σχεδόν **μηδέν**. Η αναμονή στο ΚΕΠ δεν συσχετίζεται με την έκβαση του ελέγχου.
# MAGIC
# MAGIC > **🔍 Στην πράξη:** Ο πολίτης που περίμενε 10 λεπτά και ο πολίτης που περίμενε
# MAGIC > 90 λεπτά έχουν την ίδια πιθανότητα η αίτησή τους να flagαριστεί. Λογικό —
# MAGIC > η αναμονή εξαρτάται από τον φόρτο της υπηρεσίας, όχι από την ποιότητα της αίτησης.
# MAGIC
# MAGIC ✅ **Verdict:** Legitimate feature. **Κρατάμε.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ `final_decision_amount: −0.7024` ⚠️
# MAGIC
# MAGIC **Ισχυρή αρνητική** συσχέτιση. Όσο **ψηλότερο** είναι το ποσό, τόσο **μικρότερη**
# MAGIC η πιθανότητα `passed` (= τόσο πιο πιθανό να flagαριστεί ή να απορριφθεί).
# MAGIC
# MAGIC > **🔍 Σκεφτείτε γιατί:** Αυτό το πεδίο περιέχει το ποσό **που υπολογίστηκε ΜΕΤΑ τον
# MAGIC > έλεγχο** — π.χ. πρόστιμο ή προσαύξηση. Αν η αίτηση πέρασε ομαλά → μικρό ή μηδενικό
# MAGIC > ποσό. Αν flagαρίστηκε → μεγάλο πρόστιμο. **Αρνητική συσχέτιση εκ κατασκευής.**
# MAGIC
# MAGIC ❌ **Verdict:** Αυτό το feature **ΔΕΝ ΥΠΑΡΧΕΙ** τη στιγμή που έρχεται μια νέα αίτηση.
# MAGIC Είναι **target leakage**. **Πετάμε.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎓 Το βαθύτερο μάθημα
# MAGIC
# MAGIC Στο slide ο κανόνας λέει `|corr| > 0.95` = leakage. Στην πράξη όμως, **τα πραγματικά
# MAGIC leakage features σπανίως φτάνουν 0.99**. Αραιώνονται από:
# MAGIC
# MAGIC - **Nulls** στα δεδομένα
# MAGIC - **Θόρυβο** στις μετρήσεις
# MAGIC - **Mixed effects** (το feature επηρεάζεται και από άλλους παράγοντες)
# MAGIC
# MAGIC Γι' αυτό βλέπουμε **−0.70** και όχι **−0.99**. Αυτό είναι **ΕΞΑΙΡΕΤΙΚΑ ΥΨΗΛΟ** για
# MAGIC legitimate feature, αλλά δεν χτυπάει το "εύκολο" 0.95 threshold.
# MAGIC
# MAGIC ### Αρχιτεκτονικό συμπέρασμα: γιατί χρειάζονται **τρεις** detectors
# MAGIC
# MAGIC | Detection Method | Τι πιάνει | Όριο |
# MAGIC |---|---|---|
# MAGIC | **Correlation** (Βήμα 4) | Numerical leakage | Tier system: 0.5 / 0.95 |
# MAGIC | **Temporal check** (Βήμα 5) | Future timestamps (string ή numeric) | `feature_ts > event_ts` |
# MAGIC | **Domain knowledge** | «Αυτό το πεδίο γεμίζει μετά τον έλεγχο» | Ανθρώπινη κρίση |
# MAGIC
# MAGIC Χρειάζονται **και τα τρία** μαζί. Το correlation εδώ μας έδωσε ένα yellow flag, ο
# MAGIC temporal check στο επόμενο βήμα θα δώσει το red flag, και το domain knowledge θα
# MAGIC κλειδώσει την απόφαση.
# MAGIC
# MAGIC > **⚠️ Αν είχατε στηριχτεί μόνο στο 0.95 threshold, το `final_decision_amount` θα είχε
# MAGIC > περάσει στο μοντέλο και θα είχατε καταστροφή στην παραγωγή.**

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
# MAGIC ## 🪜 Βήμα 7: Πώς τραβάμε συμπεράσματα από τα νόμιμα features
# MAGIC
# MAGIC > **📝 Σημείωση:** Σε αυτό το συνθετικό dataset, ο σχεδιασμός είναι έτσι ώστε **όλος
# MAGIC > ο διαχωρισμός passed/flagged/rejected να ζει στις leakage στήλες** που μόλις βγάλαμε.
# MAGIC > Άρα τα νόμιμα features δείχνουν λίγο σήμα — αυτό είναι **εκπαιδευτικά σωστό** γιατί
# MAGIC > δείχνει τι θα έβλεπε ο Data Scientist αν τα leakage features είχαν περάσει στο
# MAGIC > μοντέλο. Σε **πραγματικό ΑΑΔΕ dataset**, το `service_type`, ο χρόνος υποβολής και
# MAGIC > το ιστορικό πολίτη θα έδειχναν δραματικές διαφορές pass rate (συχνά 30%–95%).
# MAGIC
# MAGIC Ωραία, βγάλαμε τα leakage features. Τι μένει; Πέντε στήλες που τις **έχουμε
# MAGIC τη στιγμή της υποβολής της αίτησης**:
# MAGIC
# MAGIC | Feature | Τι ξέρουμε |
# MAGIC |---|---|
# MAGIC | `request_timestamp` | Πότε υποβλήθηκε |
# MAGIC | `service_type` | Τι είδους αίτηση είναι |
# MAGIC | `documents_submitted` | Πόσα έγγραφα έφερε |
# MAGIC | `wait_time_minutes` | Πόσο περίμενε |
# MAGIC | `citizen_id` | Ποιος πολίτης |
# MAGIC
# MAGIC Πώς **βγαίνουν συμπεράσματα** από αυτά; Ας κάνουμε εξερεύνηση ένα-ένα.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7α. `service_type` — το πιο ισχυρό legitimate feature
# MAGIC
# MAGIC Το είδος της αίτησης χωρίζει τις αιτήσεις σε «εύκολες» και «δύσκολες» κατηγορίες.
# MAGIC Ας δούμε ποια services έχουν χαμηλό vs υψηλό fail rate.

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum

# Δημιουργία binary "passed" indicator
df_analysis = df.withColumn(
    "passed_flag",
    F_when(col("audit_outcome") == "passed", 1.0).otherwise(0.0)
)

print("=== Pass rate ανά service_type ===\n")
service_breakdown = (
    df_analysis.groupBy("service_type")
    .agg(
        count("*").alias("total"),
        spark_sum("passed_flag").alias("passed"),
    )
    .withColumn("pass_rate_pct", spark_round((col("passed") / col("total")) * 100, 1))
    .orderBy(col("pass_rate_pct").desc())
)
service_breakdown.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **🔍 Τι μάθαμε:**
# MAGIC
# MAGIC Παρατηρήστε πόσο διαφορετικά συμπεριφέρονται τα services. Κάποια έχουν 80%+ pass rate
# MAGIC (τυποποιημένες, εύκολες αιτήσεις), άλλα έχουν χαμηλότερα ποσοστά (πολύπλοκες
# MAGIC διαδικασίες, περισσότερες παγίδες).
# MAGIC
# MAGIC > **💡 Insight:** Αν χτίσετε μοντέλο, το `service_type` θα είναι από τα κορυφαία
# MAGIC > predictors. Ένα `BUSINESS_PERMIT` και ένα `BIRTH_CERT` ξεκινούν με τελείως
# MAGIC > διαφορετική «βάση πιθανοτήτων».

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7β. `documents_submitted` — μήπως κρύβεται σχέση που το correlation δεν είδε;
# MAGIC
# MAGIC Το correlation βγήκε ~0.00, αλλά η σχέση μπορεί να είναι **μη γραμμική**.
# MAGIC Π.χ. και **πολύ λίγα** και **πολύ πολλά** έγγραφα μπορεί να σημαίνουν πρόβλημα.
# MAGIC Ας δούμε mean/median ανά outcome.

# COMMAND ----------

from pyspark.sql.functions import avg, expr

print("=== documents_submitted ανά outcome ===\n")
df.groupBy("audit_outcome").agg(
    count("*").alias("rows"),
    spark_round(avg("documents_submitted"), 2).alias("mean_docs"),
    expr("percentile_approx(documents_submitted, 0.5)").alias("median_docs"),
    spark_min("documents_submitted").alias("min_docs"),
    spark_max("documents_submitted").alias("max_docs"),
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **🔍 Τι μάθαμε:**
# MAGIC
# MAGIC Αν τα means είναι σχεδόν ίδια ανά outcome, τότε το feature δεν ξεχωρίζει τις
# MAGIC κατηγορίες — γι' αυτό η correlation βγήκε ~0. Αν παρατηρήσετε δραστική διαφορά
# MAGIC σε κάποια κατηγορία (π.χ. `flagged` έχει mean 25 ενώ `passed` έχει mean 8), τότε
# MAGIC υπάρχει κρυμμένη σχέση που η γραμμική correlation δεν έπιασε.
# MAGIC
# MAGIC > **💡 Insight:** Ακόμα και αν το correlation είναι μηδέν, αξίζει πάντα να
# MAGIC > κάνετε **group-by analysis**. Η correlation μετράει μόνο γραμμικές σχέσεις.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7γ. `request_timestamp` — εξάγουμε χρονικά features
# MAGIC
# MAGIC Το timestamp **από μόνο του** δεν λέει τίποτα. Αλλά μπορούμε να εξάγουμε χρήσιμα
# MAGIC παράγωγα features:
# MAGIC
# MAGIC - **Ώρα της ημέρας** — οι νυχτερινές υποβολές είναι πιο ύποπτες;
# MAGIC - **Ημέρα εβδομάδας** — τα Σαββατοκύριακα έχουν περισσότερα λάθη;
# MAGIC - **Μήνας** — εποχικότητα;

# COMMAND ----------

from pyspark.sql.functions import hour, dayofweek, to_timestamp as F_to_ts

df_temporal = df_analysis.withColumn(
    "request_ts", F_to_ts(col("request_timestamp"))
).withColumn(
    "hour_of_day", hour(col("request_ts"))
).withColumn(
    "day_of_week", dayofweek(col("request_ts"))  # 1=Sunday, 7=Saturday
)

print("=== Pass rate ανά ώρα της ημέρας (top 5 και bottom 5) ===\n")
hourly = (
    df_temporal.groupBy("hour_of_day")
    .agg(
        count("*").alias("total"),
        spark_sum("passed_flag").alias("passed"),
    )
    .withColumn("pass_rate_pct", spark_round((col("passed") / col("total")) * 100, 1))
    .orderBy("hour_of_day")
)
hourly.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **🔍 Τι μάθαμε:**
# MAGIC
# MAGIC Αν δείτε διαφορά pass rate ανά ώρα (π.χ. 04:00 = 50%, 11:00 = 75%), έχετε
# MAGIC ισχυρό σήμα. Νυχτερινές υποβολές συχνά είναι βιαστικές, με λιγότερο έλεγχο, οπότε
# MAGIC είναι πιθανότερο να flagαριστούν.
# MAGIC
# MAGIC > **💡 Insight:** Από ένα μόνο `timestamp` βγαίνουν **πολλά features** (hour, day,
# MAGIC > month, is_weekend, days_to_deadline, κ.λπ.). Αυτή είναι η ομορφιά του feature
# MAGIC > engineering.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7δ. `citizen_id` — ιστορικό συμπεριφοράς
# MAGIC
# MAGIC Το `citizen_id` **από μόνο του** είναι άχρηστο (ένας αριθμός). Αλλά μπορούμε να
# MAGIC φτιάξουμε **historical features**:
# MAGIC
# MAGIC - Πόσες αιτήσεις έχει κάνει ο πολίτης ιστορικά;
# MAGIC - Πόσες από αυτές flagαρίστηκαν;
# MAGIC - Είναι repeat offender;
# MAGIC
# MAGIC ⚠️ **Προσοχή:** Αυτά τα features πρέπει να υπολογίζονται **μόνο από το ΠΑΡΕΛΘΟΝ**
# MAGIC κάθε γραμμής, αλλιώς πέφτουμε ξανά σε temporal leakage. Στο production μοντέλο
# MAGIC χρησιμοποιούμε Spark Window functions με `rangeBetween(unboundedPreceding, -1)`.

# COMMAND ----------

# Πόσοι repeat citizens υπάρχουν;
print("=== Citizen activity distribution ===\n")
citizen_activity = df.groupBy("citizen_id").count().withColumnRenamed("count", "total_requests")

print(f"Συνολικά μοναδικοί πολίτες: {citizen_activity.count():,}")
citizen_activity.groupBy("total_requests").count().orderBy("total_requests").show(10)

# Pass rate για repeat customers vs first-timers
print("=== Pass rate για repeat vs first-time citizens ===\n")
df_with_activity = df_analysis.join(citizen_activity, on="citizen_id")
df_with_activity = df_with_activity.withColumn(
    "is_repeat",
    F_when(col("total_requests") > 1, "repeat").otherwise("first_time")
)
(
    df_with_activity.groupBy("is_repeat")
    .agg(
        count("*").alias("total"),
        spark_round(avg("passed_flag") * 100, 1).alias("pass_rate_pct"),
    )
    .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **🔍 Τι μάθαμε:**
# MAGIC
# MAGIC Αν οι repeat citizens έχουν διαφορετικό pass rate από τους first-timers, έχετε
# MAGIC ένα ισχυρό σήμα. Συχνά οι repeat citizens είναι είτε «έμπειροι» (ξέρουν τι θέλει
# MAGIC η αίτηση → υψηλό pass rate) είτε «προβληματικοί» (πιάστηκαν ξανά → χαμηλό pass rate).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7ε. Ο πραγματικός κανόνας — τα features δουλεύουν σε **συνδυασμό**
# MAGIC
# MAGIC Κανένα feature **από μόνο του** δεν προβλέπει τέλεια. Η μαγεία γίνεται όταν τα
# MAGIC συνδυάζουμε:
# MAGIC
# MAGIC ```
# MAGIC 👤 Νέος πολίτης (is_first_time=1)
# MAGIC + 📋 BUSINESS_PERMIT (high_risk service)
# MAGIC + ⏰ 23:55 της προθεσμίας (deadline=1)
# MAGIC + 📄 4 έγγραφα μόνο (low documents)
# MAGIC ──────────────────────────────────────
# MAGIC = 🚨 Πολύ υψηλή πιθανότητα να flagαριστεί
# MAGIC ```
# MAGIC
# MAGIC ### 🎯 Πώς δουλεύει το μοντέλο
# MAGIC
# MAGIC Το ML μοντέλο δεν χρησιμοποιεί ένα μόνο feature. **Παίρνει όλα τα legitimate
# MAGIC features ταυτόχρονα**, βρίσκει μοτίβα στο ιστορικό (στις 10.000 παλιές αιτήσεις)
# MAGIC και μαθαίνει:
# MAGIC
# MAGIC > *"Όταν βλέπω συνδυασμό X + Y + Z → η αίτηση έχει 73% πιθανότητα να flagαριστεί"*
# MAGIC
# MAGIC Αυτή η πιθανότητα μετά ταξινομεί τις νέες αιτήσεις:
# MAGIC
# MAGIC | Score | Action |
# MAGIC |---|---|
# MAGIC | > 80% | 🚨 Priority manual review |
# MAGIC | 50–80% | ⚠️ Standard έλεγχος |
# MAGIC | < 50% | ✅ Fast-track approval |
# MAGIC
# MAGIC Έτσι ο υπάλληλος δεν χάνει χρόνο σε εύκολες περιπτώσεις και επικεντρώνεται εκεί
# MAGIC που πραγματικά αξίζει.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 8: Καθαρό Dataset για παράδοση στον DS
# MAGIC
# MAGIC ### 8α. Drop τα leakage features
# MAGIC
# MAGIC Αφαιρούμε τις τρεις προβληματικές στήλες και κρατάμε μόνο legitimate features.
# MAGIC
# MAGIC ### 8β. Temporal split στο καθαρό dataset
# MAGIC
# MAGIC Ξανακάνουμε temporal split — αυτή τη φορά πάνω στο **καθαρό** dataset.
# MAGIC
# MAGIC ### 8γ. Persist σε Delta tables
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
