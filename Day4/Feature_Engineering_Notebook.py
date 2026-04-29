# Databricks notebook source
# MAGIC %md
# MAGIC # Άσκηση Ημέρας 4 — Feature Engineering Pipeline
# MAGIC **Ρόλος: Μηχανικοί Δεδομένων**
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4/Feature_Engineering_Notebook.py
# MAGIC > ```
# MAGIC
# MAGIC ## 🎯 Σενάριο
# MAGIC Ομάδα DE της ΑΑΔΕ ετοιμάζει **features** για μοντέλο ML που εντοπίζει ασυνήθιστες
# MAGIC φορολογικές δηλώσεις. Ξεκινάμε από raw δηλώσεις → καταλήγουμε σε καθαρό feature table.
# MAGIC
# MAGIC ## 📚 Τι είναι «feature»;
# MAGIC > **Feature** = μία στήλη που χρησιμοποιεί το μοντέλο ML για να κάνει πρόβλεψη.
# MAGIC > Π.χ. το `income` είναι raw column. Το `tax_rate = tax_paid/income*100` είναι
# MAGIC > **engineered feature** — το φτιάχνουμε εμείς από τα raw data γιατί είναι πιο
# MAGIC > κατατοπιστικό για το μοντέλο.
# MAGIC
# MAGIC ## 🧠 Τι θα μάθετε
# MAGIC - **Imputation** κενών τιμών με **median** (διάμεσο)
# MAGIC - **6 features** για ΑΑΔΕ context (tax_rate, is_high_income, YoY change, κλπ.)
# MAGIC - **Window functions** (lag, count, avg) — υπολογισμοί ανά ομάδα γραμμών
# MAGIC - **Quality checks** σε features πριν τα στείλουμε στο μοντέλο
# MAGIC - Αποθήκευση σε **Delta** + **correlation ranking** (ποιο feature «μετράει» πιο πολύ)
# MAGIC
# MAGIC **Περιβάλλον:** Databricks Free Edition (Serverless) **·** **Διάρκεια:** ~25-30 λεπτά

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 1: Schema & Volume Setup
# MAGIC
# MAGIC ### 📚 Τι είναι «schema» και «volume»;
# MAGIC - **Schema** (ή database): φάκελος που ομαδοποιεί πίνακες. Π.χ. `workspace.aade`
# MAGIC   = όλοι οι πίνακες της ΑΑΔΕ.
# MAGIC - **Volume**: φάκελος για **αρχεία** (CSV, Parquet, εικόνες) μέσα στο Unity Catalog.
# MAGIC   Δίνει access control + audit log σε raw αρχεία.
# MAGIC
# MAGIC `CREATE … IF NOT EXISTS` = αν υπάρχει ήδη, μην πετάξεις error — απλώς συνέχισε.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
print("✓ Schema workspace.aade & Volume aade_data έτοιμα")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 2: Dataset Φορολογικών Δηλώσεων
# MAGIC
# MAGIC 20 εγγραφές = 8 φορολογούμενοι × 2-3 έτη (2023-2025).
# MAGIC **Σκόπιμα έχουμε κενά** (`None`) σε `income`, `expenses`, `tax_paid` για να
# MAGIC δείξουμε imputation στο επόμενο βήμα.
# MAGIC
# MAGIC ### 📚 Τι είναι «schema» στον πίνακα;
# MAGIC > Διαφορετικό schema από πριν! Εδώ schema = η **δομή** του πίνακα: ποιες στήλες
# MAGIC > έχει, τι τύπο (string/double/integer), και αν δέχονται null. Το ορίζουμε
# MAGIC > ρητά για να αποφύγουμε type guessing από τον Spark.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, when, lit, lag, avg, count, abs as spark_abs, round as spark_round, desc
from pyspark.sql.window import Window

schema = StructType([
    StructField("afm", StringType(), False),
    StructField("name", StringType(), False),
    StructField("region", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("income", DoubleType(), True),       # True = επιτρέπει null
    StructField("expenses", DoubleType(), True),
    StructField("tax_paid", DoubleType(), True),
    StructField("status", StringType(), False),
])

data = [
    ("090000001", "Παπαδόπουλος Γιώργος", "Αττική", 2023, 42000.0, 18000.0, 8800.0, "Approved"),
    ("090000001", "Παπαδόπουλος Γιώργος", "Αττική", 2024, 45000.0, 19500.0, 9500.0, "Approved"),
    ("090000001", "Παπαδόπουλος Γιώργος", "Αττική", 2025, 48000.0, 20000.0, 10200.0, "Pending"),
    ("090000002", "Ιωάννου Μαρία", "Αττική", 2023, 31000.0, 12000.0, 5800.0, "Approved"),
    ("090000002", "Ιωάννου Μαρία", "Αττική", 2024, 33000.0, None, 6200.0, "Approved"),
    ("090000002", "Ιωάννου Μαρία", "Αττική", 2025, 35000.0, 14000.0, 6800.0, "Pending"),
    ("090000003", "Κωνσταντίνου Αλέξ.", "Θεσσαλονίκη", 2023, 55000.0, 22000.0, 12500.0, "Approved"),
    ("090000003", "Κωνσταντίνου Αλέξ.", "Θεσσαλονίκη", 2024, 58000.0, 24000.0, 13400.0, "Approved"),
    ("090000003", "Κωνσταντίνου Αλέξ.", "Θεσσαλονίκη", 2025, None, 25000.0, None, "Pending"),
    ("090000004", "Δημητρίου Ελένη", "Κρήτη", 2023, 25000.0, 10000.0, 4200.0, "Approved"),
    ("090000004", "Δημητρίου Ελένη", "Κρήτη", 2024, 27000.0, 11000.0, 4800.0, "Approved"),
    ("090000005", "Νικολάου Κώστας", "Αττική", 2023, 110000.0, 45000.0, 35000.0, "Approved"),
    ("090000005", "Νικολάου Κώστας", "Αττική", 2024, 115000.0, 48000.0, 37000.0, "Approved"),
    ("090000005", "Νικολάου Κώστας", "Αττική", 2025, 125000.0, 50000.0, 40000.0, "Pending"),
    ("090000006", "Αλεξίου Πέτρος", "Θεσσαλονίκη", 2023, 38000.0, 15000.0, 7200.0, "Approved"),
    ("090000006", "Αλεξίου Πέτρος", "Θεσσαλονίκη", 2025, 44000.0, 18000.0, 8800.0, "Approved"),
    ("090000007", "Βασιλείου Σοφία", "Πάτρα", 2023, 33000.0, 14000.0, 6400.0, "Approved"),
    ("090000007", "Βασιλείου Σοφία", "Πάτρα", 2024, None, 15000.0, None, "Approved"),
    ("090000007", "Βασιλείου Σοφία", "Πάτρα", 2025, 38000.0, 16000.0, 7500.0, "Pending"),
    ("090000008", "Γεωργίου Δημήτρης", "Αττική", 2024, 70000.0, 28000.0, 18000.0, "Approved"),
]

df_raw = spark.createDataFrame(data, schema)
print(f"✓ Dataset έτοιμο ({df_raw.count()} εγγραφές)")
df_raw.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 3: Έλεγχος Missing Values
# MAGIC
# MAGIC ### 📚 Τι είναι «missing value» / «null»;
# MAGIC > Κενό κελί στον πίνακα. Σε Python = `None`, σε SQL = `NULL`.
# MAGIC > Δεν είναι το ίδιο με `0` ή `""` — σημαίνει «δεν ξέρουμε την τιμή».
# MAGIC
# MAGIC Σε production δεν αγνοούμε ποτέ nulls — ή τα **γεμίζουμε** (imputation),
# MAGIC ή τα **διώχνουμε**, ή κρατάμε flag «αυτό είναι null».
# MAGIC
# MAGIC Ο παρακάτω κώδικας μετράει πόσα null έχει κάθε στήλη:
# MAGIC `count(when(col(c).isNull(), c))` = «μέτρα μόνο όπου είναι null».

# COMMAND ----------

print("=== Missing values ανά στήλη ===")
null_counts = df_raw.select([count(when(col(c).isNull(), c)).alias(c) for c in df_raw.columns])
null_counts.show()

print("=== Ποιες εγγραφές έχουν κενό income; ===")
df_raw.filter(col("income").isNull()).select("afm", "name", "year", "income", "tax_paid").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 4: Imputation με Median (Διάμεσο)
# MAGIC
# MAGIC ### 📚 Τι είναι «median» (διάμεσος);
# MAGIC > Ταξινομούμε όλες τις τιμές από μικρή σε μεγάλη. **Median** = η τιμή στη **μέση** της λίστας.
# MAGIC >
# MAGIC > Παράδειγμα: εισοδήματα = [25k, 31k, 35k, 42k, 110k]
# MAGIC > - **Mean** (μέσος όρος) = (25+31+35+42+110)/5 = **48,6k**
# MAGIC > - **Median** (διάμεσος) = **35k** (η μεσαία τιμή)
# MAGIC >
# MAGIC > Παρατηρήστε: το 110k τραβάει τον μέσο όρο πολύ ψηλά (τον «παραμορφώνει»).
# MAGIC > Ο median αγνοεί το outlier — γι' αυτό προτιμάται σε μεταβλητές με ακραίες τιμές
# MAGIC > (εισόδημα, τιμές ακινήτων κ.λπ.).
# MAGIC
# MAGIC ### 📚 Τι είναι «outlier»;
# MAGIC > Τιμή πολύ μακριά από τις υπόλοιπες. Π.χ. ένας φορολογούμενος με 1.000.000€
# MAGIC > σε δείγμα μέσου εισοδήματος 35k. Συχνά είναι λάθος καταχώρησης ή πραγματικά
# MAGIC > σπάνια περίπτωση — και τα δύο μπορούν να μπερδέψουν το μοντέλο.
# MAGIC
# MAGIC ### 📚 Τι είναι «imputation»;
# MAGIC > Η διαδικασία **συμπλήρωσης** missing values με μια λογική τιμή. Συνήθεις στρατηγικές:
# MAGIC > median, mean, mode (πιο συχνή τιμή), ή πρόβλεψη από άλλα features.
# MAGIC
# MAGIC `approxQuantile("income", [0.5], 0.01)` = «βρες την τιμή που είναι στο 50% του dataset
# MAGIC (δηλαδή median), με ανοχή σφάλματος 1%». Είναι γρήγορος υπολογισμός σε big data.

# COMMAND ----------

# Υπολογισμός median για κάθε αριθμητική στήλη
median_income = df_raw.filter(col("income").isNotNull()).approxQuantile("income", [0.5], 0.01)[0]
median_expenses = df_raw.filter(col("expenses").isNotNull()).approxQuantile("expenses", [0.5], 0.01)[0]
median_tax = df_raw.filter(col("tax_paid").isNotNull()).approxQuantile("tax_paid", [0.5], 0.01)[0]

print(f"Median income:   {median_income:,.0f} €")
print(f"Median expenses: {median_expenses:,.0f} €")
print(f"Median tax_paid: {median_tax:,.0f} €")

# Αντικατάσταση nulls με median
df_imputed = (df_raw
    .withColumn("income",   when(col("income").isNull(),   lit(median_income)).otherwise(col("income")))
    .withColumn("expenses", when(col("expenses").isNull(), lit(median_expenses)).otherwise(col("expenses")))
    .withColumn("tax_paid", when(col("tax_paid").isNull(), lit(median_tax)).otherwise(col("tax_paid"))))

print("\n✓ Imputation ολοκληρώθηκε. Remaining nulls:")
print(f"  income:   {df_imputed.filter(col('income').isNull()).count()}")
print(f"  expenses: {df_imputed.filter(col('expenses').isNull()).count()}")
print(f"  tax_paid: {df_imputed.filter(col('tax_paid').isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 5: Feature `tax_rate` (Φορολογικός Συντελεστής)
# MAGIC
# MAGIC **Τύπος**: `tax_rate = tax_paid / income × 100`
# MAGIC
# MAGIC Παράδειγμα: αν income = 50.000€ και tax_paid = 10.000€ → tax_rate = 20%.
# MAGIC
# MAGIC ### 🎯 Γιατί είναι χρήσιμο για ΑΑΔΕ
# MAGIC Ο **ελεγκτής** μπορεί να εντοπίσει ασυνήθιστα **χαμηλό** tax_rate σε σχέση με
# MAGIC τον κλάδο/περιοχή — πιθανή υπο-δήλωση εισοδήματος. Π.χ. αν ένας ελεύθερος
# MAGIC επαγγελματίας στη Αττική δηλώνει tax_rate = 5% ενώ ο μέσος όρος είναι 22%,
# MAGIC αξίζει διασταύρωση.

# COMMAND ----------

df_feat = df_imputed.withColumn("tax_rate", spark_round(col("tax_paid") / col("income") * 100, 2))
df_feat.select("afm", "name", "year", "income", "tax_paid", "tax_rate").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6: Feature `is_high_income`
# MAGIC
# MAGIC ### 📚 Τι είναι «boolean feature»;
# MAGIC > Στήλη που παίρνει μόνο **2 τιμές**: `1` (αληθές) ή `0` (ψευδές). Λέγεται και
# MAGIC > **binary** ή **flag**. Εδώ: `is_high_income = 1` αν `income > 60.000€`, αλλιώς `0`.
# MAGIC
# MAGIC ### 🎯 Γιατί χρήσιμο
# MAGIC Τα μοντέλα ML συχνά χειρίζονται διαφορετικά τα **high-income brackets**:
# MAGIC - διαφορετικός tax tier (κλίμακα φορολογίας)
# MAGIC - διαφορετικά risk patterns (π.χ. πιο σύνθετες δηλώσεις, εισοδήματα από εξωτερικό)
# MAGIC - διαφορετικός όγκος δεδομένων (λιγότεροι φορολογούμενοι αλλά μεγαλύτερα ποσά)

# COMMAND ----------

df_feat = df_feat.withColumn("is_high_income", when(col("income") > 60000, lit(1)).otherwise(lit(0)))
df_feat.select("afm", "name", "year", "income", "is_high_income").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 7: Feature `income_change_yoy` (Year-over-Year)
# MAGIC
# MAGIC ### 📚 Τι σημαίνει «YoY»;
# MAGIC > **Year-over-Year** = «έτος-προς-έτος». Σύγκριση τιμής φέτος vs πέρυσι.
# MAGIC > Τύπος: `(income_φέτος − income_πέρυσι) / income_πέρυσι × 100`.
# MAGIC > Π.χ. πέρυσι 40k, φέτος 44k → YoY = +10%.
# MAGIC
# MAGIC ### 📚 Τι είναι «window function»;
# MAGIC > Συνάρτηση που υπολογίζει αποτέλεσμα για κάθε γραμμή κοιτώντας **παράθυρο**
# MAGIC > γραμμών γύρω της — συνήθως ομαδοποιημένο (π.χ. ανά ΑΦΜ) και ταξινομημένο
# MAGIC > (π.χ. κατά έτος). Σε αντίθεση με το `groupBy` που μαζεύει σε μία γραμμή ανά group,
# MAGIC > το window function **κρατάει όλες τις γραμμές** και προσθέτει νέα στήλη.
# MAGIC
# MAGIC ### 📚 Τι κάνει το `lag()`;
# MAGIC > «Πάρε την τιμή της **προηγούμενης** γραμμής μέσα στο group». Δηλαδή για κάθε
# MAGIC > φορολογούμενο (partitionBy `afm`), ταξινομημένο κατά `year`, το `lag("income", 1)`
# MAGIC > μας δίνει το income του προηγούμενου έτους. Έτσι μπορούμε να συγκρίνουμε
# MAGIC > φέτος vs πέρυσι στην ίδια γραμμή.
# MAGIC
# MAGIC ### 🎯 Γιατί χρήσιμο για ΑΑΔΕ
# MAGIC Ξαφνική **πτώση 50%** στο εισόδημα χωρίς προφανή λόγο = πιθανώς ύποπτη.
# MAGIC Αντίστροφα, ξαφνική **αύξηση 200%** ίσως δείχνει νέα δραστηριότητα που δεν έχει
# MAGIC δηλωθεί σε άλλα μητρώα.

# COMMAND ----------

window_yoy = Window.partitionBy("afm").orderBy("year")
df_feat = (df_feat
    .withColumn("prev_income", lag("income", 1).over(window_yoy))
    .withColumn("income_change_yoy",
        spark_round(
            when(col("prev_income").isNotNull(),
                 (col("income") - col("prev_income")) / col("prev_income") * 100)
            .otherwise(lit(None)), 2))
    .drop("prev_income"))

df_feat.select("afm", "name", "year", "income", "income_change_yoy").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 8: Feature `declaration_count`
# MAGIC
# MAGIC **Πόσες δηλώσεις** συνολικά έχει ο φορολογούμενος στο σύστημα.
# MAGIC
# MAGIC Εδώ χρησιμοποιούμε `Window.partitionBy("afm")` **χωρίς** orderBy — δηλαδή
# MAGIC το παράθυρο = **όλες οι γραμμές** του ίδιου ΑΦΜ. Το `count("*").over(window)`
# MAGIC μετράει πόσες γραμμές έχει κάθε group και προσθέτει το πλήθος ως στήλη.
# MAGIC
# MAGIC ### 🎯 Γιατί χρήσιμο για ΑΑΔΕ
# MAGIC Φορολογούμενοι με **1 μόνο δήλωση** (νέοι ή σποραδικοί) έχουν διαφορετικό profile
# MAGIC από αυτούς με **10+ δηλώσεις** (σταθεροί). Ένα νέο ΑΦΜ που εμφανίζεται με ψηλό
# MAGIC εισόδημα από την πρώτη χρονιά χρειάζεται διαφορετικό έλεγχο από έναν έμπειρο
# MAGIC δηλωτή με σταθερά μοτίβα 10ετίας.

# COMMAND ----------

window_count = Window.partitionBy("afm")
df_feat = df_feat.withColumn("declaration_count", count("*").over(window_count))
df_feat.select("afm", "name", "year", "declaration_count").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 9: Feature `avg_income_region`
# MAGIC
# MAGIC **Μέσος όρος εισοδήματος** ανά συνδυασμό περιοχής & έτους.
# MAGIC `Window.partitionBy("region", "year")` = group ανά (Αττική-2023), (Αττική-2024), (Κρήτη-2023), …
# MAGIC και `avg("income")` υπολογίζει τον μέσο όρο για κάθε ομάδα.
# MAGIC
# MAGIC ### 🎯 Γιατί χρήσιμο για ΑΑΔΕ
# MAGIC Ένα εισόδημα **30.000€** είναι «κανονικό» στην Κρήτη, ίσως «χαμηλό» στην Αττική.
# MAGIC Το feature επιτρέπει στο μοντέλο να **συγκρίνει** τη δήλωση κάθε φορολογούμενου
# MAGIC με τον μέσο όρο της περιοχής/έτους — context-aware risk scoring.

# COMMAND ----------

window_region = Window.partitionBy("region", "year")
df_feat = df_feat.withColumn("avg_income_region", spark_round(avg("income").over(window_region), 2))
df_feat.select("afm", "region", "year", "income", "avg_income_region").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 10: Feature `expense_ratio`
# MAGIC
# MAGIC **Τύπος**: `expense_ratio = expenses / income × 100`
# MAGIC
# MAGIC Δείχνει τι ποσοστό του εισοδήματος δηλώθηκε ως δαπάνη.
# MAGIC
# MAGIC ### 🎯 Γιατί χρήσιμο για ΑΑΔΕ
# MAGIC - **expense_ratio > 80%** σε επιχείρηση = ή πολύ συγκεκριμένος κλάδος (π.χ. εστίαση,
# MAGIC   λιανεμπόριο με χαμηλά περιθώρια) ή **φουσκωμένα έξοδα** για μείωση φόρου
# MAGIC - **expense_ratio < 5%** σε ελεύθερο επαγγελματία = πιθανώς **υποδηλωμένα έξοδα**
# MAGIC   ή πραγματικά υψηλό περιθώριο (π.χ. consultant)
# MAGIC
# MAGIC Σημαντικό feature για **audit prioritization** (ποιες δηλώσεις να ελεγχθούν πρώτες).

# COMMAND ----------

df_feat = df_feat.withColumn("expense_ratio", spark_round(col("expenses") / col("income") * 100, 2))
df_feat.select("afm", "name", "year", "income", "expenses", "expense_ratio").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 11: Quality Checks
# MAGIC
# MAGIC Πριν στείλουμε τα features στο μοντέλο, **επαληθεύουμε** 4 πράγματα:
# MAGIC
# MAGIC 1. **Nulls**: δεν έχουμε κενά στα νέα features (πέρα από `income_change_yoy`
# MAGIC    στο πρώτο έτος κάθε ΑΦΜ — δεν υπάρχει «προηγούμενο» για lag).
# MAGIC 2. **Range validation**: οι τιμές είναι σε λογικά εύρη
# MAGIC    (π.χ. `tax_rate ∈ [0, 100]` — φορολογικός συντελεστής δεν μπορεί να είναι αρνητικός
# MAGIC    ή >100%).
# MAGIC 3. **Distribution stats**: min/max/mean/stddev φαίνονται OK (όχι παράξενες ακραίες τιμές).
# MAGIC 4. **Class balance** σε boolean features (αν 99% είναι `is_high_income=0` και
# MAGIC    1% είναι `1`, το dataset είναι **imbalanced** και το μοντέλο θα δυσκολευτεί
# MAGIC    να μάθει την μειοψηφική κλάση).
# MAGIC
# MAGIC ### 📚 Τι είναι «describe()»;
# MAGIC > Επιστρέφει συνοπτικά στατιστικά: count, mean, stddev, min, max ανά αριθμητική στήλη.
# MAGIC > **stddev** (standard deviation, τυπική απόκλιση) = πόσο «απλωμένες» είναι οι τιμές
# MAGIC > γύρω από τον μέσο όρο. Μικρή stddev = όλες οι τιμές κοντά στον mean· μεγάλη = πολύ διασκορπισμένες.

# COMMAND ----------

feature_cols = ["tax_rate", "is_high_income", "income_change_yoy",
                "declaration_count", "avg_income_region", "expense_ratio"]

print("=== Check 1: Nulls ===")
df_feat.select([count(when(col(c).isNull(), c)).alias(c) for c in feature_cols]).show()

print("=== Check 2: Range Validation ===")
out_of_range = df_feat.filter((col("tax_rate") < 0) | (col("tax_rate") > 100)).count()
print(f"tax_rate εκτός [0, 100]: {out_of_range} εγγραφές")
out_of_range_2 = df_feat.filter((col("expense_ratio") < 0) | (col("expense_ratio") > 100)).count()
print(f"expense_ratio εκτός [0, 100]: {out_of_range_2} εγγραφές")

print("\n=== Check 3: Distribution Statistics ===")
df_feat.select("tax_rate", "income_change_yoy", "expense_ratio", "avg_income_region").describe().show()

print("=== Check 4: Class Balance (is_high_income) ===")
df_feat.groupBy("is_high_income").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 12: Αποθήκευση σε Delta Table
# MAGIC
# MAGIC Σώζουμε τα features στον πίνακα `workspace.aade.tax_features`.
# MAGIC
# MAGIC ### 📚 Τι είναι «Delta»;
# MAGIC > Format αποθήκευσης πινάκων στο Databricks. Σε αντίθεση με απλά Parquet/CSV:
# MAGIC > - **ACID transactions**: αν δύο jobs γράφουν ταυτόχρονα, δεν χαλάνε ο ένας τον άλλον
# MAGIC > - **Time travel**: μπορείς να διαβάσεις τον πίνακα **όπως ήταν χθες** (`VERSION AS OF 5`)
# MAGIC > - **Schema enforcement**: αν προσπαθήσεις να βάλεις string σε integer στήλη, σε σταματάει
# MAGIC > - **Upserts** (`MERGE`): update + insert σε μία εντολή
# MAGIC
# MAGIC ### 📚 Τι σημαίνει «ACID»;
# MAGIC > **A**tomicity (όλα ή τίποτα) **·** **C**onsistency (μένει σε έγκυρη κατάσταση)
# MAGIC > **·** **I**solation (ταυτόχρονες ενέργειες δεν αλληλοκόβονται)
# MAGIC > **·** **D**urability (αφού γραφτεί, δεν χάνεται). Γνωστές εγγυήσεις από relational DBs,
# MAGIC > τώρα και σε big data lakes χάρη στο Delta.
# MAGIC
# MAGIC ### 🎯 Γιατί σημαντικό για ΑΑΔΕ
# MAGIC Αν αύριο σπάσει ένα feature pipeline και γράψει λανθασμένα data, μπορούμε να
# MAGIC γυρίσουμε σε προηγούμενη version του πίνακα **χωρίς restore από backup**.

# COMMAND ----------

df_feat.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.tax_features")
print("✓ Delta Table δημιουργήθηκε: workspace.aade.tax_features")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Επαλήθευση: όλα τα features σε ένα query
# MAGIC SELECT afm, name, region, year, income, tax_paid,
# MAGIC        tax_rate, is_high_income, income_change_yoy,
# MAGIC        declaration_count, avg_income_region, expense_ratio
# MAGIC FROM workspace.aade.tax_features
# MAGIC ORDER BY afm, year

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 13: Feature Importance (Correlation με tax_paid)
# MAGIC
# MAGIC ### 📚 Τι είναι «correlation» (συσχέτιση);
# MAGIC > Αριθμός από **−1 έως +1** που δείχνει πόσο «κινούνται μαζί» δύο μεταβλητές:
# MAGIC > - **+1**: όταν αυξάνεται η μία, αυξάνεται και η άλλη (τέλεια θετική συσχέτιση)
# MAGIC > - **0**: καμία σχέση
# MAGIC > - **−1**: όταν αυξάνεται η μία, μειώνεται η άλλη (τέλεια αρνητική συσχέτιση)
# MAGIC >
# MAGIC > Παράδειγμα: ύψος ↔ βάρος = ~+0.7 (όχι τέλεια, αλλά σχετίζονται).
# MAGIC > Χρώμα μαλλιών ↔ μισθός = ~0 (καμία σχέση).
# MAGIC >
# MAGIC > Στον Spark, η `df.stat.corr()` υπολογίζει **Pearson correlation** — γραμμική συσχέτιση.
# MAGIC > Δεν πιάνει μη-γραμμικές σχέσεις (π.χ. καμπύλες U-shape).
# MAGIC
# MAGIC ### 🎯 Γιατί κάνουμε correlation πριν το ML
# MAGIC Πρώτη εικόνα ποια features είναι «δυνατά» πριν ξοδέψουμε ώρες σε model training.
# MAGIC Αν ένα feature έχει correlation ~0 με το target, μάλλον δεν προσφέρει τίποτα.
# MAGIC Αντίστροφα, αν δύο features έχουν correlation ~1 μεταξύ τους, είναι **πλεονασμός**
# MAGIC (κρατάμε το ένα).
# MAGIC
# MAGIC ⚠️ **Warning**: high correlation **δεν** σημαίνει causation. Π.χ. πωλήσεις παγωτού
# MAGIC και πνιγμοί συσχετίζονται — όχι γιατί το παγωτό προκαλεί πνιγμό, αλλά γιατί και τα
# MAGIC δύο αυξάνονται το καλοκαίρι.

# COMMAND ----------

target = "tax_paid"
features_to_check = ["income", "expenses", "tax_rate", "is_high_income",
                     "income_change_yoy", "expense_ratio", "avg_income_region", "declaration_count"]

correlations = []
for feat in features_to_check:
    df_valid = df_feat.filter(col(feat).isNotNull() & col(target).isNotNull())
    corr_val = df_valid.stat.corr(feat, target)
    correlations.append((feat, round(abs(corr_val), 4), round(corr_val, 4)))

corr_schema = StructType([
    StructField("feature", StringType(), False),
    StructField("abs_correlation", DoubleType(), False),  # απόλυτη τιμή για ranking
    StructField("correlation", DoubleType(), False),       # με πρόσημο (+/−) για ερμηνεία
])
df_corr = spark.createDataFrame(correlations, corr_schema).orderBy(desc("abs_correlation"))

print("=== Feature Importance Ranking ===")
df_corr.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 14: Σύνοψη
# MAGIC
# MAGIC Τι κάναμε σήμερα:

# COMMAND ----------

print("=== Σύνοψη Pipeline ===")
print(f"  Αρχικές εγγραφές:        {df_raw.count()}")
print(f"  Features δημιουργήθηκαν: {len(feature_cols)}")
print(f"  Missing values:          imputation με median")
print(f"  Quality checks:          4 (nulls, range, distribution, balance)")
print(f"  Delta Table:             workspace.aade.tax_features")
print()
print("  Top-3 features (correlation με tax_paid):")
df_corr.limit(3).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση
# MAGIC
# MAGIC ### Τι μάθατε
# MAGIC - **Imputation** missing values με **median** (ανθεκτικό σε outliers)
# MAGIC - **6 features** για ΑΑΔΕ context: `tax_rate`, `is_high_income`, `income_change_yoy`,
# MAGIC   `declaration_count`, `avg_income_region`, `expense_ratio`
# MAGIC - **Window functions** (`lag`, `count`, `avg`) για aggregations χωρίς να χάνουμε λεπτομέρεια
# MAGIC - **Quality checks** (nulls, range, distribution, balance) πριν παραδώσουμε στο ML
# MAGIC - **Delta save** για ACID + time travel
# MAGIC - **Correlation ranking** για πρώτη εικόνα feature importance
# MAGIC
# MAGIC ### Έννοιες-κλειδιά (glossary)
# MAGIC | Όρος | Ορισμός |
# MAGIC |---|---|
# MAGIC | **Median** | Η μεσαία τιμή σε ταξινομημένη λίστα — ανθεκτική σε outliers |
# MAGIC | **Mean** | Μέσος όρος — ευαίσθητος σε outliers |
# MAGIC | **Imputation** | Συμπλήρωση κενών τιμών με λογική εκτίμηση |
# MAGIC | **Outlier** | Τιμή πολύ μακριά από τις υπόλοιπες |
# MAGIC | **YoY** | Year-over-Year, % μεταβολή σε σχέση με πέρυσι |
# MAGIC | **Window function** | Υπολογισμός ανά group γραμμών χωρίς collapse |
# MAGIC | **Correlation** | Αριθμός [−1, +1] — πόσο «κινούνται μαζί» δύο μεταβλητές |
# MAGIC | **Delta** | Format πίνακα με ACID + time travel |
# MAGIC | **ACID** | Atomicity, Consistency, Isolation, Durability |
# MAGIC
# MAGIC ### Επόμενα βήματα (production)
# MAGIC - **Train/test split** με **stratified sampling** (αν target imbalanced — διατηρεί
# MAGIC   αναλογία κλάσεων σε train & test)
# MAGIC - **Point-in-time correctness** (αποφυγή data leakage — δες Lab 2)
# MAGIC - **Feature Store** για **reusability** μεταξύ μοντέλων (ίδιο feature → πολλά μοντέλα)
# MAGIC - **ML model training** με MLflow tracking (δες Lab 3)
# MAGIC
# MAGIC **Note**: τρέχει πλήρως σε Databricks Free Edition (Serverless).
