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
# MAGIC **Σενάριο**: Ομάδα DE της ΑΑΔΕ ετοιμάζει features για μοντέλο ML που εντοπίζει
# MAGIC ασυνήθιστες φορολογικές δηλώσεις. Ξεκινάμε από raw δηλώσεις →
# MAGIC καταλήγουμε σε καθαρό feature table.
# MAGIC
# MAGIC **Τι θα μάθετε:**
# MAGIC - Imputation κενών τιμών με median
# MAGIC - 6 features για ΑΑΔΕ context (tax_rate, is_high_income, YoY change, κλπ)
# MAGIC - Quality checks σε features
# MAGIC - Αποθήκευση σε Delta + correlation ranking
# MAGIC
# MAGIC **Περιβάλλον:** Databricks Free Edition (Serverless)
# MAGIC **Διάρκεια:** ~25-30 λεπτά

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 1: Schema & Volume Setup
# MAGIC
# MAGIC Δημιουργούμε schema `workspace.aade` και volume `workspace.aade.aade_data`
# MAGIC αν δεν υπάρχουν. Όλοι οι πίνακες θα μπουν εδώ.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
print("✓ Schema workspace.aade & Volume aade_data έτοιμα")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 2: Dataset Φορολογικών Δηλώσεων
# MAGIC
# MAGIC 20 εγγραφές = 8 φορολογούμενοι × 2-3 έτη (2023-2025).
# MAGIC **Σκόπιμα έχουμε κενά** σε `income`, `expenses`, `tax_paid` για να δείξουμε imputation.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, when, lit, lag, avg, count, abs as spark_abs, round as spark_round, desc
from pyspark.sql.window import Window

schema = StructType([
    StructField("afm", StringType(), False),
    StructField("name", StringType(), False),
    StructField("region", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("income", DoubleType(), True),
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
# MAGIC Μετράμε **πόσα κενά** έχει κάθε στήλη. Σε production δεν αγνοούμε ποτέ nulls
# MAGIC — ή τα γεμίζουμε (imputation) ή τα διώχνουμε.

# COMMAND ----------

print("=== Missing values ανά στήλη ===")
null_counts = df_raw.select([count(when(col(c).isNull(), c)).alias(c) for c in df_raw.columns])
null_counts.show()

print("=== Ποιες εγγραφές έχουν κενό income; ===")
df_raw.filter(col("income").isNull()).select("afm", "name", "year", "income", "tax_paid").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 4: Imputation με Median
# MAGIC
# MAGIC Γεμίζουμε τα κενά με τη **διάμεσο** (median) — πιο ανθεκτικό από το μέσο όρο σε outliers.
# MAGIC Π.χ. ένας φορολογούμενος με 1.000.000€ θα παραμορφώσει τον μέσο όρο, αλλά όχι τη διάμεσο.

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
# MAGIC ## Βήμα 5: Feature `tax_rate`
# MAGIC
# MAGIC **Φορολογικός συντελεστής** = `tax_paid / income × 100`.
# MAGIC
# MAGIC *Γιατί χρήσιμο*: ένας elegctής μπορεί να εντοπίσει ασυνήθιστα **χαμηλό** tax_rate
# MAGIC σε σχέση με τον κλάδο/περιοχή — πιθανή υπο-δήλωση.

# COMMAND ----------

df_feat = df_imputed.withColumn("tax_rate", spark_round(col("tax_paid") / col("income") * 100, 2))
df_feat.select("afm", "name", "year", "income", "tax_paid", "tax_rate").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6: Feature `is_high_income`
# MAGIC
# MAGIC **Boolean** flag: 1 αν income > 60.000€, αλλιώς 0.
# MAGIC
# MAGIC *Γιατί*: τα μοντέλα ML συχνά χειρίζονται διαφορετικά τα high-income brackets
# MAGIC (π.χ. διαφορετικός tax tier, διαφορετικά risk patterns).

# COMMAND ----------

df_feat = df_feat.withColumn("is_high_income", when(col("income") > 60000, lit(1)).otherwise(lit(0)))
df_feat.select("afm", "name", "year", "income", "is_high_income").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 7: Feature `income_change_yoy` (Year-over-Year)
# MAGIC
# MAGIC **% μεταβολή** εισοδήματος σε σχέση με το προηγούμενο έτος.
# MAGIC
# MAGIC *Γιατί*: ξαφνική **πτώση 50%** χωρίς προφανή λόγο = πιθανώς ύποπτη.
# MAGIC Window function `lag()` φέρνει την προηγούμενη γραμμή ανά ΑΦΜ.

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
# MAGIC *Γιατί*: φορολογούμενοι με 1 μόνο δήλωση (νέοι ή sporadic) είναι διαφορετικό
# MAGIC profile από αυτούς με 10+ δηλώσεις (σταθεροί).

# COMMAND ----------

window_count = Window.partitionBy("afm")
df_feat = df_feat.withColumn("declaration_count", count("*").over(window_count))
df_feat.select("afm", "name", "year", "declaration_count").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 9: Feature `avg_income_region`
# MAGIC
# MAGIC **Μέσο εισόδημα** ανά περιοχή & έτος.
# MAGIC
# MAGIC *Γιατί*: ένα εισόδημα 30.000€ είναι «κανονικό» στην Κρήτη, ίσως «χαμηλό» στην Αττική.
# MAGIC Το feature επιτρέπει στο μοντέλο να συγκρίνει με τον μέσο όρο της περιοχής.

# COMMAND ----------

window_region = Window.partitionBy("region", "year")
df_feat = df_feat.withColumn("avg_income_region", spark_round(avg("income").over(window_region), 2))
df_feat.select("afm", "region", "year", "income", "avg_income_region").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 10: Feature `expense_ratio`
# MAGIC
# MAGIC **Λόγος δαπανών προς εισόδημα** σε ποσοστό.
# MAGIC
# MAGIC *Γιατί*: expense_ratio > 80% σε επιχείρηση = ή πολύ συγκεκριμένος κλάδος
# MAGIC (π.χ. εστίαση), ή φουσκωμένα έξοδα. Σημαντικό feature για audit prioritization.

# COMMAND ----------

df_feat = df_feat.withColumn("expense_ratio", spark_round(col("expenses") / col("income") * 100, 2))
df_feat.select("afm", "name", "year", "income", "expenses", "expense_ratio").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 11: Quality Checks
# MAGIC
# MAGIC Πριν κάνουμε save, **επαληθεύουμε**:
# MAGIC 1. Δεν έχουμε nulls στα νέα features (πέρα από `income_change_yoy` στο πρώτο έτος)
# MAGIC 2. Οι τιμές είναι σε λογικά εύρη (tax_rate ∈ [0, 100])
# MAGIC 3. Distribution stats φαίνονται OK
# MAGIC 4. Class balance σε boolean features (is_high_income)

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
# MAGIC Saving features σε `workspace.aade.tax_features`. **Delta = ACID + time travel**:
# MAGIC αν αύριο σπάσει κάτι, μπορούμε να γυρίσουμε σε προηγούμενη version.

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
# MAGIC Ποιο feature **συσχετίζεται** πιο πολύ με τον φόρο που πληρώθηκε; Αυτό μας δίνει
# MAGIC πρώτη εικόνα ποια features είναι «δυνατά» πριν χτίσουμε ML model.

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
    StructField("abs_correlation", DoubleType(), False),
    StructField("correlation", DoubleType(), False),
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
print(f"  Αρχικές εγγραφές:     {df_raw.count()}")
print(f"  Features δημιουργήθηκαν: {len(feature_cols)}")
print(f"  Missing values:       imputation με median")
print(f"  Quality checks:       4 (nulls, range, distribution, balance)")
print(f"  Delta Table:          workspace.aade.tax_features")
print()
print("  Top-3 features (correlation με tax_paid):")
df_corr.limit(3).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση
# MAGIC
# MAGIC ### Τι μάθατε:
# MAGIC - **Imputation** missing values με median (resilient σε outliers)
# MAGIC - **6 features** για ΑΑΔΕ context (tax_rate, is_high_income, YoY change, declaration_count, avg_income_region, expense_ratio)
# MAGIC - **Window functions** (lag, count, avg) για aggregations χωρίς να χάνουμε τη λεπτομέρεια
# MAGIC - **Quality checks** (nulls, range, distribution, balance)
# MAGIC - **Delta save** για ACID + time travel
# MAGIC - **Correlation ranking** για πρώτη εικόνα feature importance
# MAGIC
# MAGIC ### Επόμενα βήματα (production):
# MAGIC - Train/test split με **stratified sampling** (αν target imbalanced)
# MAGIC - **Point-in-time correctness** (αποφυγή data leakage)
# MAGIC - **Feature Store** για reusability μεταξύ μοντέλων
# MAGIC - ML model training με MLflow tracking
# MAGIC
# MAGIC **Note**: τρέχει πλήρως σε Databricks Free Edition (Serverless).
