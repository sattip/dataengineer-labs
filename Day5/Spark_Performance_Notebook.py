# Databricks notebook source
# MAGIC %md
# MAGIC # Άσκηση #1 — Spark Performance Deep Dive (Ημέρα 5)
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC
# MAGIC > **GitHub** (share this στο Zoom chat):
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day5/Spark_Performance_Notebook.py
# MAGIC > ```
# MAGIC
# MAGIC **Catalog/Schema**: `workspace.aade`
# MAGIC **Διάρκεια**: ~75–90 min (hands-on demo για αρχάριους)
# MAGIC **Prerequisite**: Έχετε δει τα slides 4–20 της Ημέρας 5 (Spark internals + optimization)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε σε αυτό το lab
# MAGIC
# MAGIC Όλα όσα ακούσατε στη θεωρία (executors, partitions, shuffle, skew, salting, prunings...) εδώ τα **πιάνετε στα χέρια σας** — με κώδικα που τρέχει, με μετρήσιμα BEFORE/AFTER, και με ΑΑΔΕ-ish σενάριο.
# MAGIC
# MAGIC Στο τέλος θα μπορείτε να:
# MAGIC
# MAGIC | # | Τεχνική | Πότε τη βγάζετε από το «εργαλειοθήκη» |
# MAGIC |---|---------|---------------------------------------|
# MAGIC | 1 | Διαβάζετε το **physical plan** (`.explain()`) | Πάντα, πριν τρέξετε βαρύ query |
# MAGIC | 2 | **Column Pruning** — `select` συγκεκριμένων στηλών | Όταν διαβάζετε wide tables |
# MAGIC | 3 | **Predicate Pushdown** — filter στο file level | Όταν φιλτράρετε σε Parquet/Delta |
# MAGIC | 4 | **Partition Pruning** — filter σε partition column | Όταν ο πίνακας είναι partitioned by date/region |
# MAGIC | 5 | Αναγνωρίζετε **shuffle** (wide transformations) | Όταν βλέπετε `Exchange` στο plan |
# MAGIC | 6 | Διαγιγνώσκετε **data skew** | Όταν 199 tasks τελειώνουν γρήγορα και 1 τρέχει 30 min |
# MAGIC | 7 | Εφαρμόζετε **salting** για skewed joins/groupBy | Όταν AQE δεν αρκεί |
# MAGIC | 8 | **Broadcast Join** για μικρό-vs-μεγάλο | Όταν ο μικρός πίνακας < 100 MB |
# MAGIC | 9 | Ενεργοποιείτε **AQE** | Παντού στο Spark 3.x — production default |
# MAGIC | 10 | **Cache** σωστά (όχι παντού) | Όταν ο ίδιος DataFrame διαβάζεται 2+ φορές |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ Το σενάριο
# MAGIC
# MAGIC Είστε **νέοι Data Engineers στην ΑΑΔΕ**. Σας δίνουν dataset 1.000.000 φορολογικών δηλώσεων από όλη την Ελλάδα. Πρέπει να βγάλετε KPIs ανά **Περιφέρεια** και **έτος**.
# MAGIC
# MAGIC Στην Ελλάδα η **Αττική** συγκεντρώνει ~70% του πληθυσμού — οπότε και ~70% των δηλώσεων. Αυτή η κατανομή θα μας προκαλέσει **data skew** όταν κάνουμε `groupBy('Περιφέρεια')` και θα δούμε live πώς το λύνουμε.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Δομή notebook
# MAGIC
# MAGIC | Cell | Θέμα | Time |
# MAGIC |---|------|------|
# MAGIC | 0 | Setup + generate synthetic data | 3 min |
# MAGIC | 1 | 🏗️ Spark Architecture — Driver/Executors/Tasks/Partitions | 7 min |
# MAGIC | 2 | 🔢 Partitions — `repartition` vs `coalesce` | 6 min |
# MAGIC | 3 | ➡️↔️ Narrow vs Wide transformations | 6 min |
# MAGIC | 4 | 🪓 Column Pruning — `select(cols)` vs `select("*")` | 6 min |
# MAGIC | 5 | 🔍 Predicate Pushdown — `WHERE` στο file level | 6 min |
# MAGIC | 6 | 🗂️ Partition Pruning — partitioned table | 7 min |
# MAGIC | 7 | 🌀 Shuffle — τι είναι, πότε συμβαίνει | 5 min |
# MAGIC | 8 | ⚖️ Data Skew — διάγνωση | 7 min |
# MAGIC | 9 | 🧂 Salting — η κλασική λύση | 10 min |
# MAGIC | 10 | 📡 Broadcast Join — μικρό-vs-μεγάλο | 6 min |
# MAGIC | 11 | 🚀 AQE — Adaptive Query Execution | 6 min |
# MAGIC | 12 | 💾 Cache — πότε βοηθάει, πότε βλάπτει | 6 min |
# MAGIC | 13 | 🩺 Spark UI roadmap | 5 min |
# MAGIC | 14 | ✅ Cheat sheet | — |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0 — Setup + Generate synthetic data
# MAGIC
# MAGIC Φτιάχνουμε ένα **realistic σύνθετο dataset** 1M φορολογικών δηλώσεων με σκόπιμη **skewed κατανομή** στις Περιφέρειες (Αττική 70%, Θεσσαλονίκη 10%, υπόλοιπες μοιράζονται το 20%).
# MAGIC
# MAGIC Όλο το lab τρέχει με αυτά τα δεδομένα — δεν χρειάζεται download από εξωτερικό URL.

# COMMAND ----------

import time
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, expr, rand, when, broadcast, concat_ws,
    floor, count, sum as spark_sum, avg
)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOL = "/Volumes/workspace/aade/aade_data"

# Realistic skew για 1M φορολογικές δηλώσεις
PERIFEREIES = [
    ("Αττική",                    0.70),  # σκόπιμο skew
    ("Κεντρική Μακεδονία",        0.10),
    ("Πελοπόννησος",              0.04),
    ("Δυτική Ελλάδα",             0.03),
    ("Θεσσαλία",                  0.03),
    ("Κρήτη",                     0.03),
    ("Ανατολική Μακεδονία-Θράκη", 0.02),
    ("Ήπειρος",                   0.02),
    ("Ιόνια Νησιά",               0.01),
    ("Στερεά Ελλάδα",             0.01),
    ("Βόρειο Αιγαίο",             0.005),
    ("Νότιο Αιγαίο",              0.005),
    ("Δυτική Μακεδονία",          0.005),
]

N_ROWS = 1_000_000

# Build region distribution expression: για κάθε row, διάλεξε region βάσει cumulative prob
cum = 0.0
case_expr = "CASE"
for region, p in PERIFEREIES:
    case_expr += f" WHEN _r < {cum + p} THEN '{region}'"
    cum += p
case_expr += " ELSE 'Αττική' END"

declarations = (
    spark.range(N_ROWS).withColumnRenamed("id", "declaration_id")
    .withColumn("_r", rand(seed=42))
    .withColumn("Περιφέρεια", expr(case_expr))
    .drop("_r")
    .withColumn("afm", (lit(100_000_000) + (col("declaration_id") % 900_000_000)).cast("string"))
    .withColumn("doy_code", (col("declaration_id") % 50 + 1).cast("string"))
    .withColumn("year", (lit(2020) + (col("declaration_id") % 5)).cast("int"))
    .withColumn("tax_category", expr("element_at(array('ΦΠΑ','Ακινήτων','Εισοδήματος','Κληρονομιάς'), CAST(declaration_id % 4 + 1 AS INT))"))
    .withColumn("tax_amount", (rand(seed=7) * 20000 + 100).cast("double"))
    .withColumn("status", expr("element_at(array('Εγκεκριμένη','Απορριφθείσα','Εκκρεμής'), CAST(declaration_id % 3 + 1 AS INT))"))
)

# Persist ως plain Parquet (όχι partitioned) στο volume + ως Delta managed table
PARQUET_PATH = f"{VOL}/declarations_perf.parquet"
declarations.write.mode("overwrite").parquet(PARQUET_PATH)

(
    declarations.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.aade.declarations_big")
)

print(f"✅ generated {N_ROWS:,} rows")
print(f"   parquet:  {PARQUET_PATH}")
print(f"   delta tbl: workspace.aade.declarations_big")
spark.table("workspace.aade.declarations_big").groupBy("Περιφέρεια").count() \
    .orderBy(F.desc("count")).show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — 🏗️ Spark Architecture: Driver, Executors, Tasks, Partitions
# MAGIC
# MAGIC Πριν μπούμε σε optimizations, ας μιλήσουμε για το τι ΣΥΜΒΑΙΝΕΙ ΠΡΑΓΜΑΤΙΚΑ όταν τρέχετε ένα `df.count()`.
# MAGIC
# MAGIC ### Η αρχιτεκτονική (visual)
# MAGIC
# MAGIC ```
# MAGIC  ┌─────────────────────────────────────────────────┐
# MAGIC  │   DRIVER (ο εγκέφαλος — 1 process)              │
# MAGIC  │   • Παίρνει τον κώδικα σας                       │
# MAGIC  │   • Φτιάχνει DAG (γράφος εργασιών)                │
# MAGIC  │   • Στέλνει TASKS στους executors                │
# MAGIC  └────────┬────────────┬───────────┬────────────────┘
# MAGIC           │            │           │
# MAGIC           ▼            ▼           ▼
# MAGIC  ┌───────────┐ ┌───────────┐ ┌───────────┐
# MAGIC  │ EXECUTOR  │ │ EXECUTOR  │ │ EXECUTOR  │   ← παράλληλοι εργάτες
# MAGIC  │  cores: 4 │ │  cores: 4 │ │  cores: 4 │
# MAGIC  │  RAM: 8GB │ │  RAM: 8GB │ │  RAM: 8GB │
# MAGIC  └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
# MAGIC        │             │             │
# MAGIC        ▼             ▼             ▼
# MAGIC      Task1         Task2         Task3
# MAGIC     (Partition1)  (Partition2)  (Partition3)
# MAGIC ```
# MAGIC
# MAGIC ### Οι βασικοί όροι
# MAGIC
# MAGIC | Όρος | Τι είναι | Αναλογία |
# MAGIC |------|----------|----------|
# MAGIC | **Driver** | Το μυαλό — 1 process που σχεδιάζει | Ο **αρχιτέκτονας** |
# MAGIC | **Executor** | Ο εργάτης — έχει cores + RAM | Ο **εργολάβος** σε εργοτάξιο |
# MAGIC | **Task** | 1 «μονάδα δουλειάς» που τρέχει σε 1 core | Ένα **τσιμεντόλιθο** να σηκωθεί |
# MAGIC | **Partition** | 1 «κομμάτι» δεδομένων που επεξεργάζεται μαζί | Ένα **παλέτι** τούβλα |
# MAGIC | **Job** | Όλη η δουλειά για να βγει ένα `action` (π.χ. `count()`) | Ένα **σπίτι** |
# MAGIC | **Stage** | Ομάδα tasks χωρίς shuffle | Ένας **όροφος** του σπιτιού |
# MAGIC
# MAGIC ### Ο χρυσός κανόνας
# MAGIC
# MAGIC > **1 Task = 1 Partition = 1 Core**.
# MAGIC > Αν έχετε 100 partitions και 10 cores → trέχουν 10 tasks παράλληλα, 10 «κύκλοι».
# MAGIC > Αν έχετε 5 partitions και 10 cores → trέχουν 5 tasks, **5 cores μένουν αδρανείς**.

# COMMAND ----------

# Δες πόσα cores έχει το cluster
print(f"defaultParallelism (cores διαθέσιμοι): {sc.defaultParallelism}")

df = spark.table("workspace.aade.declarations_big")
print(f"Default partitions του DataFrame: {df.rdd.getNumPartitions()}")

# Δες πόσα rows ανά partition (γρήγορος sanity check για skew)
from pyspark.sql.functions import spark_partition_id
partition_sizes = (
    df.withColumn("pid", spark_partition_id())
    .groupBy("pid").count()
    .orderBy("pid")
)
partition_sizes.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — 🔢 Partitions: `repartition` vs `coalesce`
# MAGIC
# MAGIC Πώς αλλάζουμε τον αριθμό των partitions; Δύο εντολές που μοιάζουν ίδιες αλλά **είναι πολύ διαφορετικές**:
# MAGIC
# MAGIC | | `repartition(n)` | `coalesce(n)` |
# MAGIC |--|--------------------|----------------|
# MAGIC | **Τι κάνει** | Ξανα-διαμοιράζει σε `n` ομοιόμορφα partitions | Ενώνει υπάρχοντα partitions, **όχι** redistribution |
# MAGIC | **Shuffle;** | ✅ ΝΑΙ (ακριβό) | ❌ ΟΧΙ (φθηνό) |
# MAGIC | **Πότε αύξηση;** | ✅ Αύξηση ή μείωση | ❌ ΜΟΝΟ μείωση |
# MAGIC | **Balance;** | ✅ Ομοιόμορφο | ⚠️ Μπορεί να μείνει skewed |
# MAGIC | **Use case** | Πριν από heavy operation | Πριν από write (μειώνεις output files) |
# MAGIC
# MAGIC ### Vibes test
# MAGIC
# MAGIC - Μετά από big filter (έμειναν λίγα rows σε πολλά partitions) → `coalesce(10)` για να γράψεις λίγα output files
# MAGIC - Πριν από heavy join σε skewed data → `repartition(200, "key")` για ομοιόμορφο shuffle

# COMMAND ----------

df = spark.table("workspace.aade.declarations_big")

print(f"Αρχικά partitions: {df.rdd.getNumPartitions()}")

# repartition → shuffle, ομοιόμορφα 16 partitions
df16 = df.repartition(16)
print(f"Μετά από repartition(16): {df16.rdd.getNumPartitions()}")

# coalesce → χωρίς shuffle, στρίβει τα partitions
df_coal = df.coalesce(4)
print(f"Μετά από coalesce(4): {df_coal.rdd.getNumPartitions()}")

# Παρατήρηση: το coalesce ΔΕΝ ισορροπεί τα δεδομένα — απλά «συγχωνεύει» partitions
sizes_coal = (df_coal.withColumn("pid", spark_partition_id())
                     .groupBy("pid").count().orderBy("pid"))
print("\n💡 Coalesce → τα partitions μπορεί να είναι άνισα σε μέγεθος:")
sizes_coal.show()

sizes_rep = (df16.withColumn("pid", spark_partition_id())
                 .groupBy("pid").count().orderBy("pid"))
print("\n💡 Repartition → ομοιόμορφα:")
sizes_rep.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — ➡️↔️ Narrow vs Wide transformations
# MAGIC
# MAGIC Όλα τα Spark transformations χωρίζονται σε **δύο κατηγορίες**:
# MAGIC
# MAGIC | | Narrow | Wide |
# MAGIC |--|--------|------|
# MAGIC | **Τι κάνει** | Κάθε output partition διαβάζει 1 input partition | Πρέπει να «ταξιδέψουν» δεδομένα μεταξύ partitions |
# MAGIC | **Shuffle;** | ❌ ΟΧΙ | ✅ ΝΑΙ — **ακριβό!** |
# MAGIC | **Παραδείγματα** | `select`, `filter`, `withColumn`, `map` | `groupBy`, `join`, `distinct`, `orderBy`, `repartition` |
# MAGIC | **Στο `.explain()`** | Κανονικά steps | Εμφανίζεται **`Exchange`** node |
# MAGIC
# MAGIC ### Γιατί μας νοιάζει;
# MAGIC
# MAGIC Το shuffle:
# MAGIC 1. **Γράφει στο δίσκο** (disk I/O)
# MAGIC 2. **Στέλνει μέσω δικτύου** (network I/O)
# MAGIC 3. **Διαβάζει πάλι από δίσκο** σε άλλο executor
# MAGIC
# MAGIC Είναι **10-100x πιο αργό** από narrow transformation. Στόχος: **όσο λιγότερα shuffles, τόσο καλύτερα**.

# COMMAND ----------

df = spark.table("workspace.aade.declarations_big")

# Narrow: μόνο filter + select — κανένα shuffle
print("="*60)
print("NARROW (filter + select) → ΟΧΙ shuffle")
print("="*60)
narrow = df.filter(col("year") == 2024).select("declaration_id", "tax_amount")
narrow.explain()

# COMMAND ----------

# Wide: groupBy — απαιτεί shuffle για να μαζευτούν τα ίδια keys στο ίδιο partition
print("="*60)
print("WIDE (groupBy) → ΝΑΙ shuffle — δες `Exchange hashpartitioning`")
print("="*60)
wide = df.groupBy("Περιφέρεια").agg(spark_sum("tax_amount").alias("total"))
wide.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 **Διάβασε το physical plan από κάτω προς τα πάνω**. Το `Exchange hashpartitioning(Περιφέρεια#...)` σου λέει: «εδώ συμβαίνει shuffle, τα δεδομένα ξανα-διαμοιράζονται με βάση το `Περιφέρεια`».
# MAGIC >
# MAGIC > Αν δεις πάνω από 1-2 `Exchange` σε ένα query, **ψάξε να τα μειώσεις**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — 🪓 Column Pruning
# MAGIC
# MAGIC Το **πρώτο** optimization που πρέπει να κάνετε ΠΑΝΤΑ: **διαβάζετε μόνο τις στήλες που χρειάζεστε**.
# MAGIC
# MAGIC ### Γιατί δουλεύει;
# MAGIC
# MAGIC Το **Parquet** και το **Delta** είναι **columnar formats** — αποθηκεύουν τη κάθε στήλη σε ξεχωριστό block. Όταν κάνεις:
# MAGIC
# MAGIC ```python
# MAGIC df.select("declaration_id", "tax_amount")
# MAGIC ```
# MAGIC
# MAGIC το Spark **παρακάμπτει εντελώς** τα blocks των άλλων στηλών. **Δεν διαβάζει** byte από αυτές.
# MAGIC
# MAGIC ### Πόσο γρήγορα;
# MAGIC
# MAGIC Πίνακας με 30 στήλες και χρειάζεσαι 3 → **10x λιγότερο I/O**.

# COMMAND ----------

import time

# BAD: διαβάζει όλες τις στήλες ακόμα κι αν χρειάζεσαι 2
print("="*60)
print("BAD — select('*')")
print("="*60)
t0 = time.time()
df_all = spark.read.parquet(PARQUET_PATH)
total_bad = df_all.agg(spark_sum("tax_amount")).collect()[0][0]
print(f"  result: {total_bad:,.2f}")
print(f"  time:   {time.time()-t0:.2f}s")
df_all.select("*").explain(mode="formatted")

# COMMAND ----------

# GOOD: μόνο η στήλη που χρειάζεσαι
print("="*60)
print("GOOD — select('tax_amount')")
print("="*60)
t0 = time.time()
df_thin = spark.read.parquet(PARQUET_PATH).select("tax_amount")
total_good = df_thin.agg(spark_sum("tax_amount")).collect()[0][0]
print(f"  result: {total_good:,.2f}  (ίδιο)")
print(f"  time:   {time.time()-t0:.2f}s")
df_thin.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 Παρατήρησε στο **`ReadSchema`** του formatted plan — με `select("tax_amount")` φαίνεται **μόνο** η στήλη `tax_amount`. Το Spark **δεν διαβάζει** τις άλλες 7.
# MAGIC >
# MAGIC > Σε production datasets με 50-100 στήλες, αυτό το «τίποτα» κάνει queries **5-10x πιο γρήγορα**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — 🔍 Predicate Pushdown
# MAGIC
# MAGIC «Predicate» = το **WHERE clause**. «Pushdown» = το **σπρώχνουμε όσο πιο κάτω γίνεται** — μέχρι το επίπεδο του αρχείου.
# MAGIC
# MAGIC ### Τι σημαίνει αυτό;
# MAGIC
# MAGIC Το Parquet έχει **min/max statistics** για κάθε **row group** (συνήθως 128 MB). Όταν γράφεις:
# MAGIC
# MAGIC ```python
# MAGIC df.filter(col("year") == 2024)
# MAGIC ```
# MAGIC
# MAGIC το Spark πριν διαβάσει ένα row group:
# MAGIC 1. Κοιτά το min/max του `year` στο metadata
# MAGIC 2. Αν `min=2020, max=2022` → **παραλείπει το row group**
# MAGIC 3. Αν `min=2023, max=2024` → διαβάζει
# MAGIC
# MAGIC ### Πώς το βλέπεις;
# MAGIC
# MAGIC Στο `.explain(mode="formatted")` ψάχνεις για **`PushedFilters`** στο `Batched Scan` node.

# COMMAND ----------

df = spark.read.parquet(PARQUET_PATH)

# Predicate filter — πρέπει να δούμε PushedFilters
print("="*60)
print("WHERE year = 2024 → ψάξε στο plan για PushedFilters: [EqualTo(year,2024)]")
print("="*60)
filtered = df.filter(col("year") == 2024).select("declaration_id", "tax_amount", "year")
filtered.explain(mode="formatted")

# COMMAND ----------

# Μέτρα την διαφορά: full scan vs pushed filter
print("="*60)
print("BAD — count() χωρίς filter (διαβάζει όλα)")
print("="*60)
t0 = time.time()
n_all = df.count()
print(f"  rows: {n_all:,}  time: {time.time()-t0:.2f}s")

print("\n" + "="*60)
print("GOOD — count() με pushdown filter (διαβάζει μόνο 2024)")
print("="*60)
t0 = time.time()
n_2024 = df.filter(col("year") == 2024).count()
print(f"  rows: {n_2024:,}  time: {time.time()-t0:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 **Filter early**. Όσο πιο πάνω στο pipeline βάλεις το `filter()`, τόσο λιγότερα δεδομένα ταξιδεύουν στα επόμενα stages.
# MAGIC >
# MAGIC > Common bug: filter ΜΕΤΑ από `join` αντί για ΠΡΙΝ. Το αποτέλεσμα είναι ίδιο, αλλά η ταχύτητα διαφέρει 5-50x.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 — 🗂️ Partition Pruning
# MAGIC
# MAGIC Το **καλύτερο pruning** όλων: όλο το **partition φακέλου** δεν διαβάζεται καν.
# MAGIC
# MAGIC ### Τι είναι;
# MAGIC
# MAGIC Όταν γράφεις έναν πίνακα με:
# MAGIC
# MAGIC ```python
# MAGIC df.write.partitionBy("year").parquet(...)
# MAGIC ```
# MAGIC
# MAGIC ο Spark φτιάχνει **ξεχωριστούς φακέλους** στο storage:
# MAGIC
# MAGIC ```
# MAGIC declarations_partitioned/
# MAGIC ├── year=2020/  ← 200k files
# MAGIC ├── year=2021/  ← 200k files
# MAGIC ├── year=2022/  ← 200k files
# MAGIC ├── year=2023/  ← 200k files
# MAGIC └── year=2024/  ← 200k files
# MAGIC ```
# MAGIC
# MAGIC Όταν γράφεις `WHERE year = 2024`, το Spark **κοιτά μόνο τον φάκελο `year=2024/`**. Οι άλλοι δεν ανοίγουν καν.
# MAGIC
# MAGIC ### Vs Predicate Pushdown
# MAGIC
# MAGIC | | Predicate Pushdown | Partition Pruning |
# MAGIC |--|---------------------|-------------------|
# MAGIC | Granularity | row group (128 MB) | ολόκληρος φάκελος |
# MAGIC | Πόσο γρήγορο | 2-10x | **10-1000x** |
# MAGIC | Απαιτείται | Parquet/Delta | partitioned table |
# MAGIC
# MAGIC ### ⚠️ Trade-off
# MAGIC
# MAGIC Partitioning σε **πολλά keys** ή σε **high-cardinality column** (π.χ. afm) → χιλιάδες μικροί φάκελοι → χειρότερο από καθόλου partitioning. **Stick to 1-2 keys με χαμηλό cardinality** (year, region).

# COMMAND ----------

# Γράφω partitioned by year
PART_PATH = f"{VOL}/declarations_partitioned.parquet"
(
    spark.read.parquet(PARQUET_PATH)
    .write.mode("overwrite")
    .partitionBy("year")
    .parquet(PART_PATH)
)

import subprocess
result = subprocess.run(["ls", PART_PATH], capture_output=True, text=True)
print("📁 Partition folders που δημιουργήθηκαν:")
print(result.stdout)

# COMMAND ----------

# Query σε partitioned: ψάξε στο plan για PartitionFilters
df_part = spark.read.parquet(PART_PATH)

print("="*60)
print("WHERE year = 2024 σε partitioned → ψάξε για PartitionFilters")
print("="*60)
result = df_part.filter(col("year") == 2024).select("declaration_id", "tax_amount")
result.explain(mode="formatted")

# COMMAND ----------

# BEFORE/AFTER: non-partitioned vs partitioned για count(WHERE year=2024)
print("="*60)
print("BAD — count year=2024 στο NON-PARTITIONED file")
print("="*60)
t0 = time.time()
n1 = spark.read.parquet(PARQUET_PATH).filter(col("year") == 2024).count()
print(f"  rows: {n1:,}  time: {time.time()-t0:.2f}s")

print("\n" + "="*60)
print("GOOD — count year=2024 στο PARTITIONED file")
print("="*60)
t0 = time.time()
n2 = spark.read.parquet(PART_PATH).filter(col("year") == 2024).count()
print(f"  rows: {n2:,}  time: {time.time()-t0:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7 — 🌀 Shuffle: τι είναι, πότε συμβαίνει
# MAGIC
# MAGIC Το **shuffle** είναι το #1 bottleneck στο Spark. Όταν κάνεις:
# MAGIC
# MAGIC ```python
# MAGIC df.groupBy("Περιφέρεια").sum("tax_amount")
# MAGIC ```
# MAGIC
# MAGIC το Spark πρέπει να μαζέψει **όλα τα rows με το ίδιο `Περιφέρεια`** στο ίδιο partition. Αυτό σημαίνει:
# MAGIC
# MAGIC 1. **Map stage** — κάθε task γράφει «shuffle files» στον δίσκο, κατηγοριοποιημένα κατά hash(Περιφέρεια)
# MAGIC 2. **Network transfer** — οι reducers κατεβάζουν τα shuffle files
# MAGIC 3. **Reduce stage** — αθροίζει
# MAGIC
# MAGIC Είναι **disk + network heavy**.

# COMMAND ----------

df = spark.table("workspace.aade.declarations_big")

# Δες live τα stages και τα shuffle reads/writes στο Spark UI
print("="*60)
print("Τρέξε αυτό και κοίτα το Spark UI → Stages → Shuffle Read/Write")
print("="*60)
t0 = time.time()
result = df.groupBy("Περιφέρεια", "year").agg(
    spark_sum("tax_amount").alias("total"),
    count("*").alias("rows")
).collect()
print(f"  time: {time.time()-t0:.2f}s")
print(f"\nResults (top 5):")
for r in sorted(result, key=lambda x: -x["total"])[:5]:
    print(f"  {r['Περιφέρεια']:30} {r['year']}  total={r['total']:>15,.0f}  rows={r['rows']:>8,}")

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 Άνοιξε το **Spark UI** (πάνω-αριστερά στο notebook → "Spark UI"). Στο **Stages tab** δες:
# MAGIC > - **Shuffle Read** — πόσα bytes διάβασαν οι reducers
# MAGIC > - **Shuffle Write** — πόσα bytes έγραψαν οι mappers
# MAGIC > - **Task duration distribution** — αν η μέγιστη διάρκεια >> διάμεση → υπάρχει **skew**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8 — ⚖️ Data Skew: η διάγνωση
# MAGIC
# MAGIC «Skew» = ανισόρροπη κατανομή δεδομένων ανά key. Το πιο κλασικό πρόβλημα σε production Spark.
# MAGIC
# MAGIC ### Πώς το βλέπεις;
# MAGIC
# MAGIC **1. Στα δεδομένα σου** — `groupBy(skewed_col).count().orderBy(desc)` — αν ένα value έχει >>20% → skew.
# MAGIC
# MAGIC **2. Στο Spark UI** — Stages tab → Task Duration:
# MAGIC ```
# MAGIC Min   25%   50%   75%   Max
# MAGIC 1s    2s    3s    4s    30 min   ← red flag
# MAGIC ```
# MAGIC
# MAGIC **3. Συμπτώματα**: job κολλάει στο 99%. 199 tasks έτοιμα, 1 ακόμα τρέχει.

# COMMAND ----------

df = spark.table("workspace.aade.declarations_big")

# Διάγνωση: πόσο skewed είναι το Περιφέρεια;
print("="*60)
print("Κατανομή ανά Περιφέρεια (η Αττική έχει σκόπιμα 70%)")
print("="*60)
df.groupBy("Περιφέρεια").count() \
    .withColumn("pct", F.round(col("count")*100.0/N_ROWS, 1)) \
    .orderBy(F.desc("count")) \
    .show(20, truncate=False)

# COMMAND ----------

# Μέτρα: groupBy χωρίς treatment του skew
print("="*60)
print("BAD — groupBy('Περιφέρεια') χωρίς skew handling")
print("="*60)
spark.conf.set("spark.sql.adaptive.enabled", "false")  # για να δούμε καθαρά το skew
t0 = time.time()
df.groupBy("Περιφέρεια").agg(spark_sum("tax_amount")).collect()
print(f"  time: {time.time()-t0:.2f}s")
print("  → Στο Spark UI θα δεις 1 task πολύ πιο αργό από τα άλλα (Αττική)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9 — 🧂 Salting: η κλασική λύση για skew
# MAGIC
# MAGIC ### Η ιδέα
# MAGIC
# MAGIC Το πρόβλημα: όλα τα rows με `Περιφέρεια = "Αττική"` πάνε στο ίδιο partition → 1 task κάνει το 70% της δουλειάς.
# MAGIC
# MAGIC Η **λύση salting**:
# MAGIC 1. **Διασκόρπισε** τα Αττική rows σε π.χ. 10 «τεχνητά» keys: `Αττική_0, Αττική_1, ... Αττική_9`
# MAGIC 2. Κάνε `groupBy("salted_key")` → τώρα η δουλειά μοιράζεται σε 10 tasks
# MAGIC 3. **Ξανα-aggregate** σβήνοντας το salt → πραγματικό KPI ανά Περιφέρεια
# MAGIC
# MAGIC ### Σχηματικά
# MAGIC
# MAGIC ```
# MAGIC ΠΡΙΝ:                            ΜΕΤΑ (salt = 10):
# MAGIC Αττική        [700k rows]   →    Αττική_0 [70k]  Αττική_5 [70k]
# MAGIC Θεσσαλονίκη   [100k rows]        Αττική_1 [70k]  Αττική_6 [70k]
# MAGIC Πελοπόννησος  [ 40k rows]        Αττική_2 [70k]  Αττική_7 [70k]
# MAGIC ...                              Αττική_3 [70k]  Αττική_8 [70k]
# MAGIC                                   Αττική_4 [70k]  Αττική_9 [70k]
# MAGIC                                   Θεσσαλονίκη_0 [10k]  ...
# MAGIC ```
# MAGIC
# MAGIC Τώρα η δουλειά μοιράζεται **πιο ομοιόμορφα**.

# COMMAND ----------

SALT_FACTOR = 10

# Βήμα 1 — Πρόσθεσε salt column (random 0..9)
df_salted = df.withColumn("salt", (rand(seed=42) * SALT_FACTOR).cast("int"))
df_salted = df_salted.withColumn("salted_key", concat_ws("_", col("Περιφέρεια"), col("salt")))

print("Παράδειγμα salted keys:")
df_salted.select("Περιφέρεια", "salted_key").distinct().orderBy("salted_key").show(15, truncate=False)

# COMMAND ----------

# Βήμα 2 — Aggregate ανά salted_key (parallelism: 13 περιφέρειες × 10 salts = 130 keys)
# Βήμα 3 — Ξανα-aggregate σβήνοντας το salt → final KPI ανά Περιφέρεια

print("="*60)
print("GOOD — Salted groupBy → ξανα-aggregate")
print("="*60)
t0 = time.time()
result = (
    df_salted.groupBy("salted_key", "Περιφέρεια")
    .agg(spark_sum("tax_amount").alias("partial_sum"))
    .groupBy("Περιφέρεια")
    .agg(spark_sum("partial_sum").alias("total"))
    .orderBy(F.desc("total"))
    .collect()
)
print(f"  time: {time.time()-t0:.2f}s")
print(f"\nTop 5 by total tax:")
for r in result[:5]:
    print(f"  {r['Περιφέρεια']:30} total={r['total']:>15,.0f}")
print("\n  → Στο Spark UI: τα tasks έχουν πιο ομοιόμορφη διάρκεια")

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 **Πότε ΔΕΝ χρειάζεστε salting**: αν έχετε **AQE enabled** (Spark 3.x default), συχνά λύνει το skew αυτόματα. Δείτε Cell 11.
# MAGIC >
# MAGIC > **Πότε χρειάζεστε**: extreme skew (>50% σε ένα key), όπου το AQE skew threshold δεν trigger-άρει. Ή σε streaming joins όπου το AQE είναι πιο limited.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10 — 📡 Broadcast Join: μικρό-vs-μεγάλο
# MAGIC
# MAGIC ### Το πρόβλημα
# MAGIC
# MAGIC Έχεις:
# MAGIC - `declarations_big` — 1.000.000 rows (το «μεγάλο»)
# MAGIC - `doy_dim` — 50 ΔΟΥ της Ελλάδας (το «μικρό»)
# MAGIC
# MAGIC Κάνεις `declarations.join(doy_dim, "doy_code")`. **Default behavior**: shuffle join — και τα 2 πίνακες κάνουν shuffle στο `doy_code`. Ακριβό!
# MAGIC
# MAGIC ### Η λύση: broadcast
# MAGIC
# MAGIC Αφού το `doy_dim` είναι μικρό (< 100 MB), **στέλνουμε ολόκληρο** το `doy_dim` σε κάθε executor. Τότε το join γίνεται **local** σε κάθε partition — **κανένα shuffle**.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC df.join(broadcast(doy_dim), "doy_code")
# MAGIC ```

# COMMAND ----------

# Φτιάχνουμε το μικρό doy_dim
doy_dim = spark.createDataFrame(
    [(str(i+1), f"ΔΟΥ {i+1}", "Αττική" if i < 25 else "Άλλη") for i in range(50)],
    ["doy_code", "doy_name", "region_group"]
)
doy_dim.show(5)

# COMMAND ----------

# BAD — shuffle join (default σε shuffle αν AQE είναι disabled)
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable auto-broadcast

print("="*60)
print("BAD — Shuffle join")
print("="*60)
df = spark.table("workspace.aade.declarations_big")
joined_bad = df.join(doy_dim, "doy_code")
joined_bad.explain()

t0 = time.time()
joined_bad.count()
print(f"  time: {time.time()-t0:.2f}s")

# COMMAND ----------

# GOOD — broadcast hint
print("="*60)
print("GOOD — Broadcast join")
print("="*60)
joined_good = df.join(broadcast(doy_dim), "doy_code")
joined_good.explain()

t0 = time.time()
joined_good.count()
print(f"  time: {time.time()-t0:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 Παρατήρησε στο plan: αντί για `SortMergeJoin` (shuffle), τώρα βλέπεις **`BroadcastHashJoin`** — και **`BroadcastExchange`** στη μία πλευρά.
# MAGIC >
# MAGIC > **Auto-broadcast**: αν αφήσεις `spark.sql.autoBroadcastJoinThreshold = 10MB` (default), το Spark κάνει broadcast αυτόματα για μικρούς πίνακες. Το `broadcast()` hint είναι κυρίως για όταν το auto δεν trigger-άρει.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 11 — 🚀 AQE: Adaptive Query Execution
# MAGIC
# MAGIC Στο **Spark 3.x default** είναι **ενεργό** (`spark.sql.adaptive.enabled = true`). Είναι το **πιο σημαντικό** feature του Spark της τελευταίας 5ετίας.
# MAGIC
# MAGIC ### Τι κάνει
# MAGIC
# MAGIC Παίρνει αποφάσεις **runtime** βασισμένες σε πραγματικά metrics από τα προηγούμενα stages:
# MAGIC
# MAGIC | Feature | Τι κάνει |
# MAGIC |---------|----------|
# MAGIC | **Dynamically coalesce shuffle partitions** | Αν μετά από shuffle τα partitions είναι μικρά, τα συγχωνεύει. Δες λιγότερα tasks → λιγότερο overhead |
# MAGIC | **Dynamically switch join strategies** | Αν runtime ένας πίνακας είναι μικρός, switch σε broadcast |
# MAGIC | **Dynamically optimize skew joins** | Σπάει αυτόματα τα skewed partitions σε μικρότερα |
# MAGIC
# MAGIC ### Πότε δεν αρκεί
# MAGIC
# MAGIC - Streaming queries (limited AQE support)
# MAGIC - Extreme skew (95%+ σε ένα key) — εκεί χρειάζεται manual salting
# MAGIC - Small clusters με ελάχιστο data — overhead AQE > όφελος

# COMMAND ----------

# Toggle AQE και δες τη διαφορά στο ίδιο skewed groupBy
df = spark.table("workspace.aade.declarations_big")

print("="*60)
print("AQE DISABLED — manual skew handling needed")
print("="*60)
spark.conf.set("spark.sql.adaptive.enabled", "false")
t0 = time.time()
df.groupBy("Περιφέρεια").agg(spark_sum("tax_amount")).collect()
t_off = time.time() - t0
print(f"  time: {t_off:.2f}s")

print("\n" + "="*60)
print("AQE ENABLED — auto-handles skew")
print("="*60)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
t0 = time.time()
df.groupBy("Περιφέρεια").agg(spark_sum("tax_amount")).collect()
t_on = time.time() - t0
print(f"  time: {t_on:.2f}s")

print(f"\n  Improvement: {((t_off - t_on) / t_off * 100):.1f}% faster με AQE")

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 **Production default**:
# MAGIC > ```python
# MAGIC > spark.conf.set("spark.sql.adaptive.enabled", "true")
# MAGIC > spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# MAGIC > spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# MAGIC > ```
# MAGIC > Στο Databricks είναι ήδη ενεργό. **Μην το απενεργοποιείτε** εκτός αν ξέρετε γιατί.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 12 — 💾 Cache: πότε βοηθάει, πότε βλάπτει
# MAGIC
# MAGIC ### Το concept
# MAGIC
# MAGIC `df.cache()` λέει στο Spark: «**κράτα αυτό το DataFrame στη μνήμη**, θα τον ξαναζητήσω».
# MAGIC
# MAGIC ### ⚠️ ΟΧΙ πανάκεια
# MAGIC
# MAGIC Κοινό λάθος αρχαρίων: «βάζω `.cache()` παντού, θα γίνει πιο γρήγορο».
# MAGIC
# MAGIC Πραγματικότητα:
# MAGIC
# MAGIC | Σενάριο | Cache; |
# MAGIC |---------|--------|
# MAGIC | Διαβάζω τον df **1 φορά** | ❌ ΟΧΙ — overhead χωρίς όφελος |
# MAGIC | Διαβάζω τον df **2-3 φορές** σε επόμενα queries | ✅ ΝΑΙ |
# MAGIC | Διαβάζω τον df **πολλές φορές** | ✅ ΝΑΙ — `MEMORY_AND_DISK` |
# MAGIC | Ο df είναι >50% του cluster memory | ⚠️ Προσοχή — eviction overhead |
# MAGIC
# MAGIC ### Πάντα `.unpersist()` όταν τελειώσεις

# COMMAND ----------

df = spark.table("workspace.aade.declarations_big")

# BAD: cache χωρίς reuse
print("="*60)
print("BAD — cache + μόνο 1 χρήση (waste)")
print("="*60)
df_cached = df.filter(col("year") == 2024).cache()
t0 = time.time()
df_cached.count()  # triggers cache materialization
print(f"  count: {time.time()-t0:.2f}s")
df_cached.unpersist()

# GOOD: cache + πολλαπλή χρήση
print("\n" + "="*60)
print("GOOD — cache + 3 χρήσεις (worth it)")
print("="*60)
df_2024 = df.filter(col("year") == 2024).cache()

t0 = time.time()
n = df_2024.count()
print(f"  1st count (materializes cache): {time.time()-t0:.2f}s, rows: {n}")

t0 = time.time()
df_2024.groupBy("Περιφέρεια").agg(spark_sum("tax_amount")).collect()
print(f"  2nd op (cache hit):             {time.time()-t0:.2f}s")

t0 = time.time()
df_2024.groupBy("status").count().collect()
print(f"  3rd op (cache hit):             {time.time()-t0:.2f}s")

df_2024.unpersist()
print("\n  ✅ unpersist() — release memory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 13 — 🩺 Spark UI roadmap
# MAGIC
# MAGIC Δεν υπάρχει optimization χωρίς **μέτρηση**. Το Spark UI είναι το διαγνωστικό σου εργαλείο.
# MAGIC
# MAGIC ### Πώς το ανοίγεις στο Databricks
# MAGIC
# MAGIC - Στο notebook → πάνω αριστερά → **"Spark UI"** link
# MAGIC - Ή στο job → "View Spark UI"
# MAGIC
# MAGIC ### Τα 5 σημαντικότερα tabs
# MAGIC
# MAGIC | Tab | Τι ψάχνεις | Red flag |
# MAGIC |-----|-----------|----------|
# MAGIC | **Jobs** | Διάρκεια ανά job, failed jobs | Job με failed stages |
# MAGIC | **Stages** | Shuffle Read/Write, task duration distribution | Max task duration >> median → **skew** |
# MAGIC | **SQL / DataFrame** | Physical plan + actual metrics | Πολλά `Exchange` nodes, no `PushedFilters` |
# MAGIC | **Storage** | Cached DataFrames + memory usage | Cache > 80% του διαθέσιμου memory |
# MAGIC | **Executors** | RAM/CPU ανά executor | Executors με 0% activity → underutilized |
# MAGIC
# MAGIC ### Διαγνωστικό cheat sheet
# MAGIC
# MAGIC | Σύμπτωμα | Πιθανή αιτία | Πού να κοιτάξεις |
# MAGIC |----------|--------------|-------------------|
# MAGIC | Job κολλάει στο 99% | Data skew | Stages → task duration |
# MAGIC | OutOfMemoryError | Skew ή πολύ μεγάλο shuffle | Stages → Shuffle Read |
# MAGIC | Πολύ αργό filter | No predicate pushdown | SQL plan → no `PushedFilters` |
# MAGIC | Slow join | Shuffle join όπου έπρεπε broadcast | SQL plan → `SortMergeJoin` αντί για `BroadcastHashJoin` |
# MAGIC | Πολύς χρόνος σε read | Διαβάζω πολλά columns | SQL plan → `ReadSchema` με πολλές στήλες |

# COMMAND ----------

# Run a representative query και ανέβα στο Spark UI για να εξασκηθείς
df = spark.table("workspace.aade.declarations_big")
big_query = (
    df.filter(col("year") >= 2023)                          # ← Predicate Pushdown
      .select("Περιφέρεια", "doy_code", "tax_amount")       # ← Column Pruning
      .join(broadcast(doy_dim), "doy_code")                  # ← Broadcast Join
      .groupBy("Περιφέρεια", "region_group")                 # ← Wide (Shuffle)
      .agg(spark_sum("tax_amount").alias("total"),
           count("*").alias("n"))
)
print("Plan: όλα τα optimizations σε ένα query")
big_query.explain()

t0 = time.time()
big_query.collect()
print(f"\n  total time: {time.time()-t0:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 14 — ✅ Cheat sheet
# MAGIC
# MAGIC ### Top 10 things to remember
# MAGIC
# MAGIC 1. **Driver = 1 process, Executors = N workers, 1 task = 1 partition = 1 core**
# MAGIC 2. **Narrow = no shuffle, Wide = shuffle (κοστίζει!)**
# MAGIC 3. **Column Pruning** — `select` μόνο όσες στήλες χρειάζεσαι (10x σε wide tables)
# MAGIC 4. **Predicate Pushdown** — `filter` σε Parquet/Delta πάει στο file level (Pushed Filters στο plan)
# MAGIC 5. **Partition Pruning** — `WHERE` σε partition col → ολόκληρος φάκελος skipped (10-1000x)
# MAGIC 6. **Filter EARLY** — πριν από joins και aggregations
# MAGIC 7. **Broadcast Join** για μικρό-vs-μεγάλο (< 100 MB ο μικρός)
# MAGIC 8. **Salting** για extreme skew (>50% σε ένα key)
# MAGIC 9. **AQE enabled** σε όλα τα production jobs
# MAGIC 10. **Cache** ΜΟΝΟ όταν διαβάζεις τον df 2+ φορές — και πάντα `.unpersist()` μετά
# MAGIC
# MAGIC ### Workflow για κάθε βαρύ query
# MAGIC
# MAGIC ```
# MAGIC 1. .explain() ΠΡΩΤΑ            ← κατάλαβε τι θα γίνει
# MAGIC 2. Run + open Spark UI           ← μέτρα
# MAGIC 3. Stages tab → check skew       ← ψάξε task duration distribution
# MAGIC 4. SQL tab → check pushdowns     ← PushedFilters, PartitionFilters
# MAGIC 5. Apply optimizations           ← cache/broadcast/salt/partition
# MAGIC 6. Re-measure                     ← επιβεβαίωσε ότι βελτιώθηκε
# MAGIC ```
# MAGIC
# MAGIC ### Anti-patterns (μην τα κάνεις)
# MAGIC
# MAGIC - ❌ `cache().count()` σε κάθε intermediate df
# MAGIC - ❌ `repartition(n)` πριν από `coalesce(m)` με μικρό m (τα πρώτα είναι waste)
# MAGIC - ❌ `select("*")` σε wide tables αν χρειάζεσαι 3 στήλες
# MAGIC - ❌ Filter ΜΕΤΑ από join (κάνε πριν)
# MAGIC - ❌ `partitionBy(high_cardinality_col)` — χιλιάδες μικροί φάκελοι
# MAGIC - ❌ Απενεργοποίηση AQE «επειδή το βλέπω παλαιό σε tutorials»

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cleanup (optional)

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS workspace.aade.declarations_big")
# import shutil
# shutil.rmtree(PARQUET_PATH, ignore_errors=True)
# shutil.rmtree(PART_PATH, ignore_errors=True)
# print("🧹 cleaned up")
