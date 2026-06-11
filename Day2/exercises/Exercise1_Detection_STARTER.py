# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Άσκηση Ημέρα 2 — Μέρος 1/3: Bronze Ingestion + Data Quality Detection
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~60' · **Δυσκολία:** ⭐⭐ Medium
# MAGIC > **Στυλ:** Συμπληρώνετε τα `_____` (κενά) σε κάθε `# TODO`. Κάθε TODO έχει **πάνω του**
# MAGIC > ένα κελί επεξήγησης που σας λέει *τι*, *γιατί* και *πώς*. Διαβάστε το πρώτα.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Το Σενάριο
# MAGIC
# MAGIC Κάθε βράδυ, το σύστημα **myDATA** της ΑΑΔΕ εξάγει τα τιμολόγια της ημέρας σε ένα CSV.
# MAGIC Όπως κάθε πραγματικό export, **δεν είναι καθαρό**: το αρχείο `mydata_invoices_MESSY.csv`
# MAGIC έχει 100 τιμολόγια με **35 σκόπιμα λάθη** σε **10 κατηγορίες**.
# MAGIC
# MAGIC Δουλειά σας ως Data Engineer **δεν** είναι να «διορθώσετε στο χέρι». Είναι να χτίσετε ένα
# MAGIC **pipeline** που:
# MAGIC 1. Φέρνει τα raw δεδομένα ως έχουν (**Bronze**) — χωρίς να χάσει τίποτα.
# MAGIC 2. **Εντοπίζει** αυτόματα κάθε πρόβλημα (αυτό το Μέρος 1).
# MAGIC 3. Τα ξεχωρίζει σε καθαρά vs προβληματικά (Μέρος 2 — Quarantine/Silver).
# MAGIC 4. Τα εμπλουτίζει & συνοψίζει για το Business (Μέρος 3 — Gold).
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε στο Μέρος 1
# MAGIC
# MAGIC - Γιατί το `inferSchema=True` είναι **παγίδα** για κωδικούς (ΑΦΜ) → πρώτη πραγματική απόφαση.
# MAGIC - Πώς γράφουμε ένα **Bronze** Delta table με audit metadata.
# MAGIC - Το **idiom** για NULL counts σε *όλες* τις στήλες με μία γραμμή.
# MAGIC - `rlike` (regex), `isin` (enum), Window dedup detection, `left_anti` join (orphans).
# MAGIC - Πώς συντάσσουμε ένα **DQ Report** που διαβάζεται από άνθρωπο.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + αυτόματο download των CSV από GitHub
# MAGIC
# MAGIC **Δεν χρειάζεται να αλλάξετε τίποτα εδώ.** Δημιουργεί schema/volumes και κατεβάζει τα
# MAGIC αρχεία από το public repo. Είναι **idempotent** (αν τρέξει 2η φορά, δεν ξανακατεβάζει).

# COMMAND ----------

import urllib.request, os

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.mydata_raw")

REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2"
MYDATA_VOLUME = "/Volumes/workspace/aade/mydata_raw"
MASTER_VOLUME = "/Volumes/workspace/aade/aade_data"

for fname in ["taxpayers.csv", "doy.csv", "employees.csv", "declarations.csv"]:
    target = f"{MASTER_VOLUME}/{fname}"
    if not os.path.exists(target):
        urllib.request.urlretrieve(f"{REPO}/{fname}", target)
        print(f"✅ {fname}")
    else:
        print(f"⏭️  {fname}")

mydata_target = f"{MYDATA_VOLUME}/mydata_invoices_MESSY.csv"
if not os.path.exists(mydata_target):
    urllib.request.urlretrieve(f"{REPO}/mydata_invoices_MESSY.csv", mydata_target)
    print("✅ mydata_invoices_MESSY.csv")
else:
    print("⏭️  mydata_invoices_MESSY.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Imports
# MAGIC
# MAGIC Δεν συμπληρώνετε κάτι εδώ — απλώς τρέξτε το. Σημειώστε ότι το `sum` της PySpark
# MAGIC το κάνουμε import ως `spark_sum` ώστε να **μην** συγκρούεται με το built-in `sum()` της Python.

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, row_number,
    trim, regexp_replace, lit, to_date, current_date, length,
    regexp_extract, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, DateType

print("✓ Imports ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — `inferSchema` και η παγίδα του ΑΦΜ
# MAGIC
# MAGIC Όταν διαβάζουμε CSV, ο Spark δεν ξέρει τους τύπους. Με `inferSchema=True` **σαρώνει** τα
# MAGIC δεδομένα και μαντεύει. Για μια στήλη με σκέτα ψηφία (όπως το **ΑΦΜ**), θα μαντέψει `bigint`.
# MAGIC
# MAGIC **Γιατί είναι πρόβλημα;**
# MAGIC - Το ΑΦΜ είναι **αναγνωριστικό**, όχι αριθμός. Δεν κάνουμε ποτέ πρόσθεση σε ΑΦΜ.
# MAGIC - Αν ένα ΑΦΦ έχει αρχικό μηδενικό (`012345678`), ως αριθμός γίνεται `12345678` → **το χάσαμε**.
# MAGIC - Αργότερα θέλουμε regex `^\d{9}$` πάνω του → χρειάζεται **string**.
# MAGIC
# MAGIC **Κανόνας:** το `inferSchema=True` είναι ΟΚ για γρήγορη εξερεύνηση, αλλά για production
# MAGIC δίνουμε explicit τύπο στα identifiers. Στο Bronze εδώ θα διαβάσουμε με `inferSchema` (για να
# MAGIC δούμε τι μαντεύει ο Spark) και θα **κρατήσουμε** το `issuer_afm` ως string από την αρχή.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Bronze ingest: διαβάστε το messy CSV
# MAGIC
# MAGIC Συμπληρώστε τις δύο options ανάγνωσης:
# MAGIC - η πρώτη λέει στον Spark ότι η 1η γραμμή είναι **ονόματα στηλών**
# MAGIC - η δεύτερη του ζητάει να **μαντέψει** τους τύπους
# MAGIC
# MAGIC *Hint:* τα ονόματα των options είναι `"header"` και `"inferSchema"`, τιμή `"true"`.

# COMMAND ----------

df_raw = (
    spark.read
         .option("__________", "true")   # TODO 1a: header
         .option("__________", "true")   # TODO 1b: inferSchema
         .csv(f"{MYDATA_VOLUME}/mydata_invoices_MESSY.csv")
)

print(f"Raw invoices: {df_raw.count()} rows")   # αναμένεται 100
df_raw.printSchema()
df_raw.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **🔎 Παρατηρήστε το `printSchema()` παραπάνω.** Τι τύπο έδωσε ο Spark στο `issuer_afm`;
# MAGIC Αν λέει `integer`/`long` → αυτό ακριβώς είναι η παγίδα της ΕΝΝΟΙΑΣ 1. Στο Μέρος 2 θα το
# MAGIC χειριστούμε. Για το Bronze, **κρατάμε τα δεδομένα ως έχουν** — το Bronze είναι «το ασφαλές
# MAGIC αντίγραφο της πραγματικότητας».

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Audit metadata στο Bronze
# MAGIC
# MAGIC Στο Bronze προσθέτουμε «σφραγίδες» που απαντούν αργότερα στο *«από πού & πότε ήρθε αυτή η γραμμή;»*:
# MAGIC - `_ingested_at` — πότε τη φέραμε (`current_timestamp()`),
# MAGIC - `_source_file` — από ποιο αρχείο.
# MAGIC
# MAGIC ⚠️ **Unity Catalog gotcha:** η παλιά `input_file_name()` **δεν** δουλεύει σε UC Standard clusters.
# MAGIC Χρησιμοποιούμε την κρυφή στήλη `_metadata.file_path`.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# TODO 2a: προσθέστε τη χρονοσφραγίδα (συνάρτηση χωρίς ορίσματα)
df_bronze = df_raw.withColumn("_ingested_at", ___________________)

# TODO 2b: προσθέστε το source file path (UC-safe — χρησιμοποιήστε _metadata)
df_bronze = df_bronze.withColumn("_source_file", col("________________"))

df_bronze.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mydata_raw")
print("✓ Bronze saved: workspace.aade.mydata_raw")
display(spark.table("workspace.aade.mydata_raw").select("invoice_id", "issuer_afm", "_ingested_at", "_source_file").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — Το NULL-count idiom (μία γραμμή, όλες οι στήλες)
# MAGIC
# MAGIC Θέλουμε να μετρήσουμε NULLs σε **κάθε** στήλη. Αντί να γράψουμε 12 ξεχωριστά queries,
# MAGIC χτίζουμε μια **list comprehension** από εκφράσεις:
# MAGIC
# MAGIC ```python
# MAGIC df.select([ count(when(col(c).isNull(), c)).alias(c) for c in df.columns ])
# MAGIC ```
# MAGIC
# MAGIC **Πώς διαβάζεται:** για κάθε στήλη `c`, «μέτρα τις γραμμές όπου το `c` είναι NULL».
# MAGIC Το `when(condition, value)` επιστρέφει το `c` μόνο όταν είναι NULL· το `count()` αγνοεί τα
# MAGIC NULL αποτελέσματα → μετράει μόνο τα «χτυπήματα».

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — NULL counts σε όλες τις στήλες
# MAGIC
# MAGIC Συμπληρώστε το σώμα της comprehension. *Hint:* `count(when(col(c).isNull(), c)).alias(c)`.

# COMMAND ----------

print("=" * 60)
print("DQ REPORT — myDATA Invoices")
print("=" * 60)

print("\n[1] NULLs ανά στήλη:")
null_counts = df_raw.select([
    count(when(col(c)._________, c)).alias(c)   # TODO 3: συνθήκη "είναι NULL"
    for c in df_raw.columns
])
null_counts.show()
# Αναμένεται: issuer_afm = 5 nulls, vat_amount = 2 nulls

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — Εντοπισμός duplicates σε primary key
# MAGIC
# MAGIC Το `invoice_id` πρέπει να είναι **μοναδικό**. Για να βρούμε διπλότυπα:
# MAGIC «ομαδοποίησε ανά κλειδί, μέτρα, κράτα όσα έχουν count > 1».
# MAGIC
# MAGIC ```python
# MAGIC df.groupBy("key").count().filter(col("count") > 1)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Βρείτε διπλά invoice_id

# COMMAND ----------

print("[2] Duplicate invoice_id:")
dups = (
    df_raw
    .groupBy("__________")          # TODO 4a: το κλειδί
    .count()
    .filter(col("count") > ___)     # TODO 4b: το όριο για «διπλό»
)
print(f"   → {dups.count()} invoice_ids εμφανίζονται > 1 φορά")   # αναμένεται 3
dups.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 5 — Regex validation με `rlike`
# MAGIC
# MAGIC Το ΑΦΜ πρέπει να είναι **ακριβώς 9 ψηφία**. Το pattern: `^\d{9}$`
# MAGIC - `^` αρχή, `$` τέλος (αλλιώς θα έκανε match και το «123456789ΧΧ»),
# MAGIC - `\d` ψηφίο, `{9}` ακριβώς 9 φορές.
# MAGIC
# MAGIC `col("x").rlike(pattern)` → True αν ταιριάζει. Θέλουμε τα **κακά** → βάζουμε `~` (not):
# MAGIC `~col("issuer_afm").rlike(r"^\d{9}$")`. Προσέξτε να εξαιρέσουμε τα NULL (αλλιώς μετριούνται κι αυτά).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — Βρείτε ΑΦΜ με λάθος μορφή (όχι 9 ψηφία)

# COMMAND ----------

print("[3] Bad issuer_afm (not 9 digits):")
bad_afm = df_raw.filter(
    col("issuer_afm").isNotNull() &
    (~col("issuer_afm").cast("string").rlike(r"_________"))   # TODO 5: regex 9 ψηφίων
)
print(f"   → {bad_afm.count()} rows με κακό ΑΦΜ")   # αναμένεται 4
bad_afm.select("invoice_id", "issuer_afm").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 6 — Αρνητικά ποσά (business rule)
# MAGIC
# MAGIC Ένα τιμολόγιο δεν μπορεί να έχει αρνητικό `net_amount`. Φιλτράρετε τα `< 0`.

# COMMAND ----------

print("[4] Negative net_amount:")
neg = df_raw.filter(col("net_amount") ___ 0)   # TODO 6: τελεστής σύγκρισης
print(f"   → {neg.count()} rows με αρνητικό ποσό")   # αναμένεται 3
neg.select("invoice_id", "net_amount").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 6 — Ασφαλές parsing ημερομηνιών με `try_to_date`
# MAGIC
# MAGIC Μερικές ημερομηνίες έχουν λάθος format (π.χ. `2025/02/27`). Αν κάναμε σκέτο `to_date`,
# MAGIC θα «έσκαγε» ή θα γύριζε NULL σιωπηλά. Το `try_to_date(col, fmt)` επιστρέφει NULL **αντί για error**
# MAGIC όταν δεν ταιριάζει — ώστε να συνεχίσει το pipeline. Μετά συγκρίνουμε με `current_date()`
# MAGIC για να βρούμε **μελλοντικές** ημερομηνίες (αδύνατες για ήδη εκδοθέν τιμολόγιο).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 7 — Μελλοντικές ημερομηνίες

# COMMAND ----------

print("[5] Future issue_date (> σήμερα):")
parseable = df_raw.withColumn("parsed_date", expr("try_to_date(issue_date, 'yyyy-MM-dd')"))
future = parseable.filter(col("parsed_date") ___ current_date())   # TODO 7: «μεγαλύτερη από σήμερα»
print(f"   → {future.count()} rows με future date")   # αναμένεται ~4
future.select("invoice_id", "issue_date").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 7 — Έλεγχος επιτρεπτών τιμών (enum) με `isin`
# MAGIC
# MAGIC Το `status` επιτρέπεται να είναι μόνο: **Υποβληθέν, Ακυρωμένο, Εκκρεμές**.
# MAGIC `col("status").isin(valid_list)` → True αν ανήκει στη λίστα. Τα **άκυρα** = `~ ... isin(...)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 8 — Άκυρες τιμές status

# COMMAND ----------

valid_statuses = ["Υποβληθέν", "Ακυρωμένο", "Εκκρεμές"]
print("[6] Invalid status:")
invalid_status = df_raw.filter(~col("status").______(valid_statuses))   # TODO 8: μέθοδος enum
print(f"   → {invalid_status.count()} rows με άγνωστο status")   # αναμένεται 3
invalid_status.groupBy("status").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 9 — Whitespace στο issuer_name
# MAGIC
# MAGIC Κάποια ονόματα έχουν κενά μπροστά/πίσω. Μια γραμμή «χρειάζεται trim» όταν
# MAGIC `issuer_name != trim(issuer_name)`. Συμπληρώστε τη συνάρτηση που κόβει τα κενά.

# COMMAND ----------

print("[7] issuer_name με leading/trailing whitespace:")
ws = df_raw.filter(col("issuer_name") != ______(col("issuer_name")))   # TODO 9: συνάρτηση trim
print(f"   → {ws.count()} rows χρειάζονται trim")   # αναμένεται ~5

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 10 — Λάθος format ημερομηνίας (όχι YYYY-MM-DD)
# MAGIC
# MAGIC Με regex βρίσκουμε όσες ΔΕΝ ταιριάζουν στο `^\d{4}-\d{2}-\d{2}$`.

# COMMAND ----------

print("[8] Bad date format (not YYYY-MM-DD):")
bad_date = df_raw.filter(~col("issue_date").rlike(r"_____________________"))   # TODO 10: regex YYYY-MM-DD
print(f"   → {bad_date.count()} rows με λάθος date format")   # αναμένεται 3
bad_date.select("invoice_id", "issue_date").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 11 — NULL vat_amount

# COMMAND ----------

print("[9] NULL vat_amount:")
null_vat = df_raw.filter(col("vat_amount")._________)   # TODO 11: «είναι NULL»
print(f"   → {null_vat.count()} rows χωρίς vat_amount")   # αναμένεται 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 8 — Orphans με `left_anti` join
# MAGIC
# MAGIC Ένα `receiver_afm` μπορεί να **μην υπάρχει** στο master `taxpayers`. Αυτά λέγονται «ορφανά».
# MAGIC Ο πιο καθαρός τρόπος: **`left_anti` join** = «κράτα από τα αριστερά ΟΣΕΣ ΔΕΝ βρίσκουν ταίρι δεξιά».
# MAGIC
# MAGIC ```python
# MAGIC left.join(right, left.k == right.k, "left_anti")
# MAGIC ```
# MAGIC (Σκεφτείτε το ως «αντι-join»: το αντίθετο του inner join.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 12 — Ορφανά receiver_afm

# COMMAND ----------

taxpayers = spark.read.csv(f"{MASTER_VOLUME}/taxpayers.csv", header=True, inferSchema=True)
valid_afms = taxpayers.select(col("ΑΦΜ").cast("string").alias("ΑΦΜ"))

print("[10] Orphan receiver_afm (not in taxpayers master):")
orphan = df_raw.select("invoice_id", "receiver_afm").join(
    valid_afms,
    df_raw.receiver_afm == valid_afms.ΑΦΜ,
    "___________"                      # TODO 12: τύπος join για «όσα ΔΕΝ ταιριάζουν»
)
print(f"   → {orphan.count()} rows με άγνωστο receiver_afm")   # αναμένεται 11 (4 διαφορετικά «ανύπαρκτα» ΑΦΜ)
orphan.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση Μέρους 1
# MAGIC
# MAGIC Τρέξτε το παρακάτω self-check. Αν όλα είναι `OK`, περάσατε. Αν κάτι είναι `FAIL`,
# MAGIC ξαναδείτε το αντίστοιχο TODO.

# COMMAND ----------

results = {
    "Bronze rows = 100":              df_raw.count() == 100,
    "Bronze table γράφτηκε":          spark.catalog.tableExists("workspace.aade.mydata_raw"),
    "NULL issuer_afm = 5":            df_raw.filter(col("issuer_afm").isNull()).count() == 5,
    "NULL vat_amount = 2":            df_raw.filter(col("vat_amount").isNull()).count() == 2,
    "Duplicate invoice_id = 3":       df_raw.groupBy("invoice_id").count().filter(col("count") > 1).count() == 3,
    "Negative amounts = 3":           df_raw.filter(col("net_amount") < 0).count() == 3,
    "Bad status = 3":                 df_raw.filter(~col("status").isin(valid_statuses)).count() == 3,
    "Orphan receiver_afm = 11":       orphan.count() == 11,
}
print("=" * 50)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 50)
print("🎉 Τέλος Μέρους 1!" if all(results.values()) else "⚠️  Κάτι λείπει — δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise2_Cleanse_Quarantine_STARTER`
# MAGIC
# MAGIC Τώρα που **ξέρουμε** τι χαλάει, στο Μέρος 2 θα ξεχωρίσουμε τα προβληματικά (Quarantine)
# MAGIC και θα καθαρίσουμε τα υπόλοιπα (Silver).
