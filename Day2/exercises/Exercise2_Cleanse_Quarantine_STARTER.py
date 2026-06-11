# Databricks notebook source
# MAGIC %md
# MAGIC # 🧹 Άσκηση Ημέρα 2 — Μέρος 2/3: Quarantine + Cleansing (Bronze → Silver)
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~70' · **Δυσκολία:** ⭐⭐⭐ Medium-Hard
# MAGIC > **Προαπαιτούμενο:** Έχετε τρέξει το Μέρος 1 (υπάρχει το `workspace.aade.mydata_raw`).
# MAGIC
# MAGIC ## 📖 Πού είμαστε
# MAGIC
# MAGIC Στο Μέρος 1 **εντοπίσαμε** 10 κατηγορίες προβλημάτων. Τώρα παίρνουμε αποφάσεις:
# MAGIC ποια λάθη είναι **θανατηφόρα** (η γραμμή πάει σε *Quarantine*) και ποια **διορθώνονται**
# MAGIC (μένουν στο *Silver*). Αυτή η διάκριση είναι η καρδιά της δουλειάς ενός DE.
# MAGIC
# MAGIC | # | Issue | Απόφαση |
# MAGIC |---|-------|---------|
# MAGIC | 1 | NULL issuer_afm (critical key) | 🔴 Drop → Quarantine |
# MAGIC | 2 | Duplicate invoice_id | 🔴 Dedup (κράτα 1) |
# MAGIC | 3 | Bad AFM (<9 ψηφία) | 🟡 → NULL (flag) |
# MAGIC | 4 | Negative net_amount | 🔴 Drop |
# MAGIC | 5 | Future issue_date | 🟡 Flag (μένει) |
# MAGIC | 6 | Invalid status | 🟡 → "UNKNOWN" |
# MAGIC | 7 | Whitespace issuer_name | 🟢 Trim |
# MAGIC | 8 | Bad date format (YYYY/MM/DD) | 🟢 Normalize |
# MAGIC | 9 | NULL vat_amount | 🟡 Recompute (net × rate) |
# MAGIC | 10 | Orphan receiver_afm | 🟡 Flag (μένει) |
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - Το **quarantine pattern**: ποτέ δεν πετάμε «σιωπηλά» — τα κακά πάνε σε ξεχωριστό table.
# MAGIC - Αλυσίδες `when(...).when(...).otherwise(...)` — το «if/elif/else» της PySpark.
# MAGIC - `regexp_replace` για normalize, `trim` για whitespace.
# MAGIC - **Recompute** τιμών (γεμίζουμε NULL vat από net × συντελεστή).
# MAGIC - **Deduplication με Window** (`row_number().over(...)`) — το πιο σημαντικό pattern της ημέρας.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Setup + φόρτωση Bronze

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, row_number,
    trim, regexp_replace, lit, to_date, current_date, length, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

MASTER_VOLUME = "/Volumes/workspace/aade/aade_data"
valid_statuses = ["Υποβληθέν", "Ακυρωμένο", "Εκκρεμές"]

df_raw = spark.table("workspace.aade.mydata_raw")
print(f"Bronze rows: {df_raw.count()}")   # 100 (+ audit columns από το Μέρος 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Το Quarantine Pattern (γιατί δεν «πετάμε» γραμμές)
# MAGIC
# MAGIC Αν απλώς φιλτράραμε τις κακές γραμμές, μετά από έναν μήνα ο προϊστάμενος ρωτάει
# MAGIC *«γιατί λείπουν 12 τιμολόγια;»* και δεν έχουμε απάντηση. Αντί γι' αυτό:
# MAGIC
# MAGIC 1. **Flag** κάθε γραμμή με boolean στήλες (`has_null_afm`, `has_negative_amount`, ...).
# MAGIC 2. **Quarantine table** = όσες έχουν ΟΠΟΙΟΔΗΠΟΤΕ critical flag → για investigation.
# MAGIC 3. **Silver** = οι υπόλοιπες, καθαρισμένες.
# MAGIC
# MAGIC Έτσι τίποτα δεν χάνεται — απλώς διαχωρίζεται. Αυτό είναι **auditable**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Flag τις προβληματικές γραμμές
# MAGIC
# MAGIC Συμπληρώστε τις boolean εκφράσεις. *Hints:*
# MAGIC - `has_null_afm`: το ΑΦΜ είναι NULL → `.isNull()`
# MAGIC - `has_bad_afm`: όχι NULL **και** δεν ταιριάζει `^\d{9}$`
# MAGIC - `has_negative_amount`: `net_amount < 0`
# MAGIC - `has_bad_date`: δεν ταιριάζει `^\d{4}-\d{2}-\d{2}$`
# MAGIC - `has_invalid_status`: δεν είναι μέσα στα `valid_statuses`

# COMMAND ----------

flagged = (
    df_raw
    .withColumn("has_null_afm",        col("issuer_afm").__________)                              # TODO 1a
    .withColumn("has_bad_afm",         col("issuer_afm").isNotNull() &
                                       (~col("issuer_afm").cast("string").rlike(r"__________")))   # TODO 1b: regex 9 ψηφία
    .withColumn("has_negative_amount", col("net_amount") ___ 0)                                    # TODO 1c
    .withColumn("has_bad_date",        ~col("issue_date").rlike(r"_____________________"))         # TODO 1d: regex YYYY-MM-DD
    .withColumn("has_invalid_status",  ~col("status").______(valid_statuses))                      # TODO 1e: enum
)

flagged.select(
    "invoice_id", "issuer_afm", "net_amount", "issue_date", "status",
    "has_null_afm", "has_bad_afm", "has_negative_amount", "has_bad_date", "has_invalid_status"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Συνδυασμός flags με `|` (OR)
# MAGIC
# MAGIC Στην PySpark χρησιμοποιούμε `|` (OR), `&` (AND), `~` (NOT) — **όχι** τα λεκτικά `or/and/not`.
# MAGIC Κάθε όρος μπαίνει σε **παρενθέσεις**. Quarantine = «έχει έστω ΕΝΑ critical πρόβλημα».
# MAGIC
# MAGIC > Σημείωση απόφασης: το `future_date` και το `orphan` τα θεωρούμε 🟡 (flag, ΟΧΙ quarantine)
# MAGIC > — οπότε ΔΕΝ μπαίνουν στο φίλτρο quarantine. Μπαίνουν μόνο τα 🔴 critical.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Φτιάξτε το Quarantine table

# COMMAND ----------

quarantine = flagged.filter(
    col("has_null_afm")  ___  col("has_bad_afm")  ___  col("has_negative_amount")
    ___ col("has_bad_date")  ___  col("has_invalid_status")     # TODO 2: ο τελεστής OR (×4 φορές)
)

print(f"Quarantined rows: {quarantine.count()}")
quarantine.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mydata_quarantine")
print("✓ Quarantine saved: workspace.aade.mydata_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — `when().otherwise()` = το if/elif/else της PySpark
# MAGIC
# MAGIC ```python
# MAGIC when(condition1, value1).when(condition2, value2).otherwise(default)
# MAGIC ```
# MAGIC Διαβάζεται: «αν condition1 → value1, αλλιώς αν condition2 → value2, αλλιώς default».
# MAGIC Το θα χρησιμοποιήσουμε για:
# MAGIC - **Bad AFM → NULL**: αν ταιριάζει `^\d{9}$` κράτα το, αλλιώς `lit(None)`.
# MAGIC - **Invalid status → "UNKNOWN"**: αν είναι valid κράτα το, αλλιώς `lit("UNKNOWN")`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — Recompute: γέμισμα NULL vat από net × συντελεστή
# MAGIC
# MAGIC Όταν `vat_amount` είναι NULL, μπορούμε να το **υπολογίσουμε** από το `vat_category`:
# MAGIC `ΦΠΑ 24%→0.24`, `13%→0.13`, `6%→0.06`, `Απαλλαγή→0.0`. Χτίζουμε ένα `vat_rate_expr`
# MAGIC με `when`, και μετά: «αν vat_amount είναι NULL → net × rate, αλλιώς κράτα το υπάρχον».

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Cleansing pipeline (το μεγάλο βήμα)
# MAGIC
# MAGIC Συμπληρώστε τα κενά ένα-ένα. Κάθε `# →` σχόλιο εξηγεί τι κάνει η γραμμή.

# COMMAND ----------

# Συντελεστές ΦΠΑ (case expression)
vat_rate_expr = (
    when(col("vat_category") == "ΦΠΑ 24%", lit(0.24))
    .when(col("vat_category") == "ΦΠΑ 13%", lit(0.13))
    .when(col("vat_category") == "ΦΠΑ 6%",  lit(0.06))
    .when(col("vat_category") == "Απαλλαγή", lit(0.0))
    .otherwise(lit(None))
)

clean = (
    df_raw
    # → 1. Drop NULL critical fields (issuer_afm, invoice_id)
    .filter(col("issuer_afm").__________ & col("invoice_id").__________)          # TODO 3a: «δεν είναι NULL» (×2)
    # → 2. Normalize date: αντικατάσταση "/" με "-"  (2025/02/27 → 2025-02-27)
    .withColumn("issue_date", regexp_replace(col("issue_date"), "___", "___"))    # TODO 3b: from "/" to "-"
    # → 3. Trim whitespace στο όνομα
    .withColumn("issuer_name", ______(col("issuer_name")))                        # TODO 3c: συνάρτηση trim
    # → 4. Bad AFM → NULL (κράτα μόνο τα 9-ψήφια)
    .withColumn("issuer_afm",
        when(col("issuer_afm").cast("string").rlike(r"^\d{9}$"), col("issuer_afm"))
        .otherwise(lit(______)))                                                  # TODO 3d: τιμή για «κενό» (None)
    # → 5. Drop negative amounts (business rule)
    .filter(col("net_amount") ___ 0)                                             # TODO 3e: «>= 0»
    # → 6. Invalid status → UNKNOWN
    .withColumn("status",
        when(col("status").isin(valid_statuses), col("status"))
        .otherwise(lit("__________")))                                            # TODO 3f: η τιμή fallback
    # → 7. Recompute NULL vat_amount = net × rate
    .withColumn("vat_rate", vat_rate_expr)
    .withColumn("vat_amount",
        when(col("vat_amount").isNull() & col("vat_rate").isNotNull(),
             (col("net_amount") * col("________")).cast(DoubleType()))           # TODO 3g: ποια στήλη πολλαπλασιάζει το net;
        .otherwise(col("vat_amount")))
    # → 8. Recompute total = net + vat
    .withColumn("total_amount",
        (col("net_amount") ___ col("vat_amount")).cast(DoubleType()))            # TODO 3h: τελεστής πρόσθεσης
    .drop("vat_rate")
)

print(f"Μετά το cleansing (πριν dedup): {clean.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 5 — Deduplication με Window (το πιο σημαντικό pattern)
# MAGIC
# MAGIC Έχουμε διπλά `invoice_id`. Θέλουμε να κρατήσουμε **ένα** ανά κλειδί — το **πιο πρόσφατο**.
# MAGIC Το pattern σε 3 βήματα:
# MAGIC
# MAGIC ```python
# MAGIC w = Window.partitionBy("invoice_id").orderBy(col("issue_date").desc())  # «ομάδες ανά κλειδί, ταξινόμηση φθίνουσα»
# MAGIC df.withColumn("rn", row_number().over(w))   # δίνει 1,2,3... μέσα σε κάθε ομάδα
# MAGIC   .filter(col("rn") == 1)                    # κράτα μόνο το πρώτο (το πιο πρόσφατο)
# MAGIC   .drop("rn")
# MAGIC ```
# MAGIC
# MAGIC - `partitionBy` = «ομαδοποίησε ανά» (σαν groupBy αλλά **χωρίς** να καταρρέει τις γραμμές).
# MAGIC - `orderBy(...desc())` = μέσα σε κάθε ομάδα, βάλε το νεότερο πρώτο.
# MAGIC - `row_number()` = αρίθμηση 1,2,3 μέσα στην ομάδα → `rn==1` είναι ο «νικητής».

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Dedup με Window (κράτα το νεότερο ανά invoice_id)

# COMMAND ----------

w = Window.partitionBy("__________").orderBy(col("issue_date").______())   # TODO 4a: κλειδί · TODO 4b: φθίνουσα

clean_dedup = (
    clean
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == ___)        # TODO 4c: ποιον αριθμό κρατάμε;
    .drop("rn")
)

print(f"Πριν dedup:  {clean.count()}")
print(f"Μετά dedup:  {clean_dedup.count()}")
print(f"Αφαιρέθηκαν: {clean.count() - clean_dedup.count()} διπλότυπα")

# Save as Silver
clean_dedup.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mydata_clean")
print("✓ Silver saved: workspace.aade.mydata_clean")
display(clean_dedup.limit(8))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 2

# COMMAND ----------

raw_n   = spark.table("workspace.aade.mydata_raw").count()
quar_n  = spark.table("workspace.aade.mydata_quarantine").count()
silver_n = spark.table("workspace.aade.mydata_clean").count()

results = {
    "Quarantine table υπάρχει":        spark.catalog.tableExists("workspace.aade.mydata_quarantine"),
    "Silver table υπάρχει":            spark.catalog.tableExists("workspace.aade.mydata_clean"),
    "Καμία NULL issuer_afm στο Silver": spark.table("workspace.aade.mydata_clean").filter(col("issuer_afm").isNull()).count() == 0,
    "Κανένα αρνητικό net στο Silver":   spark.table("workspace.aade.mydata_clean").filter(col("net_amount") < 0).count() == 0,
    "Κανένα διπλό invoice_id στο Silver": spark.table("workspace.aade.mydata_clean").groupBy("invoice_id").count().filter(col("count") > 1).count() == 0,
    "Καμία NULL vat_amount στο Silver":  spark.table("workspace.aade.mydata_clean").filter(col("vat_amount").isNull()).count() == 0,
}
print("=" * 50)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 50)
print(f"Bronze {raw_n}  →  Quarantine {quar_n}  +  Silver {silver_n}")
print("🎉 Τέλος Μέρους 2!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise3_Enrich_Gold_STARTER`
# MAGIC
# MAGIC Το Silver είναι καθαρό αλλά «φτωχό»: έχει μόνο ΑΦΜ, όχι ονόματα/κλάδους/ΔΟΥ. Στο Μέρος 3
# MAGIC θα το **εμπλουτίσουμε** με joins και θα φτιάξουμε το **Gold** για το Business.
