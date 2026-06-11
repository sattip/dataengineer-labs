# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉🥈🥇 Άσκηση Ημέρα 1 — Μέρος 2/3: Medallion (Bronze → Silver → Gold)
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~75' · **Δυσκολία:** ⭐⭐ Medium
# MAGIC > **Προαπαιτούμενο:** Έχετε τρέξει το Μέρος 1 (schemas + volume + declarations.csv).
# MAGIC
# MAGIC ## 📖 Πού είμαστε
# MAGIC
# MAGIC Έχουμε το θεμέλιο. Τώρα χτίζουμε την **Medallion Architecture** — το design pattern που θα
# MAGIC χρησιμοποιείτε σε **κάθε** project: τα δεδομένα ρέουν Bronze → Silver → Gold, και κάθε ζώνη
# MAGIC έχει συγκεκριμένο σκοπό.
# MAGIC
# MAGIC | Ζώνη | Σκοπός | Κανόνας |
# MAGIC |---|---|---|
# MAGIC | 🥉 **Bronze** | Raw, ασφαλές αντίγραφο της πραγματικότητας | Ως έχει, immutable, + audit metadata |
# MAGIC | 🥈 **Silver** | Καθαρό, **σωστοί τύποι**, validated | Cast types, rename, fix AFM, filter |
# MAGIC | 🥇 **Gold** | Business-ready aggregates | groupBy/agg για BI/dashboards |
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - **Bronze write** με audit metadata (`_ingested_at`, `_source_file`).
# MAGIC - Η **διόρθωση του ΑΦΜ** → string (η flagship απόφαση τύπων της Ημέρας 1).
# MAGIC - `cast(...)` τύπων + `alias(...)` για καθαρά (Αγγλικά) ονόματα από τα Ελληνικά.
# MAGIC - `groupBy + agg` με πολλά metrics + **conditional aggregation**.

# COMMAND ----------

# DBTITLE 1,Config + φόρτωση (έτοιμο)
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, current_timestamp, round as spark_round
)

CATALOG       = "workspace"
SCHEMA_BRONZE = "aade_bronze"
SCHEMA_SILVER = "aade_silver"
SCHEMA_GOLD   = "aade_gold"
LANDING_PATH  = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/landing"

BRONZE_TBL = f"{CATALOG}.{SCHEMA_BRONZE}.declarations_raw"
SILVER_TBL = f"{CATALOG}.{SCHEMA_SILVER}.declarations_clean"
GOLD_TBL   = f"{CATALOG}.{SCHEMA_GOLD}.declarations_by_category_region"

df = spark.read.option("header","true").option("inferSchema","true").csv(f"{LANDING_PATH}/declarations.csv")
print(f"Raw: {df.count()} γραμμές · στήλες (Ελληνικά): {df.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Bronze = raw + audit metadata
# MAGIC
# MAGIC Στο Bronze **δεν** πειράζουμε τα δεδομένα (ούτε καν τα ελληνικά ονόματα). Προσθέτουμε μόνο
# MAGIC «σφραγίδες» για το *«πότε & από πού ήρθε»*: `_ingested_at` και `_source_file`.
# MAGIC ⚠️ Σε Unity Catalog χρησιμοποιούμε `_metadata.file_path` (όχι το deprecated `input_file_name()`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Γράψτε το Bronze table

# COMMAND ----------

bronze = (
    df
    .withColumn("_ingested_at", _________________)        # TODO 1a: current_timestamp()
    .withColumn("_source_file", col("________________"))  # TODO 1b: _metadata.file_path
)
bronze.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TBL)   # (έτοιμο)
print(f"✓ Bronze: {BRONZE_TBL} ({spark.table(BRONZE_TBL).count()} γραμμές)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Silver: σωστοί τύποι + το ΑΦΜ ως string
# MAGIC
# MAGIC Στο Silver κάνουμε τα δεδομένα **αξιόπιστα**:
# MAGIC - **Τύποι:** `Ποσό_EUR` → `double`, `Φορ_Ετος` → `int`, `Ημερομηνία` → `date`.
# MAGIC - **ΑΦΜ → string** (όχι αριθμός! identifier — κρατάει αρχικά μηδενικά, επιτρέπει regex).
# MAGIC - **Καθαρά ονόματα:** από Ελληνικά → snake_case Αγγλικά (`Κατηγορία_Φόρου` → `tax_category`).
# MAGIC
# MAGIC Το pattern: `col("Παλιό").cast("τύπος").alias("new_name")`.
# MAGIC
# MAGIC > 💡 **Best practice:** ιδανικά δίνουμε explicit `StructType` schema στο read. Εδώ το κάνουμε με
# MAGIC > `cast` στο Silver — ίδιο αποτέλεσμα, πιο ευανάγνωστο για μάθηση.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Χτίστε το Silver (cast + rename)
# MAGIC
# MAGIC Συμπληρώστε τους τύπους/ονόματα στα κενά.

# COMMAND ----------

silver = (
    spark.table(BRONZE_TBL)
    .select(
        col("ΔηλωσηID").cast("int").alias("declaration_id"),
        col("Ημερομηνία").cast("date").alias("declaration_date"),
        col("ΑΦΜ").cast("______").alias("afm"),                 # TODO 2a: τύπος για identifier → "string"
        col("Επωνυμία").alias("business_name"),
        col("ΔΟΥID").cast("int").alias("doy_id"),
        col("Κατηγορία_Φόρου").alias("__________"),             # TODO 2b: alias → "tax_category"
        col("Βάση_Φόρου").cast("double").alias("tax_base"),
        col("Συντελεστής_Pct").cast("double").alias("rate_pct"),
        col("Ποσό_EUR").cast("______").alias("amount_eur"),     # TODO 2c: τύπος για χρήμα → "double"
        col("Κατάσταση").alias("status"),
        col("Περιφέρεια").alias("region"),
        col("Πόλη").alias("city"),
        col("Φορ_Ετος").cast("int").alias("tax_year"),
    )
    .filter(col("amount_eur") ___ 0)    # TODO 2d: business rule «ποσό >= 0»
)
silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_TBL)
print(f"✓ Silver: {SILVER_TBL} ({spark.table(SILVER_TBL).count()} γραμμές)")
spark.table(SILVER_TBL).printSchema()   # επιβεβαιώστε: afm = string!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — Gold: aggregates για το Business
# MAGIC
# MAGIC Το Business δεν θέλει 300 γραμμές — θέλει *«πόσος φόρος ανά Κατηγορία × Περιφέρεια, πόσες εγκρίθηκαν»*.
# MAGIC `groupBy(...).agg(...)` με `.alias(...)` σε κάθε metric. **Conditional count:**
# MAGIC `sum(when(col("status")=="Εγκεκριμένη", 1).otherwise(0))` → πλήθος υπό συνθήκη.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Χτίστε το Gold (ανά tax_category × region)

# COMMAND ----------

gold = (
    spark.table(SILVER_TBL)
    .groupBy("____________", "______")                       # TODO 3a: "tax_category", "region"
    .agg(
        count("*").alias("n_declarations"),
        spark_sum("amount_eur").alias("total_tax_eur"),
        avg("amount_eur").alias("avg_tax_eur"),
        spark_sum(when(col("status") == "____________", 1).otherwise(0)).alias("approved"),   # TODO 3b: "Εγκεκριμένη"
        spark_sum(when(col("status") == "Απορριφθείσα", 1).otherwise(0)).alias("rejected"),
        spark_sum(when(col("status") == "Εκκρεμής", 1).otherwise(0)).alias("pending"),
    )
    .orderBy(______("total_tax_eur"))                        # TODO 3c: desc
)
gold.write.format("delta").mode("overwrite").saveAsTable(GOLD_TBL)
print(f"✓ Gold: {GOLD_TBL} ({spark.table(GOLD_TBL).count()} γραμμές)")
gold.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Insight: Top κατηγορίες φόρου σε έσοδα

# COMMAND ----------

print("=== TOP κατηγορίες φόρου (συνολικά έσοδα) ===")
(
    spark.table(GOLD_TBL)
    .groupBy("____________")                                 # TODO 4: "tax_category"
    .agg(spark_sum("total_tax_eur").alias("revenue"),
         spark_sum("approved").alias("approved"))
    .orderBy(desc("revenue"))
    .show(truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 2

# COMMAND ----------

afm_type = dict(spark.table(SILVER_TBL).dtypes)["afm"]
results = {
    "Bronze table = 300":          spark.table(BRONZE_TBL).count() == 300,
    "Bronze έχει _source_file":    "_source_file" in spark.table(BRONZE_TBL).columns,
    "Silver afm είναι string":     afm_type == "string",
    "Silver = 300 (clean data)":   spark.table(SILVER_TBL).count() == 300,
    "Silver: καθαρά ονόματα":      "tax_category" in spark.table(SILVER_TBL).columns,
    "Silver: amount_eul double":   dict(spark.table(SILVER_TBL).dtypes)["amount_eur"] == "double",
    "Gold table υπάρχει":          spark.catalog.tableExists(GOLD_TBL),
    "Gold: conditional counts":    set(["approved","rejected","pending"]).issubset(set(spark.table(GOLD_TBL).columns)),
}
print("=" * 50)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 50)
print("🎉 Τέλος Μέρους 2!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise3_Governance_Contracts_STARTER`
# MAGIC
# MAGIC Έχουμε pipeline Bronze→Silver→Gold. Στο Μέρος 3 βάζουμε **governance** (ρόλοι, grants) και
# MAGIC ένα **Data Contract** — την «πύλη ποιότητας» που αποτρέπει βρώμικα δεδομένα να φτάσουν στο Silver.
