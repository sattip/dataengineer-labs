# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 — LIVE DEMO End-to-End (Ημέρα 2)
# MAGIC **Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ**
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC
# MAGIC > **GitHub** (share this στο Zoom chat):
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2/Lab1_LIVEDEMO_FreeEdition.py
# MAGIC > ```
# MAGIC
# MAGIC Αυτή είναι η **πραγματική εκδοχή** που τρέξαμε ζωντανά — 6 cells, end-to-end pipeline
# MAGIC με το πραγματικό ΑΑΔΕ dataset.
# MAGIC
# MAGIC ## Προαπαιτούμενα
# MAGIC
# MAGIC 1. Databricks Free Edition — Serverless compute
# MAGIC 2. Volume `/Volumes/workspace/aade/aade_data/` με τα 4 CSV (το Cell 0 παρακάτω τα κατεβάζει αυτόματα):
# MAGIC    - `declarations.csv` (300 rows)
# MAGIC    - `taxpayers.csv` (20 rows)
# MAGIC    - `doy.csv` (8 rows)
# MAGIC    - `employees.csv` (4 rows)
# MAGIC
# MAGIC ## Expected Final Output
# MAGIC
# MAGIC | Κατηγορία | Total EUR | Approved | Declarations |
# MAGIC |-----------|-----------|----------|--------------|
# MAGIC | ΦΠΑ | 1.474.273€ | 50 | 77 |
# MAGIC | Εισοδήματος | 1.356.405€ | 33 | 64 |
# MAGIC | Μισθοδοσίας | 921.678€ | 53 | 84 |
# MAGIC | Ακινήτων | 103.459€ | 55 | 75 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + Download CSVs από GitHub
# MAGIC
# MAGIC Δημιουργεί schema/volume και κατεβάζει τα 4 master CSVs απευθείας από το public repo.
# MAGIC **Idempotent** — αν τα αρχεία έχουν ήδη ανέβει χειροκίνητα, η λήψη παραλείπεται.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
print("✅ workspace.aade.aade_data ready\n")

import urllib.request, os

REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2"
VOLUME = "/Volumes/workspace/aade/aade_data"

files = ["declarations.csv", "taxpayers.csv", "doy.csv", "employees.csv"]
for fname in files:
    target = f"{VOLUME}/{fname}"
    if os.path.exists(target):
        print(f"⏭️  {fname} (already in volume)")
    else:
        try:
            urllib.request.urlretrieve(f"{REPO}/{fname}", target)
            print(f"✅ {fname}")
        except Exception as e:
            print(f"❌ {fname}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 1 — Δημιουργία Schema

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
print("✓ Schema workspace.aade έτοιμο")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Cell 2 — Ανάγνωση 4 CSVs από Volume

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, avg, when, desc, row_number
from pyspark.sql.window import Window

VOLUME = "/Volumes/workspace/aade/aade_data"

declarations = spark.read.csv(f"{VOLUME}/declarations.csv", header=True, inferSchema=True)
taxpayers    = spark.read.csv(f"{VOLUME}/taxpayers.csv",    header=True, inferSchema=True)
doy          = spark.read.csv(f"{VOLUME}/doy.csv",          header=True, inferSchema=True)
employees    = spark.read.csv(f"{VOLUME}/employees.csv",    header=True, inferSchema=True)

print(f"declarations: {declarations.count()} rows")
print(f"taxpayers:    {taxpayers.count()} rows")
print(f"doy:          {doy.count()} rows")
print(f"employees:    {employees.count()} rows")

declarations.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Cell 3 — Data Quality Checks
# MAGIC
# MAGIC Τρεις βασικοί έλεγχοι:
# MAGIC 1. Null counts ανά στήλη
# MAGIC 2. Duplicates σε primary key (ΔηλωσηID)
# MAGIC 3. Range validation (Ποσό_EUR ∈ [0, 10M])
# MAGIC 4. Valid status values

# COMMAND ----------

print("=== NULL counts ===")
null_counts = declarations.select([
    count(when(col(c).isNull(), c)).alias(c) for c in declarations.columns
])
null_counts.show()

print("=== Duplicate ΔηλωσηID ===")
dups = declarations.groupBy("ΔηλωσηID").count().filter(col("count") > 1)
print(f"Duplicates: {dups.count()}")
dups.show()

print("=== Bad Ποσό_EUR (negative or > 10M) ===")
bad_amounts = declarations.filter(
    (col("Ποσό_EUR") < 0) | (col("Ποσό_EUR") > 10_000_000)
)
print(f"Out-of-range: {bad_amounts.count()}")

print("=== Distinct Κατάσταση values ===")
declarations.groupBy("Κατάσταση").count().orderBy(desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔗 Cell 4 — 3-Way JOIN (Enrichment)
# MAGIC
# MAGIC declarations **LEFT JOIN** taxpayers (Κλάδος) **+** doy (ΔΟΥ_Ονομα) **+** employees (Ονοματεπώνυμο)

# COMMAND ----------

enriched = (
    declarations.alias("d")
    .join(
        taxpayers.select(col("ΑΦΜ"), col("Κλάδος")),
        on="ΑΦΜ", how="left"
    )
    .join(
        doy.select(col("ΔΟΥID"), col("ΔΟΥ_Ονομα")),
        on="ΔΟΥID", how="left"
    )
    .join(
        employees.select(
            col("ΥπάλληλοςID"),
            col("Ονοματεπώνυμο").alias("employee_name")
        ),
        on="ΥπάλληλοςID", how="left"
    )
)

print(f"Enriched rows: {enriched.count()}")
enriched.select(
    "ΔηλωσηID", "ΑΦΜ", "Επωνυμία", "Κλάδος",
    "ΔΟΥ_Ονομα", "Κατηγορία_Φόρου", "Ποσό_EUR", "Κατάσταση"
).show(8, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Cell 5 — Gold Aggregation + Delta Write
# MAGIC
# MAGIC Aggregation ανά `Κατηγορία_Φόρου × Περιφέρεια` και write σε 2 Delta tables:
# MAGIC - `workspace.aade.declarations_enriched` (full enriched)
# MAGIC - `workspace.aade.gold_tax_by_region` (aggregated)

# COMMAND ----------

gold = (
    enriched.groupBy("Κατηγορία_Φόρου", "Περιφέρεια")
    .agg(
        count("*").alias("total"),
        spark_sum("Ποσό_EUR").alias("total_tax_eur"),
        avg("Συντελεστής_Pct").alias("avg_rate"),
        spark_sum(when(col("Κατάσταση") == "Εγκεκριμένη", 1).otherwise(0)).alias("approved"),
        spark_sum(when(col("Κατάσταση") == "Εκκρεμής", 1).otherwise(0)).alias("pending"),
        spark_sum(when(col("Κατάσταση") == "Απορριφθείσα", 1).otherwise(0)).alias("rejected"),
    )
    .orderBy(desc("total_tax_eur"))
)

print("=== GOLD — Τέλος ανά Κατηγορία × Περιφέρεια ===")
gold.show(15, truncate=False)

# Delta writes
gold.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.aade.gold_tax_by_region")

enriched.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.aade.declarations_enriched")

print("✓ Delta tables written:")
print("  - workspace.aade.declarations_enriched")
print("  - workspace.aade.gold_tax_by_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Cell 6 — Final Summary

# COMMAND ----------

print("=== SUMMARY — Total τέλος ανά Κατηγορία Φόρου ===")
gold.groupBy("Κατηγορία_Φόρου") \
    .agg(
        spark_sum("total_tax_eur").alias("total_eur"),
        spark_sum("approved").alias("approved"),
        spark_sum("total").alias("declarations"),
    ) \
    .orderBy(desc("total_eur")) \
    .show(truncate=False)

print("=== Tables στο workspace.aade ===")
spark.sql("SHOW TABLES IN workspace.aade").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση
# MAGIC
# MAGIC **Τι πέτυχες:**
# MAGIC - ✓ Schema aade στο workspace catalog
# MAGIC - ✓ 4 CSVs loaded σε DataFrames
# MAGIC - ✓ 4 DQ checks (όλα passed)
# MAGIC - ✓ 3-way JOIN με 300 enriched rows
# MAGIC - ✓ Gold aggregation με 6 metrics
# MAGIC - ✓ 2 Delta tables production-ready
# MAGIC
# MAGIC **Επόμενο βήμα (Ημέρα 3):**
# MAGIC Θα χρησιμοποιήσεις το `workspace.aade.declarations_enriched` table ως starting
# MAGIC point για Delta Lake operations (MERGE, Time Travel, Schema Evolution).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cleanup (προαιρετικά)
# MAGIC
# MAGIC Αν θες να αφαιρέσεις τα Delta tables:

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS workspace.aade.gold_tax_by_region")
# spark.sql("DROP TABLE IF EXISTS workspace.aade.declarations_enriched")
# spark.sql("DROP SCHEMA IF EXISTS workspace.aade CASCADE")
# print("Cleanup done")
