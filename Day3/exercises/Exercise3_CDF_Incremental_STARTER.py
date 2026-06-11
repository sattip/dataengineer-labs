# Databricks notebook source
# MAGIC %md
# MAGIC # 🔄 Άσκηση Ημέρα 3 — Μέρος 3/3: Change Data Feed + Incremental ETL
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~50' · **Δυσκολία:** ⭐⭐⭐ Medium-Hard
# MAGIC > Self-contained.
# MAGIC
# MAGIC ## 📖 Το πρόβλημα
# MAGIC
# MAGIC Το Gold (π.χ. «έσοδα ανά περιφέρεια») το ξαναϋπολογίζουμε από το μηδέν κάθε βράδυ. Με
# MAGIC εκατομμύρια γραμμές αυτό είναι **αργό & ακριβό**, ενώ μόνο λίγες γραμμές άλλαξαν.
# MAGIC Η λύση: **Change Data Feed (CDF)** — το Delta «θυμάται» *ποιες γραμμές άλλαξαν και πώς*,
# MAGIC ώστε να ενημερώνουμε το Gold **incrementally** (μόνο τα deltas).
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - Πώς ενεργοποιείται το **CDF** (`delta.enableChangeDataFeed`).
# MAGIC - Πώς διαβάζουμε τις αλλαγές (`readChangeFeed` / `table_changes`) και το `_change_type`.
# MAGIC - Το **incremental aggregation pattern**: insert/update_postimage = `+`, delete/update_preimage = `-`.
# MAGIC - Validation: το incremental Gold **= ** το full recompute (απόδειξη ορθότητας).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + source table (έτοιμο)

# COMMAND ----------

import urllib.request, os
from pyspark.sql.functions import col, when, sum as spark_sum, lit

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3"
if not os.path.exists(f"{VOLUME}/declarations.csv"):
    urllib.request.urlretrieve(f"{REPO}/declarations.csv", f"{VOLUME}/declarations.csv")

SRC  = "workspace.aade.declarations_cdf"
GOLD = "workspace.aade.revenue_by_region_gold"

raw = spark.read.option("header","true").option("inferSchema","true").csv(f"{VOLUME}/declarations.csv")
src = raw.select(
    col("ΔηλωσηID").cast("int").alias("declaration_id"),
    col("Ποσό_EUR").cast("double").alias("amount_eur"),
    col("Περιφέρεια").alias("region"),
)
src.write.format("delta").mode("overwrite").saveAsTable(SRC)
print(f"✓ {SRC}: {spark.table(SRC).count()} γραμμές")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Ενεργοποίηση CDF
# MAGIC
# MAGIC Το CDF **δεν** είναι default. Ενεργοποιείται με table property:
# MAGIC ```sql
# MAGIC ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ```
# MAGIC ⚠️ Καταγράφει αλλαγές **μόνο από εδώ και μπρος** (όχι αναδρομικά).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Ενεργοποιήστε το CDF

# COMMAND ----------

spark.sql(f"ALTER TABLE {SRC} SET TBLPROPERTIES (delta.____________________ = true)")   # TODO 1: enableChangeDataFeed
print("CDF property:")
display(spark.sql(f"SHOW TBLPROPERTIES {SRC}").filter("key LIKE '%changeDataFeed%'"))

# Σημείο αναφοράς: η version ΠΡΙΝ τις αλλαγές
start_v = spark.sql(f"DESCRIBE HISTORY {SRC}").selectExpr("max(version) v").collect()[0]["v"]
print(f"start_version = {start_v}")

# COMMAND ----------

# DBTITLE 1,Initial Gold (full build, έτοιμο)
gold0 = spark.table(SRC).groupBy("region").agg(spark_sum("amount_eur").alias("total_amount"))
gold0.write.format("delta").mode("overwrite").saveAsTable(GOLD)
print(f"✓ Initial Gold: {spark.table(GOLD).count()} περιφέρειες")

# COMMAND ----------

# DBTITLE 1,Κάνε αλλαγές στο source (έτοιμο) — insert / update / delete
spark.sql(f"INSERT INTO {SRC} VALUES (9001, 5000.0, 'Αττική'), (9002, 3000.0, 'Κρήτη')")          # +
spark.sql(f"UPDATE {SRC} SET amount_eur = amount_eur + 1000 WHERE declaration_id IN (10, 11)")    # ±
spark.sql(f"DELETE FROM {SRC} WHERE declaration_id IN (20, 21)")                                  # -
print("✓ Έγιναν insert/update/delete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Διάβασε το Change Feed
# MAGIC
# MAGIC ```python
# MAGIC spark.read.format("delta")
# MAGIC     .option("readChangeFeed", "true")
# MAGIC     .option("startingVersion", start_v + 1)
# MAGIC     .table(SRC)
# MAGIC ```
# MAGIC Επιστρέφει τις αλλαγμένες γραμμές + στήλη **`_change_type`** με τιμές:
# MAGIC `insert`, `delete`, `update_preimage` (πριν), `update_postimage` (μετά).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Διαβάστε τις αλλαγές (CDF)

# COMMAND ----------

changes = (
    spark.read.format("delta")
    .option("________________", "true")          # TODO 2a: readChangeFeed
    .option("startingVersion", start_v + 1)
    .table(SRC)
)
print("=== Change Feed ===")
changes.select("declaration_id", "region", "amount_eur", "_change_type").show(truncate=False)
print("Κατανομή _change_type:")
changes.groupBy("_change_type").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — Incremental aggregation: + και −
# MAGIC
# MAGIC Για ένα **άθροισμα** ανά region, κάθε αλλαγή έχει «πρόσημο»:
# MAGIC - `insert`, `update_postimage` → **προσθέτουν** (`+amount`)
# MAGIC - `delete`, `update_preimage` → **αφαιρούν** (`-amount`)
# MAGIC
# MAGIC Αθροίζοντας τα signed amounts ανά region παίρνουμε το **net_delta** που πρέπει να εφαρμοστεί στο Gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Υπολογίστε το net_delta ανά region

# COMMAND ----------

signed = changes.withColumn(
    "signed_amount",
    when(col("_change_type").isin("insert", "________________"), col("amount_eur"))    # TODO 3a: "update_postimage"
    .when(col("_change_type").isin("delete", "________________"), -col("amount_eur"))  # TODO 3b: "update_preimage"
    .otherwise(lit(0.0))
)
net = signed.groupBy("region").agg(spark_sum("signed_amount").alias("net_delta"))
net.createOrReplaceTempView("region_net_delta")
print("=== net_delta ανά region ===")
net.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — MERGE τα deltas στο Gold (incremental update)
# MAGIC
# MAGIC Matched region → πρόσθεσε το net_delta· νέα region → insert. Συμπληρώστε τα clauses.

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {GOLD} g
    USING region_net_delta d
    ON g.region = d.region
    WHEN __________ THEN UPDATE SET g.total_amount = g.total_amount ___ d.net_delta    -- TODO 4a: MATCHED · TODO 4b: «+»
    WHEN NOT MATCHED THEN INSERT (region, total_amount) VALUES (d.region, d.net_delta)
""")
print("✓ Gold ενημερώθηκε incrementally")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 3 — Incremental == Full recompute
# MAGIC
# MAGIC Η απόδειξη ορθότητας: το **incremental** Gold πρέπει να ταυτίζεται με ένα **full recompute**
# MAGIC από το τρέχον source.

# COMMAND ----------

incremental = {r["region"]: r["total_amount"]
               for r in spark.table(GOLD).collect()}
full = {r["region"]: r["total"]
        for r in spark.table(SRC).groupBy("region").agg(spark_sum("amount_eur").alias("total")).collect()}

# tolerance comparison (double-addition order διαφέρει → σύγκριση με ανοχή 0.01€)
all_regions = set(incremental) | set(full)
match = all(abs(incremental.get(r, 0.0) - full.get(r, 0.0)) < 0.01 for r in all_regions)
results = {
    "CDF ενεργοποιήθηκε":            spark.sql(f"SHOW TBLPROPERTIES {SRC}").filter("key LIKE '%changeDataFeed%' AND value='true'").count() == 1,
    "Change feed έχει _change_type": "_change_type" in changes.columns,
    "Υπάρχουν insert+update+delete":  changes.select("_change_type").distinct().count() >= 3,
    "Incremental Gold == Full recompute": match,
}
print("=" * 55)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 55)
if not match:
    print("Δείτε διαφορές:")
    for reg in sorted(all_regions):
        if abs(incremental.get(reg, 0.0) - full.get(reg, 0.0)) >= 0.01:
            print(f"  {reg}: incremental={incremental.get(reg)} vs full={full.get(reg)}")
print("🎉🎉 ΟΛΟΚΛΗΡΩΣΑΤΕ ΟΛΗ ΤΗΝ ΑΣΚΗΣΗ DAY 3!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Τι χτίσατε (Ημέρα 3 — Delta Production)
# MAGIC
# MAGIC ```
# MAGIC Μέρος 1: MERGE/UPDATE/DELETE + schema evolution   (ACID DML)
# MAGIC Μέρος 2: Time Travel + RESTORE + OPTIMIZE/ZORDER/VACUUM   (history + maintenance)
# MAGIC Μέρος 3: Change Data Feed → incremental Gold refresh   (production ETL)
# MAGIC ```
# MAGIC Αυτά είναι ακριβώς τα Delta patterns που τρέχουν σε production lakehouse.
# MAGIC
# MAGIC **Cleanup (προαιρετικό):**
# MAGIC ```python
# MAGIC # for t in ["tax_declarations_silver","tax_declarations_tt","declarations_cdf","revenue_by_region_gold"]:
# MAGIC #     spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")
# MAGIC ```
