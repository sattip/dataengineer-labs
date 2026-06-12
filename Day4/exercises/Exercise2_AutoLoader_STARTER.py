# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 Άσκηση Ημέρα 4 — Μέρος 2/4: Auto Loader (Incremental File Ingestion)
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~80' · **Δυσκολία:** ⭐⭐⭐ Hard · **~15 TODOs**
# MAGIC > Self-contained.
# MAGIC
# MAGIC ## 📖 Το πρόβλημα
# MAGIC
# MAGIC Τα ΚΕΠ ρίχνουν **αρχεία** σε ένα folder συνεχώς. Πώς ξέρουμε ποια διαβάσαμε ήδη; Το
# MAGIC **Auto Loader** (`cloudFiles`) το κάνει αυτόματα μέσω **checkpoint** — κάθε run διαβάζει
# MAGIC **μόνο τα νέα** (exactly-once). Θα προσθέσουμε audit metadata, θα χειριστούμε **schema drift**
# MAGIC (νέα στήλη) με rescued data, και θα χτίσουμε ένα **Silver aggregate**.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - `cloudFiles` reader: `format`, `schemaLocation`, `inferColumnTypes`, **`rescuedDataColumn`**, `schemaEvolutionMode`.
# MAGIC - Audit metadata (`_metadata.file_path`, ingestion ts) μέσα σε stream.
# MAGIC - `checkpointLocation` + `Trigger.AvailableNow` (incremental, exactly-once).
# MAGIC - **Schema drift**: νέα στήλη → πάει στο `_rescued_data` αντί να σπάσει το pipeline.
# MAGIC - Silver aggregation από το Bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Cell 0 — Setup + clean state + land batch 1 (έτοιμο)

# COMMAND ----------

import urllib.request, os, pandas as pd
from pyspark.sql.functions import col, current_timestamp, count, avg, round as spark_round

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
VOLUME = "/Volumes/workspace/aade/aade_data"
REPO = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4"
if not os.path.exists(f"{VOLUME}/kep_requests.csv"):
    urllib.request.urlretrieve(f"{REPO}/kep_requests.csv", f"{VOLUME}/kep_requests.csv")

LANDING    = f"{VOLUME}/kep_landing"
SCHEMA_LOC = f"{VOLUME}/_schemas/kep_autoloader"
CKPT       = f"{VOLUME}/_checkpoints/kep_autoloader"
BRONZE     = "workspace.aade.kep_bronze_autoloader"
SILVER     = "workspace.aade.kep_silver_by_service"

for p in [LANDING, SCHEMA_LOC, CKPT]:
    dbutils.fs.rm(p, recurse=True)
os.makedirs(LANDING, exist_ok=True)
spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")
spark.sql(f"DROP TABLE IF EXISTS {SILVER}")

pdf = pd.read_csv(f"{VOLUME}/kep_requests.csv")
pdf.iloc[0:4000].to_csv(f"{LANDING}/kep_batch1a.csv", index=False)
pdf.iloc[4000:6000].to_csv(f"{LANDING}/kep_batch1b.csv", index=False)
print("✓ Batch 1 landed (6000 σε 2 αρχεία):", [f.name for f in dbutils.fs.ls(LANDING)])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Ο Auto Loader reader (με rescued data)
# MAGIC
# MAGIC - `cloudFiles.schemaLocation` — πού κρατά/εξελίσσει το schema.
# MAGIC - `cloudFiles.rescuedDataColumn` — μια στήλη όπου «διασώζονται» πεδία που δεν ταιριάζουν στο schema.
# MAGIC - `cloudFiles.schemaEvolutionMode = "rescue"` — νέες στήλες πάνε στο rescued (το pipeline ΔΕΝ σπάει).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Auto Loader reader + audit columns

# COMMAND ----------

def build_reader():
    return (
        spark.readStream
        .format("__________")                               # TODO 1a: cloudFiles
        .option("cloudFiles.format", "___")                 # TODO 1b: csv
        .option("cloudFiles.schemaLocation", __________)    # TODO 1c: SCHEMA_LOC
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaEvolutionMode", "______")  # TODO 1d: rescue
        .option("header", "true")
        .load(LANDING)
        # audit columns:
        .withColumn("_source_file", col("________________"))  # TODO 1e: _metadata.file_path
        .withColumn("_ingested_at", ________())               # TODO 1f: current_timestamp
    )

df_stream = build_reader()
print("✓ reader έτοιμος · isStreaming =", df_stream.isStreaming)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Γράψτε στο Bronze (batch 1)
# MAGIC
# MAGIC checkpoint = η «μνήμη» του τι διαβάστηκε · `availableNow` = επεξεργάσου ό,τι υπάρχει & σταμάτα.

# COMMAND ----------

def run_autoloader():
    q = (build_reader().writeStream
         .format("delta")
         .option("checkpointLocation", __________)          # TODO 2a: CKPT
         .option("mergeSchema", "true")
         .trigger(__________=True)                          # TODO 2b: availableNow
         .toTable(BRONZE))
    q.awaitTermination()

run_autoloader()
count_b1 = spark.table(BRONZE).count()
print(f"✓ Μετά BATCH 1: Bronze = {count_b1}")               # 6000
display(spark.table(BRONZE).select("request_id","service_type","_source_file","_ingested_at").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Land batch 2 + re-run (incremental!)
# MAGIC
# MAGIC Ο ίδιος stream/checkpoint διαβάζει **μόνο** τα νέα αρχεία.

# COMMAND ----------

pdf.iloc[6000:8000].to_csv(f"{LANDING}/kep_batch2a.csv", index=False)
pdf.iloc[8000:10000].to_csv(f"{LANDING}/kep_batch2b.csv", index=False)
print("✓ Batch 2 landed. Σύνολο αρχείων:", len([f.name for f in dbutils.fs.ls(LANDING) if f.name.endswith('.csv')]))

# 3a: ξανατρέξτε τον ίδιο Auto Loader (incremental)
________________                                            # TODO 3a: run_autoloader()
count_b2 = spark.table(BRONZE).count()
print(f"✓ Μετά BATCH 2: Bronze = {count_b2} (+{count_b2 - count_b1} νέα, ΟΧΙ ξανά τα 6000)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — Schema drift: νέα στήλη → rescued data
# MAGIC
# MAGIC Σήμερα τα ΚΕΠ προσθέτουν στήλη `priority` στο export. Με `schemaEvolutionMode="rescue"`,
# MAGIC η νέα στήλη **δεν σπάει** το pipeline — οι τιμές της «διασώζονται» στο `_rescued_data`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Land batch 3 (με extra στήλη) + re-run

# COMMAND ----------

batch3 = pdf.iloc[0:100].copy()
batch3["request_id"] = range(10001, 10101)   # νέα ids
batch3["priority"]   = "HIGH"                 # ⚠️ ΝΕΑ στήλη (schema drift)
batch3.to_csv(f"{LANDING}/kep_batch3_newcol.csv", index=False)
print("✓ Batch 3 landed με extra στήλη 'priority'")

run_autoloader()
count_b3 = spark.table(BRONZE).count()
rescued = spark.table(BRONZE).filter(col("_rescued_data").________).count()   # TODO 4: isNotNull()
print(f"✓ Μετά BATCH 3: Bronze = {count_b3} (+100) · γραμμές με rescued data = {rescued}")
display(spark.table(BRONZE).filter(col("_rescued_data").isNotNull()).select("request_id","_rescued_data").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — Silver aggregation από το Bronze
# MAGIC
# MAGIC Από το Bronze (raw events) χτίζουμε ένα Silver KPI table ανά `service_type`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — Silver: KPIs ανά service_type

# COMMAND ----------

silver = (
    spark.table(BRONZE)
    .filter(col("request_id") <= 10000)            # αγνόησε το drift-batch demo
    .groupBy("____________")                       # TODO 5a: service_type
    .agg(
        count("*").alias("total_requests"),
        spark_round(avg("wait_time_minutes"), 1).alias("avg_wait_min"),
    )
    .orderBy(col("total_requests").desc())
)
silver.write.format("delta").mode("overwrite").saveAsTable(SILVER)
print(f"✓ Silver: {spark.table(SILVER).count()} service types")
spark.table(SILVER).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 2

# COMMAND ----------

results = {
    "Batch 1 → 6000":                 count_b1 == 6000,
    "Batch 2 → 10000 (incremental)":  count_b2 == 10000,
    "Incremental: +4000 (όχι +10000)": (count_b2 - count_b1) == 4000,
    "Audit col _source_file υπάρχει":  "_source_file" in spark.table(BRONZE).columns,
    "Batch 3 → +100 (10100)":         count_b3 == 10100,
    "Schema drift → rescued data":    rescued >= 100,
    "Checkpoint δημιουργήθηκε":        len(dbutils.fs.ls(CKPT)) > 0,
    "Silver = 5 service types":        spark.table(SILVER).count() == 5,
}
print("=" * 55)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 55)
print("🎉 Τέλος Μέρους 2!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Επόμενο → `Exercise3_Streaming_Merge_STARTER`
# MAGIC
# MAGIC Auto Loader = incremental αρχεία. Στο Μέρος 3: full Structured Streaming + `foreachBatch` +
# MAGIC `MERGE` = streaming **upsert** σε Silver, με dedup & exactly-once.
