# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Άσκηση Ημέρα 2 — Μέρος 3/3: Enrichment (Joins) + Gold + Insights
# MAGIC ### Fill-in-the-Blank · Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ
# MAGIC
# MAGIC > **Διάρκεια:** ~60' · **Δυσκολία:** ⭐⭐⭐ Medium-Hard
# MAGIC > **Προαπαιτούμενο:** Έχετε τρέξει Μέρη 1 & 2 (υπάρχει το `workspace.aade.mydata_clean`).
# MAGIC
# MAGIC ## 📖 Πού είμαστε
# MAGIC
# MAGIC Το **Silver** είναι καθαρό αλλά «μιλάει σε κωδικούς»: ξέρει `issuer_afm` αλλά όχι το όνομα,
# MAGIC τον κλάδο ή τη ΔΟΥ. Το Business δεν θέλει ΑΦΜ — θέλει *«πόσος τζίρος ανά Κλάδο × Περιφέρεια»*.
# MAGIC Άρα: **enrichment** (joins με master data) → **aggregation** (Gold) → **insights**.
# MAGIC
# MAGIC ## 🎯 Τι θα μάθετε
# MAGIC
# MAGIC - Τους **τύπους join** (`inner` vs `left`) και **γιατί** στο enrichment θέλουμε `left`.
# MAGIC - Πώς χειριζόμαστε τα **ελληνικά ονόματα στηλών** των master (ΑΦΜ, Επωνυμία, Κλάδος…).
# MAGIC - **groupBy + agg** με πολλαπλά metrics ταυτόχρονα.
# MAGIC - **Conditional aggregation** — μέτρα μόνο όσα πληρούν συνθήκη μέσα στο agg.
# MAGIC - **Delta write modes** (`overwrite` vs `append`) και γιατί έχει σημασία.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Setup + φόρτωση Silver + master data

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, when, desc, round as spark_round, broadcast
)

MASTER_VOLUME = "/Volumes/workspace/aade/aade_data"

clean_dedup = spark.table("workspace.aade.mydata_clean")
taxpayers   = spark.read.csv(f"{MASTER_VOLUME}/taxpayers.csv", header=True, inferSchema=True)
doy         = spark.read.csv(f"{MASTER_VOLUME}/doy.csv",       header=True, inferSchema=True)

print(f"Silver rows: {clean_dedup.count()}")
print("taxpayers columns:", taxpayers.columns)
print("doy columns:", doy.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 1 — Διάλεξε & μετονόμασε στήλες ΠΡΙΝ το join
# MAGIC
# MAGIC Τα master CSV έχουν **ελληνικά** ονόματα (`ΑΦΜ`, `Επωνυμία`, `Κλάδος`...). Καλή πρακτική:
# MAGIC κάνε `select` **μόνο** τις στήλες που χρειάζεσαι και **alias** τες σε καθαρά ονόματα
# MAGIC που ταιριάζουν με το αριστερό table. Έτσι:
# MAGIC - το join γίνεται απλό (`on="issuer_afm"`),
# MAGIC - δεν κουβαλάμε άχρηστες στήλες,
# MAGIC - αποφεύγουμε διπλές/συγκρουόμενες στήλες μετά το join.
# MAGIC
# MAGIC ⚠️ **Τύποι join key:** το `ΑΦΜ` στο master είναι αριθμός (inferSchema). Το `issuer_afm` στο
# MAGIC Silver είναι string. Για να ταιριάξουν, κάνουμε `cast("string")` στο master.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 1 — Προετοιμάστε το taxpayers για join
# MAGIC
# MAGIC Συμπληρώστε τα alias ώστε το join key να λέγεται `issuer_afm`.

# COMMAND ----------

taxpayers_enrich = taxpayers.select(
    col("ΑΦΜ").cast("string").alias("__________"),   # TODO 1a: alias = το key που θα κάνουμε join (issuer_afm)
    col("Επωνυμία").alias("official_name"),
    col("Κλάδος").alias("sector"),
    col("Περιφέρεια").alias("region"),
    col("ΔΟΥID").alias("ΔΟΥID"),
)
taxpayers_enrich.show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 2 — `inner` vs `left` join (και γιατί εδώ ΘΕΛΟΥΜΕ left)
# MAGIC
# MAGIC - **inner**: κρατά μόνο γραμμές που βρίσκουν ταίρι **και** στα δύο.
# MAGIC - **left** (left outer): κρατά **ΟΛΕΣ** τις αριστερές· αν δεν βρει ταίρι → NULL στις δεξιές.
# MAGIC
# MAGIC Στο enrichment **δεν** θέλουμε να *χάσουμε* τιμολόγια επειδή λείπει ένα όνομα από το master.
# MAGIC Άρα → **left**. (Αν κάναμε inner, ένα τιμολόγιο με «flagged-to-NULL» ΑΦΜ θα εξαφανιζόταν.)
# MAGIC
# MAGIC 💡 **Bonus — broadcast:** τα master είναι μικρά (20 & 8 γραμμές). Με `broadcast(small_df)`
# MAGIC ο Spark στέλνει αντίγραφο σε κάθε node και αποφεύγει ακριβό shuffle. Σε production join
# MAGIC «μεγάλο ⨝ μικρό» αυτό είναι τεράστια διαφορά ταχύτητας.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 2 — Join Silver με taxpayers + doy

# COMMAND ----------

doy_enrich = doy.select(col("ΔΟΥID"), col("ΔΟΥ_Ονομα").alias("doy_name"))

enriched = (
    clean_dedup
    .join(broadcast(taxpayers_enrich), on="__________", how="______")   # TODO 2a: key=issuer_afm · TODO 2b: τύπος=left
    .join(broadcast(doy_enrich),       on="ΔΟΥID",       how="______")   # TODO 2c: τύπος=left
)

print(f"Enriched rows: {enriched.count()}")   # πρέπει να είναι ΙΣΟ με το Silver (left join δεν χάνει)
enriched.select(
    "invoice_id", "issuer_afm", "official_name", "sector", "region",
    "doy_name", "net_amount", "vat_amount", "total_amount", "status"
).show(8, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 3 — groupBy + agg με πολλά metrics
# MAGIC
# MAGIC ```python
# MAGIC df.groupBy("sector", "region").agg(
# MAGIC     count("*").alias("invoice_count"),
# MAGIC     spark_sum("net_amount").alias("total_net_eur"),
# MAGIC     avg("net_amount").alias("avg_invoice_eur"),
# MAGIC )
# MAGIC ```
# MAGIC Κάθε metric παίρνει `.alias(...)` — αλλιώς θα έχει άσχημο όνομα τύπου `sum(net_amount)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 4 — Conditional aggregation (το «μέτρα μόνο αν...»)
# MAGIC
# MAGIC Θέλουμε π.χ. *πόσα τιμολόγια είναι «Υποβληθέν»* μέσα σε κάθε ομάδα. Το pattern:
# MAGIC ```python
# MAGIC spark_sum(when(col("status") == "Υποβληθέν", 1).otherwise(0)).alias("submitted")
# MAGIC ```
# MAGIC «Για κάθε γραμμή βάλε 1 αν είναι submitted αλλιώς 0, μετά άθροισέ τα» → πλήθος υπό συνθήκη.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 3 — Gold aggregation: ανά Κλάδο (sector) × Περιφέρεια (region)

# COMMAND ----------

gold = (
    enriched
    .groupBy("______", "______")                                       # TODO 3a: sector, region
    .agg(
        count("*").alias("invoice_count"),
        spark_sum("net_amount").alias("total_net_eur"),
        spark_sum("vat_amount").alias("total_vat_eur"),
        spark_sum("total_amount").alias("total_with_vat_eur"),
        avg("net_amount").alias("avg_invoice_eur"),
        # conditional counts ανά status:
        spark_sum(when(col("status") == "Υποβληθέν", 1).otherwise(0)).alias("submitted"),
        spark_sum(when(col("status") == "________", 1).otherwise(0)).alias("cancelled"),   # TODO 3b: "Ακυρωμένο"
        spark_sum(when(col("status") == "________", 1).otherwise(0)).alias("pending"),     # TODO 3c: "Εκκρεμές"
        spark_sum(when(col("status") == "UNKNOWN",   1).otherwise(0)).alias("unknown_status"),
    )
    .orderBy(______("total_with_vat_eur"))                              # TODO 3d: φθίνουσα ταξινόμηση (desc)
)

print("=== GOLD — myDATA ανά Κλάδο × Περιφέρεια ===")
gold.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 ΕΝΝΟΙΑ 5 — Delta write modes
# MAGIC
# MAGIC - `mode("overwrite")` — αντικαθιστά πλήρως το table. Για **idempotent** rebuilds (Gold).
# MAGIC - `mode("append")` — προσθέτει. Για incremental ingestion (π.χ. καθημερινό Bronze load).
# MAGIC
# MAGIC Εδώ το Gold είναι «παράγωγο» — το ξαναχτίζουμε κάθε φορά → **overwrite**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 4 — Γράψτε το Gold Delta table

# COMMAND ----------

gold.write.format("______").mode("__________").saveAsTable("workspace.aade.mydata_gold")   # TODO 4a: "delta" · TODO 4b: "overwrite"
print("✓ Gold saved: workspace.aade.mydata_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 5 — Business Insight: Top κλάδοι σε τζίρο
# MAGIC
# MAGIC Από το Gold, αθροίστε ανά `sector` και δείξτε τους κορυφαίους. Συμπληρώστε το groupBy
# MAGIC και την ταξινόμηση.

# COMMAND ----------

print("=== TOP κλάδοι σε συνολικό τζίρο (με ΦΠΑ) ===")
(
    gold.groupBy("______")                                  # TODO 5a: sector
        .agg(spark_sum("total_with_vat_eur").alias("revenue"),
             spark_sum("invoice_count").alias("invoices"))
        .orderBy(desc("revenue"))                           # (έτοιμο)
        .show(5, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ TODO 6 — Before vs After (η «απόδειξη» αξίας του pipeline)
# MAGIC
# MAGIC Συμπληρώστε τα table names για να δείξετε τι κάναμε.

# COMMAND ----------

raw_n    = spark.table("workspace.aade.__________").count()   # TODO 6a: mydata_raw
quar_n   = spark.table("workspace.aade.__________").count()   # TODO 6b: mydata_quarantine
silver_n = spark.table("workspace.aade.__________").count()   # TODO 6c: mydata_clean
gold_n   = spark.table("workspace.aade.mydata_gold").count()

print("=" * 55)
print("BEFORE vs AFTER — myDATA Pipeline")
print("=" * 55)
print(f"  Bronze (raw):        {raw_n} τιμολόγια")
print(f"  Quarantine (bad):    {quar_n} σε καραντίνα")
print(f"  Silver (clean):      {silver_n} καθαρά")
print(f"  Gold (aggregated):   {gold_n} γραμμές KPI (Κλάδος × Περιφέρεια)")
print("=" * 55)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Self-check Μέρους 3 (τελικό)

# COMMAND ----------

results = {
    "Gold table υπάρχει":              spark.catalog.tableExists("workspace.aade.mydata_gold"),
    "Enriched = Silver (left join)":   enriched.count() == clean_dedup.count(),
    "Gold έχει official_name μέσω join": "official_name" in enriched.columns,
    "Gold έχει conditional counts":    set(["submitted","cancelled","pending"]).issubset(set(gold.columns)),
    "4 tables στο schema":             spark.sql("SHOW TABLES IN workspace.aade").filter(col("tableName").like("mydata_%")).count() >= 4,
}
print("=" * 50)
for k, v in results.items():
    print(f"  {'✅ OK  ' if v else '❌ FAIL'} — {k}")
print("=" * 50)
print("🎉🎉 ΟΛΟΚΛΗΡΩΣΑΤΕ ΟΛΗ ΤΗΝ ΑΣΚΗΣΗ DAY 2!" if all(results.values()) else "⚠️  Δείτε τα FAIL.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏁 Τι χτίσατε (end-to-end Medallion pipeline)
# MAGIC
# MAGIC ```
# MAGIC mydata_invoices_MESSY.csv (100 rows, 35 issues)
# MAGIC        │  read + audit metadata
# MAGIC        ▼
# MAGIC   🥉 Bronze: mydata_raw           ← Μέρος 1 (+ DQ detection)
# MAGIC        │  flag + split
# MAGIC        ├──────────────► 🚨 mydata_quarantine   (critical rows)
# MAGIC        ▼
# MAGIC   🥈 Silver: mydata_clean         ← Μέρος 2 (cleanse + dedup)
# MAGIC        │  join taxpayers + doy
# MAGIC        ▼
# MAGIC   🥇 Gold: mydata_gold            ← Μέρος 3 (enrich + aggregate)
# MAGIC        │
# MAGIC        ▼  Power BI / dashboards
# MAGIC ```
# MAGIC
# MAGIC **Cleanup (προαιρετικό):**
# MAGIC ```python
# MAGIC # for t in ["mydata_raw","mydata_quarantine","mydata_clean","mydata_gold"]:
# MAGIC #     spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")
# MAGIC ```
