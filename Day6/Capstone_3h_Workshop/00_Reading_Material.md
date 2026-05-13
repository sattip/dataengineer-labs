# 📖 Day 6 — Reading Material (Pre-Exercise Study)

**Διάρκεια διαβάσματος:** ~25 λεπτά
**Σκοπός:** Σύνοψη όλων των βασικών εννοιών των 5 πρώτων ημερών σε ένα έγγραφο, ώστε οι trainees να μπορούν να ξεκινήσουν τις practical exercises αμέσως μετά.

---

## Πίνακας Περιεχομένων

1. [Medallion Architecture](#1-medallion-architecture)
2. [Delta Lake Essentials](#2-delta-lake-essentials)
3. [Data Loading Patterns](#3-data-loading-patterns)
4. [Data Quality Patterns](#4-data-quality-patterns)
5. [Window Functions](#5-window-functions)
6. [Streaming Basics](#6-streaming-basics)
7. [Performance Optimization](#7-performance-optimization)
8. [Unity Catalog Governance](#8-unity-catalog-governance)
9. [Common Pitfalls Checklist](#9-common-pitfalls-checklist)

---

## 1. Medallion Architecture

Η medallion architecture είναι ο **de-facto** τρόπος οργάνωσης data pipelines στο Databricks. Χωρίζει το pipeline σε 3 quality layers:

| Layer | Τι περιέχει | Mode | Παράδειγμα ΑΑΔΕ |
|---|---|---|---|
| 🥉 **Bronze** | Raw data όπως ήρθε | Append-only | TAXIS CSV files χωρίς cleanup |
| 🥈 **Silver** | Cleansed + validated | MERGE (upsert) | Filter invalid AFM, deduplicate |
| 🥇 **Gold** | Business-ready aggregations | Overwrite ή MERGE | Daily KPIs, citizen 360 view |

**Βασικός κανόνας**: ποτέ να μην παρακάμπτεις το layer. Όλα τα data περνάνε από όλα τα stages.

### Γιατί 3 layers;
- **Replay**: αν χαλάσει το Silver, ξανατρέχουμε από Bronze (όχι από source DB)
- **Audit trail**: το Bronze είναι «αντίγραφο της αλήθειας»
- **Decoupling**: source schema αλλάζει → μόνο το Silver MERGE χρειάζεται update

---

## 2. Delta Lake Essentials

### Τι είναι το Delta;
Storage format πάνω από Parquet που προσθέτει:
- **ACID transactions** — αν 2 writers γράφουν ταυτόχρονα, δεν χαλάει η consistency
- **Time travel** — `VERSION AS OF`, `TIMESTAMP AS OF`
- **Schema enforcement** — δεν δέχεται incompatible writes
- **MERGE INTO** — upsert σε μία transaction
- **Z-Ordering** — multi-column data clustering

### Κρίσιμα commands

```sql
-- Δημιουργία με metadata
CREATE TABLE workspace.aade.silver_tax
USING DELTA
COMMENT 'Validated tax declarations'
TBLPROPERTIES ('data_owner' = 'aade', 'layer' = 'silver')
AS SELECT * FROM bronze_tax WHERE afm IS NOT NULL;

-- Time travel
SELECT * FROM silver_tax VERSION AS OF 5;
SELECT * FROM silver_tax TIMESTAMP AS OF '2026-04-01';

-- History
DESCRIBE HISTORY silver_tax;

-- Cleanup παλιών versions
VACUUM silver_tax RETAIN 168 HOURS;  -- 7 ημέρες

-- Optimize file layout
OPTIMIZE silver_tax ZORDER BY (afm);
```

### TBLPROPERTIES που χρησιμοποιούμε
| Property | Σημασία |
|---|---|
| `delta.autoOptimize.optimizeWrite` | Auto-merge μικρά files κατά το write |
| `delta.autoOptimize.autoCompact` | Background compaction μετά από writes |
| `delta.enableChangeDataFeed` | Παρακολούθηση row-level changes (CDC) |
| `delta.minReaderVersion` | Δηλώνει minimum Delta version για read |

---

## 3. Data Loading Patterns

### Πότε τι χρησιμοποιούμε

| Pattern | Πότε | Παράδειγμα |
|---|---|---|
| **Full Load** | Small data (< 10 GB), initial load, dimension tables | Πίνακας Νομοί Ελλάδας (75 rows) full reload καθημερινά |
| **Incremental Load** | Όταν source έχει `updated_at` ή `created_at` | TAXIS declarations: WHERE updated_at > last_load |
| **CDC** | High-volume με frequent updates | Real-time replication από Oracle TAXIS DB |
| **Auto Loader** | File-based sources στο data lake | CSV/JSON files που γράφουν τα APIs |

### Auto Loader Quick Reference

```python
from pyspark.sql.functions import col, current_timestamp

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("/Volumes/workspace/aade/raw/tax/")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)  # ή continuous=True για 24/7
    .toTable("workspace.aade.bronze_tax")
)
```

**Σημείωση**: `input_file_name()` ΔΕΝ υποστηρίζεται σε Unity Catalog. Χρησιμοποιήστε `_metadata.file_path` αντί.

---

## 4. Data Quality Patterns

### Οι 6 διαστάσεις ποιότητας (DMBOK)

| Διάσταση | Τι ελέγχει | SQL pattern |
|---|---|---|
| **Completeness** | Όλα τα required fields γεμάτα; | `afm IS NOT NULL` |
| **Validity** | Συμμορφώνεται με κανόνες; | `LENGTH(afm) = 9` |
| **Accuracy** | Αντιστοιχεί στην πραγματικότητα; | `tax_amount BETWEEN 0 AND 10000000` |
| **Consistency** | Ίδια data ίδια παντού; | JOIN check μεταξύ tables |
| **Timeliness** | Πότε vs πότε έπρεπε; | `_ingested_at < deadline` |
| **Uniqueness** | Δεν υπάρχουν duplicates; | `ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1` |

### Pattern: quarantine αντί για drop

```python
# ΛΑΘΟΣ: drop χωρίς log
silver = bronze.filter(col("afm").isNotNull())

# ΣΩΣΤΟ: split valid/invalid με log
invalid = bronze.filter(col("afm").isNull())
invalid.write.format("delta").mode("append").saveAsTable("workspace.aade.quarantine_tax")

silver = bronze.filter(col("afm").isNotNull())

# Log metrics
print(f"Valid:   {silver.count()} rows")
print(f"Invalid: {invalid.count()} rows (quarantined)")
```

---

## 5. Window Functions

Πιο χρήσιμα window functions για data engineering:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lag, lead, sum as spark_sum, avg

# Deduplication: κρατάω την πιο πρόσφατη γραμμή ανά ΑΦΜ
w = Window.partitionBy("afm").orderBy(col("updated_at").desc())
df_deduped = df.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")

# YoY change: σύγκριση με προηγούμενο έτος
w_yoy = Window.partitionBy("afm").orderBy("year")
df_yoy = df.withColumn("prev_year_tax", lag("tax_amount", 1).over(w_yoy))

# Running total: cumulative sum ανά Περιφέρεια
w_run = Window.partitionBy("region").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
df_run = df.withColumn("running_total", spark_sum("tax_amount").over(w_run))

# Avg ανά partition (χωρίς ordering)
w_avg = Window.partitionBy("region")
df_avg = df.withColumn("region_avg", avg("tax_amount").over(w_avg))
```

---

## 6. Streaming Basics

### Trigger options

| Trigger | Mode | Παράδειγμα use |
|---|---|---|
| `availableNow=True` | Process current data + exit | Notebook demos, scheduled batches |
| `processingTime="30 seconds"` | Micro-batch κάθε 30s | Near real-time analytics |
| `continuous="1 second"` | Continuous processing | Ultra-low latency (limited support) |
| (default) | Micro-batch ASAP | Standard streaming |

### Streaming queries σε notebook

```python
# Pattern 1: start + await (όλο το dataset, μετά exit)
query = (df.writeStream
    .format("delta")
    .option("checkpointLocation", chk)
    .trigger(availableNow=True)
    .toTable("workspace.aade.bronze_tax"))

query.awaitTermination()  # blocks until complete
```

### MERGE σε streaming με foreachBatch

```python
def merge_to_silver(microbatch_df, batch_id):
    microbatch_df.createOrReplaceTempView("updates")
    microbatch_df.sparkSession.sql("""
        MERGE INTO workspace.aade.silver_tax t
        USING updates s ON t.statement_id = s.statement_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

(streaming_df.writeStream
    .foreachBatch(merge_to_silver)
    .option("checkpointLocation", chk)
    .trigger(availableNow=True)
    .start()
    .awaitTermination())
```

---

## 7. Performance Optimization

### Top 5 optimizations με max impact

| # | Τεχνική | Πότε | Code |
|---|---|---|---|
| 1 | **Broadcast Join** | small table < 100MB | `df.join(broadcast(dim), "key")` |
| 2 | **Z-Order** | High-cardinality filter column | `OPTIMIZE t ZORDER BY (afm)` |
| 3 | **Partitioning** | Time-series queries | `PARTITIONED BY (year, month)` |
| 4 | **Column Pruning** | Wide tables | `SELECT col1, col2` αντί `SELECT *` |
| 5 | **Predicate Pushdown** | Parquet/Delta filters | `WHERE region = 'Αττική'` |

### Adaptive Query Execution (AQE) — Spark 3.x default

Στο Databricks είναι **always-on** στο Serverless. Αυτόματα:
- Συγχωνεύει μικρά shuffle partitions
- Switch-άρει σε broadcast αν runtime ένας πίνακας είναι μικρός
- Σπάει skewed partitions

**Σε Serverless δεν μπορείς να το disable** — όλες οι `spark.conf.set(...)` σε `spark.sql.adaptive.*` αποτυγχάνουν με `CONFIG_NOT_AVAILABLE`.

---

## 8. Unity Catalog Governance

### 3-Level Namespace

```
catalog.schema.object
   ↓       ↓      ↓
workspace.aade.silver_tax
```

### Permissions cheat-sheet

```sql
-- Δίνω permissions σε ομάδα
GRANT USE CATALOG ON CATALOG workspace TO `aade_analysts`;
GRANT USE SCHEMA ON SCHEMA workspace.aade TO `aade_analysts`;
GRANT SELECT ON ALL TABLES IN SCHEMA workspace.aade TO `aade_analysts`;

-- Column mask για PII
CREATE FUNCTION mask_afm(afm STRING) RETURNS STRING
RETURN CASE WHEN is_member('aade_auditors') THEN afm
            ELSE CONCAT('***', RIGHT(afm, 4)) END;

ALTER TABLE workspace.aade.silver_tax ALTER COLUMN afm SET MASK mask_afm;

-- Tags για classification
ALTER TABLE workspace.aade.silver_tax ALTER COLUMN afm
SET TAGS ('sensitivity' = 'pii', 'compliance' = 'gdpr');
```

---

## 9. Common Pitfalls Checklist

Πριν αρχίσετε τις exercises, βεβαιωθείτε ότι ξέρετε ΓΙΑΤΙ αυτά είναι λάθος:

| ❌ Anti-pattern | ✅ Καλύτερη πρακτική |
|---|---|
| `SELECT *` σε wide table | `SELECT specific_columns` |
| `df.rdd.getNumPartitions()` σε Serverless | DataFrame API: `spark_partition_id()` |
| `input_file_name()` σε UC | `col("_metadata.file_path")` |
| Repartition σε every step | Repartition only πριν heavy operation |
| Cache everywhere | Cache only αν διαβάζεται 2+ φορές |
| GRANT σε individual users | GRANT σε groups |
| `mlflow.log_model(model, artifact_path="model")` | `mlflow.log_model(sk_model=model, name="model")` |
| MERGE χωρίς matching key index | OPTIMIZE + ZORDER στη join column πριν MERGE |

---

> **🎯 Όταν τελειώσετε το διάβασμα, ανοίξτε το `01_Exercises_Foundations.py` και αρχίστε.**
> **Trainer**: ζητείστε να σας ρωτήσουν 1-2 quick questions πριν προχωρήσουν.
