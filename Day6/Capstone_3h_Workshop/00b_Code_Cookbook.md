# 📔 Code Cookbook — Copy-Paste Patterns

**Σκοπός:** Κάθε pattern που χρειάζεστε για τις exercises, **έτοιμο σε copy-paste form**.
Δεν χρειάζεται να ψάχνετε docs — το έχετε όλο εδώ.

**Διάρκεια ανάγνωσης:** Σκανάρετε το όταν κολλήσετε — μην το διαβάσετε γραμμή-γραμμή.

---

## 📑 Quick Index

1. [Imports & Setup](#1-imports--setup)
2. [Schema & Volume](#2-schema--volume)
3. [Read CSV → Delta](#3-read-csv--delta)
4. [Column Comments & Tags](#4-column-comments--tags)
5. [Data Quality Patterns](#5-data-quality-patterns)
6. [MERGE INTO Patterns](#6-merge-into-patterns)
7. [Window Functions](#7-window-functions)
8. [Auto Loader Streaming](#8-auto-loader-streaming)
9. [foreachBatch + MERGE](#9-foreachbatch--merge)
10. [Schema Evolution](#10-schema-evolution)
11. [Time Travel & RESTORE](#11-time-travel--restore)
12. [Joins (Inner/Left/Full)](#12-joins)
13. [Aggregations](#13-aggregations)
14. [DateTime Operations](#14-datetime-operations)
15. [Common Gotchas](#15-common-gotchas)

---

## 1. Imports & Setup

```python
import os
import logging
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, to_timestamp, to_date,
    upper, lower, trim, length, concat, concat_ws, split,
    row_number, lag, lead, rank, dense_rank,
    sum as spark_sum, avg, count, count_distinct, min as spark_min, max as spark_max,
    desc, asc, broadcast, expr, regexp_replace
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType, BooleanType, DateType
)
from delta.tables import DeltaTable

# Σιγάζουμε GRPC noise σε Serverless
for n in ("pyspark.sql.connect.client.core", "pyspark.sql.connect",
          "pyspark", "py4j", "grpc"):
    logging.getLogger(n).setLevel(logging.CRITICAL)
```

---

## 2. Schema & Volume

```python
# Schema (database)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.my_schema")

# Με metadata
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS workspace.my_schema
    COMMENT 'Personal schema for training'
    WITH DBPROPERTIES (
        'environment' = 'training',
        'owner' = 'george'
    )
""")

# Volume (filesystem για files)
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.my_schema.raw")

# Path: /Volumes/workspace/my_schema/raw/
import os
os.makedirs("/Volumes/workspace/my_schema/raw/sub_folder", exist_ok=True)
```

---

## 3. Read CSV → Delta

### Pattern A: SQL (πιο συμπαγές)
```sql
CREATE OR REPLACE TABLE workspace.my_schema.bronze_tax
USING DELTA
COMMENT 'Bronze layer description'
TBLPROPERTIES (
    'layer' = 'bronze',
    'data_owner' = 'aade',
    'pii_present' = 'true'
)
AS
SELECT *,
    current_timestamp() AS _ingested_at
FROM read_files(
    '/Volumes/workspace/my_schema/raw/file.csv',
    format => 'csv',
    header => true,
    inferSchema => true
)
```

### Pattern B: Python DataFrame API
```python
df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/my_schema/raw/file.csv")
    .withColumn("_ingested_at", current_timestamp()))

(df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.my_schema.bronze_tax"))
```

### Pattern C: Add `_source_file` σε UC
```python
# ⚠️ Σε UC, ΟΧΙ input_file_name() — χρησιμοποίησε _metadata.file_path
df = (spark.read
    .option("header", "true")
    .csv("/Volumes/.../*.csv")
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_ingested_at", current_timestamp()))
```

### Pattern D: Multiple CSV files (wildcard)
```python
df = spark.read.option("header", "true").csv("/Volumes/.../taxis/*.csv")
# Auto Loader είναι καλύτερο για production — δείτε section 8
```

---

## 4. Column Comments & Tags

```sql
-- Column comment (documentation)
ALTER TABLE workspace.aade.bronze_tax
ALTER COLUMN afm
COMMENT 'Αριθμός Φορολογικού Μητρώου — 9 ψηφία';

-- Column tags (key-value metadata)
ALTER TABLE workspace.aade.bronze_tax
ALTER COLUMN afm SET TAGS (
    'sensitivity' = 'pii',
    'compliance' = 'gdpr',
    'data_class' = 'restricted'
);

-- Table tags
ALTER TABLE workspace.aade.bronze_tax SET TAGS (
    'cost_center' = 'data-engineering',
    'business_owner' = 'finance-team'
);

-- View tags (in information_schema)
SELECT * FROM system.information_schema.column_tags
WHERE schema_name = 'aade' AND tag_name = 'sensitivity';
```

---

## 5. Data Quality Patterns

### Pattern A: Simple filter (drop invalid)
```python
silver = (bronze
    .filter(col("afm").isNotNull())
    .filter(length(col("afm")) == 9)
    .filter(col("tax_amount") >= 0))
```

### Pattern B: Quarantine με reason
```python
# Step 1: ορισμός validation expression
is_valid = (
    col("afm").isNotNull() &
    (length(col("afm")) == 9) &
    (col("tax_amount") >= 0) &
    (col("region") != "INVALID")
)

# Step 2: split
silver = bronze.filter(is_valid)

quarantine = (bronze
    .filter(~is_valid)
    .withColumn("_reason",
        when(col("afm").isNull(),                "NULL_AFM")
        .when(length(col("afm")) != 9,           "INVALID_AFM_FORMAT")
        .when(col("tax_amount") < 0,             "NEGATIVE_AMOUNT")
        .when(col("region") == "INVALID",        "INVALID_REGION")
        .otherwise("UNKNOWN")))

# Step 3: write και τα 2
silver.write.format("delta").mode("overwrite").saveAsTable("workspace.x.silver_x")
quarantine.write.format("delta").mode("overwrite").saveAsTable("workspace.x.quarantine_x")
```

### Pattern C: DQ metrics tracking
```python
dq_records = []

def log_dq(source, rule, failed, total):
    dq_records.append({
        "source": source,
        "rule": rule,
        "failed_count": failed,
        "total_count": total,
        "failure_pct": round(failed / max(total, 1) * 100, 2),
        "checked_at": datetime.utcnow(),
    })

total = bronze.count()
log_dq("taxis", "afm_not_null", bronze.filter(col("afm").isNull()).count(), total)
log_dq("taxis", "afm_9_digits", bronze.filter(length(col("afm")) != 9).count(), total)

# Save
(spark.createDataFrame(pd.DataFrame(dq_records))
    .write.format("delta").mode("append")
    .saveAsTable("workspace.x.dq_summary"))
```

### Pattern D: Deduplication
```python
# Κρατάω την πιο πρόσφατη version ανά ID
w = Window.partitionBy("statement_id").orderBy(col("updated_at").desc())
deduped = (df
    .withColumn("rn", row_number().over(w))
    .filter("rn = 1")
    .drop("rn"))

# Εναλλακτικά (simpler αλλά λιγότερο control):
deduped = df.dropDuplicates(["statement_id"])
```

---

## 6. MERGE INTO Patterns

### Pattern A: Simple upsert (Python)
```python
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "workspace.aade.silver_tax")
(target.alias("t")
    .merge(source_df.alias("s"), "t.statement_id = s.statement_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
```

### Pattern B: Conditional update (μόνο newer rows)
```python
(target.alias("t")
    .merge(source_df.alias("s"), "t.statement_id = s.statement_id")
    .whenMatchedUpdate(
        condition="s.submitted_at > t.submitted_at",  # ⬅️ skip παλιά
        set={
            "tax_amount":    "s.tax_amount",
            "status":        "s.status",
            "submitted_at":  "s.submitted_at",
            "_updated_at":   "current_timestamp()"
        })
    .whenNotMatchedInsertAll()
    .execute())
```

### Pattern C: Με delete clause
```python
(target.alias("t")
    .merge(source_df.alias("s"), "t.statement_id = s.statement_id")
    .whenMatchedDelete(condition="s._cdc_op = 'DELETE'")  # ⬅️ CDC delete
    .whenMatchedUpdateAll(condition="s._cdc_op IN ('INSERT','UPDATE')")
    .whenNotMatchedInsertAll(condition="s._cdc_op IN ('INSERT','UPDATE')")
    .execute())
```

### Pattern D: SQL MERGE
```sql
MERGE INTO workspace.aade.silver_tax t
USING source_view s
ON t.statement_id = s.statement_id

WHEN MATCHED AND s.submitted_at > t.submitted_at THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *
```

---

## 7. Window Functions

### Pattern A: Row number ανά group
```python
# Latest row per partition
w = Window.partitionBy("afm").orderBy(col("submitted_at").desc())
df_latest = (df
    .withColumn("rn", row_number().over(w))
    .filter("rn = 1")
    .drop("rn"))
```

### Pattern B: Lag/Lead (προηγούμενη/επόμενη γραμμή)
```python
w = Window.partitionBy("afm").orderBy("fiscal_year")
df = (df
    .withColumn("prev_income", lag("declared_income", 1).over(w))
    .withColumn("yoy_change",
        (col("declared_income") - col("prev_income")) / col("prev_income") * 100))
```

### Pattern C: Running total
```python
w = Window.partitionBy("region").orderBy("date").rowsBetween(
    Window.unboundedPreceding, 0
)
df = df.withColumn("running_total_tax", spark_sum("tax_amount").over(w))
```

### Pattern D: Group-level average (χωρίς ordering)
```python
w = Window.partitionBy("region")
df = (df
    .withColumn("region_avg", avg("tax_amount").over(w))
    .withColumn("vs_avg", col("tax_amount") / col("region_avg")))
```

### Pattern E: Rank / Dense Rank
```python
w = Window.partitionBy("region").orderBy(col("tax_amount").desc())
df = (df
    .withColumn("rank_in_region", rank().over(w))          # 1, 2, 2, 4
    .withColumn("dense_rank", dense_rank().over(w)))       # 1, 2, 2, 3

# Top 3 per region
top3 = df.filter(col("rank_in_region") <= 3)
```

---

## 8. Auto Loader Streaming

### Pattern A: Basic incremental file ingestion
```python
src_path = "/Volumes/workspace/aade/raw/taxis"
chk_path = "/Volumes/workspace/aade/raw/checkpoints/bronze_tax"

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{chk_path}/_schema")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(src_path)

    # transformations εδώ
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))

    .writeStream
    .format("delta")
    .option("checkpointLocation", chk_path)
    .trigger(availableNow=True)               # ⬅️ run-and-stop
    .toTable("workspace.aade.bronze_tax")
    .awaitTermination())                       # ⬅️ block until done
```

### Pattern B: Με schema evolution
```python
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{chk_path}/_schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")   # ⬅️ NEW
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(src_path)
    .writeStream
    .format("delta")
    .option("checkpointLocation", chk_path)
    .option("mergeSchema", "true")                               # ⬅️ NEW
    .trigger(availableNow=True)
    .toTable("workspace.aade.bronze_tax")
    .awaitTermination())
```

### Pattern C: Continuous (production 24/7)
```python
.trigger(processingTime="30 seconds")  # κάθε 30s
# ή
.trigger(continuous="1 second")        # ultra low latency (limited support)
# ή (default)
.trigger()                              # micro-batch ASAP
```

---

## 9. foreachBatch + MERGE

```python
def merge_to_silver(microbatch_df, batch_id):
    """Streaming MERGE — runs ανά microbatch."""

    # Step 1: dedupe σε επίπεδο microbatch
    w = Window.partitionBy("statement_id").orderBy(col("_ingested_at").desc())
    dedup = (microbatch_df
        .withColumn("_rn", row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("_merged_at", current_timestamp()))

    # Step 2: MERGE
    target = DeltaTable.forName(
        microbatch_df.sparkSession,
        "workspace.aade.silver_tax"
    )
    (target.alias("t")
        .merge(dedup.alias("s"), "t.statement_id = s.statement_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

    print(f"  ✓ Batch {batch_id}: processed {dedup.count()} rows")


# Apply
(spark.readStream
    .table("workspace.aade.bronze_tax")
    .writeStream
    .foreachBatch(merge_to_silver)
    .option("checkpointLocation", "/Volumes/.../silver_merge_chk")
    .trigger(availableNow=True)
    .start()
    .awaitTermination())
```

---

## 10. Schema Evolution

```python
# Option 1: αυτόματη evolution κατά το write
(df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("workspace.aade.bronze_tax"))

# Option 2: Auto Loader schema evolution
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# Modes:
#   - addNewColumns:  νέες columns προστίθενται (default σε νέα versions)
#   - rescue:         saves κάθε «παράξενο» field σε _rescued_data column
#   - failOnNewColumns: σπάει αν εμφανιστεί νέα column

# Option 3: SQL ALTER TABLE ADD COLUMNS
ALTER TABLE workspace.aade.bronze_tax
ADD COLUMNS (tax_office_code STRING COMMENT 'New from TAXIS v2');
```

---

## 11. Time Travel & RESTORE

```sql
-- See history
DESCRIBE HISTORY workspace.aade.silver_tax;

-- Read previous version
SELECT * FROM workspace.aade.silver_tax VERSION AS OF 5;
SELECT * FROM workspace.aade.silver_tax TIMESTAMP AS OF '2026-04-30 23:59:59';

-- Rollback table to previous version
RESTORE TABLE workspace.aade.silver_tax TO VERSION AS OF 5;
RESTORE TABLE workspace.aade.silver_tax TO TIMESTAMP AS OF '2026-04-30 12:00:00';

-- Cleanup παλιών versions (default 7 days retention)
VACUUM workspace.aade.silver_tax;
VACUUM workspace.aade.silver_tax RETAIN 168 HOURS;  -- 7 days
```

```python
# Python equivalent
df_prev = spark.read.format("delta").option("versionAsOf", 5).load("...")
df_prev = spark.read.format("delta").option("timestampAsOf", "2026-04-30").load("...")
```

---

## 12. Joins

```python
# Inner join (default)
result = df1.join(df2, "afm")

# Left join (keep all from df1)
result = df1.join(df2, "afm", "left")

# Full outer join (keep all from both)
result = df1.join(df2, "afm", "fullouter")

# Με διαφορετικά column names
result = df1.join(df2, df1.afm == df2.taxpayer_afm, "left")

# Multi-column join
result = df1.join(df2, ["afm", "fiscal_year"], "inner")

# Broadcast join (μικρός πίνακας στα δεξιά)
result = big_df.join(broadcast(small_df), "afm")

# Anti-join (στο df1 που ΔΕΝ υπάρχουν στο df2)
result = df1.join(df2, "afm", "left_anti")
```

---

## 13. Aggregations

```python
# Basic groupBy
result = (df.groupBy("region")
    .agg(
        count("*").alias("total_count"),
        spark_sum("tax_amount").alias("total_tax"),
        avg("tax_amount").alias("avg_tax"),
        spark_max("tax_amount").alias("max_tax"),
        spark_min("tax_amount").alias("min_tax"),
        count_distinct("afm").alias("unique_taxpayers"),
    ))

# Conditional aggregation
result = (df.groupBy("region")
    .agg(
        spark_sum(when(col("status") == "APPROVED", 1).otherwise(0)).alias("approved"),
        spark_sum(when(col("status") == "REJECTED", 1).otherwise(0)).alias("rejected"),
    ))

# Multi-column groupBy
result = df.groupBy("region", "fiscal_year").agg(...)

# Με expressions
result = df.groupBy("region").agg(
    expr("SUM(tax_amount * 0.85) AS net_revenue")
)
```

---

## 14. DateTime Operations

```python
# Parse string → timestamp
df = df.withColumn("ts", to_timestamp(col("submitted_at")))
df = df.withColumn("dt", to_date(col("submitted_at")))

# Format specific
df = df.withColumn("ts", to_timestamp(col("submitted_at"), "yyyy-MM-dd HH:mm:ss"))

# Current timestamp
df = df.withColumn("now", current_timestamp())

# Extract components
df = (df
    .withColumn("year",  F.year("ts"))
    .withColumn("month", F.month("ts"))
    .withColumn("day",   F.dayofmonth("ts"))
    .withColumn("hour",  F.hour("ts")))

# Date arithmetic
df = df.withColumn("days_old", F.datediff(current_timestamp(), col("submitted_at")))
df = df.withColumn("tomorrow", F.date_add(col("dt"), 1))

# Filter by date
df = df.filter(col("submitted_at") >= "2025-01-01")
df = df.filter(col("submitted_at") >= F.date_sub(current_timestamp(), 30))
```

---

## 15. Common Gotchas

### ❌ `input_file_name()` σε UC → `UC_COMMAND_NOT_SUPPORTED`
```python
# ❌ Wrong
df.withColumn("_src", F.input_file_name())

# ✅ Right
df.withColumn("_src", col("_metadata.file_path"))
```

### ❌ `df.rdd.getNumPartitions()` σε Serverless → `NOT_IMPLEMENTED`
```python
# ❌ Wrong (σε Serverless Spark Connect)
n = df.rdd.getNumPartitions()

# ✅ Right
n = df.withColumn("pid", F.spark_partition_id()).select("pid").distinct().count()
```

### ❌ `spark.conf.set("spark.sql.adaptive.enabled", "false")` → `CONFIG_NOT_AVAILABLE`
```python
# ❌ Wrong σε Serverless — AQE είναι always-on
spark.conf.set("spark.sql.adaptive.enabled", "false")

# ✅ Right — wrap σε try/except
try:
    spark.conf.set("spark.sql.adaptive.enabled", "false")
except Exception:
    print("AQE always-on σε Serverless")
```

### ❌ `mlflow.log_model(model, artifact_path="model")` (MLflow 2.20+)
```python
# ❌ Old API
mlflow.sklearn.log_model(model, artifact_path="model")

# ✅ New API
mlflow.sklearn.log_model(sk_model=model, name="model")
```

### ❌ Empty MERGE σε empty table

Python — δεν δουλεύει αν το table δεν υπάρχει:
```python
# ❌ Wrong — DeltaTable δεν υπάρχει
target = DeltaTable.forName(spark, "workspace.x.silver_tax")  # NoSuchTableException
```

SQL — δημιουργήστε πρώτα empty shell:
```sql
CREATE OR REPLACE TABLE workspace.x.silver_tax (
    statement_id STRING,
    afm STRING
) USING DELTA;
```

Μετά κάνετε MERGE κανονικά.

### ❌ foreachBatch με λάθος signature
```python
# ❌ Wrong — μόνο 1 argument
def merge_to_silver(df):
    ...

# ✅ Right — ΠΑΝΤΑ 2 args: (microbatch_df, batch_id)
def merge_to_silver(microbatch_df, batch_id):
    # batch_id είναι long (incremental ανά microbatch)
    ...
```

### ❌ Schema location collision μεταξύ streaming queries
Κάθε `cloudFiles.schemaLocation` πρέπει να είναι **unique per stream**. Αν 2 streams μοιραστούν path:
- Schema από το ένα stream υπερισχύει του άλλου
- Mysterious "schema doesn't match" errors

✅ Right: `{checkpoint_dir}/bronze_stream_taxis/_schema` και `{checkpoint_dir}/bronze_stream_mydata/_schema`.

### ❌ Streaming query χωρίς `awaitTermination()`
```python
# ❌ Wrong — η query τρέχει στο background, το notebook συνεχίζει χωρίς να περιμένει
query = df.writeStream.toTable("...").trigger(availableNow=True)

# ✅ Right
query = df.writeStream.toTable("...").trigger(availableNow=True)
query.awaitTermination()  # ⬅️ blocks until complete
```

### ❌ Forgot to set `mergeSchema=true` σε schema evolution
```python
# Αν προστεθούν νέες columns στο source, χωρίς mergeSchema θα σπάσει
.writeStream
.option("mergeSchema", "true")     # ⬅️ απαραίτητο
.toTable("...")
```

### ❌ Conflicting schema σε MERGE
```python
# Αν source και target έχουν διαφορετικό schema, MERGE σπάει
# Λύση: explicit column mapping σε .whenMatchedUpdate({...})
```

---

## 🔍 Debug Patterns

### Δες τι περιέχει το DataFrame
```python
df.show(10, truncate=False)              # top 10 rows
df.printSchema()                          # schema
df.describe().show()                      # statistics
df.count()                                # row count
df.distinct().count()                     # distinct rows
df.columns                                # column names list
```

### Δες ιστορία ενός Delta table
```sql
DESCRIBE HISTORY workspace.aade.silver_tax;
DESCRIBE DETAIL workspace.aade.silver_tax;
DESCRIBE TABLE EXTENDED workspace.aade.silver_tax;
SHOW CREATE TABLE workspace.aade.silver_tax;
```

### Catalog Explorer browse (UI)
- Sidebar → Catalog → workspace → schema → table
- Tabs: Sample data | Columns | Lineage | Permissions | History | Quality

### Spark UI για performance
- Click στο "View" στο cell output
- Tabs: Jobs | Stages | Storage | SQL/DataFrame
- Look for: shuffle bytes, spill to disk, task duration variance

---

> **🎯 Tip**: Bookmark αυτό το αρχείο. Θα το χρειαστείς ξανά μετά το workshop.
