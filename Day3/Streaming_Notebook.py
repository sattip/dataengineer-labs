# Databricks notebook source
# MAGIC %md
# MAGIC # Άσκηση #7 — Spark Structured Streaming + Delta (Ημέρα 3)
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC
# MAGIC > **GitHub** (share this στο Zoom chat):
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/Streaming_Notebook.py
# MAGIC > ```
# MAGIC
# MAGIC **Catalog/Schema**: `workspace.aade`
# MAGIC **Διάρκεια**: ~25 min
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Σενάριο
# MAGIC
# MAGIC Είσαι **Data Engineer στο Υπουργείο Υποδομών**. Έχεις IoT sensors σε γέφυρες της χώρας — κάθε sensor στέλνει **ανά λεπτό** μετρήσεις δονήσεων + θερμοκρασίας. Συνολικά ~5M events/day.
# MAGIC
# MAGIC Πρέπει να φτιάξεις pipeline που:
# MAGIC 1. **Δέχεται live events** (streaming ingestion)
# MAGIC 2. Τα **γράφει σε Bronze Delta table**
# MAGIC 3. Υπολογίζει **per-sensor aggregations** σε **5-min windows**
# MAGIC 4. Ενεργοποιεί **alert** όταν δονήσεις > threshold
# MAGIC 5. Κρατάει **historical batch view** πάνω στα ίδια δεδομένα (unified analytics)
# MAGIC
# MAGIC ## 🧠 Key concept: Delta = Unified Batch + Streaming
# MAGIC
# MAGIC Το **κορυφαίο feature** του Delta Lake: **το ίδιο table** μπορεί να:
# MAGIC - Διαβάζεται streaming (`readStream`) — για live dashboards
# MAGIC - Διαβάζεται batch (`spark.read.table`) — για historical analysis
# MAGIC - Γράφεται streaming **και** batch ταυτόχρονα — Lambda architecture **χωρίς δύο pipelines**
# MAGIC
# MAGIC Σε vanilla Hadoop/Hive: χρειάζεσαι **Lambda architecture** = 2 codebases (batch + speed layer). Με Delta: **Kappa architecture** = 1 codebase.
# MAGIC
# MAGIC ## 📋 Notebook structure
# MAGIC
# MAGIC | Cell | Step | Time |
# MAGIC |---|------|------|
# MAGIC | 0 | Setup (schema, volume, checkpoint dir) | 30s |
# MAGIC | 1 | Generate synthetic IoT events (rate source) | 5s |
# MAGIC | 2 | Write streaming → Bronze Delta | 30s + 60s run |
# MAGIC | 3 | Batch query πάνω στο Bronze (παράλληλα με stream) | 5s |
# MAGIC | 4 | Streaming aggregation με watermark (events/min ανά sensor) | 30s + run |
# MAGIC | 5 | Alert filter (high vibration → Slack-ready output) | 30s |
# MAGIC | 6 | Stop streams + cleanup | 5s |
# MAGIC | 7 | Production patterns (Auto Loader, Kafka, foreachBatch) | — |
# MAGIC
# MAGIC ⚠️ **ΠΡΟΣΟΧΗ**: τα streaming queries τρέχουν **continuously**. Πρέπει να τα **σταματήσεις** όταν τελειώσεις (Cell 6) — αλλιώς τρώνε compute budget.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0 — Setup

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

# Streaming queries χρειάζονται checkpoint directory — fault tolerance
CHECKPOINT_BASE = "/Volumes/workspace/aade/aade_data/_checkpoints"
print(f"✅ Checkpoint base: {CHECKPOINT_BASE}")

# Cleanup old checkpoints (idempotent re-run safety)
dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
dbutils.fs.mkdirs(CHECKPOINT_BASE)

# Drop old streaming tables αν υπάρχουν (clean slate)
for t in ["bridge_sensors_bronze", "bridge_sensors_alerts"]:
    spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")

print("✅ Setup ready. Schema/volume/checkpoint clean.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 1 — Generate synthetic IoT events
# MAGIC
# MAGIC Δεν έχουμε real Kafka/Event Hubs. Χρησιμοποιούμε το **`rate` source** του Spark — generates events με σταθερό rowsPerSecond.
# MAGIC
# MAGIC Πάνω από αυτό, προσομοιώνουμε bridge sensors:
# MAGIC - **5 γέφυρες** (sensor_id 1–5)
# MAGIC - Tυχαίες δονήσεις (vibration_hz) και θερμοκρασία (temp_c)
# MAGIC - Κάποιες θα πέφτουν σε "alarm zone" (vibration > 8 Hz)

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, rand, when, current_timestamp, expr, from_unixtime
)

# rate source: ένα event ανά second, με incrementing value
events_stream = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)  # 10 events/sec
    .load()
)

# Transform σε bridge sensor events
sensor_stream = (events_stream
    .withColumn("sensor_id", (col("value") % 5 + 1).cast("int"))
    .withColumn("vibration_hz", (rand() * 12).cast("double"))   # 0-12 Hz
    .withColumn("temp_c", (15 + rand() * 20).cast("double"))     # 15-35 °C
    .withColumn("event_time", col("timestamp"))
    .select("sensor_id", "event_time", "vibration_hz", "temp_c")
)

print("✅ Streaming source defined: 10 events/sec, 5 sensors, vibration + temp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 2 — Write streaming → Bronze Delta
# MAGIC
# MAGIC Το stream γράφει σε **continuous mode** στο Bronze Delta table. **Trigger 10 sec** = κάθε 10 sec γίνεται μία νέα write.

# COMMAND ----------

bronze_query = (sensor_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
    .trigger(processingTime="10 seconds")
    .toTable("workspace.aade.bridge_sensors_bronze")
)

print(f"✅ Bronze stream started. Query ID: {bronze_query.id}")
print(f"   Status: {bronze_query.status}")
print(f"\n⏳ Awaiting first batch (~15 sec)...")

# Περιμένουμε λίγο για να γίνει το πρώτο batch
import time
time.sleep(20)

# Check progress
print(f"\n📊 Last progress: {bronze_query.lastProgress.get('numInputRows', 0) if bronze_query.lastProgress else 'no batch yet'} rows in last batch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 3 — Batch query πάνω στο Bronze (παράλληλα με stream)
# MAGIC
# MAGIC Εδώ δείχνεται το **unified analytics** του Delta. Ενώ το stream συνεχίζει να γράφει, εμείς διαβάζουμε σαν να ήταν σταθερό batch table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Batch read πάνω σε Delta table που γεμίζει streaming
# MAGIC SELECT
# MAGIC   sensor_id,
# MAGIC   COUNT(*) AS event_count,
# MAGIC   ROUND(AVG(vibration_hz), 2) AS avg_vibration,
# MAGIC   ROUND(MAX(vibration_hz), 2) AS max_vibration,
# MAGIC   ROUND(AVG(temp_c), 1) AS avg_temp
# MAGIC FROM workspace.aade.bridge_sensors_bronze
# MAGIC GROUP BY sensor_id
# MAGIC ORDER BY sensor_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 4 — Streaming aggregation με watermark
# MAGIC
# MAGIC ### Τι είναι watermark;
# MAGIC
# MAGIC Σε streaming, events μπορούν να φτάσουν **late** (π.χ. network delay). Το **watermark** λέει στο Spark «μετά από X λεπτά μην περιμένεις πια — δώσε το αποτέλεσμα».
# MAGIC
# MAGIC ```
# MAGIC withWatermark("event_time", "2 minutes") = "για κάθε window, περίμενε
# MAGIC                                            μέχρι και 2 min late events
# MAGIC                                            πριν κλείσει το window"
# MAGIC ```
# MAGIC
# MAGIC Χωρίς watermark, το Spark θα κρατούσε **όλα** τα windows σε μνήμη για πάντα → memory leak.

# COMMAND ----------

from pyspark.sql.functions import window, avg, count

# Source = ξανά διαβάζουμε το Bronze ως stream (ναι, μπορούμε!)
bronze_stream = (spark.readStream
    .format("delta")
    .table("workspace.aade.bridge_sensors_bronze"))

# 1-min tumbling windows ανά sensor, με 2-min watermark
agg_stream = (bronze_stream
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window("event_time", "1 minute"),
        "sensor_id"
    )
    .agg(
        count("*").alias("events"),
        avg("vibration_hz").alias("avg_vibration"),
        avg("temp_c").alias("avg_temp")
    )
)

# Display live (refreshes κάθε λίγα δευτερόλεπτα)
display_query = (agg_stream.writeStream
    .format("memory")
    .queryName("live_aggregations")
    .outputMode("complete")
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"✅ Aggregation stream started. Query ID: {display_query.id}")
print("\n⏳ Wait 30 sec, then run the next cell to see live aggregations...")

import time
time.sleep(30)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Live view του aggregation stream (memory sink)
# MAGIC SELECT
# MAGIC   window.start AS window_start,
# MAGIC   sensor_id,
# MAGIC   events,
# MAGIC   ROUND(avg_vibration, 2) AS avg_vibration,
# MAGIC   ROUND(avg_temp, 1) AS avg_temp
# MAGIC FROM live_aggregations
# MAGIC ORDER BY window_start DESC, sensor_id
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 5 — Alert filter (high vibration)
# MAGIC
# MAGIC Επιχειρηματικός κανόνας: **vibration > 8 Hz = potential structural risk → alert**.
# MAGIC
# MAGIC Σε production, το `foreachBatch` θα έστελνε:
# MAGIC - Slack notification
# MAGIC - SMS στον engineer on-call
# MAGIC - Insert σε ServiceNow ticket queue

# COMMAND ----------

# Stream alerts → ξεχωριστό Delta table
alerts_stream = (bronze_stream
    .filter(col("vibration_hz") > 8.0)
    .withColumn("alert_severity",
        when(col("vibration_hz") > 10.0, lit("critical"))
        .otherwise(lit("warning")))
    .withColumn("alert_time", current_timestamp())
)

alerts_query = (alerts_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/alerts")
    .trigger(processingTime="10 seconds")
    .toTable("workspace.aade.bridge_sensors_alerts")
)

print(f"✅ Alerts stream started. Query ID: {alerts_query.id}")

import time
time.sleep(20)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Δες τα alerts που έχουν συμβεί
# MAGIC SELECT
# MAGIC   alert_severity,
# MAGIC   sensor_id,
# MAGIC   ROUND(vibration_hz, 2) AS vibration_hz,
# MAGIC   event_time,
# MAGIC   alert_time
# MAGIC FROM workspace.aade.bridge_sensors_alerts
# MAGIC ORDER BY alert_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Counts ανά severity
# MAGIC SELECT alert_severity, COUNT(*) AS alert_count
# MAGIC FROM workspace.aade.bridge_sensors_alerts
# MAGIC GROUP BY alert_severity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6 — Stop streams + cleanup
# MAGIC
# MAGIC ⚠️ **ΥΠΟΧΡΕΩΤΙΚΟ**: τα streaming queries τρέχουν συνέχεια — αν δεν τα σταματήσεις, καταναλώνουν compute.

# COMMAND ----------

# Stop όλα τα active streaming queries
for q in spark.streams.active:
    print(f"🛑 Stopping query: {q.name} ({q.id})")
    q.stop()

# Wait να κλείσουν
import time
time.sleep(5)

print(f"\n✅ Active streams μετά το stop: {len(spark.streams.active)}")

# COMMAND ----------

# Final τι γράφτηκε
bronze_count = spark.sql("SELECT COUNT(*) AS n FROM workspace.aade.bridge_sensors_bronze").collect()[0]["n"]
alerts_count = spark.sql("SELECT COUNT(*) AS n FROM workspace.aade.bridge_sensors_alerts").collect()[0]["n"]
print(f"📊 Final counts:")
print(f"   bridge_sensors_bronze: {bronze_count} events")
print(f"   bridge_sensors_alerts: {alerts_count} alerts")
print(f"\n   Alert rate: {100.0 * alerts_count / bronze_count if bronze_count else 0:.1f}% (expected ~30% since vibration > 8 Hz σε ομοιόμορφη 0-12)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 7 — Production patterns
# MAGIC
# MAGIC ### Pattern 1: Auto Loader (αντί για rate source)
# MAGIC
# MAGIC Σε production, αρχεία πέφτουν σε cloud storage (S3/ADLS/GCS). **Auto Loader** ανιχνεύει νέα files αυτόματα:
# MAGIC
# MAGIC ```python
# MAGIC stream = (spark.readStream
# MAGIC   .format("cloudFiles")
# MAGIC   .option("cloudFiles.format", "json")
# MAGIC   .option("cloudFiles.schemaLocation", "/Volumes/.../_schema")
# MAGIC   .load("/Volumes/.../landing/"))
# MAGIC ```
# MAGIC
# MAGIC Πλεονεκτήματα: incremental file discovery (δεν re-scan-άρει όλα), schema inference, scale.
# MAGIC
# MAGIC ### Pattern 2: Kafka source
# MAGIC
# MAGIC ```python
# MAGIC stream = (spark.readStream
# MAGIC   .format("kafka")
# MAGIC   .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
# MAGIC   .option("subscribe", "bridge_sensors")
# MAGIC   .load())
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: foreachBatch (custom output)
# MAGIC
# MAGIC ```python
# MAGIC def send_alerts_to_slack(batch_df, batch_id):
# MAGIC     critical = batch_df.filter("alert_severity = 'critical'").collect()
# MAGIC     for row in critical:
# MAGIC         requests.post(SLACK_WEBHOOK, json={"text": f"🚨 Sensor {row.sensor_id}"})
# MAGIC
# MAGIC alerts_stream.writeStream.foreachBatch(send_alerts_to_slack).start()
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 4: Trigger types
# MAGIC
# MAGIC | Trigger | Use case |
# MAGIC |---|---|
# MAGIC | `processingTime="10 seconds"` | Soft real-time (default) |
# MAGIC | `availableNow=True` | Run once, process όλα τα νέα data, stop |
# MAGIC | `continuous="1 second"` | Continuous mode (experimental, sub-second latency) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Wrap-up
# MAGIC
# MAGIC | Concept | Command | Use case |
# MAGIC |---|---|---|
# MAGIC | **Streaming source** | `spark.readStream.format("rate"/"kafka"/"cloudFiles")` | Synthetic / Kafka / file-based |
# MAGIC | **Streaming sink** | `.writeStream.format("delta").toTable(...)` | Append to Bronze/Silver/Gold |
# MAGIC | **Watermark** | `.withWatermark("col", "X minutes")` | Bound late events |
# MAGIC | **Window aggregation** | `.groupBy(window("col", "1 minute"))` | Time-bucketed metrics |
# MAGIC | **Trigger** | `.trigger(processingTime="10 seconds")` | Control batch frequency |
# MAGIC | **Stop streams** | `for q in spark.streams.active: q.stop()` | **ΠΑΝΤΑ** στο τέλος |
# MAGIC
# MAGIC ### Unified batch + streaming
# MAGIC
# MAGIC Το ίδιο `bridge_sensors_bronze` Delta table είναι:
# MAGIC - **Streamable** (`readStream` → live aggregations, alerts)
# MAGIC - **Queryable** (`spark.sql` → ad-hoc analytics, BI dashboards)
# MAGIC - **Time-travelable** (`VERSION AS OF` → historical reproducibility)
# MAGIC
# MAGIC **Ένα table, τρία usage patterns** — αυτό είναι το Delta promise.
# MAGIC
# MAGIC ### Κλειστή ερώτηση για τους trainees
# MAGIC
# MAGIC > "Σε Lambda architecture έχεις 2 codebases — batch (Hive/Spark batch) + speed (Storm/Flink). Πόσος κώδικας θα χρειαζόταν για το ίδιο pipeline σε Delta;"
# MAGIC
# MAGIC **Απάντηση**: **Ένα notebook**. Το Delta unified batch+streaming επιτρέπει Kappa architecture — single codebase, μόνο διαφορετικά read/write modes.
