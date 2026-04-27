# Databricks notebook source
# MAGIC %md
# MAGIC # Άσκηση #7 — Spark Structured Streaming + Auto Loader (Ημέρα 3)
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
# MAGIC Είσαι **Data Engineer στο Υπουργείο Υποδομών**. IoT sensors σε γέφυρες στέλνουν JSON files σε cloud storage (S3/ADLS). Πρέπει να:
# MAGIC
# MAGIC 1. **Διαβάζεις τα νέα files incremental** (όχι re-process των παλιών)
# MAGIC 2. Τα **γράφεις σε Bronze Delta**
# MAGIC 3. Υπολογίζεις **per-sensor aggregations**
# MAGIC 4. Ενεργοποιείς **alert** όταν δονήσεις > threshold
# MAGIC 5. Κρατάς **historical batch view** πάνω στα ίδια δεδομένα
# MAGIC
# MAGIC ## 🧠 Key concepts
# MAGIC
# MAGIC ### 1. Auto Loader (`cloudFiles`)
# MAGIC Production pattern για ingestion από cloud storage. Auto-discover νέα files, schema inference, exactly-once.
# MAGIC
# MAGIC ### 2. `trigger(availableNow=True)`
# MAGIC One-shot streaming run: «process όλα τα νέα data, stop». Σε Databricks Free Edition / Serverless είναι **το μόνο supported trigger** (continuous `processingTime` δεν υποστηρίζεται).
# MAGIC Ιδανικό για **scheduled batch jobs** που χρησιμοποιούν streaming API για incremental processing.
# MAGIC
# MAGIC ### 3. Delta = Unified Batch + Streaming
# MAGIC Το ίδιο Delta table είναι streamable (`readStream`) **και** queryable (`spark.sql`). **1 codebase**, όχι Lambda architecture.
# MAGIC
# MAGIC ## 📋 Notebook structure
# MAGIC
# MAGIC | Cell | Step |
# MAGIC |---|------|
# MAGIC | 0 | Setup (schema, volume, checkpoint, landing dir) |
# MAGIC | 1 | Generate batch 1 of synthetic IoT JSON files (50 events) |
# MAGIC | 2 | Auto Loader → Bronze Delta (incremental ingest) |
# MAGIC | 3 | Batch query πάνω στο Bronze (unified analytics) |
# MAGIC | 4 | Streaming aggregation με watermark + tumbling windows |
# MAGIC | 5 | Alert stream (high vibration → Delta + Slack-ready) |
# MAGIC | 6 | Generate batch 2 (50 more events) — δείξε incremental |
# MAGIC | 7 | Re-run streams — process ΜΟΝΟ τα νέα files |
# MAGIC | 8 | Cleanup (drop tables, remove checkpoints) |
# MAGIC | 9 | Production patterns (continuous trigger, Kafka, foreachBatch) |
# MAGIC
# MAGIC ⚠️ Όλα τα streams χρησιμοποιούν `trigger(availableNow=True)` — **self-terminate**, όχι runaway compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0 — Setup

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

VOL = "/Volumes/workspace/aade/aade_data"
LANDING = f"{VOL}/sensor_landing"
CHECKPOINT_BASE = f"{VOL}/_checkpoints_streaming"
SCHEMA_LOC = f"{VOL}/_schema_streaming"

# Clean slate (idempotent re-runs)
for path in [LANDING, CHECKPOINT_BASE, SCHEMA_LOC]:
    dbutils.fs.rm(path, recurse=True)
    dbutils.fs.mkdirs(path)

for t in ["bridge_sensors_bronze", "bridge_sensors_alerts", "bridge_sensors_agg"]:
    spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")

print(f"✅ Setup ready.")
print(f"   Landing dir:      {LANDING}")
print(f"   Checkpoint base:  {CHECKPOINT_BASE}")
print(f"   Schema location:  {SCHEMA_LOC}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 1 — Generate batch 1 of synthetic events
# MAGIC
# MAGIC Σε production, sensors push events σε cloud storage as JSON. Εδώ προσομοιώνουμε γράφοντας 50 events σε ένα JSON file.
# MAGIC
# MAGIC **5 γέφυρες** (sensor_id 1–5), τυχαίες δονήσεις (vibration_hz 0–12) και θερμοκρασία (temp_c 15–35).

# COMMAND ----------

import json, random, time
from datetime import datetime, timedelta

random.seed(42)

def generate_events(n, batch_id):
    """Παράγει n synthetic events και τα γράφει σε JSON file στο landing dir."""
    base_time = datetime.utcnow()
    events = []
    for i in range(n):
        events.append({
            "sensor_id": random.randint(1, 5),
            "event_time": (base_time + timedelta(seconds=i)).isoformat(),
            "vibration_hz": round(random.uniform(0, 12), 2),
            "temp_c": round(random.uniform(15, 35), 1),
        })
    # Write as JSON Lines (1 record per line — Auto Loader friendly)
    file_path = f"{LANDING}/batch_{batch_id}.json"
    content = "\n".join(json.dumps(e) for e in events)
    dbutils.fs.put(file_path, content, overwrite=True)
    return file_path, len(events)

path, count = generate_events(50, batch_id=1)
print(f"✅ Wrote {count} events to {path}")

# Verify file landed
files = dbutils.fs.ls(LANDING)
for f in files:
    print(f"   📄 {f.name}  ({f.size} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 2 — Auto Loader → Bronze Delta
# MAGIC
# MAGIC `cloudFiles` = Databricks Auto Loader. Production-grade ingestion από cloud storage:
# MAGIC - **Incremental file discovery** — διαβάζει μόνο τα νέα files (μέσω checkpoint)
# MAGIC - **Schema inference** — αυτόματα από το πρώτο file
# MAGIC - **Exactly-once** — checkpoint-protected
# MAGIC
# MAGIC ### Trigger: `availableNow=True`
# MAGIC One-shot run: «process όλα τα available files, stop». Self-terminates → no runaway compute.

# COMMAND ----------

bronze_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(LANDING)
)

# Write με availableNow trigger — process current files, stop
bronze_query = (bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
    .trigger(availableNow=True)
    .toTable("workspace.aade.bridge_sensors_bronze")
)

bronze_query.awaitTermination()  # Wait for one-shot to finish

count = spark.sql("SELECT COUNT(*) AS n FROM workspace.aade.bridge_sensors_bronze").collect()[0]["n"]
print(f"✅ Bronze ingested: {count} events from batch 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 3 — Batch query πάνω στο Bronze (unified analytics)
# MAGIC
# MAGIC Παρόλο που γράφτηκε με streaming API, το table είναι κανονικό Delta. **Batch SQL** δουλεύει.

# COMMAND ----------

# MAGIC %sql
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
# MAGIC ### Watermark: γιατί το χρειαζόμαστε;
# MAGIC
# MAGIC Σε streaming, events μπορούν να φτάσουν **late** (network delay). Watermark λέει στο Spark «μετά από X λεπτά μην περιμένεις πια — κλείσε το window».
# MAGIC
# MAGIC ```python
# MAGIC .withWatermark("event_time", "2 minutes")
# MAGIC ```
# MAGIC Χωρίς αυτό: το Spark κρατάει **όλα** τα windows σε memory για πάντα → OOM.

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, current_timestamp, window, avg, count, to_timestamp
)

# Read Bronze ως stream + windowed aggregation
bronze_for_agg = (spark.readStream
    .format("delta")
    .table("workspace.aade.bridge_sensors_bronze")
    .withColumn("event_time", to_timestamp(col("event_time"))))

agg_stream = (bronze_for_agg
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

# Write με availableNow + outputMode("append") — μόνο windows που έχουν "κλείσει"
# (έχει περάσει το watermark)
# Note: για demo με μικρά data, χρησιμοποιούμε complete mode + memory sink
agg_query = (agg_stream.writeStream
    .format("memory")
    .queryName("live_aggregations")
    .outputMode("complete")
    .trigger(availableNow=True)
    .start()
)

agg_query.awaitTermination()
print(f"✅ Aggregation completed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Live view: aggregations ανά sensor × 1-min window
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
# MAGIC ## Βήμα 5 — Alert stream (high vibration)
# MAGIC
# MAGIC Επιχειρηματικός κανόνας: **vibration > 8 Hz → alert**. Σε production: foreachBatch → Slack / SMS / ServiceNow.

# COMMAND ----------

bronze_for_alerts = (spark.readStream
    .format("delta")
    .table("workspace.aade.bridge_sensors_bronze"))

alerts_stream = (bronze_for_alerts
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
    .trigger(availableNow=True)
    .toTable("workspace.aade.bridge_sensors_alerts")
)

alerts_query.awaitTermination()

alerts_count = spark.sql("SELECT COUNT(*) AS n FROM workspace.aade.bridge_sensors_alerts").collect()[0]["n"]
print(f"✅ Alerts processed: {alerts_count}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   alert_severity,
# MAGIC   sensor_id,
# MAGIC   ROUND(vibration_hz, 2) AS vibration_hz,
# MAGIC   alert_time
# MAGIC FROM workspace.aade.bridge_sensors_alerts
# MAGIC ORDER BY alert_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT alert_severity, COUNT(*) AS alert_count
# MAGIC FROM workspace.aade.bridge_sensors_alerts
# MAGIC GROUP BY alert_severity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6 — Generate batch 2 (incremental demo)
# MAGIC
# MAGIC Νέα events φτάνουν στο landing dir — π.χ. ένα νέο sensor batch κάθε ώρα σε production. Auto Loader **θα διαβάσει μόνο τα νέα files**, όχι ξανά τα παλιά.

# COMMAND ----------

path, count = generate_events(50, batch_id=2)
print(f"✅ Wrote {count} new events to {path}")

# Επιβεβαίωση: τώρα βλέπουμε 2 files στο landing
files = dbutils.fs.ls(LANDING)
print(f"\n📂 Files in landing: {len(files)}")
for f in files:
    print(f"   📄 {f.name}  ({f.size} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 7 — Re-run streams — Auto Loader picks up ONLY new files
# MAGIC
# MAGIC Εκτελούμε ξανά τα ίδια streams. Λόγω checkpoint, Auto Loader **παραλείπει** τα ήδη-διαβασμένα files και διαβάζει μόνο `batch_2.json`.

# COMMAND ----------

bronze_count_before = spark.sql("SELECT COUNT(*) AS n FROM workspace.aade.bridge_sensors_bronze").collect()[0]["n"]

# Re-run Bronze stream (ίδιο checkpoint → incremental)
bronze_stream_2 = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(LANDING))

(bronze_stream_2.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
    .trigger(availableNow=True)
    .toTable("workspace.aade.bridge_sensors_bronze")
).awaitTermination()

bronze_count_after = spark.sql("SELECT COUNT(*) AS n FROM workspace.aade.bridge_sensors_bronze").collect()[0]["n"]
new_records = bronze_count_after - bronze_count_before

print(f"📊 Bronze before run 2: {bronze_count_before}")
print(f"📊 Bronze after run 2:  {bronze_count_after}")
print(f"📊 New records ingested: {new_records}")
assert new_records == 50, f"Expected 50 new, got {new_records}"
print(f"\n✅ Auto Loader διάβασε ΜΟΝΟ τα νέα files (50, όχι 100). Incremental works.")

# COMMAND ----------

# Re-run alerts stream (ίδιο pattern)
bronze_for_alerts_2 = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "false")  # explicit, default
    .table("workspace.aade.bridge_sensors_bronze"))

(bronze_for_alerts_2
    .filter(col("vibration_hz") > 8.0)
    .withColumn("alert_severity",
        when(col("vibration_hz") > 10.0, lit("critical"))
        .otherwise(lit("warning")))
    .withColumn("alert_time", current_timestamp())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/alerts")
    .trigger(availableNow=True)
    .toTable("workspace.aade.bridge_sensors_alerts")
).awaitTermination()

alerts_total = spark.sql("SELECT COUNT(*) AS n FROM workspace.aade.bridge_sensors_alerts").collect()[0]["n"]
print(f"📊 Total alerts μετά τα 2 batches: {alerts_total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 8 — Cleanup
# MAGIC
# MAGIC Sanity check ότι δεν τρέχουν active streams. Με `availableNow`, τα streams self-terminate, αλλά καλό είναι να τσεκάρουμε.

# COMMAND ----------

active = spark.streams.active
print(f"Active streams: {len(active)}")
for q in active:
    print(f"   🛑 Stopping: {q.name} ({q.id})")
    q.stop()

print(f"\n✅ Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 9 — Production patterns
# MAGIC
# MAGIC ### Pattern 1: Continuous streaming (όχι σε Free Edition Serverless)
# MAGIC
# MAGIC Σε **dedicated cluster** (όχι Serverless), μπορείς να χρησιμοποιήσεις `processingTime` trigger για continuous mode:
# MAGIC ```python
# MAGIC .trigger(processingTime="10 seconds")  # Free Edition Serverless ΔΕΝ το υποστηρίζει
# MAGIC ```
# MAGIC
# MAGIC Free Edition limitation: μόνο `availableNow` (one-shot) ή `Once` (deprecated).
# MAGIC
# MAGIC ### Pattern 2: Scheduled `availableNow` jobs (production-friendly)
# MAGIC
# MAGIC Δημιούργησε Databricks Job που τρέχει το notebook **κάθε 5 min**. Κάθε run:
# MAGIC - Auto Loader βρίσκει νέα files
# MAGIC - Process-άρει με streaming API
# MAGIC - Self-terminates
# MAGIC - Επόμενο run: επανάληψη
# MAGIC
# MAGIC Πλεονέκτημα: cost = O(work), όχι O(continuous compute).
# MAGIC
# MAGIC ### Pattern 3: Kafka source
# MAGIC
# MAGIC ```python
# MAGIC stream = (spark.readStream
# MAGIC   .format("kafka")
# MAGIC   .option("kafka.bootstrap.servers", "broker:9092")
# MAGIC   .option("subscribe", "bridge_sensors")
# MAGIC   .option("startingOffsets", "earliest")
# MAGIC   .load())
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 4: foreachBatch (custom output)
# MAGIC
# MAGIC ```python
# MAGIC def send_alerts_to_slack(batch_df, batch_id):
# MAGIC     critical = batch_df.filter("alert_severity = 'critical'").collect()
# MAGIC     for row in critical:
# MAGIC         requests.post(SLACK_WEBHOOK, json={"text": f"🚨 Sensor {row.sensor_id}"})
# MAGIC
# MAGIC alerts_stream.writeStream.foreachBatch(send_alerts_to_slack).trigger(availableNow=True).start()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Wrap-up
# MAGIC
# MAGIC | Concept | Command |
# MAGIC |---|---|
# MAGIC | Auto Loader (cloud storage ingest) | `format("cloudFiles")` |
# MAGIC | One-shot streaming (Free Edition friendly) | `trigger(availableNow=True)` |
# MAGIC | Continuous streaming (dedicated clusters) | `trigger(processingTime="X seconds")` |
# MAGIC | Watermark (bound late events) | `withWatermark("col", "X minutes")` |
# MAGIC | Window aggregation | `groupBy(window("col", "1 minute"))` |
# MAGIC | Wait for one-shot completion | `query.awaitTermination()` |
# MAGIC
# MAGIC ### Key takeaway
# MAGIC
# MAGIC **`trigger(availableNow=True)` + scheduled job = batch-style cost με streaming-style code**.
# MAGIC
# MAGIC Σε real production:
# MAGIC - Files πέφτουν σε S3/ADLS
# MAGIC - Databricks Job τρέχει κάθε 5–60 min
# MAGIC - Auto Loader picks up incremental
# MAGIC - Bronze → Silver → Gold αλυσίδα γεμίζει αυτόματα
# MAGIC - Zero manual file tracking
# MAGIC
# MAGIC ### Κλειστή ερώτηση για τους trainees
# MAGIC
# MAGIC > "Πότε προτιμάς `availableNow` και πότε `processingTime`;"
# MAGIC
# MAGIC **Απάντηση**:
# MAGIC - **availableNow** = scheduled batch jobs (cost-effective, ιδανικό για >5 min latency)
# MAGIC - **processingTime** = continuous, sub-minute latency (ακριβότερο, dedicated cluster only)
