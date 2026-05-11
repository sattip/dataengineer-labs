# Databricks notebook source
# MAGIC %md
# MAGIC # 🅱️ Scenario B — flaky_pipeline (Ημέρα 5)
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC
# MAGIC > **GitHub** (share this στο Zoom chat):
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day5/flaky_pipeline.py
# MAGIC > ```
# MAGIC
# MAGIC **Companion οδηγός**: `Day5/labs_walkthrough/Scenario_B_Job_Notifications.md`
# MAGIC **Catalog/Schema**: `aade_lab.public`
# MAGIC
# MAGIC ## 🎯 Τι κάνει αυτό το notebook
# MAGIC
# MAGIC Realistic 4-stage ΑΑΔΕ-ish pipeline που:
# MAGIC
# MAGIC 1. **Stage 1 — Ingest**: γεννά synthetic φορολογικές δηλώσεις (Bronze)
# MAGIC 2. **Stage 2 — DQ checks**: validation rules, μπορεί να αποτύχει εδώ
# MAGIC 3. **Stage 3 — Silver**: clean + enrich
# MAGIC 4. **Stage 4 — Gold**: KPI aggregations για dashboard / SQL Alerts
# MAGIC
# MAGIC **Failure rate** ελέγχεται από widget (default 30%). Σε success run, παράγει real metrics που το SQL Alert του Βήματος 6 παρακολουθεί.
# MAGIC
# MAGIC ## 🚨 Production patterns που διδάσκει
# MAGIC
# MAGIC | # | Pattern | Cell |
# MAGIC |---|---------|------|
# MAGIC | 1 | **Log ΠΡΙΝ raise** — sacred audit rule | 3, 5 |
# MAGIC | 2 | **Structured logging** ανά stage (rows, duration, severity) | 3 |
# MAGIC | 3 | **Configurable failure** για demos (widgets) | 1 |
# MAGIC | 4 | **Multiple failure modes** — DQ vs schema vs transient | 5 |
# MAGIC | 5 | **Idempotent setup** — re-runnable χωρίς cleanup | 2 |
# MAGIC | 6 | **Success metrics → audit table** — τροφοδοτεί SQL Alerts | 8 |
# MAGIC
# MAGIC ## 📋 Πώς το χρησιμοποιείς
# MAGIC
# MAGIC 1. **Import** στο Workspace ως `flaky_pipeline`
# MAGIC 2. Δημιούργησε **Job** που τρέχει αυτό (βλ. Scenario_B Βήμα 3)
# MAGIC 3. Email notification on failure + retry policy (Βήματα 4-5)
# MAGIC 4. **Run πολλές φορές** — μερικά τρέχουν success, μερικά failure
# MAGIC 5. SQL Alert (Βήμα 6) παρακολουθεί `pipeline_runs.status = 'FAILED'`
# MAGIC
# MAGIC ## 🎬 Πλήρες demo recipe (~30 min)
# MAGIC
# MAGIC Για ολοκληρωμένο live demo που δείχνει **ΚΑΙ success ΚΑΙ failure** scenarios, τρέξε αυτή τη σειρά runs (από το Job → **Run now with different parameters** σε κάθε βήμα):
# MAGIC
# MAGIC | # | force_mode | failure_rate | Τι θα δουν οι εκπαιδευόμενοι | Διάρκεια |
# MAGIC |---|------------|--------------|------------------------------|----------|
# MAGIC | 1 | `success` | (any) | **Καθαρό success run** — όλα τα 4 stages πράσινα, Gold KPIs γράφτηκαν, no email sent | 3' |
# MAGIC | 2 | `fail_dq` | (any) | **DQ failure** — Bronze ✅, DQ ❌, Silver/Gold SKIPPED, email arrives | 3' |
# MAGIC | 3 | `fail_schema` | (any) | **Silver failure** (schema drift) — DQ ✅, Silver ❌, retry policy kicks in | 5' |
# MAGIC | 4 | `fail_transient` | (any) | **Transient Gold error** — όλα τα tabs πράσινα μέχρι το Gold, retry policy το διορθώνει στο 2ο attempt | 5' |
# MAGIC | 5 | `auto` | `0.3` | **Realistic mixed** — τρέξε 3-4 φορές, δες mix success/fail στο audit table | 5' |
# MAGIC | 6 | — | — | **SQL Editor**: query το `pipeline_runs`, δες full audit trail | 4' |
# MAGIC | 7 | — | — | **SQL Alert**: δες το alert να γίνεται triggered + email arrives | 5' |
# MAGIC
# MAGIC **Σύνολο**: ~30 λεπτά για demo που καλύπτει success path, multiple failure modes, retries, audit logging, και SQL Alerts.
# MAGIC
# MAGIC > 💡 **Όλα τα tables (`pipeline_runs`, `daily_kpis`) είναι additive** — κάθε run προσθέτει rows. Στο τέλος του demo θα έχεις rich audit table για να δείξεις πραγματικό production-grade observability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Configurable widgets (controlled demos)
# MAGIC
# MAGIC Με widgets μπορούμε στο Job UI να ορίσουμε:
# MAGIC - `failure_rate` — 0.0 (πάντα success), 0.3 (default), 1.0 (πάντα fail)
# MAGIC - `force_mode` — `'auto'`, `'success'`, `'fail_dq'`, `'fail_schema'`, `'fail_transient'`
# MAGIC
# MAGIC Έτσι ο instructor κάνει predictable demo: «τώρα θα δούμε success... τώρα forced failure στο DQ stage».

# COMMAND ----------

dbutils.widgets.text("failure_rate", "0.30", "Failure rate (0.0-1.0)")
dbutils.widgets.dropdown("force_mode", "auto",
    ["auto", "success", "fail_dq", "fail_schema", "fail_transient"], "Force mode")

FAILURE_RATE = float(dbutils.widgets.get("failure_rate"))
FORCE_MODE = dbutils.widgets.get("force_mode")
print(f"⚙️  Configuration: failure_rate={FAILURE_RATE}, force_mode={FORCE_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Setup catalog, schema, audit table
# MAGIC
# MAGIC Richer schema από το original — `stage`, `duration_ms`, `rows_processed`, `severity` — ώστε να μπορούμε να φτιάχνουμε **per-stage metrics** στο audit και να συνδέονται με SQL Alerts.

# COMMAND ----------

import random
import time
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit, col, when, expr, rand, sum as spark_sum, count

spark.sql("CREATE CATALOG IF NOT EXISTS aade_lab")
spark.sql("CREATE SCHEMA IF NOT EXISTS aade_lab.public")

# Audit table — per-stage events
spark.sql("""
  CREATE TABLE IF NOT EXISTS aade_lab.public.pipeline_runs (
    run_id          STRING,
    stage           STRING,
    status          STRING,    -- STARTED / SUCCESS / FAILED / SKIPPED
    severity        STRING,    -- INFO / WARN / ERROR
    rows_processed BIGINT,
    duration_ms    BIGINT,
    error_msg       STRING,
    run_ts          TIMESTAMP
  ) USING DELTA
""")

# Gold KPI table — output του success path, μετράται από SQL Alerts
spark.sql("""
  CREATE TABLE IF NOT EXISTS aade_lab.public.daily_kpis (
    run_id              STRING,
    run_date            DATE,
    total_declarations  BIGINT,
    total_amount_eur    DOUBLE,
    avg_amount_eur      DOUBLE,
    rejected_count      BIGINT,
    approved_count      BIGINT,
    pending_count       BIGINT,
    written_ts          TIMESTAMP
  ) USING DELTA
""")

print("✅ Setup complete — aade_lab.public.{pipeline_runs, daily_kpis} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Helper: structured logging + failure decision
# MAGIC
# MAGIC Ένας μικρός `log_event()` helper εξασφαλίζει ότι **κάθε** stage event καταγράφεται στο audit table με συνέπεια. Το `should_fail_at(stage)` αποφασίζει αν το συγκεκριμένο stage πρέπει να αποτύχει βάσει του widget config.

# COMMAND ----------

RUN_ID = f"run_{int(datetime.now().timestamp())}"
print(f"🆔 RUN_ID: {RUN_ID}")

def log_event(stage, status, severity="INFO", rows=None, duration_ms=None, error_msg=None):
    """Atomic insert στο pipeline_runs — sacred audit rule."""
    rows_val = rows if rows is not None else "NULL"
    duration_val = duration_ms if duration_ms is not None else "NULL"
    error_val = f"'{error_msg.replace(chr(39), chr(39)+chr(39))}'" if error_msg else "NULL"
    spark.sql(f"""
      INSERT INTO aade_lab.public.pipeline_runs
      VALUES ('{RUN_ID}', '{stage}', '{status}', '{severity}',
              {rows_val}, {duration_val}, {error_val}, current_timestamp())
    """)
    icon = {"SUCCESS": "✅", "FAILED": "❌", "STARTED": "▶️", "SKIPPED": "⏭️"}.get(status, "•")
    print(f"  {icon} [{stage:8}] {status:8} {f'rows={rows}' if rows else ''} {f'{duration_ms}ms' if duration_ms else ''} {error_msg or ''}")

def should_fail_at(stage):
    """Decide if this stage should fail. Forced mode overrides random."""
    mode_to_stage = {
        "fail_dq":        "DQ",
        "fail_schema":    "SILVER",
        "fail_transient": "GOLD",
    }
    if FORCE_MODE == "success":
        return False
    if FORCE_MODE in mode_to_stage:
        return mode_to_stage[FORCE_MODE] == stage
    # auto mode: random per-stage with decreasing probability
    return random.random() < FAILURE_RATE * 0.4   # 40% of overall failure rate per stage

log_event("PIPELINE", "STARTED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — Stage 1: Ingest (Bronze)
# MAGIC
# MAGIC Γεννάμε 10.000 synthetic φορολογικές δηλώσεις. Αυτό το stage **σπάνια** αποτυγχάνει — είναι local synthetic data.

# COMMAND ----------

t0 = time.time()
try:
    bronze = (
        spark.range(10_000).withColumnRenamed("id", "declaration_id")
        .withColumn("afm", (lit(100_000_000) + (col("declaration_id") % 900_000_000)).cast("string"))
        .withColumn("doy_code", (col("declaration_id") % 50 + 1).cast("string"))
        .withColumn("tax_amount", (rand(seed=42) * 20000 + 100).cast("double"))
        .withColumn("status_raw", expr("element_at(array('Εγκεκριμένη','Απορριφθείσα','Εκκρεμής','UNKNOWN'), CAST(declaration_id % 4 + 1 AS INT))"))
        .withColumn("ingested_at", current_timestamp())
    )
    n_bronze = bronze.count()
    bronze.write.mode("overwrite").saveAsTable("aade_lab.public.bronze_declarations")
    log_event("BRONZE", "SUCCESS", rows=n_bronze, duration_ms=int((time.time()-t0)*1000))
except Exception as e:
    log_event("BRONZE", "FAILED", severity="ERROR",
              duration_ms=int((time.time()-t0)*1000), error_msg=str(e)[:200])
    log_event("PIPELINE", "FAILED", severity="ERROR", error_msg=f"Bronze stage failed: {str(e)[:100]}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — Stage 2: DQ Checks
# MAGIC
# MAGIC Πρώτο stage που μπορεί να αποτύχει «ρεαλιστικά». Έλεγχοι:
# MAGIC - `afm` length = 9
# MAGIC - `tax_amount > 0`
# MAGIC - `status_raw IN valid_set`
# MAGIC
# MAGIC Αν `force_mode = fail_dq` ή random hits → raise + log.

# COMMAND ----------

t0 = time.time()
try:
    if should_fail_at("DQ"):
        # Simulate DQ failure — sometimes 1% rule breaks → reject batch
        raise Exception("DQ check failed: 12% rows with invalid AFM (threshold 1%)")

    bronze = spark.table("aade_lab.public.bronze_declarations")
    bad_afm   = bronze.filter("length(afm) != 9").count()
    bad_amt   = bronze.filter("tax_amount <= 0").count()
    bad_stat  = bronze.filter("status_raw NOT IN ('Εγκεκριμένη','Απορριφθείσα','Εκκρεμής','UNKNOWN')").count()
    total = bronze.count()
    bad_total = bad_afm + bad_amt + bad_stat
    bad_pct = (bad_total / total) * 100 if total else 0

    if bad_pct > 5:
        raise Exception(f"DQ check failed: {bad_pct:.1f}% bad rows (threshold 5%) — afm={bad_afm}, amt={bad_amt}, stat={bad_stat}")

    severity = "WARN" if bad_pct > 1 else "INFO"
    log_event("DQ", "SUCCESS", severity=severity,
              rows=total, duration_ms=int((time.time()-t0)*1000),
              error_msg=f"bad_pct={bad_pct:.2f}%" if bad_pct > 0 else None)

except Exception as e:
    log_event("DQ", "FAILED", severity="ERROR",
              duration_ms=int((time.time()-t0)*1000), error_msg=str(e)[:200])
    log_event("PIPELINE", "FAILED", severity="ERROR", error_msg=f"DQ failed: {str(e)[:100]}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 — Stage 3: Silver (clean + normalize)
# MAGIC
# MAGIC Καθαρή έκδοση: filter bad rows, normalize status. Σπάνια failure mode: schema drift.

# COMMAND ----------

t0 = time.time()
try:
    if should_fail_at("SILVER"):
        raise Exception("SchemaEvolutionError: column 'tax_amount' type changed from DOUBLE to DECIMAL(10,2)")

    silver = (
        spark.table("aade_lab.public.bronze_declarations")
        .filter("length(afm) = 9 AND tax_amount > 0")
        .withColumn("status",
            when(col("status_raw").isin("Εγκεκριμένη", "Απορριφθείσα", "Εκκρεμής"), col("status_raw"))
            .otherwise(lit("UNKNOWN")))
        .drop("status_raw")
    )
    n_silver = silver.count()
    silver.write.mode("overwrite").saveAsTable("aade_lab.public.silver_declarations")
    log_event("SILVER", "SUCCESS", rows=n_silver, duration_ms=int((time.time()-t0)*1000))

except Exception as e:
    log_event("SILVER", "FAILED", severity="ERROR",
              duration_ms=int((time.time()-t0)*1000), error_msg=str(e)[:200])
    log_event("PIPELINE", "FAILED", severity="ERROR", error_msg=f"Silver failed: {str(e)[:100]}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7 — Stage 4: Gold (KPI aggregation)
# MAGIC
# MAGIC Παράγει daily KPIs που τροφοδοτούν dashboards και SQL Alerts. Πιθανή failure mode: transient compute error (timeout, network).

# COMMAND ----------

t0 = time.time()
try:
    if should_fail_at("GOLD"):
        raise Exception("TransientError: serverless warehouse timeout after 60s")

    silver = spark.table("aade_lab.public.silver_declarations")
    kpis = silver.agg(
        count("*").alias("total_declarations"),
        spark_sum("tax_amount").alias("total_amount_eur"),
        expr("avg(tax_amount)").alias("avg_amount_eur"),
        spark_sum(when(col("status") == "Απορριφθείσα", 1).otherwise(0)).alias("rejected_count"),
        spark_sum(when(col("status") == "Εγκεκριμένη", 1).otherwise(0)).alias("approved_count"),
        spark_sum(when(col("status") == "Εκκρεμής", 1).otherwise(0)).alias("pending_count"),
    ).collect()[0]

    spark.sql(f"""
      INSERT INTO aade_lab.public.daily_kpis VALUES (
        '{RUN_ID}', current_date(),
        {kpis['total_declarations']},
        {kpis['total_amount_eur']},
        {kpis['avg_amount_eur']},
        {kpis['rejected_count']},
        {kpis['approved_count']},
        {kpis['pending_count']},
        current_timestamp()
      )
    """)

    log_event("GOLD", "SUCCESS",
              rows=kpis['total_declarations'],
              duration_ms=int((time.time()-t0)*1000),
              error_msg=f"total_eur={kpis['total_amount_eur']:.2f}, avg={kpis['avg_amount_eur']:.2f}")

except Exception as e:
    log_event("GOLD", "FAILED", severity="ERROR",
              duration_ms=int((time.time()-t0)*1000), error_msg=str(e)[:200])
    log_event("PIPELINE", "FAILED", severity="ERROR", error_msg=f"Gold failed: {str(e)[:100]}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8 — Pipeline SUCCESS — log final event

# COMMAND ----------

log_event("PIPELINE", "SUCCESS", rows=kpis['total_declarations'])
print(f"\n🎉 Pipeline {RUN_ID} completed successfully")
print(f"   Total declarations: {kpis['total_declarations']:,}")
print(f"   Total amount:       {kpis['total_amount_eur']:,.2f} EUR")
print(f"   Approved/Rejected/Pending: {kpis['approved_count']:,} / {kpis['rejected_count']:,} / {kpis['pending_count']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9 — Verify — audit log & today's KPIs

# COMMAND ----------

print(">>> This run — all stages")
display(spark.sql(f"""
  SELECT stage, status, severity, rows_processed, duration_ms, error_msg, run_ts
  FROM aade_lab.public.pipeline_runs
  WHERE run_id = '{RUN_ID}'
  ORDER BY run_ts
"""))

# COMMAND ----------

print(">>> Last 10 PIPELINE-level results (what the SQL Alert monitors)")
display(spark.sql("""
  SELECT run_id, status, severity, rows_processed, error_msg, run_ts
  FROM aade_lab.public.pipeline_runs
  WHERE stage = 'PIPELINE' AND status IN ('SUCCESS', 'FAILED')
  ORDER BY run_ts DESC
  LIMIT 10
"""))

# COMMAND ----------

print(">>> Today's Gold KPIs (από τα success runs)")
display(spark.sql("""
  SELECT run_id, total_declarations, total_amount_eur, avg_amount_eur,
         approved_count, rejected_count, pending_count, written_ts
  FROM aade_lab.public.daily_kpis
  WHERE run_date = current_date()
  ORDER BY written_ts DESC
  LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔔 SQL Alert query (για το Βήμα 6 του Σεναρίου Β)
# MAGIC
# MAGIC Στο Databricks SQL Editor, φτιάξε **Query**:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(*) AS failures_last_hour
# MAGIC FROM aade_lab.public.pipeline_runs
# MAGIC WHERE stage = 'PIPELINE'
# MAGIC   AND status = 'FAILED'
# MAGIC   AND run_ts >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC ```
# MAGIC
# MAGIC Μετά **Alert** πάνω σε αυτή τη query: trigger όταν `failures_last_hour > 2`.
# MAGIC
# MAGIC ## 🧹 Cleanup (optional)
# MAGIC
# MAGIC ```python
# MAGIC # spark.sql("DROP TABLE IF EXISTS aade_lab.public.bronze_declarations")
# MAGIC # spark.sql("DROP TABLE IF EXISTS aade_lab.public.silver_declarations")
# MAGIC # spark.sql("DROP TABLE IF EXISTS aade_lab.public.daily_kpis")
# MAGIC # spark.sql("DROP TABLE IF EXISTS aade_lab.public.pipeline_runs")
# MAGIC ```
