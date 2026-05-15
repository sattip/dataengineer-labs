# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Task 3: Gold Aggregations
# MAGIC
# MAGIC **Workflow Job Task** — depends_on task_2_silver
# MAGIC
# MAGIC - **Source**: 4 Silver Delta tables
# MAGIC - **Target**: 3 Gold analytical tables
# MAGIC - **Pattern**: GroupBy aggregations + JOINs + Window functions
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day6/AADE_Workflow_Job/03_gold_aggregations.py
# MAGIC > ```

# COMMAND ----------

import logging
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, count, sum as spark_sum,
    avg, max as spark_max, coalesce, round as F_round, date_format
)

for n in ("pyspark.sql.connect.client.core", "py4j", "grpc"):
    logging.getLogger(n).setLevel(logging.CRITICAL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 1 — Citizen 360 (per-AFM denormalized view)

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE workspace.aade.gold_citizen_360 AS
    WITH tax_agg AS (
        SELECT afm,
               COUNT(*) AS total_declarations,
               SUM(tax_amount) AS total_tax_paid,
               SUM(CASE WHEN status='APPROVED' THEN 1 ELSE 0 END) AS approved_count,
               SUM(CASE WHEN status='REJECTED' THEN 1 ELSE 0 END) AS rejected_count
        FROM workspace.aade.silver_tax_declarations_clean
        GROUP BY afm
    ),
    invoice_agg AS (
        SELECT issuer_afm AS afm,
               COUNT(*) AS invoices_issued,
               SUM(total_amount) AS total_invoiced
        FROM workspace.aade.silver_invoices_clean
        GROUP BY issuer_afm
    ),
    kep_agg AS (
        SELECT afm,
               COUNT(*) AS kep_events_count,
               AVG(duration_seconds) AS avg_kep_duration_sec
        FROM workspace.aade.silver_kep_events_clean
        GROUP BY afm
    ),
    efka_agg AS (
        SELECT afm,
               SUM(contribution_amount) AS total_efka_contributions,
               MAX(category) AS efka_category
        FROM workspace.aade.silver_efka_contributions_clean
        GROUP BY afm
    )
    SELECT
        COALESCE(t.afm, i.afm, k.afm, e.afm) AS afm,
        COALESCE(t.total_declarations, 0)    AS total_declarations,
        COALESCE(t.total_tax_paid, 0)        AS total_tax_paid,
        COALESCE(t.approved_count, 0)        AS approved_count,
        COALESCE(t.rejected_count, 0)        AS rejected_count,
        COALESCE(i.invoices_issued, 0)       AS invoices_issued,
        COALESCE(i.total_invoiced, 0)        AS total_invoiced,
        COALESCE(k.kep_events_count, 0)      AS kep_events_count,
        ROUND(COALESCE(k.avg_kep_duration_sec, 0), 1) AS avg_kep_duration_sec,
        COALESCE(e.total_efka_contributions, 0)       AS total_efka_contributions,
        COALESCE(e.efka_category, 'Unknown')          AS efka_category,
        current_timestamp() AS computed_at
    FROM tax_agg t
    FULL OUTER JOIN invoice_agg i ON t.afm = i.afm
    FULL OUTER JOIN kep_agg     k ON COALESCE(t.afm, i.afm) = k.afm
    FULL OUTER JOIN efka_agg    e ON COALESCE(t.afm, i.afm, k.afm) = e.afm
""")

print(f"✓ gold_citizen_360: {spark.table('workspace.aade.gold_citizen_360').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 2 — Daily KPIs

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE workspace.aade.gold_daily_kpis AS
    WITH tax_daily AS (
        SELECT date(submitted_at) AS day,
               COUNT(*) AS declarations,
               SUM(tax_amount) AS tax_collected
        FROM workspace.aade.silver_tax_declarations_clean
        GROUP BY date(submitted_at)
    ),
    kep_daily AS (
        SELECT date(event_ts) AS day,
               COUNT(*) AS kep_events
        FROM workspace.aade.silver_kep_events_clean
        GROUP BY date(event_ts)
    )
    SELECT
        COALESCE(t.day, k.day) AS day,
        COALESCE(t.declarations, 0)   AS declarations,
        COALESCE(t.tax_collected, 0)  AS tax_collected,
        COALESCE(k.kep_events, 0)     AS kep_events,
        current_timestamp() AS computed_at
    FROM tax_daily t
    FULL OUTER JOIN kep_daily k ON t.day = k.day
    ORDER BY day DESC
""")

print(f"✓ gold_daily_kpis: {spark.table('workspace.aade.gold_daily_kpis').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 3 — Pipeline Health (audit trail)

# COMMAND ----------

audit_rows = []
for src, silver_table in [
    ("taxis",  "silver_tax_declarations_clean"),
    ("mydata", "silver_invoices_clean"),
    ("kep",    "silver_kep_events_clean"),
    ("efka",   "silver_efka_contributions_clean"),
]:
    bronze_cnt = spark.table(f"workspace.aade.bronze_{src}").count()
    silver_cnt = spark.table(f"workspace.aade.{silver_table}").count()
    audit_rows.append({
        "source": src,
        "bronze_count": bronze_cnt,
        "silver_count": silver_cnt,
        "drop_rate_pct": round((bronze_cnt - silver_cnt) / max(bronze_cnt, 1) * 100, 2),
        "computed_at": __import__("datetime").datetime.utcnow(),
    })

import pandas as pd
spark.createDataFrame(pd.DataFrame(audit_rows)).write.format("delta") \
    .mode("append").saveAsTable("workspace.aade.gold_pipeline_health")

print(f"✓ gold_pipeline_health: appended {len(audit_rows)} rows")

# COMMAND ----------

# Final summary
summary = {
    "gold_citizen_360":      spark.table("workspace.aade.gold_citizen_360").count(),
    "gold_daily_kpis":       spark.table("workspace.aade.gold_daily_kpis").count(),
    "gold_pipeline_health":  spark.table("workspace.aade.gold_pipeline_health").count(),
}
print(f"\n=== Gold task complete ===\n{summary}")

dbutils.notebook.exit(f"OK: gold tables refreshed ({summary})")
