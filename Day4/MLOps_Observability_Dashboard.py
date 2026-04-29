# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 MLOps Observability Dashboard — Mock Data Generator + Dashboard Queries
# MAGIC
# MAGIC **Ρόλος:** MLOps Engineer / SRE στην ΑΑΔΕ
# MAGIC **Διάρκεια:** ~20'
# MAGIC **Περιβάλλον:** Databricks Free Edition (Serverless) με Unity Catalog
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4/MLOps_Observability_Dashboard.py
# MAGIC > ```
# MAGIC
# MAGIC ## 🎯 Στόχος
# MAGIC > Παράγουμε **mock data 90 ημερών** για το `aade_risk_scorer` μοντέλο και χτίζουμε
# MAGIC > observability dashboard που ένας MLOps engineer θα έβλεπε καθημερινά για να
# MAGIC > παρακολουθεί την υγεία του production μοντέλου.
# MAGIC
# MAGIC ## 📚 Τι είναι «observability»;
# MAGIC > Σε αντίθεση με το απλό **monitoring** (παρακολουθώ συγκεκριμένες μετρικές),
# MAGIC > το **observability** σου επιτρέπει να **εξηγήσεις γιατί** συμβαίνει κάτι:
# MAGIC > - **Logs** — τι έγινε
# MAGIC > - **Metrics** — πόσες φορές, πόσο γρήγορα
# MAGIC > - **Traces** — ποια request πέρασαν από πού
# MAGIC >
# MAGIC > Σε MLOps context προσθέτουμε:
# MAGIC > - **Predictions** — τι έβγαλε το μοντέλο
# MAGIC > - **Drift signals** — αλλάζει η πραγματικότητα;
# MAGIC > - **Quality metrics** — πόσο σωστό είναι;
# MAGIC > - **Audit trail** — ποιος, πότε, σε ποια data
# MAGIC
# MAGIC ## 📊 Τα 6 panels του dashboard
# MAGIC | # | Panel | Τι δείχνει | Source table |
# MAGIC |---|---|---|---|
# MAGIC | 1 | **Daily Volume** | Πόσες predictions ανά ημέρα | `mock_predictions_timeseries` |
# MAGIC | 2 | **Score Distribution** | Histogram risk scores | `mock_predictions_timeseries` |
# MAGIC | 3 | **Drift PSI Trend** | PSI ανά feature ανά εβδομάδα | `mock_drift_history` |
# MAGIC | 4 | **Model Performance** | AUC/Precision/Recall πάροδος | `mock_performance_history` |
# MAGIC | 5 | **Endpoint Latency** | p50/p95/p99 latency | `mock_endpoint_metrics` |
# MAGIC | 6 | **Alerts & Incidents** | Active alerts, severity | `mock_alerts` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 0: Setup

# COMMAND ----------

import os
import logging

# Σιγάζουμε noisy GRPC warnings σε Spark Connect
logging.getLogger("pyspark.sql.connect.client.core").setLevel(logging.CRITICAL)
logging.getLogger("py4j").setLevel(logging.CRITICAL)
logging.getLogger("grpc").setLevel(logging.CRITICAL)

# UC setup (idempotent)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

print("✓ Schema & Volume έτοιμα")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 1: Mock Predictions Timeseries (90 ημέρες)
# MAGIC
# MAGIC ### 📚 Τι generουμε
# MAGIC Για κάθε ημέρα τις τελευταίες **90 ημέρες**:
# MAGIC - **Total predictions** — πόσες δηλώσεις σκοραρίστηκαν
# MAGIC - **High-risk count** — πόσες ξεπέρασαν το 0.8 threshold
# MAGIC - **Avg/median risk score** — κεντρική τάση
# MAGIC - **p95/p99 risk score** — outliers
# MAGIC
# MAGIC ### 🎯 Ρεαλιστικό σενάριο
# MAGIC - **Σαββατοκύριακα**: λιγότερες δηλώσεις (χαμηλότερη κίνηση TAXIS)
# MAGIC - **Τέλος μήνα/τριμήνου**: spike (deadline υποβολής)
# MAGIC - **Drift incident** στις τελευταίες 7 ημέρες (αύξηση high-risk)

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)

# 90 ημέρες ιστορικότητας
end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
days = [end_date - timedelta(days=i) for i in range(90, 0, -1)]

predictions = []
for i, day in enumerate(days):
    # Base traffic: 12k/day
    base = 12000
    # Σαββατοκύριακο effect: 40% drop
    weekday = day.weekday()
    if weekday >= 5:
        base *= 0.55
    # End-of-month spike (last 3 days of month)
    if day.day >= 28:
        base *= 1.6
    # Last 7 days drift incident: +25% high-risk
    drift_active = i >= 83  # τελευταίες 7 ημέρες
    high_risk_pct = 0.06 if not drift_active else 0.085

    total = int(base * np.random.uniform(0.92, 1.08))
    high_risk = int(total * high_risk_pct * np.random.uniform(0.9, 1.1))
    avg_score = float(np.random.uniform(0.32, 0.38))
    if drift_active:
        avg_score += 0.04
    median_score = avg_score - 0.05
    p95_score = float(np.random.uniform(0.78, 0.85))
    p99_score = float(np.random.uniform(0.91, 0.96))

    predictions.append({
        "date": day.date(),
        "total_predictions": total,
        "high_risk_count": high_risk,
        "high_risk_pct": round(high_risk / total * 100, 2),
        "avg_risk_score": round(avg_score, 4),
        "median_risk_score": round(median_score, 4),
        "p95_risk_score": round(p95_score, 4),
        "p99_risk_score": round(p99_score, 4),
        "model_version": "1" if i < 60 else "2",  # version bump στις 60
    })

pdf_pred = pd.DataFrame(predictions)
df_pred = spark.createDataFrame(pdf_pred)
df_pred.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mock_predictions_timeseries")

print(f"✓ Saved {len(predictions)} ημέρες σε workspace.aade.mock_predictions_timeseries")
print("\n=== Preview (τελευταίες 7 ημέρες) ===")
df_pred.orderBy("date", ascending=False).limit(7).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 2: Mock Drift History (per feature, weekly)
# MAGIC
# MAGIC PSI για 6 features ανά εβδομάδα. Στόχος: να φαίνεται καθαρά πότε ξεκινά drift
# MAGIC και σε ποιο feature.

# COMMAND ----------

features_list = ["income", "expenses", "tax_rate", "expense_ratio",
                 "declaration_count", "income_change_yoy"]

drift_history = []
weeks = 13  # 90 ημέρες ≈ 13 εβδομάδες
week_dates = [end_date - timedelta(weeks=w) for w in range(weeks, 0, -1)]

for w_idx, week_start in enumerate(week_dates):
    for feat in features_list:
        # Baseline noise
        psi = np.random.uniform(0.02, 0.08)
        ks_stat = np.random.uniform(0.03, 0.09)
        ks_pval = np.random.uniform(0.15, 0.85)

        # Drift escalation στις τελευταίες 4 εβδομάδες για income
        if feat == "income" and w_idx >= 9:
            psi = 0.08 + (w_idx - 9) * 0.07  # 0.08 → 0.15 → 0.22 → 0.29
            ks_stat = 0.10 + (w_idx - 9) * 0.05
            ks_pval = max(0.001, 0.2 - (w_idx - 9) * 0.06)

        # tax_rate drift τις 2 τελευταίες εβδομάδες
        if feat == "tax_rate" and w_idx >= 11:
            psi = 0.12 + (w_idx - 11) * 0.04
            ks_pval = 0.03

        if psi < 0.1:
            severity = "ok"
        elif psi < 0.2:
            severity = "watch"
        else:
            severity = "retrain"

        drift_history.append({
            "week_start": week_start.date(),
            "feature": feat,
            "psi": round(psi, 4),
            "ks_statistic": round(ks_stat, 4),
            "ks_pvalue": round(ks_pval, 4),
            "severity": severity,
            "model_version": "2" if w_idx >= 9 else "1",
        })

pdf_drift = pd.DataFrame(drift_history)
df_drift = spark.createDataFrame(pdf_drift)
df_drift.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mock_drift_history")

print(f"✓ Saved {len(drift_history)} drift records ({weeks} weeks × {len(features_list)} features)")
print("\n=== Latest week drift signals ===")
spark.sql("""
    SELECT feature, psi, ks_pvalue, severity
    FROM workspace.aade.mock_drift_history
    WHERE week_start = (SELECT MAX(week_start) FROM workspace.aade.mock_drift_history)
    ORDER BY psi DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 3: Mock Performance History (weekly AUC/Precision/Recall)
# MAGIC
# MAGIC Όταν έχουμε **labels** (post-audit confirmation), μετράμε πραγματικό performance.
# MAGIC Μετά από `30+ ημέρες` lag: ξέρουμε ποια από τις predictions ήταν σωστή.

# COMMAND ----------

perf_history = []
for w_idx, week_start in enumerate(week_dates):
    # Baseline: AUC ~0.85
    base_auc = 0.85
    # Performance degradation στις τελευταίες 3 εβδομάδες λόγω drift
    if w_idx >= 10:
        base_auc -= (w_idx - 9) * 0.015

    auc = round(base_auc + np.random.uniform(-0.012, 0.012), 4)
    precision = round(0.78 + np.random.uniform(-0.04, 0.04) - max(0, (w_idx - 9) * 0.01), 4)
    recall = round(0.71 + np.random.uniform(-0.04, 0.04) - max(0, (w_idx - 9) * 0.012), 4)
    f1 = round(2 * precision * recall / (precision + recall), 4)

    # Confusion matrix counts (mock totals)
    weekly_total = 80000 + int(np.random.uniform(-8000, 8000))
    actual_positives = int(weekly_total * 0.07)
    tp = int(actual_positives * recall)
    fn = actual_positives - tp
    fp = int(tp * (1 - precision) / max(precision, 0.01))
    tn = weekly_total - tp - fn - fp

    perf_history.append({
        "week_start": week_start.date(),
        "auc": auc,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "true_positives": tp,
        "false_positives": fp,
        "true_negatives": tn,
        "false_negatives": fn,
        "total_evaluated": weekly_total,
        "model_version": "2" if w_idx >= 9 else "1",
    })

pdf_perf = pd.DataFrame(perf_history)
df_perf = spark.createDataFrame(pdf_perf)
df_perf.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mock_performance_history")

print(f"✓ Saved {len(perf_history)} weekly performance snapshots")
print("\n=== Performance trend (last 5 weeks) ===")
spark.sql("""
    SELECT week_start, auc, precision, recall, f1_score
    FROM workspace.aade.mock_performance_history
    ORDER BY week_start DESC
    LIMIT 5
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 4: Mock Endpoint Metrics (hourly latency/throughput/errors)
# MAGIC
# MAGIC ### 📚 Τι είναι «p50, p95, p99 latency»;
# MAGIC > **Percentiles** της κατανομής χρόνου απόκρισης:
# MAGIC > - **p50** (median): «οι μισές requests είναι πιο γρήγορες από αυτό»
# MAGIC > - **p95**: «μόνο 5% requests είναι πιο αργές»
# MAGIC > - **p99**: «μόνο 1% requests είναι πιο αργές» — τα **worst cases**
# MAGIC >
# MAGIC > Στο SLA συνήθως δεσμευόμαστε για **p95 < X ms** (όχι average — οι averages
# MAGIC > κρύβουν spikes).

# COMMAND ----------

endpoint_metrics = []
# 7 τελευταίες ημέρες × 24 ώρες = 168 hourly snapshots
for h in range(168, 0, -1):
    ts = end_date - timedelta(hours=h)
    hour = ts.hour

    # Traffic patterns: peak στις 10-12 και 17-19 (TAXIS submissions)
    if 10 <= hour <= 12 or 17 <= hour <= 19:
        rps_base = 45
    elif 8 <= hour <= 22:
        rps_base = 25
    else:  # night
        rps_base = 5

    rps = max(0, rps_base + int(np.random.normal(0, 5)))
    requests = rps * 3600

    # Latency: usually 80-120ms, p99 ~250ms
    p50 = round(np.random.uniform(75, 120), 1)
    p95 = round(p50 + np.random.uniform(60, 100), 1)
    p99 = round(p95 + np.random.uniform(80, 150), 1)

    # Inject incident: latency spike στην προ-τελευταία ημέρα 14:00-16:00
    incident = (h >= 32 and h <= 34)
    if incident:
        p50 *= 3
        p95 *= 4
        p99 *= 5

    error_rate = round(np.random.uniform(0.01, 0.08), 4)
    if incident:
        error_rate = round(np.random.uniform(2.5, 4.0), 4)
    errors = int(requests * error_rate / 100)

    endpoint_metrics.append({
        "ts": ts,
        "requests_per_hour": requests,
        "rps": rps,
        "latency_p50_ms": p50,
        "latency_p95_ms": p95,
        "latency_p99_ms": p99,
        "errors": errors,
        "error_rate_pct": error_rate,
        "endpoint": "aade-risk-scorer-prod",
    })

pdf_ep = pd.DataFrame(endpoint_metrics)
df_ep = spark.createDataFrame(pdf_ep)
df_ep.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mock_endpoint_metrics")

print(f"✓ Saved {len(endpoint_metrics)} hourly endpoint metrics")
print("\n=== Latency percentiles για τις τελευταίες 6 ώρες ===")
spark.sql("""
    SELECT ts, rps, latency_p50_ms, latency_p95_ms, latency_p99_ms, error_rate_pct
    FROM workspace.aade.mock_endpoint_metrics
    ORDER BY ts DESC
    LIMIT 6
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 5: Mock Alerts & Incidents
# MAGIC
# MAGIC ### 📚 Τι είναι «alert» vs «incident»;
# MAGIC > - **Alert** = αυτόματη ειδοποίηση όταν μετρική σπάει threshold (π.χ. PSI > 0.2)
# MAGIC > - **Incident** = ομαδοποίηση συσχετιζόμενων alerts σε **μία περίπτωση** που
# MAGIC >   χρειάζεται response (PagerDuty, runbook, post-mortem)
# MAGIC >
# MAGIC > Σχέση: **πολλά alerts → 1 incident** (deduplication).

# COMMAND ----------

alerts = [
    {"alert_id": "ALT-1001", "fired_at": end_date - timedelta(days=1, hours=2),
     "severity": "critical", "metric": "drift_psi_income",
     "value": 0.29, "threshold": 0.2, "status": "open",
     "owner": "ml_team", "incident_id": "INC-205"},

    {"alert_id": "ALT-1002", "fired_at": end_date - timedelta(days=1, hours=2, minutes=15),
     "severity": "warning", "metric": "model_auc_drop",
     "value": 0.79, "threshold": 0.82, "status": "open",
     "owner": "ml_team", "incident_id": "INC-205"},

    {"alert_id": "ALT-1003", "fired_at": end_date - timedelta(days=1, hours=14),
     "severity": "critical", "metric": "endpoint_p99_latency",
     "value": 1240.0, "threshold": 500.0, "status": "resolved",
     "owner": "platform_team", "incident_id": "INC-204"},

    {"alert_id": "ALT-1004", "fired_at": end_date - timedelta(days=1, hours=14, minutes=5),
     "severity": "critical", "metric": "endpoint_error_rate",
     "value": 3.4, "threshold": 1.0, "status": "resolved",
     "owner": "platform_team", "incident_id": "INC-204"},

    {"alert_id": "ALT-1005", "fired_at": end_date - timedelta(days=2, hours=8),
     "severity": "warning", "metric": "high_risk_queue_size",
     "value": 850, "threshold": 500, "status": "acknowledged",
     "owner": "audit_team", "incident_id": "INC-203"},

    {"alert_id": "ALT-1006", "fired_at": end_date - timedelta(days=5),
     "severity": "info", "metric": "drift_psi_tax_rate",
     "value": 0.12, "threshold": 0.1, "status": "resolved",
     "owner": "ml_team", "incident_id": None},

    {"alert_id": "ALT-1007", "fired_at": end_date - timedelta(days=8, hours=3),
     "severity": "warning", "metric": "audit_log_lag_minutes",
     "value": 45.0, "threshold": 30.0, "status": "resolved",
     "owner": "platform_team", "incident_id": "INC-202"},
]

pdf_alerts = pd.DataFrame(alerts)
df_alerts = spark.createDataFrame(pdf_alerts)
df_alerts.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.mock_alerts")

print(f"✓ Saved {len(alerts)} alerts")
print("\n=== Open alerts (priority order) ===")
spark.sql("""
    SELECT alert_id, fired_at, severity, metric, value, threshold, owner
    FROM workspace.aade.mock_alerts
    WHERE status = 'open'
    ORDER BY
      CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
      fired_at DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 6: Inline Visualizations
# MAGIC
# MAGIC Πριν φτιάξουμε το Databricks SQL Dashboard, ας δούμε τα κύρια panels inline
# MAGIC με matplotlib για γρήγορο visual feedback.

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Panel 1: Daily prediction volume + high-risk pct
pdf_pred_local = spark.table("workspace.aade.mock_predictions_timeseries").toPandas()
pdf_pred_local["date"] = pd.to_datetime(pdf_pred_local["date"])
pdf_pred_local = pdf_pred_local.sort_values("date")

fig, ax1 = plt.subplots(figsize=(13, 4.5))
ax1.bar(pdf_pred_local["date"], pdf_pred_local["total_predictions"],
        color="#4C72B0", alpha=0.55, label="Total predictions")
ax1.set_ylabel("Total predictions / day", color="#4C72B0")
ax1.tick_params(axis="y", labelcolor="#4C72B0")
ax1.set_xlabel("Date")

ax2 = ax1.twinx()
ax2.plot(pdf_pred_local["date"], pdf_pred_local["high_risk_pct"],
         color="#C44E52", linewidth=2, marker="o", markersize=3, label="High-risk %")
ax2.axhline(y=8, color="orange", linestyle="--", alpha=0.6, label="Watch threshold (8%)")
ax2.set_ylabel("High-risk %", color="#C44E52")
ax2.tick_params(axis="y", labelcolor="#C44E52")

plt.title("Panel 1 — Daily Prediction Volume & High-Risk Rate (90 ημέρες)")
fig.legend(loc="upper left", bbox_to_anchor=(0.1, 0.95))
plt.tight_layout()
plt.show()

# COMMAND ----------

# Panel 3: Drift PSI heatmap (week × feature)
pdf_drift_local = spark.table("workspace.aade.mock_drift_history").toPandas()
pdf_drift_local["week_start"] = pd.to_datetime(pdf_drift_local["week_start"])
pivot = pdf_drift_local.pivot(index="feature", columns="week_start", values="psi")

fig, ax = plt.subplots(figsize=(13, 4))
im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn_r", vmin=0, vmax=0.3)
ax.set_yticks(range(len(pivot.index)))
ax.set_yticklabels(pivot.index)
ax.set_xticks(range(len(pivot.columns)))
ax.set_xticklabels([d.strftime("%Y-%m-%d") for d in pivot.columns], rotation=45, ha="right")
plt.colorbar(im, ax=ax, label="PSI")
ax.set_title("Panel 3 — Drift PSI Heatmap (πράσινο=ok, κόκκινο=retrain)")
# Annotate cells με τιμές
for i in range(len(pivot.index)):
    for j in range(len(pivot.columns)):
        val = pivot.values[i, j]
        ax.text(j, i, f"{val:.2f}", ha="center", va="center",
                fontsize=7, color="black" if val < 0.2 else "white")
plt.tight_layout()
plt.show()

# COMMAND ----------

# Panel 4: Performance trend (AUC, Precision, Recall, F1)
pdf_perf_local = spark.table("workspace.aade.mock_performance_history").toPandas()
pdf_perf_local["week_start"] = pd.to_datetime(pdf_perf_local["week_start"])
pdf_perf_local = pdf_perf_local.sort_values("week_start")

fig, ax = plt.subplots(figsize=(13, 4.5))
ax.plot(pdf_perf_local["week_start"], pdf_perf_local["auc"],
        marker="o", linewidth=2, label="AUC", color="#4C72B0")
ax.plot(pdf_perf_local["week_start"], pdf_perf_local["precision"],
        marker="s", linewidth=2, label="Precision", color="#55A868")
ax.plot(pdf_perf_local["week_start"], pdf_perf_local["recall"],
        marker="^", linewidth=2, label="Recall", color="#C44E52")
ax.plot(pdf_perf_local["week_start"], pdf_perf_local["f1_score"],
        marker="d", linewidth=2, label="F1", color="#8172B2")
ax.axhline(y=0.85, color="gray", linestyle="--", alpha=0.5, label="AUC SLA (0.85)")
ax.set_ylabel("Score")
ax.set_xlabel("Week")
ax.set_title("Panel 4 — Model Performance Trend (Weekly)")
ax.legend(loc="lower left")
ax.set_ylim([0.6, 0.95])
plt.tight_layout()
plt.show()

# COMMAND ----------

# Panel 5: Endpoint latency trend (last 7 days, hourly)
pdf_ep_local = spark.table("workspace.aade.mock_endpoint_metrics").toPandas()
pdf_ep_local["ts"] = pd.to_datetime(pdf_ep_local["ts"])
pdf_ep_local = pdf_ep_local.sort_values("ts")

fig, (ax_lat, ax_err) = plt.subplots(2, 1, figsize=(13, 7), sharex=True)
ax_lat.plot(pdf_ep_local["ts"], pdf_ep_local["latency_p50_ms"], label="p50", linewidth=1, alpha=0.8)
ax_lat.plot(pdf_ep_local["ts"], pdf_ep_local["latency_p95_ms"], label="p95", linewidth=1, alpha=0.8)
ax_lat.plot(pdf_ep_local["ts"], pdf_ep_local["latency_p99_ms"], label="p99", linewidth=1, alpha=0.8)
ax_lat.axhline(y=500, color="red", linestyle="--", alpha=0.5, label="p99 SLA (500ms)")
ax_lat.set_ylabel("Latency (ms)")
ax_lat.set_title("Panel 5a — Endpoint Latency (last 7 days)")
ax_lat.legend(loc="upper right")

ax_err.fill_between(pdf_ep_local["ts"], 0, pdf_ep_local["error_rate_pct"],
                    color="#C44E52", alpha=0.4)
ax_err.plot(pdf_ep_local["ts"], pdf_ep_local["error_rate_pct"], color="#C44E52")
ax_err.axhline(y=1.0, color="orange", linestyle="--", alpha=0.5, label="Error rate threshold (1%)")
ax_err.set_ylabel("Error rate (%)")
ax_err.set_xlabel("Time")
ax_err.set_title("Panel 5b — Error Rate")
ax_err.legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 7: Dashboard SQL Queries (Databricks SQL Dashboard)
# MAGIC
# MAGIC ### 📚 Πώς φτιάχνεται Databricks SQL Dashboard;
# MAGIC > 1. **SQL Editor** → Create Query (κάθε query = 1 panel)
# MAGIC > 2. **Visualization** → Διαλέγουμε τύπο (counter, line, bar, heatmap, table)
# MAGIC > 3. **Dashboards** → Create → Add visualization
# MAGIC > 4. **Refresh schedule** → Auto-refresh every 5 min
# MAGIC > 5. **Subscriptions** → email/Slack alerts όταν η τιμή σπάει threshold
# MAGIC
# MAGIC Παρακάτω βλέπετε τα 6 SQL queries που μπορείτε να αντιγράψετε στο SQL Editor
# MAGIC και να μετατρέψετε σε panels.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1 — KPI Counters (top-of-dashboard)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(total_predictions)               AS predictions_30d,
# MAGIC   SUM(high_risk_count)                 AS high_risk_30d,
# MAGIC   ROUND(AVG(high_risk_pct), 2)         AS avg_high_risk_pct_30d,
# MAGIC   ROUND(AVG(avg_risk_score), 4)        AS avg_risk_score_30d
# MAGIC FROM workspace.aade.mock_predictions_timeseries
# MAGIC WHERE date >= DATE_SUB(current_date(), 30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2 — Daily Volume (line chart)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date, total_predictions, high_risk_count, high_risk_pct
# MAGIC FROM workspace.aade.mock_predictions_timeseries
# MAGIC ORDER BY date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3 — Drift PSI Heatmap (pivoted matrix)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT week_start, feature, psi, severity
# MAGIC FROM workspace.aade.mock_drift_history
# MAGIC ORDER BY week_start, feature

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4 — Model Performance Trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT week_start, auc, precision, recall, f1_score, model_version
# MAGIC FROM workspace.aade.mock_performance_history
# MAGIC ORDER BY week_start

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 5 — Endpoint p95/p99 Latency (last 24h)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ts,
# MAGIC   latency_p50_ms,
# MAGIC   latency_p95_ms,
# MAGIC   latency_p99_ms,
# MAGIC   error_rate_pct,
# MAGIC   rps
# MAGIC FROM workspace.aade.mock_endpoint_metrics
# MAGIC WHERE ts >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC ORDER BY ts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 6 — Active Alerts (with severity color)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   alert_id,
# MAGIC   fired_at,
# MAGIC   severity,
# MAGIC   metric,
# MAGIC   value,
# MAGIC   threshold,
# MAGIC   status,
# MAGIC   owner,
# MAGIC   incident_id
# MAGIC FROM workspace.aade.mock_alerts
# MAGIC WHERE status IN ('open', 'acknowledged')
# MAGIC ORDER BY
# MAGIC   CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
# MAGIC   fired_at DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 7 — Health Status Card (single value with conditional color)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_perf AS (
# MAGIC   SELECT auc FROM workspace.aade.mock_performance_history
# MAGIC   ORDER BY week_start DESC LIMIT 1
# MAGIC ),
# MAGIC latest_drift AS (
# MAGIC   SELECT MAX(psi) AS max_psi FROM workspace.aade.mock_drift_history
# MAGIC   WHERE week_start = (SELECT MAX(week_start) FROM workspace.aade.mock_drift_history)
# MAGIC ),
# MAGIC open_alerts AS (
# MAGIC   SELECT COUNT(*) AS open_count FROM workspace.aade.mock_alerts
# MAGIC   WHERE status = 'open' AND severity = 'critical'
# MAGIC )
# MAGIC SELECT
# MAGIC   ROUND((SELECT auc FROM latest_perf), 4)         AS current_auc,
# MAGIC   ROUND((SELECT max_psi FROM latest_drift), 4)    AS max_drift_psi,
# MAGIC   (SELECT open_count FROM open_alerts)            AS critical_alerts_open,
# MAGIC   CASE
# MAGIC     WHEN (SELECT open_count FROM open_alerts) > 0 THEN '🔴 DEGRADED'
# MAGIC     WHEN (SELECT max_psi FROM latest_drift) > 0.2 THEN '🟡 WATCH'
# MAGIC     WHEN (SELECT auc FROM latest_perf) < 0.8 THEN '🟡 WATCH'
# MAGIC     ELSE '🟢 HEALTHY'
# MAGIC   END                                              AS overall_status

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 8: Dashboard Build Instructions (step-by-step)
# MAGIC
# MAGIC ### Πώς να φτιάξετε το Dashboard στο Databricks
# MAGIC
# MAGIC 1. **Sidebar → SQL** (αριστερό μενού)
# MAGIC 2. **Create → Query** για κάθε query παραπάνω
# MAGIC    - Επικολλήστε το SQL
# MAGIC    - Διαλέξτε **SQL warehouse** (Free Edition: serverless starter)
# MAGIC    - Ονομάστε π.χ. `dashboard_predictions_volume`, `dashboard_drift_heatmap`, …
# MAGIC    - **Run** → δείτε τα data
# MAGIC 3. Σε κάθε query → **Add Visualization**
# MAGIC    | Query | Viz type | Config |
# MAGIC    |---|---|---|
# MAGIC    | Q1 KPI Counters | **Counter** ×4 | Value column = κάθε metric |
# MAGIC    | Q2 Daily Volume | **Line Chart** | X=date, Y=total_predictions, second Y=high_risk_pct |
# MAGIC    | Q3 Drift PSI | **Heatmap** | X=week_start, Y=feature, Color=psi (RdYlGn_r) |
# MAGIC    | Q4 Performance | **Line Chart** | X=week_start, Y=[auc, precision, recall, f1] |
# MAGIC    | Q5 Latency | **Line Chart** | X=ts, Y=[p50, p95, p99] |
# MAGIC    | Q6 Alerts | **Table** | Με conditional formatting στο severity |
# MAGIC    | Q7 Health Status | **Counter** | Value=overall_status |
# MAGIC 4. **Dashboards → Create Dashboard**
# MAGIC    - Add Visualization → επιλέξτε από τα 7 queries
# MAGIC    - Drag-drop layout: Q7 πάνω-αριστερά, Q1 KPIs πάνω, μεγάλα charts κάτω
# MAGIC 5. **Schedule**
# MAGIC    - Refresh: every 15 min (production: 5 min)
# MAGIC    - Email subscribers: ml-team, audit-team, platform-team
# MAGIC 6. **Alerts** (από SQL Editor)
# MAGIC    - Q1: alert αν `avg_high_risk_pct_30d > 8`
# MAGIC    - Q3: alert αν οποιοδήποτε feature έχει `psi > 0.2` τις τελευταίες 7 ημέρες
# MAGIC    - Q5: alert αν `latency_p99_ms > 500` για 3 συνεχόμενα data points

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση
# MAGIC
# MAGIC ### 🎓 Τι παράξατε
# MAGIC | Πίνακας | Records | Τι περιέχει |
# MAGIC |---|---|---|
# MAGIC | `mock_predictions_timeseries` | 90 | Daily prediction stats (volume, score percentiles) |
# MAGIC | `mock_drift_history` | 78 | Weekly PSI/KS ανά feature (13 weeks × 6 features) |
# MAGIC | `mock_performance_history` | 13 | Weekly AUC/Precision/Recall + confusion matrix |
# MAGIC | `mock_endpoint_metrics` | 168 | Hourly latency p50/p95/p99 + error rate (7 days) |
# MAGIC | `mock_alerts` | 7 | Active + resolved alerts με severity & ownership |
# MAGIC
# MAGIC ### 📖 Mini-glossary
# MAGIC | Όρος | Σημασία |
# MAGIC |---|---|
# MAGIC | **Observability** | Logs + Metrics + Traces — γιατί συνέβη κάτι |
# MAGIC | **KPI** | Key Performance Indicator — top-line μετρική |
# MAGIC | **SLA** | Service Level Agreement — δέσμευση π.χ. p99 < 500ms |
# MAGIC | **SLO** | Service Level Objective — εσωτερικός στόχος (πιο αυστηρός από SLA) |
# MAGIC | **Latency p99** | Worst 1% response time — αυτό «πονάει» τους χρήστες |
# MAGIC | **RPS** | Requests Per Second — throughput |
# MAGIC | **Alert** | Αυτόματη ειδοποίηση όταν metric σπάει threshold |
# MAGIC | **Incident** | Ομαδοποίηση συσχετιζόμενων alerts |
# MAGIC | **Runbook** | Documented βήματα response σε γνωστό incident |
# MAGIC | **Post-mortem** | Blameless review μετά από incident |
# MAGIC
# MAGIC ### 🔄 Επόμενα βήματα (production)
# MAGIC - Replace mock με **real data sources**: production audit table + endpoint logs
# MAGIC - Συνδέστε με **PagerDuty/Opsgenie** για on-call rotation
# MAGIC - Προσθέστε **Slack webhook** για warning-level alerts
# MAGIC - Δημιουργήστε **runbooks** για κάθε γνωστό alert pattern
# MAGIC - Setup **post-mortem template** για INC-* tickets
# MAGIC
# MAGIC > **🎯 Take-home**: «Ένα μοντέλο σε production χωρίς dashboard είναι σαν αυτοκίνητο
# MAGIC > χωρίς ταμπλό — προχωράει, αλλά δεν ξέρεις πότε θα σπάσει.»
