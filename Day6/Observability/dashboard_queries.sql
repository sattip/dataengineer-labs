-- =====================================================================
-- Pipeline & Job Health Dashboard — 10 SQL Queries
-- =====================================================================
-- Κάθε query αντιστοιχεί σε ένα panel του Lakeview Dashboard.
-- Όλα τα data έρχονται από `system.*` tables (auto-maintained από Databricks).
--
-- Use cases:
--  - Q1-Q3:  KPI counters (top of dashboard)
--  - Q4-Q5:  Time-series charts
--  - Q6-Q7:  Top-N tables (slowest, most failures)
--  - Q8:    Per-job drill-down table (click → open Job UI)
--  - Q9:    DLT pipelines health
--  - Q10:   Cost breakdown (DBU consumption)
-- =====================================================================


-- =====================================================================
-- Q1: Active Jobs Count (KPI counter)
-- =====================================================================
SELECT COUNT(*) AS active_jobs
FROM system.workflow.jobs
WHERE delete_time IS NULL;


-- =====================================================================
-- Q2: Total Runs (last 7 days)
-- =====================================================================
SELECT COUNT(*) AS total_runs_7d
FROM system.workflow.job_run_timeline
WHERE period_start_time >= current_timestamp() - INTERVAL 7 DAYS
  AND period_end_time IS NOT NULL;


-- =====================================================================
-- Q3: Success Rate (last 7 days) — Critical KPI
-- =====================================================================
SELECT
    COUNT(*)                                                    AS total_runs,
    SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS successful,
    SUM(CASE WHEN result_state = 'FAILED'    THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN result_state = 'TIMEDOUT'  THEN 1 ELSE 0 END) AS timed_out,
    SUM(CASE WHEN result_state = 'CANCELED'  THEN 1 ELSE 0 END) AS canceled,
    ROUND(
        100.0 * SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2
    ) AS success_rate_pct
FROM system.workflow.job_run_timeline
WHERE period_start_time >= current_timestamp() - INTERVAL 7 DAYS
  AND period_end_time IS NOT NULL;


-- =====================================================================
-- Q4: Daily Success Rate Trend (last 30 days) — Line Chart
-- =====================================================================
SELECT
    date(period_start_time)                                AS run_date,
    COUNT(*)                                               AS total_runs,
    SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
    SUM(CASE WHEN result_state = 'FAILED'    THEN 1 ELSE 0 END) AS failed,
    ROUND(
        100.0 * SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2
    ) AS success_rate_pct
FROM system.workflow.job_run_timeline
WHERE period_start_time >= current_timestamp() - INTERVAL 30 DAYS
  AND period_end_time IS NOT NULL
GROUP BY date(period_start_time)
ORDER BY run_date;


-- =====================================================================
-- Q5: Average Duration Trend per Job (last 30 days) — Multi-line Chart
-- =====================================================================
WITH durations AS (
    SELECT
        j.name                                                AS job_name,
        date(rt.period_start_time)                            AS run_date,
        AVG(
            unix_timestamp(rt.period_end_time) - unix_timestamp(rt.period_start_time)
        ) / 60.0                                              AS avg_duration_min
    FROM system.workflow.job_run_timeline rt
    JOIN system.workflow.jobs j
      ON rt.job_id = j.job_id
    WHERE rt.period_start_time >= current_timestamp() - INTERVAL 30 DAYS
      AND rt.period_end_time IS NOT NULL
      AND rt.result_state = 'SUCCEEDED'
    GROUP BY j.name, date(rt.period_start_time)
)
SELECT * FROM durations
ORDER BY run_date, job_name;


-- =====================================================================
-- Q6: Top 10 Slowest Jobs (by avg duration last 7 days)
-- =====================================================================
SELECT
    j.name                                                 AS job_name,
    j.creator                                              AS owner,
    COUNT(*)                                               AS runs_count,
    ROUND(AVG(
        unix_timestamp(rt.period_end_time) - unix_timestamp(rt.period_start_time)
    ) / 60.0, 2)                                           AS avg_duration_min,
    ROUND(MAX(
        unix_timestamp(rt.period_end_time) - unix_timestamp(rt.period_start_time)
    ) / 60.0, 2)                                           AS max_duration_min,
    ROUND(
        100.0 * SUM(CASE WHEN rt.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)
        / COUNT(*), 2
    )                                                      AS success_rate_pct
FROM system.workflow.job_run_timeline rt
JOIN system.workflow.jobs j
  ON rt.job_id = j.job_id
WHERE rt.period_start_time >= current_timestamp() - INTERVAL 7 DAYS
  AND rt.period_end_time IS NOT NULL
GROUP BY j.name, j.creator
ORDER BY avg_duration_min DESC
LIMIT 10;


-- =====================================================================
-- Q7: Recent Failures (last 24h) — Drill-down table
-- =====================================================================
SELECT
    j.name                                              AS job_name,
    rt.period_start_time                                AS started_at,
    ROUND(
        (unix_timestamp(rt.period_end_time) - unix_timestamp(rt.period_start_time)) / 60.0,
        2
    )                                                   AS duration_min,
    rt.result_state                                     AS status,
    rt.termination_code                                 AS error_code,
    rt.run_id                                           AS run_id,
    j.creator                                           AS owner
FROM system.workflow.job_run_timeline rt
JOIN system.workflow.jobs j
  ON rt.job_id = j.job_id
WHERE rt.period_start_time >= current_timestamp() - INTERVAL 24 HOURS
  AND rt.result_state IN ('FAILED', 'TIMEDOUT', 'ERROR')
ORDER BY rt.period_start_time DESC
LIMIT 50;


-- =====================================================================
-- Q8: Per-Job Health Summary (Master drill-down table)
-- =====================================================================
WITH job_stats AS (
    SELECT
        j.job_id,
        j.name,
        j.creator,
        COUNT(rt.run_id)                                          AS total_runs_7d,
        SUM(CASE WHEN rt.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS successes,
        SUM(CASE WHEN rt.result_state = 'FAILED'    THEN 1 ELSE 0 END) AS failures,
        SUM(CASE WHEN rt.result_state = 'TIMEDOUT'  THEN 1 ELSE 0 END) AS timeouts,
        MAX(rt.period_start_time)                                 AS last_run_at,
        ROUND(AVG(
            unix_timestamp(rt.period_end_time) - unix_timestamp(rt.period_start_time)
        ) / 60.0, 2)                                              AS avg_duration_min
    FROM system.workflow.jobs j
    LEFT JOIN system.workflow.job_run_timeline rt
      ON j.job_id = rt.job_id
      AND rt.period_start_time >= current_timestamp() - INTERVAL 7 DAYS
    WHERE j.delete_time IS NULL
    GROUP BY j.job_id, j.name, j.creator
)
SELECT
    job_id,
    name                                                        AS job_name,
    creator                                                     AS owner,
    total_runs_7d,
    successes,
    failures,
    timeouts,
    ROUND(100.0 * successes / NULLIF(total_runs_7d, 0), 2)      AS success_rate_pct,
    last_run_at,
    avg_duration_min,
    CASE
        WHEN total_runs_7d = 0 THEN '⚪ NO RUNS'
        WHEN failures > 0 AND failures / NULLIF(total_runs_7d, 0) > 0.2 THEN '🔴 DEGRADED'
        WHEN failures > 0 THEN '🟡 WATCH'
        ELSE '🟢 HEALTHY'
    END                                                          AS health_status
FROM job_stats
ORDER BY
    CASE health_status
        WHEN '🔴 DEGRADED' THEN 1
        WHEN '🟡 WATCH'    THEN 2
        WHEN '🟢 HEALTHY'  THEN 3
        ELSE 4
    END,
    failures DESC;


-- =====================================================================
-- Q9: DLT Pipelines Health
-- =====================================================================
SELECT
    p.name                                                     AS pipeline_name,
    p.creator                                                  AS owner,
    COUNT(pr.update_id)                                        AS total_updates_7d,
    SUM(CASE WHEN pr.state = 'COMPLETED' THEN 1 ELSE 0 END)    AS successful,
    SUM(CASE WHEN pr.state = 'FAILED'    THEN 1 ELSE 0 END)    AS failed,
    ROUND(AVG(
        unix_timestamp(pr.update_end_time) - unix_timestamp(pr.update_start_time)
    ) / 60.0, 2)                                               AS avg_duration_min,
    MAX(pr.update_start_time)                                  AS last_update_at,
    CASE
        WHEN SUM(CASE WHEN pr.state = 'FAILED' THEN 1 ELSE 0 END) > 0
             AND SUM(CASE WHEN pr.state = 'FAILED' THEN 1 ELSE 0 END) /
                 NULLIF(COUNT(pr.update_id), 0) > 0.2 THEN '🔴 DEGRADED'
        WHEN SUM(CASE WHEN pr.state = 'FAILED' THEN 1 ELSE 0 END) > 0 THEN '🟡 WATCH'
        WHEN COUNT(pr.update_id) = 0 THEN '⚪ NO RUNS'
        ELSE '🟢 HEALTHY'
    END                                                         AS health_status
FROM system.lakeflow.pipelines p
LEFT JOIN system.lakeflow.pipeline_runs pr
  ON p.pipeline_id = pr.pipeline_id
  AND pr.update_start_time >= current_timestamp() - INTERVAL 7 DAYS
WHERE p.deleted_time IS NULL
GROUP BY p.pipeline_id, p.name, p.creator
ORDER BY failed DESC, total_updates_7d DESC;


-- =====================================================================
-- Q10: DBU Cost Breakdown (last 7 days)
-- =====================================================================
SELECT
    CASE
        WHEN usage_metadata.job_id IS NOT NULL THEN 'Job'
        WHEN usage_metadata.dlt_pipeline_id IS NOT NULL THEN 'DLT Pipeline'
        WHEN usage_metadata.warehouse_id IS NOT NULL THEN 'SQL Warehouse'
        WHEN usage_metadata.cluster_id IS NOT NULL THEN 'Interactive Cluster'
        ELSE 'Other'
    END                                                          AS resource_type,
    COALESCE(
        usage_metadata.job_name,
        usage_metadata.dlt_pipeline_name,
        usage_metadata.warehouse_name,
        usage_metadata.cluster_name,
        'unattributed'
    )                                                            AS resource_name,
    SKU_NAME                                                     AS sku,
    SUM(usage_quantity)                                          AS total_dbus,
    ROUND(SUM(usage_quantity) * 0.40, 2)                         AS estimated_cost_usd
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY resource_type, resource_name, SKU_NAME
ORDER BY total_dbus DESC
LIMIT 20;


-- =====================================================================
-- BONUS Q11: Task-level Drill-down για specific job_id
-- =====================================================================
-- Parameter: {{job_id}} — αντικαθίσταται από dashboard filter
--
SELECT
    tr.task_key,
    tr.period_start_time                                         AS started_at,
    ROUND(
        (unix_timestamp(tr.period_end_time) - unix_timestamp(tr.period_start_time)) / 60.0,
        2
    )                                                            AS duration_min,
    tr.result_state                                              AS status,
    tr.run_id,
    tr.attempt_number
FROM system.workflow.job_task_run_timeline tr
WHERE tr.job_id = '{{job_id}}'
  AND tr.period_start_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY tr.period_start_time DESC, tr.task_key;
