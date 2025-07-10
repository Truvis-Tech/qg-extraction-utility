DROP TABLE IF EXISTS rule_jobs_failing_very_frequently CASCADE;

CREATE TABLE rule_jobs_failing_very_frequently AS
SELECT
    5 as rule_id,
    'hsbc' AS org_id,
    project_id AS project_name,
    job_id as log_id,
    'NA' as schema_name,
    query,
    user_email AS user_id,
    error_result_reason,
    error_result_message,
    total_slot_ms,
    CASE
        WHEN total_slot_ms IS NULL THEN CAST(0.0 AS DOUBLE PRECISION)
        ELSE CAST(ROUND(CEIL(total_slot_ms / 1000.0 / 60.0) * COALESCE(0.06, 0.001), 3) AS DOUBLE PRECISION)
    END AS cost
FROM "JOBS_BY_PROJECT"
WHERE
    query IS NOT NULL
     AND error_result_reason IS NOT NULL
ORDER BY total_slot_ms DESC;
