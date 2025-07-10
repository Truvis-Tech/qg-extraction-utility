DROP TABLE IF EXISTS rule_jobs_scanning_high_volume_of_data CASCADE;

CREATE TABLE rule_jobs_scanning_high_volume_of_data AS
WITH job_stats AS (
    SELECT
        project_id AS project_name,
        job_id,
        query,
        user_email AS user_id,
        total_bytes_processed::DECIMAL / 1024.0 / 1024.0 / 1024.0 AS scan_size_in_gb,
        total_slot_ms,
        CASE
            WHEN total_slot_ms IS NULL THEN 0.0
            ELSE ROUND(
                CEIL(total_slot_ms::DECIMAL / 1000.0 / 60.0) * COALESCE(0.06, 0.001)::DECIMAL,
                3
            )::DECIMAL(10, 3)
        END AS cost
    FROM "JOBS_BY_PROJECT"
    WHERE query IS NOT NULL
    AND error_result_reason IS NULL
    AND total_bytes_processed::DECIMAL / 1024.0 / 1024.0 / 1024.0 > 20
),
unnested_queries AS (
    SELECT DISTINCT
        7 AS rule_id,
        'hsbc' AS org_id,
        project_name,
        'NA' AS schema_name,
        job_id AS log_id,
        query,
        user_id,
        scan_size_in_gb,
        total_slot_ms,
        cost
    FROM job_stats
)
SELECT *
FROM unnested_queries
ORDER BY scan_size_in_gb DESC;