DROP TABLE IF EXISTS rule_table_clone CASCADE;

CREATE TABLE rule_table_clone AS
WITH clone_candidates AS (
    SELECT *,
        COUNT(*) FILTER (WHERE relationship_type = 'DEPENDS_ON')
        OVER (PARTITION BY s_id) AS depends_on_count
    FROM base_query_info
)
SELECT DISTINCT
    11 AS rule_id,
    'hsbc' AS org_id,
    database as project_name,
    target_database,
    target_schema as schema_name,
    target_entity_name,
    source_database,
    log_id,
    query,
    0.0 AS cost
FROM clone_candidates
WHERE statement_type = 'INSERT'
    AND has_select_all = TRUE
    AND has_where_clause <> TRUE
    AND depends_on_count = 1;
