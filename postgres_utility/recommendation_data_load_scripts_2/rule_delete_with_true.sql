DROP TABLE IF EXISTS rule_delete_with_true CASCADE;

CREATE TABLE rule_delete_with_true AS
SELECT DISTINCT
    2 as rule_id,
    'hsbc' as org_id,
    target_database as project_name,
    target_schema as schema_name,
    target_entity_name as table_name,
    log_id,
    query,
    table_size,
    0.0 as cost
FROM base_query_info
WHERE statement_type = 'DELETE'
    AND has_true_condition = TRUE
    AND target_schema IS NOT NULL
    AND relationship_type = 'ACCESSES';