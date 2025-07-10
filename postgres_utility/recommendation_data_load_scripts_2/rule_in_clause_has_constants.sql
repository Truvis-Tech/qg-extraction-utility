DROP TABLE IF EXISTS rule_in_clause_has_constants CASCADE;

CREATE TABLE rule_in_clause_has_constants AS
SELECT DISTINCT
    3 as rule_id,
    'hsbc' as org_id,
    target_database as project_name,
    target_schema as schema_name,
    target_entity_name as table_name,
    log_id,
    query,
    table_size,
    0.0 as cost
FROM base_query_info
WHERE has_in_with_constant = TRUE;