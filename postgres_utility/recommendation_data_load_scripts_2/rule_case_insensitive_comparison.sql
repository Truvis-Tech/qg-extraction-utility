DROP TABLE IF EXISTS rule_case_insensitive_comparison CASCADE;

CREATE TABLE rule_case_insensitive_comparison AS
SELECT DISTINCT
    1 as rule_id,
    'hsbc' as org_id,
    target_database as project_name,
    target_schema as schema_name,
    target_entity_name as table_name,
    log_id,
    query
FROM base_query_info
WHERE has_case_insensitive_comparison = TRUE;
