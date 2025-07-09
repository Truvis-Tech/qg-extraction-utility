DROP TABLE IF EXISTS rule_order_by_inside_subquery CASCADE;

CREATE TABLE rule_order_by_inside_subquery AS
SELECT DISTINCT
    9 as rule_id,
    'hsbc' as org_id,
    database as project_name,
    schema as schema_name,
    log_id,
    query,
    0.0 as cost
FROM base_query_info
WHERE is_oder_by_inside_sub_query = TRUE;