-- Drop the table if it exists
DROP TABLE IF EXISTS {schema}.table_metadata CASCADE;

-- Create the table with metadata
CREATE TABLE {schema}.table_metadata AS
SELECT
    'BIG_QUERY' AS source_product,
    t."TABLE_CATALOG" AS "database",
    t."TABLE_SCHEMA" AS "schema",
    t."TABLE_NAME" AS table_name,
    '' AS username,
    COALESCE(ts.size_bytes / (1024.0 * 1024.0), 0.0) AS size_mb,
    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT AS create_at,
    COALESCE(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP),  EXTRACT(EPOCH FROM t."CREATION_TIME"))::BIGINT AS update_at
FROM
    {schema}."TABLES" t
LEFT JOIN
    {schema}."TABLES__" ts
    ON t."TABLE_CATALOG" = ts.project_id
       AND t."TABLE_SCHEMA" = ts.dataset_id
       AND t."TABLE_NAME" = ts.table_id
WHERE
    t."TABLE_TYPE" = 'BASE TABLE';
