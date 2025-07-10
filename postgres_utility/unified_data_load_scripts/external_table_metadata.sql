-- Drop the table if it exists
DROP TABLE IF EXISTS {schema}.external_table_metadata CASCADE;

-- Create the external table
CREATE TABLE {schema}.external_table_metadata AS
SELECT
    'BIG_QUERY' AS source_product, 
    t."TABLE_CATALOG" AS "database",  
    t."TABLE_SCHEMA" AS "schema",
    t."TABLE_NAME" AS external_table_name,
    t."TABLE_TYPE" AS external_table_type,
    REGEXP_REPLACE(
        (REGEXP_MATCHES(t."DDL", 'uris\s*=\s*\[(.*?)\]', 'i'))[1], '"', '', 'g'
    ) AS external_object_name,
    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT AS create_at,
    EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT AS update_at,
    '{instance}' AS instance
FROM
    {schema}."TABLES" t  
WHERE
    t."TABLE_TYPE" = 'EXTERNAL';  
