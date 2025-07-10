-- Drop the table if it exists
DROP TABLE IF EXISTS {schema}.procedure_metadata CASCADE;

-- Create the table with the specified schema
CREATE TABLE {schema}.procedure_metadata (
  source_product TEXT,
  "database" TEXT,
  schema TEXT,
  procedure_name TEXT,
  user_name TEXT,
  sql_query TEXT,
  create_at BIGINT,
  update_at BIGINT,
  instance TEXT
);

-- Insert data into the table
INSERT INTO {schema}.procedure_metadata (
  source_product,
  "database",
  schema,
  procedure_name,
  user_name,
  sql_query,
  create_at,
  update_at,
  instance
)
SELECT
  'BIG_QUERY' AS source_product,
  r."ROUTINE_CATALOG" AS "database",
  r."ROUTINE_SCHEMA" AS schema,
  r."ROUTINE_NAME" AS procedure_name,
  '' AS user_name,
  r."DDL" AS sql_query,
  EXTRACT(EPOCH FROM r."CREATED")::BIGINT AS create_at,
  EXTRACT(EPOCH FROM r."LAST_ALTERED")::BIGINT AS update_at,
  '{instance}' AS instance
FROM
  {schema}."ROUTINES" r
WHERE
  r."ROUTINE_TYPE" = 'PROCEDURE';
