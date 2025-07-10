-- Drop the table if it exists
DROP TABLE IF EXISTS {schema}.macro_metadata CASCADE;

-- Create the table
CREATE TABLE {schema}.macro_metadata (
  "database" TEXT,
  "schema" TEXT,
  "macro_name" TEXT,
  user_name TEXT,
  sql_query TEXT,
  create_at BIGINT,
  update_at BIGINT,
  instance TEXT
);
