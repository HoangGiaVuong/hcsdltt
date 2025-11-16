-- Enable pg_stat_statements extension
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Reset statistics (optional)
SELECT pg_stat_statements_reset();
