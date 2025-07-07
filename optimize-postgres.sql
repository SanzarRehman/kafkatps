-- PostgreSQL Configuration for High-TPS Kafka Processing
-- Run this script to optimize your database for high-throughput message processing

-- Increase connection limits to handle backpressure scenarios
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET max_prepared_transactions = 200;

-- Memory settings for high-performance inserts
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET work_mem = '16MB';

-- WAL settings for high-throughput writes
ALTER SYSTEM SET wal_level = minimal;
ALTER SYSTEM SET max_wal_size = '1GB';
ALTER SYSTEM SET min_wal_size = '80MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';

-- Optimize for bulk inserts
ALTER SYSTEM SET synchronous_commit = off;
ALTER SYSTEM SET fsync = off;
ALTER SYSTEM SET full_page_writes = off;
ALTER SYSTEM SET wal_compression = on;

-- Connection pooling optimization
ALTER SYSTEM SET tcp_keepalives_idle = 300;
ALTER SYSTEM SET tcp_keepalives_interval = 30;
ALTER SYSTEM SET tcp_keepalives_count = 3;

-- Logging for monitoring
ALTER SYSTEM SET log_min_duration_statement = 1000;
ALTER SYSTEM SET log_connections = on;
ALTER SYSTEM SET log_disconnections = on;

-- Apply changes
SELECT pg_reload_conf();

-- Create optimized indexes for loan table
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_loan_member_id ON loan(member_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_loan_created_date ON loan(created_date);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_loan_tenant_id ON loan(tenant_id);

-- Optimize loan table for high-throughput inserts
ALTER TABLE loan SET (fillfactor = 85);
VACUUM ANALYZE loan;

-- Show current connection status
SELECT 
    state,
    COUNT(*) as connection_count,
    MAX(now() - state_change) as max_duration
FROM pg_stat_activity 
WHERE state IS NOT NULL 
GROUP BY state;

-- Show database settings
SELECT name, setting, unit 
FROM pg_settings 
WHERE name IN ('max_connections', 'shared_buffers', 'synchronous_commit', 'fsync');

COMMIT;