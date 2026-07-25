# Database Schema Migration Manual for PostgreSQL

When applying migrations to production databases, follow this workflow to prevent locking tables:

1. Always set a lock timeout (lock_timeout) before performing DDL statements to avoid stalling user queries:
   ```sql
   SET lock_timeout = '5s';
   ```
2. Create indexes in a non-blocking mode (CONCURRENTLY):
   ```sql
   CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
   ```
3. Add columns with default values safely:
   ```sql
   ALTER TABLE billing_transactions ADD COLUMN status VARCHAR(20) DEFAULT 'pending';
   ```
4. Update planner statistics after structure modifications:
   ```sql
   ANALYZE users;
   ```
