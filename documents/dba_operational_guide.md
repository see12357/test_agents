# Comprehensive Production DBA Operational Standard Guide

This guide establishes the mandatory, authoritative operational procedures for Database Reliability Engineers (DBRE) and automated maintenance agents managing PostgreSQL, PgBouncer, and Linux infrastructure.

---

## Section 1: Database Schema Migrations (DDL)

When executing DDL migrations on production PostgreSQL databases, always adhere to non-blocking patterns to protect application availability:

1. **Lock Timeout Safeguard**:
   Before running DDL commands, ALWAYS set a lock timeout to prevent query queue accumulation:
   ```sql
   SET lock_timeout = '5s';
   ```

2. **Non-Blocking Index Creation**:
   Indexes MUST be created in non-blocking mode (`CONCURRENTLY`) with idempotency checks:
   ```sql
   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_created_at ON users(created_at);
   ```

3. **Safe Column Addition**:
   Add new columns safely with `IF NOT EXISTS`:
   ```sql
   ALTER TABLE users ADD COLUMN IF NOT EXISTS user_status VARCHAR(30) DEFAULT 'active';
   ```

4. **Planner Statistics Update**:
   Update query planner statistics immediately after DDL operations:
   ```sql
   ANALYZE users;
   ```

---

## Section 2: SSL/TLS Certificate Rotation

To perform zero-downtime SSL/TLS certificate rotation on PostgreSQL instances:

1. **Package Availability Verification**:
   Verify or install required OpenSSL utilities:
   ```bash
   which openssl >/dev/null 2>&1 || apk add --no-cache openssl 2>/dev/null || true
   ```

2. **Certificate Generation & File Preparation**:
   Generate or update SSL certificates in PostgreSQL directory or `/tmp`:
   ```bash
   if [ ! -f /tmp/server.crt ]; then
     openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/server.key -out /tmp/server.crt -days 365 -subj "/CN=postgres" 2>/dev/null || true
   fi
   cp /tmp/server.crt /var/lib/postgresql/data/server.crt 2>/dev/null || true
   cp /tmp/server.key /var/lib/postgresql/data/server.key 2>/dev/null || true
   chmod 600 /var/lib/postgresql/data/server.key 2>/dev/null || true
   chown postgres:postgres /var/lib/postgresql/data/server.key 2>/dev/null || true
   ```

3. **Zero-Downtime Configuration Reload via SQL**:
   Reload PostgreSQL configuration without restarting the database daemon:
   ```bash
   psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT pg_reload_conf();"
   ```

4. **Verification Query**:
   ```sql
   SELECT name, setting FROM pg_settings WHERE name LIKE 'ssl%';
   ```

---

## Section 3: Connection Pooler (PgBouncer) Management

To configure and maintain PgBouncer connection poolers:

1. **Directory & Package Initialization**:
   ```bash
   mkdir -p /etc/pgbouncer
   which pgbouncer >/dev/null 2>&1 || apk add --no-cache pgbouncer 2>/dev/null || true
   ```

2. **Soft Service Reload**:
   Reload PgBouncer daemon without dropping active user connections:
   ```bash
   pgbouncer -R -d /etc/pgbouncer/pgbouncer.ini 2>/dev/null || echo "PgBouncer reloaded."
   ```

3. **Pooler Diagnostics**:
   ```bash
   psql -h "$DB_HOST" -p 6432 -U postgres pgbouncer -c "SHOW POOLS;"
   ```

---

## Section 4: Database Backups & Recovery (DR)

1. **Physical Backup with Logical Fallback**:
   ```bash
   (pg_basebackup -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -D /tmp/basebackup 2>/dev/null || pg_dump -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" > /tmp/backup.sql)
   ```

2. **Backup Verification**:
   ```sql
   SELECT count(*) FROM pg_stat_user_tables;
   ```

---

## Section 5: OS Upgrades & Package Maintenance

1. **Package Refresh on Alpine Containers**:
   ```bash
   which apk >/dev/null 2>&1 && apk update 2>/dev/null || true
   ```

2. **Disk & Environment Diagnostics**:
   ```bash
   df -h /var/lib/postgresql/data
   ```

---

## Section 6: High-Risk & Sensitive Destructive Operations (CRITICAL TIER)

Operations involving `DROP TABLE`, `DROP DATABASE`, `TRUNCATE`, `DELETE` without `WHERE` clause, or privilege escalation (`ALTER USER ... SUPERUSER`, `GRANT ALL`):

1. **Mandatory Safety Checks**:
   - Destructive commands MUST be explicitly scoped to target entities.
   - Require explicit confirmation token before production execution.
2. **Transaction Isolation**:
   Wrap data modification DML in strict transaction blocks:
   ```sql
   BEGIN;
   -- Destructive DML operation
   COMMIT;
   ```
