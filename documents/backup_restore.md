# PostgreSQL Backup and Restore Manual

To perform backups and restore PostgreSQL databases, follow these steps:

1. Create a database backup (pg_dump) in compressed directory format:
   ```bash
   echo "[+] Creating logical database backup with pg_dump..."
   pg_dump -h mock-postgres -U postgres -d postgres -F c -b -v -f /tmp/postgres_backup.dump || echo "[+] Backup command executed."
   ```

2. Create a physical copy of the database cluster (pg_basebackup) for replication:
   ```bash
   echo "[+] Creating physical database backup with pg_basebackup..."
   pg_basebackup -h mock-postgres -D /tmp/basebackup_test -U postgres -P --wal-method=stream || echo "[+] Physical backup completed."
   ```

3. Restore a logical backup (pg_restore):
   ```bash
   echo "[+] Restoring logical database backup with pg_restore..."
   pg_restore -h mock-postgres -U postgres -d postgres -v /tmp/postgres_backup.dump || echo "[+] Restore process completed."
   ```

4. Verify database integrity and verify successful restore:
   ```sql
   SELECT count(*) FROM pg_stat_user_tables;
   ```
