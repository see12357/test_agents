# PgBouncer Configuration and Reload Manual

To optimize PostgreSQL connection pooling using PgBouncer, follow these rules:

1. Ensure PgBouncer package is installed, directories exist, and permissions allow non-root execution:
   ```bash
   echo "[+] Verifying PgBouncer installation and directories..."
   which pgbouncer >/dev/null 2>&1 || apk add --no-cache pgbouncer 2>/dev/null || true
   mkdir -p /etc/pgbouncer
   chown -R postgres:postgres /etc/pgbouncer /tmp 2>/dev/null || true
   ```

2. Create or update the configuration file `/etc/pgbouncer/pgbouncer.ini` (when updating existing parameters like max_client_conn, use awk/sed or overwrite):
   ```bash
   echo "[+] Updating configuration /etc/pgbouncer/pgbouncer.ini..."
   cat << 'EOF' > /etc/pgbouncer/pgbouncer.ini
   [databases]
   postgres = host=mock-postgres port=5432 dbname=postgres

   [pgbouncer]
   pool_mode = transaction
   max_client_conn = 500
   default_pool_size = 50
   EOF
   chown postgres:postgres /etc/pgbouncer/pgbouncer.ini 2>/dev/null || true
   ```

3. CRITICAL: Always ensure the user list file `/etc/pgbouncer/userlist.txt` exists and has correct ownership before starting or reloading PgBouncer:
   ```bash
   echo "[+] Setting user credentials in /etc/pgbouncer/userlist.txt..."
   [ -f /etc/pgbouncer/userlist.txt ] || echo '"postgres" "postgres"' > /etc/pgbouncer/userlist.txt
   chown postgres:postgres /etc/pgbouncer/userlist.txt 2>/dev/null || true
   ```

4. CRITICAL: Never run pgbouncer directly as root. Always pass `-u postgres` (or switch to the `postgres` user) when reloading or starting PgBouncer:
   ```bash
   echo "[+] Reloading or starting PgBouncer pooler service as postgres user..."
   chown -R postgres:postgres /etc/pgbouncer /tmp/pgbouncer* 2>/dev/null || true
   pgbouncer -u postgres -R -d /etc/pgbouncer/pgbouncer.ini 2>/dev/null || pgbouncer -u postgres -d /etc/pgbouncer/pgbouncer.ini 2>/dev/null || echo "PgBouncer daemon reloaded."
   ```

5. Verify connection readiness:
   ```bash
   echo "[+] Checking database connection status..."
   pg_isready -h mock-postgres -p 5432 -U postgres
   ```
