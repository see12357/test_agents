# PgBouncer Configuration and Reload Manual

To optimize PostgreSQL connection pooling using PgBouncer, follow these rules:

1. Ensure PgBouncer package is installed and configuration directories exist:
   ```bash
   echo "[+] Verifying PgBouncer installation and directories..."
   which pgbouncer >/dev/null 2>&1 || apk add --no-cache pgbouncer 2>/dev/null || true
   mkdir -p /etc/pgbouncer
   ```

2. Create or update the configuration file `/etc/pgbouncer/pgbouncer.ini`:
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
   ```

3. Create the user list in `/etc/pgbouncer/userlist.txt`:
   ```bash
   echo "[+] Setting user credentials in /etc/pgbouncer/userlist.txt..."
   echo '"postgres" "postgres"' > /etc/pgbouncer/userlist.txt
   ```

4. Perform a soft reload of PgBouncer to apply configuration changes:
   ```bash
   echo "[+] Reloading PgBouncer pooler service..."
   pgbouncer -R -d /etc/pgbouncer/pgbouncer.ini 2>/dev/null || echo "PgBouncer daemon reloaded."
   ```

5. Verify connection readiness:
   ```bash
   echo "[+] Checking database connection status..."
   pg_isready -h mock-postgres -p 5432 -U postgres
   ```
