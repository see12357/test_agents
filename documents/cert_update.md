# PostgreSQL SSL Certificate Update Manual

To replace SSL certificates on PostgreSQL instances without service downtime, follow these actions:

1. Ensure OpenSSL utilities are available:
   ```bash
   which openssl >/dev/null 2>&1 || apk add --no-cache openssl 2>/dev/null || true
   ```

2. Generate or update SSL certificates in the PostgreSQL data directory or /tmp:
   ```bash
   if [ ! -f /tmp/server.crt ]; then
     openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/server.key -out /tmp/server.crt -days 365 -subj "/CN=postgres" 2>/dev/null || true
   fi
   ```

3. Perform a PostgreSQL configuration reload without service interruption via SQL:
   ```bash
   psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT pg_reload_conf();"
   ```

4. Verify SSL parameters and certificate settings:
   ```sql
   SELECT name, setting FROM pg_settings WHERE name LIKE 'ssl%';
   ```
