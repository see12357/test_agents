# Security Compliance and Risk Audit Manual

To audit database security and check standards compliance on Patroni and PostgreSQL clusters:

1. Check user access permissions and verify the presence of unused accounts with superuser rights:
   ```sql
   SELECT usename FROM pg_shadow WHERE usesuper = true;
   ```
2. Verify that SSL encryption is enabled:
   ```sql
   SHOW ssl;
   ```
3. Check that the logs record all failed authorization attempts and critical operations.
4. Generate a compliance report for the information security service.
