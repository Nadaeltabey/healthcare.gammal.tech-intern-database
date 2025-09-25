# healthcare.gammal.tech — PostgreSQL schema (production-ready)

This repository contains the full PostgreSQL schema for `healthcare.gammal.tech`.

**Important:** Replace placeholder secrets stored in the `config` table with actual secret-manager values before using in production.

## Requirements
- PostgreSQL (12+ recommended)
- Extensions:
  - `pgcrypto`
  - `uuid-ossp`

## Files
- `healthcare_gammal_tech.sql` — full schema, ETL functions, views, RLS policies, sample data, and utilities.
- `README.md` — this file.

## Usage
Run the SQL file with `psql` (recommended to run in a safe non-production DB first):
```bash
psql -U your_user -d your_db -f healthcare_gammal_tech.sql
```

## Notes
- Remove sample inserts before production.
- Use a secrets manager for `pgp_sym_key` and `data_salt`.
- Ensure scheduled jobs (ETL, refreshes, DQ) are configured in Airflow/cron with proper monitoring.
