#!/usr/bin/env bash
set -euo pipefail

# Bootstrap service roles; schema DDL is maintained as versioned migrations.
: "${PIPELINE_DB_PASSWORD:?Set PIPELINE_DB_PASSWORD}"
: "${DBT_DB_PASSWORD:?Set DBT_DB_PASSWORD}"
: "${PIPELINE_READER_PASSWORD:?Set PIPELINE_READER_PASSWORD}"

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=ingest_password="$PIPELINE_DB_PASSWORD" \
  --set=dbt_password="$DBT_DB_PASSWORD" \
  --set=reader_password="$PIPELINE_READER_PASSWORD" <<'SQL'
CREATE ROLE pipeline_ingest LOGIN PASSWORD :'ingest_password';
CREATE ROLE dbt_transform LOGIN PASSWORD :'dbt_password';
CREATE ROLE pipeline_reader LOGIN PASSWORD :'reader_password';
SQL
