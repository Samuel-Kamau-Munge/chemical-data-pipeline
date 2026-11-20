#!/usr/bin/env bash
set -euo pipefail

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

CREATE SCHEMA raw;
CREATE SCHEMA analytics AUTHORIZATION dbt_transform;
CREATE SCHEMA staging AUTHORIZATION dbt_transform;
CREATE SCHEMA marts AUTHORIZATION dbt_transform;

CREATE TABLE raw.ingestion_files (
    source_sha256 CHAR(64) PRIMARY KEY
        CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    source_filename TEXT NOT NULL,
    byte_count BIGINT NOT NULL CHECK (byte_count >= 0),
    ingested_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('processing', 'completed', 'failed')),
    total_rows INTEGER NOT NULL DEFAULT 0 CHECK (total_rows >= 0),
    accepted_rows INTEGER NOT NULL DEFAULT 0 CHECK (accepted_rows >= 0),
    rejected_rows INTEGER NOT NULL DEFAULT 0 CHECK (rejected_rows >= 0),
    last_error TEXT,
    CHECK (total_rows = accepted_rows + rejected_rows OR status = 'processing')
);

CREATE TABLE raw.lab_result_events (
    event_id CHAR(64) PRIMARY KEY
        CHECK (event_id ~ '^[0-9a-f]{64}$'),
    source_sha256 CHAR(64) NOT NULL
        REFERENCES raw.ingestion_files(source_sha256),
    row_number INTEGER NOT NULL CHECK (row_number >= 2),
    status TEXT NOT NULL CHECK (status IN ('accepted', 'quarantined')),
    record JSONB NOT NULL CHECK (jsonb_typeof(record) = 'object'),
    raw_record JSONB NOT NULL CHECK (jsonb_typeof(raw_record) = 'object'),
    validation_errors JSONB NOT NULL
        CHECK (jsonb_typeof(validation_errors) = 'array'),
    occurred_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source_sha256, row_number),
    CHECK (
        (status = 'accepted' AND jsonb_array_length(validation_errors) = 0)
        OR
        (status = 'quarantined' AND jsonb_array_length(validation_errors) > 0)
    )
);

CREATE INDEX lab_result_events_status_idx
    ON raw.lab_result_events (status);
CREATE INDEX lab_result_events_source_idx
    ON raw.lab_result_events (source_sha256, row_number);

GRANT USAGE ON SCHEMA raw TO pipeline_ingest, pipeline_reader, dbt_transform;
GRANT SELECT, INSERT, UPDATE
    ON raw.ingestion_files, raw.lab_result_events TO pipeline_ingest;
GRANT SELECT ON raw.ingestion_files, raw.lab_result_events
    TO pipeline_reader, dbt_transform;
GRANT USAGE, CREATE ON SCHEMA analytics TO dbt_transform;
GRANT USAGE, CREATE ON SCHEMA staging, marts TO dbt_transform;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_transform IN SCHEMA staging
    GRANT SELECT ON TABLES TO pipeline_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_transform IN SCHEMA marts
    GRANT SELECT ON TABLES TO pipeline_reader;
GRANT USAGE ON SCHEMA staging, marts TO pipeline_reader;
SQL
