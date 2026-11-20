# Data governance

This document defines the demonstration project's current data contract and
governance boundaries. It is not a site policy or regulatory determination.

## Data domains and classification

| Domain | Example fields | Proposed classification | Purpose |
| --- | --- | --- | --- |
| Laboratory result | analyte, value, unit, method, instrument | Internal operational data | Quality and process analysis |
| Sample/batch reference | sample_id, batch_id | Internal, potentially confidential | Trace results to a sample and production batch |
| Source lineage | source hash, file name, CSV row number, ingestion times | Internal audit/operational metadata | Reproduce and investigate ingestion |
| Quarantine details | raw row, validation errors | Internal; may include sensitive identifiers | Correct source data and monitor data quality |
| Credentials/configuration | database passwords, Airflow auth | Secret | Authenticate services |

No human or patient data is required by the current contract. Do not put
personal, customer-confidential, export-controlled, or regulated information
into sample fields without a reviewed data-classification and access design.

## Data contract

The CSV contract requires `sample_id`, `batch_id`, `analyte`, `value`, `unit`,
`measured_at`, `method`, and `instrument`. Header names are case-sensitive
after whitespace trimming. See the [README](../README.md#input-csv-format) for
the field semantics, accepted unit aliases, timestamp rules, and validation
behavior.

Kafka messages have `schema_version: 1`, a deterministic `event_id`, a
`source_sha256`, and an event type. File lifecycle events are
`file.started`, `file.completed`, and `file.failed`; row events use
`lab_result` and distinguish `accepted` from `quarantined`. Consumers reject
unknown versions and malformed identities. Breaking message changes require
an explicit versioned topic or schema migration plan; the current demo does
not use an external schema registry.

## Lineage

For accepted and quarantined rows, lineage connects:

1. the original content-addressed CSV archive;
2. the SHA-256 source identifier and CSV record number;
3. the deterministic row/event identifier;
4. the Kafka event and PostgreSQL raw row; and
5. dbt staging and mart models.

The raw table retains both normalized and parsed source-row JSON. The original
CSV bytes remain the authoritative source representation. SHA-256 identifies
content but does not prove who produced it or make storage immutable.

## Quality and stewardship

- The producer performs required-field, decimal, supported-unit, and timestamp
  checks before publishing.
- PostgreSQL constraints enforce event identity, status, row-count, and JSON
  shape invariants.
- dbt source and model tests cover required identifiers, uniqueness,
  relationships, accepted statuses/units, and key model fields.
- Quarantined rows remain visible for investigation; correction and
  re-ingestion are human-controlled.
- A laboratory or quality data steward should own the analyte/method/unit
  dictionary, acceptable ranges, source-system mappings, and quarantine
  disposition process. No such site-specific owner or specification is
  configured in this demo.

The producer validates supported spellings but does not convert values or
check an analyte against a laboratory specification. Do not treat dbt
aggregates as quality release decisions.

## Retention and access

No retention, deletion, backup, or archival policy is implemented. Define
those rules with data owners and applicable quality/compliance teams. Protect
the raw archive, PostgreSQL, Kafka volumes, Airflow logs, and exported marts
according to the classification of their data. Restrict access to quarantine
payloads and raw rows to authorized laboratory and platform roles.

The PostgreSQL demo uses separate `pipeline_ingest`, `pipeline_reader`, and
`dbt_transform` roles. These are service roles, not per-human identities.
Airflow Simple Auth Manager is for local development only. Production requires
an approved identity provider, auditable access reviews, managed secrets,
encrypted transport and storage, backup/restore testing, and a validated
record-retention plan.
