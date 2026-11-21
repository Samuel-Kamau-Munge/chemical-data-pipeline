# Real-Time Chemical Lab Data Platform

An operational, near-real-time data pipeline for chemical laboratory results.
Airflow polls the incoming-file location every minute; each new CSV is
validated and streamed as keyed Kafka events, persisted in PostgreSQL,
reconciled, and transformed and tested by dbt. Docker Compose runs the
integrated pipeline locally, and Kubernetes manifests support deployment of
the consumer workload.

> **Integration boundary:** The current source adapter consumes CSV exports.
> Plant LIMS and instrument interfaces, site-approved laboratory rules,
> high-availability infrastructure, and formal production validation still
> require configuration and approval by the responsible plant teams. This
> pipeline is not a manufacturing or process-control system.

## Current operating capabilities

| Area | Project evidence | Current status |
| --- | --- | --- |
| Python ingestion | Strict CSV parser, content-addressed raw archive, deterministic row IDs, event producer | Implemented |
| Data validation | Required-field, finite-decimal, unit-alias, timestamp, Kafka-envelope, PostgreSQL constraint, and dbt tests | Implemented |
| Kafka | Keyed versioned events, topic setup, consumer group, manual offset commits, optional TLS/SASL_SSL client support | Implemented; at-least-once; local broker plaintext |
| PostgreSQL | Raw event/manifests tables, constraints, service roles, checksummed versioned migrations, ingestion reconciliation | Implemented |
| dbt | Source declarations, staging views, analytical marts, schema tests, generated schema strategy | Implemented; run with Compose |
| Airflow | Every-minute file polling, publish, wait, reconcile, `dbt build`, report; optional HTTPS failure webhook | Implemented; notifications require configuration |
| Kubernetes | Restricted consumer Deployment, ServiceAccount, resource limits, probes, and egress NetworkPolicy | Deployment manifest; requires cluster services |
| Governance | Data contract, proposed classification, lineage, ownership, retention and quality responsibilities | Documented; site approval outstanding |
| Pipeline security | Least-privilege roles, optional Kafka TLS clients, HTTPS alert hook, container hardening, network policy | Controls implemented; production review outstanding |
| CI | Python unit tests, dbt parse, PostgreSQL migration/reconciliation integration test | Configured in GitHub Actions |

The Compose services and DAG have been exercised together locally, including
Kafka ingestion, PostgreSQL persistence, reconciliation, and dbt tests.
Kubernetes deployment and site-specific LIMS/instrument integration have not
been exercised or configured.

## Data engineering problem addressed

The pipeline addresses a common lab-data ingestion problem: CSV batches can
contain a mix of usable results and malformed measurements or unsupported
units. Loading such files directly into analytics risks either rejecting the
whole batch or allowing bad values into downstream reporting. The pipeline
validates each row, retains the original input, quarantines invalid rows with
explicit reasons, and continues processing valid rows. Kafka events, durable
PostgreSQL manifests, reconciliation, and dbt tests provide traceability from
source file through analytical models.

The included synthetic fixture makes this behavior concrete. These are
fixture errors, not records from a plant or evidence of a historical incident:

| GitHub CSV line | Input | Validation finding and disposition |
| --- | --- | --- |
| [Line 4](https://github.com/Samuel-Kamau-Munge/chemical-data-pipeline/blob/main/examples/lab-results.csv#L4) | `S-102,B-42,chloride,not-a-number,mg/L,2026-11-19T10:20:00Z,IC,IC-1` | `value must be a finite decimal number`; the original row is quarantined. |
| [Line 5](https://github.com/Samuel-Kamau-Munge/chemical-data-pipeline/blob/main/examples/lab-results.csv#L5) | `S-103,B-42,chloride,2.60,unsupported-unit,2026-11-19T10:30:00Z,IC,IC-1` | `unit is not in the supported alias list: unsupported-unit`; the original row is quarantined. |

For this four-row fixture, the exercised pipeline accepted two rows and
quarantined two. It also avoids republishing a file whose content hash is
already marked completed, commits Kafka offsets only after PostgreSQL commits,
and checks manifest counts against persisted events before dbt runs. These are
implemented behavior and fixture-level evidence, not measurements from a
production plant.

This repository does **not** substantiate reductions in query processing
time, Azure costs, or pipeline downtime. It contains no Azure deployment or
billing data, no before/after query benchmark, and no production incident or
availability history. The one-minute Airflow poll is a pickup cadence, not a
processing-time or uptime guarantee; the local Compose stack is not highly
available. Those claims require measurements from the target environment.

## Architecture

```text
Incoming CSV export (polled every minute)
   |
   v
Airflow DAG -> Python parser, validation, raw-byte archive
                         |
                         v
                  Kafka: lab-results.v1
                         |
                         v
                 Python consumer group
                         |
                         v
           PostgreSQL raw schema (events + manifests)
                         |
                         v
                  dbt staging views
                         |
                         v
                dbt analytical marts
```

Airflow checks for new files every minute and coordinates the workflow; Kafka
decouples validated row events from database persistence; PostgreSQL is the
analytical source of truth; dbt transforms and tests the raw records. The
Kubernetes deployment targets the consumer workload. Docker Compose supplies
the single-machine runtime.

## Repository layout

```text
chemical_pipeline/
  pipeline.py                 CSV parsing, validation, SQLite compatibility mode
  streaming.py                Validated, versioned Kafka producer
  postgres_consumer.py        Kafka consumer, PostgreSQL writes, event checks
  kafka_config.py             Optional TLS/SASL_SSL client configuration
  migrations.py               Locked, checksummed PostgreSQL migration runner
  reconciliation.py           PostgreSQL manifest-to-event reconciliation
  alerting.py                 HTTPS Airflow failure webhook
  cli.py                      Local and streaming command-line interface
dags/
  chemical_lab_csv_pipeline.py
                               Airflow orchestration DAG
dbt/
  dbt_project.yml
  profiles.yml                Environment-variable-only database credentials
  macros/generate_schema_name.sql
  models/
    sources.yml               PostgreSQL raw source contract
    staging/                  Typed accepted and quarantine views
    marts/                    Batch/analyte and ingestion-quality models
    schema.yml                dbt data-quality tests and descriptions
infra/postgres/init/
  001_roles.sh                Local service roles
infra/postgres/migrations/
  0001_initial_schema.sql     Versioned PostgreSQL baseline
k8s/base/                     Kustomize consumer Deployment and policy
docs/
  data-governance.md
  security.md
  production-readiness.md     Status register and unresolved approval gates
examples/
  lab-results.csv             Synthetic test fixture; not plant data
tests/
  test_pipeline.py
  test_events.py
  test_migrations.py
  test_reconciliation.py
  test_migrations_integration.py
compose.yaml                  Local Kafka, PostgreSQL, consumer, and Airflow
Dockerfile                    Non-root Python consumer image
Dockerfile.airflow            Airflow/dbt runtime image
scripts/init-local.sh         Generates local credentials and prepares data folders
.github/workflows/ci.yml      Unit tests and dbt parse checks
```

## End-to-end data contract

### Input CSV

Required, case-sensitive headers (surrounding header whitespace is trimmed):

```text
sample_id,batch_id,analyte,value,unit,measured_at,method,instrument
```

See [`examples/lab-results.csv`](examples/lab-results.csv) for synthetic
accepted and invalid rows. The core parser reads UTF-8 with an optional BOM,
rejects duplicate/empty/missing headers and malformed CSV, and retains
additional columns in the raw record.

Row validation:

- Every required field must be non-empty.
- `value` must be a finite decimal, retained as text to avoid binary
  floating-point rounding.
- Supported unit spellings are `mg/L`, `g/L`, `ug/L` (also `µg/L`/`μg/L`),
  `mg/kg`, `g/kg`, `%`/`percent`, and `pH`. Aliases normalize spelling only;
  no conversion or analyte equivalence is inferred.
- `measured_at` must be ISO-8601 parseable. Offset-aware timestamps normalize
  to UTC. Offset-free timestamps remain timezone-unknown; no plant timezone is
  guessed.
- A rejected row is retained in quarantine with its original parsed payload
  and validation messages. Structurally invalid files fail as a whole.

No sample/analyte specification ranges, method-specific unit rules, or
business-key deduplication are configured.

### Kafka event envelope

Topic: `lab-results.v1`, created with three partitions in the local Compose
deployment. Messages use the source SHA-256 as the Kafka key so events for the
same file are ordered in one partition.

All envelopes contain:

```json
{
  "schema_version": 1,
  "event_id": "sha256-hex",
  "event_type": "lab_result",
  "source_sha256": "sha256-hex",
  "occurred_at": "ISO-8601 UTC timestamp"
}
```

File lifecycle events are `file.started`, `file.completed`, and `file.failed`.
Row events use `event_type: "lab_result"` and a status of `accepted` or
`quarantined`, plus the CSV row number, normalized or original record,
`raw_record`, and validation errors. The event ID is deterministic from the
source hash and row number (or the file lifecycle event), enabling database
deduplication and replay.

The producer waits for Kafka acknowledgements (`acks=all`). The consumer
disables automatic offset commits and commits a Kafka offset only after the
corresponding PostgreSQL transaction succeeds. A replay after a database
commit/offset failure is safe because PostgreSQL event IDs and source-row
identities are unique. This is **at-least-once delivery with idempotent
database writes**, not distributed exactly-once processing.

### PostgreSQL raw model

The versioned migration creates `raw.ingestion_files` and
`raw.lab_result_events`. A dedicated one-shot Compose service applies pending
migrations before starting the consumer or Airflow.

`raw.ingestion_files` columns:

```text
source_sha256 CHAR(64) PRIMARY KEY
source_filename TEXT NOT NULL
byte_count BIGINT NOT NULL
ingested_at TIMESTAMPTZ NOT NULL
completed_at TIMESTAMPTZ NULL
status TEXT NOT NULL                 -- processing | completed | failed
total_rows INTEGER NOT NULL
accepted_rows INTEGER NOT NULL
rejected_rows INTEGER NOT NULL
last_error TEXT NULL
```

`raw.lab_result_events` columns:

```text
event_id CHAR(64) PRIMARY KEY
source_sha256 CHAR(64) NOT NULL REFERENCES raw.ingestion_files
row_number INTEGER NOT NULL
status TEXT NOT NULL                 -- accepted | quarantined
record JSONB NOT NULL                -- normalized accepted row or raw rejected row
raw_record JSONB NOT NULL            -- parsed CSV row before normalization
validation_errors JSONB NOT NULL     -- JSON array
occurred_at TIMESTAMPTZ NOT NULL
```

Database checks constrain digest shape, lifecycle/status values, JSON object or
array types, nonnegative counters, row numbers, and file-count reconciliation.
The event table also has a unique `(source_sha256, row_number)` constraint and
indexes on status and source/row.

The consumer has only `SELECT`, `INSERT`, and `UPDATE` on these two raw tables;
it is not a database superuser. `pipeline_reader` can read ingestion status.
`dbt_transform` can read raw and create models in the `staging` and `marts`
schemas. Credentials are separate and supplied through the local `.env`, not
stored in the repository.

### SQLite CLI model

The original `ingest`/`export` commands remain as a dependency-free,
single-process SQLite compatibility path and are covered by unit tests. They
use a separate SQLite database, not the Kafka/PostgreSQL Compose path:

- `ingestion_files(source_sha256, source_filename, ingested_at, byte_count,
  total_rows, accepted_rows, rejected_rows)` stores per-file metadata.
- `lab_results(record_id, source_sha256, row_number, sample_id, batch_id,
  analyte, value, unit, original_value, original_unit, measured_at,
  timestamp_timezone_known, method, instrument, raw_record_json)` stores
  accepted rows.
- `quarantined_rows(record_id, source_sha256, row_number, errors_json,
  raw_record_json)` stores invalid rows and validation messages.

The SQLite schema is created by `connect()` in `chemical_pipeline/pipeline.py`
and has no migration mechanism. The API also exposes the frozen `IngestResult`
dataclass with `source_sha256`, `status`, `total_rows`, `accepted_rows`, and
`rejected_rows`. The streaming path shares the CSV parser and validator but
persists to PostgreSQL instead.

### dbt models

| Model | Type | Purpose |
| --- | --- | --- |
| `stg_lab_results` | View in `staging` | Cast accepted JSON records to typed fields; retains source hash, row, original values, method, instrument, unit, and timestamp certainty |
| `stg_quarantined_results` | View in `staging` | Exposes rejected payload fields and validation errors for review |
| `fct_batch_analyte_quality` | Table in `marts` | Counts, distinct samples, min/max/average, methods, measurement window, timezone-unknown count, grouped by batch/analyte/unit (units are never mixed) |
| `agg_ingestion_quality` | Table in `marts` | File status, total/accepted/rejected counts, rejection fraction, and file-level error |

dbt declares raw sources and tests key fields, uniqueness, source relationships,
accepted statuses/units, and basic model invariants. dbt tests do not represent
laboratory release criteria or establish that a result is within specification.
The `generate_schema_name` macro maps custom schemas directly to `staging` and
`marts` (instead of prefixing them with the profile's `analytics` schema).

## Run the real-time stack with Docker Compose

Requirements: Docker Engine and the Docker Compose v2 plugin. From the project
root:

The Compose stack pins PostgreSQL `16.6-alpine`, Apache Kafka `4.0.0`, and
Airflow `3.1.0` on Python `3.12`; the Python consumer uses
`kafka-python==2.2.15` and `psycopg==3.2.9`, while dbt uses
`dbt-core==1.7.10` and `dbt-postgres==1.7.10`. Airflow `standalone` uses its own
SQLite metadata database in the `airflow_data` volume; PostgreSQL is the
pipeline's raw/analytics database.

```bash
./scripts/init-local.sh
docker compose up --build -d
docker compose ps
```

The setup script creates a local `.env` with four distinct random hexadecimal
passwords and prepares `data/`. The file is mode `0600` and ignored by Git.
Never commit `.env`; use a managed secrets system for shared environments.
Kafka and PostgreSQL stay on an internal-only Compose network; Airflow joins a
separate UI network and publishes only its web UI to host loopback
(`127.0.0.1:8081`).

The included synthetic CSV is a test fixture, not operational plant data. To
exercise the pipeline, copy it into the watched folder:

```bash
cp examples/lab-results.csv data/incoming/
```

The Airflow DAG is paused on first initialization. Open
<http://127.0.0.1:8081>, obtain the generated development admin password from
the local Airflow container output or its Airflow home, inspect
`chemical_lab_csv_pipeline`, then unpause it. The DAG:

1. checks `data/incoming/` every minute and publishes each new `.csv`;
2. waits until Kafka events are persisted into PostgreSQL;
3. reconciles each file manifest with persisted row-event counts;
4. runs `dbt build` (models plus data tests); and
5. reports file errors and failed upstream tasks.

Failed Airflow tasks can send a minimal HTTPS webhook notification when
`PIPELINE_ALERT_WEBHOOK_URL` is configured, with an optional bearer token in
`PIPELINE_ALERT_WEBHOOK_TOKEN`. Keep tokens out of URLs and inject them through
a managed secret in shared environments. The payload contains only DAG, task,
run, and attempt identifiers; it contains no sample or result values. Without
an endpoint, failures remain visible in Airflow state and logs, but no external
notification is sent.

Successfully ingested files remain in the incoming folder but are not
republished: the DAG checks the content hash against completed PostgreSQL
manifests. A changed file produces a new hash and a new ingestion. `data/raw/`
retains content-addressed CSV bytes, and generated outputs and Airflow/dbt
artifacts are excluded from Git. Producers should write to a temporary path
and atomically rename completed CSVs into `data/incoming/`.

Inspect service logs:

```bash
docker compose logs --tail=100 airflow
docker compose logs --tail=100 consumer
docker compose logs --tail=100 kafka
docker compose logs --tail=100 postgres
```

Stop the local stack without deleting its databases or volumes:

```bash
docker compose down
```

The `migrate` service maintains `pipeline_meta.schema_migrations`, takes a
PostgreSQL advisory lock, applies each SQL migration transactionally, and
refuses to run if an already-applied migration's checksum changes. Future
changes must be forward-only and preserve the prior application contract
during rollout.

`docker compose down -v` deletes the stack's PostgreSQL, Kafka, and Airflow
volumes. Use that only when intentionally resetting all local stack state.

### CLI

The standard-library SQLite compatibility path remains available:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
python -m chemical_pipeline ingest examples/lab-results.csv
python -m chemical_pipeline export
```

Install optional streaming clients for host-side Kafka/PostgreSQL commands:

```bash
python -m pip install -e '.[streaming]'
```

For a configured PostgreSQL environment, use the admin-only migration command
and the read-only reconciliation command:

```bash
PIPELINE_ADMIN_DB_URL="$PIPELINE_ADMIN_DB_URL" \
  python -m chemical_pipeline migrate
PIPELINE_READER_DB_URL="$PIPELINE_READER_DB_URL" \
  python -m chemical_pipeline reconcile <source-sha256> [<source-sha256> ...]
```

The streaming commands are also available:

```bash
python -m chemical_pipeline publish examples/lab-results.csv \
  --bootstrap-servers localhost:9092
PIPELINE_DB_URL="$PIPELINE_DB_URL" \
  python -m chemical_pipeline consume --bootstrap-servers localhost:9092
```

In the local Compose deployment, Kafka and PostgreSQL are intentionally not
exposed on host ports. Use the Airflow DAG or execute publisher commands from
a container on the Compose network.

The local broker uses plaintext Kafka. For a TLS-enabled external broker, set
`PIPELINE_KAFKA_SECURITY_PROTOCOL=SSL` or `SASL_SSL` and
provide a trusted `PIPELINE_KAFKA_SSL_CA_FILE`; `SASL_SSL` additionally requires
`PIPELINE_KAFKA_SASL_MECHANISM`, `PIPELINE_KAFKA_SASL_USERNAME`, and
`PIPELINE_KAFKA_SASL_PASSWORD`. Credentials over plaintext are rejected. Broker
ACLs and principal provisioning remain responsibilities of the Kafka platform.

## Airflow

The DAG ID is `chemical_lab_csv_pipeline`, scheduled every minute with catchup
disabled and at most one active DAG run. This gives new files a polling delay
of up to one minute before validation and Kafka publication, plus processing
time; it is near-real-time micro-batching, not a hard latency guarantee.
Naive measurement timestamps remain timezone-unknown until the site timezone
is formally established. Its task graph is:

```text
publish_incoming_files
        |
        v
wait_for_postgres
        |
        v
reconcile_postgres
        |
        v
dbt_build
        |
        v
report_run
```

Completed files are hash-checked and skipped; empty polling intervals skip
downstream work. `wait_for_postgres` is a rescheduling sensor, not a busy loop.
Airflow task states expose failures at each stage. Failure webhooks use the
optional HTTPS endpoint described above.

The Airflow image uses Airflow `3.1.0`, Python `3.12`, and dbt-postgres
`1.7.10`, with dbt and protobuf aligned to the Airflow 3.1.0 constraints.
Airflow `standalone` and its Simple Auth Manager are for local operation only.
For production, deploy separately managed Airflow components with an approved
identity provider and metadata database.

## Kubernetes deployment

`k8s/base/` contains a Kustomize deployment for the non-root Kafka/PostgreSQL
consumer, plus a restricted ServiceAccount, resource requests/limits,
startup/readiness probes, optional Kafka CA Secret mount, and an egress
NetworkPolicy. It deliberately does not deploy Kafka, PostgreSQL, or Airflow.
Provision those services and the
PostgreSQL schema/roles first. The manifests expect Kafka/PostgreSQL pods
labeled `app.kubernetes.io/name: kafka` and `app.kubernetes.io/name: postgres`
in the `chemical-lab` namespace. The default Kafka service address is
`kafka-bootstrap.chemical-lab.svc.cluster.local:9092`; adjust the ConfigMap
for your cluster.

Build the consumer image and load it into the cluster (or publish it to an
approved registry and update `k8s/base/deployment.yaml`):

```bash
docker build -t chemical-lab-pipeline:local .
# For kind, for example:
kind load docker-image chemical-lab-pipeline:local --name <cluster-name>
```

Create a protected local environment file outside version control containing
the database URL:

```text
PIPELINE_DB_URL=postgresql://pipeline_ingest:<hex-password>@postgres.chemical-lab.svc.cluster.local:5432/chemical_lab
```

After creating that file under the ignored `.secrets/` directory, create the
namespace and Secret, then apply the Kustomize base:

```bash
kubectl apply -f k8s/base/namespace.yaml
kubectl create secret generic pipeline-consumer-secrets \
  --namespace chemical-lab \
  --from-env-file=.secrets/consumer.env
kubectl apply -k k8s/base
kubectl -n chemical-lab get deployment,pods
```

Restrict the environment file to its owner (`chmod 600`) and do not put real
passwords in a committed YAML manifest or command history. Use your cluster's
secret-management integration for shared environments. For an external
`SASL_SSL` broker, set the ConfigMap protocol to `SASL_SSL`, populate the
SASL username/password keys in the mounted consumer Secret, and create the
`kafka-ca` Secret with its `ca.crt` key. This Kubernetes Secret configuration
is a deployment option, not a managed secrets integration. The NetworkPolicy
requires a CNI that enforces Kubernetes NetworkPolicy and may need adaptation
for a managed Kafka/PostgreSQL service or different cluster DNS. These are
application deployment manifests, not a complete production cluster chart.

## Data governance and security

The project proposes classifications for lab values, sample/batch identifiers,
lineage, raw quarantine payloads, and service credentials, and documents
ownership, quality responsibilities, and unresolved retention requirements in
[`docs/data-governance.md`](docs/data-governance.md). The threat boundaries,
implemented controls, and gaps are in [`docs/security.md`](docs/security.md).

Implemented pipeline controls include:

- validation before publishing and explicit quarantine instead of silent drops;
- deterministic identities, raw source retention, database constraints, and
  replay-safe writes;
- checksummed PostgreSQL schema migrations and manifest-to-row reconciliation;
- optional TLS/SASL_SSL client transport and HTTPS Airflow failure notification;
- least-privilege database service roles;
- non-root consumer containers with a read-only root filesystem and dropped
  Linux capabilities;
- Compose services on an internal network, with only the local Airflow UI
  bound to loopback;
- restricted Kubernetes service account and egress policy; and
- ignored local credentials and generated data.

The local broker is plaintext; client TLS/SASL_SSL support does not configure
broker ACLs or managed identities. The project also lacks encryption at rest,
production identity/authorization, backups, retention enforcement, signed or
immutable audit records, an external schema registry, and regulatory
validation. The synthetic CSV format has no human or patient data. Do not
connect the pipeline to a plant control network or use its marts for release
decisions.

## Tests and CI

Run unit tests:

```bash
python3 -m unittest discover -s tests -v
```

The unit suite exercises SQLite ingestion/export, normalization, quarantine,
idempotency, Kafka envelope and security configuration, migration discovery,
alert payloads, and reconciliation behavior. GitHub Actions runs the suite on
Python 3.10, 3.11, and 3.12, a no-database `dbt parse`, and a PostgreSQL-backed
migration/reconciliation integration test.

The full Kafka/PostgreSQL/Airflow/dbt flow requires Docker Compose and has not
been run in this environment because Docker is unavailable. The Kubernetes
manifest requires a cluster; the project does not claim to have deployed it.

## Production readiness

The project now has a versioned database migration path, event-count
reconciliation, optional HTTPS failure notifications, and optional secure
Kafka client transport. It remains a hardening reference, **not production
ready**: site-specific interfaces and laboratory rules are undefined, the
local services are not highly available, and security, backup, retention,
audit, quality, and compliance controls still require environment-specific
implementation and owner approval. See the
[production-readiness register](docs/production-readiness.md) for evidence
and unresolved gates.
