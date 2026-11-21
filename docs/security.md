# Security design and threat boundaries

This is a local portfolio demonstration, not a production security
certification. Security controls are included to make trust boundaries
visible; their effectiveness depends on deployment configuration and a
supporting infrastructure.

## Trust boundaries

```text
untrusted CSV
    |
    v
Python validator + content-addressed raw archive
    |
    v
Kafka topic (validated event envelope)
    |
    v
least-privilege PostgreSQL ingestion role
    |
    v
dbt transform role -> staging/marts
    ^
    |
Airflow orchestration, not a plant control channel
```

CSV files and Kafka payloads are data, not executable instructions. The
consumer validates event versions, event IDs, row numbers, status values,
file-name basenames, and basic payload shapes before writing to PostgreSQL.
SQL values are passed as parameters; dbt uses project-controlled SQL. No
component sends commands to manufacturing equipment.

## Implemented controls

- Source CSV bytes are SHA-256 addressed and archived before parsing. CSV
  inputs are parsed with Python's strict CSV reader; individual invalid rows
  are quarantined instead of silently discarded.
- Kafka publishing requests `acks=all`, retries transient sends, and uses a
  stable source hash as the message key. The consumer disables automatic
  offset commits, commits only after its PostgreSQL transaction, and uses
  deterministic event IDs plus database uniqueness constraints for
  idempotent replay. This is **at-least-once**, not cross-system
  exactly-once processing.
- PostgreSQL checks constrain digest shape, lifecycle/status values, JSON
  object/array types, nonnegative counts, and row-count reconciliation.
- PostgreSQL DDL is applied by a separate migration command with an advisory
  lock, a transactional checksum ledger, and immutable applied migration
  files. Airflow also reconciles completed file manifests against persisted
  accepted/quarantined row events before running dbt.
- Kafka clients can use TLS with a configured CA, and `SASL_SSL` with PLAIN or
  SCRAM credentials. TLS certificate verification is enabled; credentials with
  `PLAINTEXT` are rejected. The Compose broker remains plaintext and no
  broker-side ACLs are configured.
- Airflow failure callbacks can POST a minimal run/task identifier payload to
  an explicitly configured HTTPS webhook, with an optional bearer token from
  separate secret injection. Redirects and query-string credentials are
  rejected; a missing endpoint is logged, and notifications are off by default.
- PostgreSQL uses distinct ingestion, read-only status, and dbt transform
  roles. The consumer is not a database superuser.
- The Compose demo keeps PostgreSQL and Kafka on an internal-only network and
  off host-published ports. Airflow joins that network plus a separate UI
  network; only the UI is published to host loopback (`127.0.0.1`).
- The consumer container runs non-root, drops Linux capabilities, uses a
  read-only root filesystem, and disables privilege escalation.
- Kubernetes examples use a restricted service account, non-root security
  context, read-only root filesystem, dropped capabilities, resource
  requests/limits, and an egress NetworkPolicy.
- `.env`, local data, generated Airflow auth files, logs, and build artifacts
  are excluded from Git. `.env.example` contains placeholders only.

## Demo-only limitations

- Kafka uses plaintext inside an isolated local Docker network. The client
  supports TLS/SASL_SSL for an appropriately configured external broker, but
  there is no broker authentication, ACL, schema registry, or production
  multi-broker replication in this Compose demo.
- Kubernetes manifests expect Kafka/PostgreSQL services and labels in the same
  namespace. NetworkPolicy is enforced only by a compatible cluster CNI, and
  DNS/egress rules may need adjustment for a cluster's DNS and managed
  services.
- Compose uses a single Kafka broker and PostgreSQL instance; named volumes
  are not backups or high availability.
- Airflow `standalone` and Simple Auth Manager are local-development
  components. They do not provide production identity, scheduler resilience,
  metadata-database HA, or an approval boundary.
- Secrets passed as environment variables may be visible to privileged
  container/runtime operators. Use a managed secrets system and encrypt
  Kubernetes Secrets at rest in production.
- The code does not impose data retention, per-user access, electronic
  signatures, immutable audit logs, or regulatory validation.
- The optional webhook is not a monitoring service or incident-response
  process. Delivery, escalation, SLOs, and endpoint credential management must
  be configured and tested by the deployment owner.

## Local secret setup

Copy `.env.example` to `.env` and use distinct random hexadecimal values for
each password, for example `openssl rand -hex 32`. Hex avoids URI-reserved
characters in the service database URLs. Keep `.env` out of source control,
limit filesystem permissions, and rotate all values that may have been
exposed.

## Before production

See the [production-readiness register](production-readiness.md) for all
remaining infrastructure and approval gates. Do not connect this demo to an
OT or process-control network without a separate approved architecture.
