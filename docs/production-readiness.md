# Production readiness register

**Current decision: NOT PRODUCTION READY.** This register tracks implementation
evidence and the remaining plant-specific approvals. “Implemented” means a
generic code path or local reference exists; it is not evidence that a
production service has been deployed, validated, or approved.

The supplied operational requirements identify the LIMS, instruments,
analytes, methods, specifications, units, timezone, infrastructure providers,
retention, recovery objectives, alert destinations, and applicable standards
as **not yet defined**. No vendor, regulatory, laboratory, or plant behavior is
assumed in this project.

## Status by requirement

| Requirement | Current evidence | Status and remaining gate |
| --- | --- | --- |
| Site-specific LIMS and instrument integration | CSV contract and synthetic fixture only | **Blocked.** Select actual systems, interfaces, authentication, and reviewed field mappings. |
| Analyte/method/specification rules | Generic required-field and finite-decimal validation | **Blocked.** Laboratory and quality owners must approve analyte/method mappings, limits, QC rules, and disposition. |
| Units, conversions, timezone, classification | Supported unit spellings only; no conversions or plant timezone inferred; classification is a proposal | **Blocked.** Laboratory/metrology and data owners must approve units, precision, timezone, classification, and access restrictions. |
| Operational alerting | Optional HTTPS Airflow failure webhook with a separately injected bearer token; minimal run/task identifiers only | **Partial.** Configure an approved endpoint and token through a managed secret, test delivery/escalation, and define SLOs and on-call ownership. |
| Operational reconciliation | Airflow compares terminal manifests with persisted accepted/quarantined row counts before dbt | **Implemented in code; not deployed.** Add recovery procedures and validate behavior against real operational failure scenarios. |
| PostgreSQL schema migrations | One-shot runner, advisory lock, per-migration transaction, checksum ledger, integration test | **Implemented for the demo baseline.** Production migration identity, grants, backup/rollback strategy, and release approval remain to be designed. |
| Backward compatibility | Policy below: additive database changes and versioned event/topic contracts | **Policy documented, not a complete compatibility test matrix.** Add tests for each approved application/database version before release. |
| Kafka/PostgreSQL/Airflow high availability | Compose uses one Kafka broker, one PostgreSQL instance, and Airflow `standalone` | **Blocked.** Choose an approved platform, topology, replication/failover, Airflow executor and HA metadata database; exercise failover. |
| Secure Kafka transport and ACLs | Producer/consumer support TLS and SASL over TLS with CA verification; local broker remains plaintext | **Partial.** Configure broker-side TLS, principals, topic/group ACLs, certificate rotation, and managed-broker policy. |
| Managed secrets | Local `.env` and Kubernetes Secret examples only | **Blocked.** Select a secrets manager/operator, identity integration, rotation, and encryption-at-rest policy. Never commit secret values. |
| Encrypted backups and restore tests | No backup or restore implementation | **Blocked.** Set RPO/RTO, key custody, backup scope, retention, off-site protections, and recurring restore evidence. |
| Retention enforcement | No automatic deletion or archival | **Blocked.** Establish approved retention, legal/quality holds, quarantine treatment, and deletion authorization before implementation. |
| Auditable human access controls | Separate PostgreSQL service roles; no individual identity or immutable access audit | **Partial.** Integrate approved identity, human/service role mapping, audit logging, access reviews, and evidence retention. |
| Formal quality/security/compliance validation | Unit tests, a PostgreSQL migration/reconciliation integration test, and dbt tests | **Not performed.** Applicable standards, validation protocol, evidence repository, reviewers, and authorized sign-off are undefined. |

## Migration and compatibility policy

PostgreSQL migrations are named `NNNN_description.sql` and recorded in
`pipeline_meta.schema_migrations` with a SHA-256 checksum. The runner serializes
concurrent executions with a PostgreSQL advisory lock and applies each
migration and ledger entry in one transaction. An applied migration is
immutable: a checksum change stops execution. Add a new migration instead.

Use an expand-and-contract rollout for changes that might overlap application
versions:

1. Add compatible schema elements without removing or changing existing
   behavior.
2. Deploy readers/writers that tolerate both old and new representations.
3. Backfill and verify data; run compatibility and reconciliation tests.
4. Switch producers/readers only after the overlap version is proven.
5. Remove obsolete schema only in a later approved release after old
   application versions are retired.

For Kafka, additive envelope fields may be introduced without changing
`schema_version` only when old consumers can ignore them. A breaking contract
change requires a new schema version and versioned topic, a documented
dual-read/producer migration window, and tests for every supported consumer.
The current demo has no schema registry or automated multi-version matrix.
SQLite demo tables do not have a migration mechanism.

## Deployment and approval gates

Before any production connection or production-readiness claim:

1. Record system, instrument, field, unit, method, specification, timezone,
   classification, owner, and steward decisions approved by the relevant
   stakeholders.
2. Select the target infrastructure and configure HA, TLS, broker/database
   identities and ACLs, managed secrets, network policy, monitoring, and
   escalation.
3. Define backup, restore, disaster-recovery, retention, legal-hold, access
   audit, and change-management procedures; exercise and retain evidence.
4. Execute the approved quality/security validation protocol against that
   deployment and obtain formal laboratory, quality, security, compliance,
   plant-operations, and infrastructure approvals as applicable.
5. Record a release-specific validation result. Passing this repository's
   automated tests alone is not production acceptance.

Do not connect this demonstration to a plant control network or use its
analytical outputs for laboratory release decisions.
