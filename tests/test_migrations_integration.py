from __future__ import annotations

import hashlib
import os
import unittest
import uuid

from chemical_pipeline.migrations import apply_migrations
from chemical_pipeline.reconciliation import reconcile_source_files


DATABASE_URL = os.environ.get("PIPELINE_TEST_ADMIN_DB_URL")


@unittest.skipUnless(DATABASE_URL, "PostgreSQL integration URL is not configured")
class PostgresMigrationIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        import psycopg
        from psycopg import sql

        with psycopg.connect(DATABASE_URL, autocommit=True) as connection:
            for role in ("pipeline_ingest", "dbt_transform", "pipeline_reader"):
                exists = connection.execute(
                    "SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)
                ).fetchone()
                if not exists:
                    connection.execute(
                        sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role))
                    )

    def test_migration_repeatability_and_row_reconciliation(self) -> None:
        import psycopg

        applied = apply_migrations(DATABASE_URL)
        self.assertIn("0001", applied)
        self.assertEqual(apply_migrations(DATABASE_URL), [])

        source_hash = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
        event_id = hashlib.sha256(f"{source_hash}:row:2".encode()).hexdigest()
        with psycopg.connect(DATABASE_URL) as connection:
            connection.execute(
                """
                INSERT INTO raw.ingestion_files (
                    source_sha256, source_filename, byte_count, ingested_at,
                    status, total_rows, accepted_rows, rejected_rows
                ) VALUES (%s, 'integration.csv', 100, now(), 'processing', 0, 0, 0)
                """,
                (source_hash,),
            )
            connection.execute(
                """
                INSERT INTO raw.lab_result_events (
                    event_id, source_sha256, row_number, status, record,
                    raw_record, validation_errors, occurred_at
                ) VALUES (%s, %s, 2, 'accepted', '{}'::jsonb, '{}'::jsonb,
                          '[]'::jsonb, now())
                """,
                (event_id, source_hash),
            )
            connection.execute(
                """
                UPDATE raw.ingestion_files
                SET status = 'completed', total_rows = 1, accepted_rows = 1
                WHERE source_sha256 = %s
                """,
                (source_hash,),
            )
            self.assertEqual(
                reconcile_source_files(connection, [source_hash]), 1
            )
            connection.execute(
                """
                UPDATE raw.ingestion_files
                SET total_rows = 2, accepted_rows = 2
                WHERE source_sha256 = %s
                """,
                (source_hash,),
            )
            with self.assertRaisesRegex(RuntimeError, "count mismatch"):
                reconcile_source_files(connection, [source_hash])


if __name__ == "__main__":
    unittest.main()
