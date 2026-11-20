from __future__ import annotations

import unittest

from chemical_pipeline.reconciliation import reconcile_source_files


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.parameters = None

    def execute(self, query, parameters):
        self.parameters = parameters
        return self

    def fetchall(self):
        return self.rows


class ReconciliationTests(unittest.TestCase):
    def test_accepts_matching_completed_file_counts(self) -> None:
        source_hash = "a" * 64
        connection = FakeConnection(
            [(source_hash, "completed", 3, 2, 1, 3, 2, 1)]
        )
        self.assertEqual(reconcile_source_files(connection, [source_hash]), 1)

    def test_rejects_count_mismatch(self) -> None:
        source_hash = "b" * 64
        connection = FakeConnection(
            [(source_hash, "completed", 3, 2, 1, 2, 1, 1)]
        )
        with self.assertRaisesRegex(RuntimeError, "count mismatch"):
            reconcile_source_files(connection, [source_hash])

    def test_rejects_missing_manifest(self) -> None:
        connection = FakeConnection([])
        with self.assertRaisesRegex(RuntimeError, "missing file manifests"):
            reconcile_source_files(connection, ["c" * 64])

    def test_rejects_non_completed_manifest(self) -> None:
        source_hash = "d" * 64
        connection = FakeConnection(
            [(source_hash, "processing", 0, 0, 0, 0, 0, 0)]
        )
        with self.assertRaisesRegex(RuntimeError, "non-completed file"):
            reconcile_source_files(connection, [source_hash])


if __name__ == "__main__":
    unittest.main()
