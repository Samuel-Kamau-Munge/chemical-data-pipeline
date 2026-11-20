from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from chemical_pipeline.migrations import migration_files


class MigrationFileTests(unittest.TestCase):
    def test_orders_versioned_migrations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "0002_add_index.sql").write_text("SELECT 2;", encoding="utf-8")
            (root / "0001_initial_schema.sql").write_text(
                "SELECT 1;", encoding="utf-8"
            )

            self.assertEqual(
                [version for version, _ in migration_files(root)],
                ["0001", "0002"],
            )

    def test_rejects_non_versioned_sql_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "initial.sql").write_text("SELECT 1;", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "invalid migration filename"):
                migration_files(root)

    def test_rejects_duplicate_versions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("0001_first.sql", "0001_second.sql"):
                (root / name).write_text("SELECT 1;", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "duplicate migration version"):
                migration_files(root)


if __name__ == "__main__":
    unittest.main()
