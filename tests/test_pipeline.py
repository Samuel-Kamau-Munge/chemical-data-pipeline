from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from chemical_pipeline.pipeline import export_curated, ingest_csv


HEADERS = [
    "sample_id",
    "batch_id",
    "analyte",
    "value",
    "unit",
    "measured_at",
    "method",
    "instrument",
]


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.database = self.root / "pipeline.sqlite"
        self.raw_dir = self.root / "raw"

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def _write_csv(self, rows: list[list[str]]) -> Path:
        path = self.root / "results.csv"
        with path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(HEADERS)
            writer.writerows(rows)
        return path

    def test_ingests_and_normalizes_without_losing_source_values(self) -> None:
        source = self._write_csv(
            [
                [
                    "S-1",
                    "B-1",
                    "chloride",
                    "1.2300",
                    "mg/l",
                    "2026-01-02T03:04:05+03:00",
                    "IC",
                    "IC-4",
                ]
            ]
        )

        result = ingest_csv(source, self.database, self.raw_dir)
        output = self.root / "curated.jsonl"
        exported = export_curated(self.database, output)
        record = json.loads(output.read_text(encoding="utf-8").strip())

        self.assertEqual((result.accepted_rows, result.rejected_rows), (1, 0))
        self.assertEqual(exported, 1)
        self.assertEqual(record["unit"], "mg/L")
        self.assertEqual(record["original_unit"], "mg/l")
        self.assertEqual(record["value"], "1.2300")
        self.assertEqual(record["measured_at"], "2026-01-02T00:04:05Z")
        self.assertEqual(record["timestamp_timezone_known"], 1)
        self.assertEqual(record["row_number"], 2)
        self.assertTrue((self.raw_dir / f"{result.source_sha256}.csv").exists())

    def test_quarantines_invalid_rows_and_marks_naive_time(self) -> None:
        source = self._write_csv(
            [
                ["S-1", "B-1", "pH", "7.1", "pH", "2026-01-02T03:04:05", "LAB", "M-1"],
                ["S-2", "B-1", "pH", "not-a-number", "pH", "bad-time", "LAB", "M-1"],
            ]
        )

        result = ingest_csv(source, self.database, self.raw_dir)
        output = self.root / "curated.jsonl"
        export_curated(self.database, output)
        records = [json.loads(line) for line in output.read_text().splitlines()]

        self.assertEqual((result.accepted_rows, result.rejected_rows), (1, 1))
        self.assertEqual(records[0]["timestamp_timezone_known"], 0)
        self.assertEqual(records[0]["measured_at"], "2026-01-02T03:04:05")

    def test_reingesting_identical_file_is_idempotent(self) -> None:
        source = self._write_csv(
            [["S-1", "B-1", "sodium", "4", "mg/L", "2026-01-02", "ICP", "I-2"]]
        )

        first = ingest_csv(source, self.database, self.raw_dir)
        second = ingest_csv(source, self.database, self.raw_dir)

        self.assertEqual(first.status, "ingested")
        self.assertEqual(second.status, "skipped_duplicate")
        self.assertEqual(first.source_sha256, second.source_sha256)
        self.assertEqual((second.total_rows, second.accepted_rows), (1, 1))


if __name__ == "__main__":
    unittest.main()
