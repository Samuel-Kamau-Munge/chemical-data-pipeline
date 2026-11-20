from __future__ import annotations

import hashlib
import unittest

from chemical_pipeline.postgres_consumer import _validate_event
from chemical_pipeline.streaming import _event


class EventContractTests(unittest.TestCase):
    def test_accepts_validated_row_event_with_deterministic_identity(self) -> None:
        source_sha256 = "a" * 64
        record = {
            "sample_id": "S-1",
            "batch_id": "B-1",
            "analyte": "chloride",
            "value": "2.50",
            "unit": "mg/L",
            "original_value": "2.50",
            "original_unit": "mg/L",
            "measured_at": "2026-11-19T10:00:00Z",
            "timestamp_timezone_known": 1,
            "method": "IC",
            "instrument": "IC-1",
        }
        event = _event(
            source_sha256,
            "row:2",
            "lab_result",
            row_number=2,
            status="accepted",
            record=record,
            raw_record=record,
            validation_errors=[],
        )

        self.assertEqual(_validate_event(event), event)
        self.assertEqual(
            event["event_id"],
            hashlib.sha256(f"{source_sha256}:row:2".encode()).hexdigest(),
        )

    def test_accepts_additive_event_metadata_for_compatible_updates(self) -> None:
        source_sha256 = "d" * 64
        event = _event(
            source_sha256,
            "file:started",
            "file.started",
            file_name="results.csv",
            byte_count=10,
        )
        event["producer_metadata"] = {"release": "2026.11"}

        self.assertEqual(_validate_event(event), event)

    def test_rejects_row_event_with_mismatched_event_id(self) -> None:
        source_sha256 = "b" * 64
        event = _event(
            source_sha256,
            "row:2",
            "lab_result",
            row_number=2,
            status="quarantined",
            record={"value": "bad"},
            raw_record={"value": "bad"},
            validation_errors=["value must be a finite decimal number"],
        )
        event["event_id"] = "0" * 64

        with self.assertRaisesRegex(ValueError, "does not match"):
            _validate_event(event)

    def test_rejects_file_completion_with_unreconciled_counts(self) -> None:
        event = _event(
            "c" * 64,
            "file:completed",
            "file.completed",
            file_name="results.csv",
            total_rows=3,
            accepted_rows=2,
            rejected_rows=0,
        )

        with self.assertRaisesRegex(ValueError, "do not reconcile"):
            _validate_event(event)


if __name__ == "__main__":
    unittest.main()
