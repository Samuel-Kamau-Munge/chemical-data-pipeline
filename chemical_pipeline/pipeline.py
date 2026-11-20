from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator


REQUIRED_COLUMNS = (
    "sample_id",
    "batch_id",
    "analyte",
    "value",
    "unit",
    "measured_at",
    "method",
    "instrument",
)

# These are spelling aliases only; no unit conversion or analyte-specific
# equivalence is inferred.
UNIT_ALIASES = {
    "mg/l": "mg/L",
    "g/l": "g/L",
    "ug/l": "ug/L",
    "µg/l": "ug/L",
    "μg/l": "ug/L",
    "mg/kg": "mg/kg",
    "g/kg": "g/kg",
    "%": "%",
    "percent": "%",
    "ph": "pH",
}


class PipelineInputError(ValueError):
    """Raised when an input file cannot be interpreted as a lab-results CSV."""


@dataclass(frozen=True)
class IngestResult:
    source_sha256: str
    status: str
    total_rows: int
    accepted_rows: int
    rejected_rows: int


def connect(database_path: Path) -> sqlite3.Connection:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS ingestion_files (
            source_sha256 TEXT PRIMARY KEY,
            source_filename TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            byte_count INTEGER NOT NULL,
            total_rows INTEGER NOT NULL DEFAULT 0,
            accepted_rows INTEGER NOT NULL DEFAULT 0,
            rejected_rows INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS lab_results (
            record_id TEXT PRIMARY KEY,
            source_sha256 TEXT NOT NULL REFERENCES ingestion_files(source_sha256),
            row_number INTEGER NOT NULL,
            sample_id TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            analyte TEXT NOT NULL,
            value TEXT NOT NULL,
            unit TEXT NOT NULL,
            original_value TEXT NOT NULL,
            original_unit TEXT NOT NULL,
            measured_at TEXT NOT NULL,
            timestamp_timezone_known INTEGER NOT NULL,
            method TEXT NOT NULL,
            instrument TEXT NOT NULL,
            raw_record_json TEXT NOT NULL,
            UNIQUE(source_sha256, row_number)
        );

        CREATE TABLE IF NOT EXISTS quarantined_rows (
            record_id TEXT PRIMARY KEY,
            source_sha256 TEXT NOT NULL REFERENCES ingestion_files(source_sha256),
            row_number INTEGER NOT NULL,
            errors_json TEXT NOT NULL,
            raw_record_json TEXT NOT NULL,
            UNIQUE(source_sha256, row_number)
        );

        CREATE INDEX IF NOT EXISTS idx_lab_results_sample
            ON lab_results(sample_id);
        CREATE INDEX IF NOT EXISTS idx_lab_results_batch
            ON lab_results(batch_id);
        """
    )
    return connection


def _archive_source(source_path: Path, raw_dir: Path) -> tuple[str, int, Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    byte_count = 0
    temporary_path: Path | None = None

    try:
        with source_path.open("rb") as source:
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=".incoming-", dir=raw_dir, delete=False
            ) as temporary:
                temporary_path = Path(temporary.name)
                while chunk := source.read(1024 * 1024):
                    digest.update(chunk)
                    byte_count += len(chunk)
                    temporary.write(chunk)

        source_sha256 = digest.hexdigest()
        archived_path = raw_dir / f"{source_sha256}.csv"
        if archived_path.exists():
            if _file_sha256(archived_path) != source_sha256:
                raise OSError(f"Raw archive integrity check failed: {archived_path}")
        else:
            os.replace(temporary_path, archived_path)
            temporary_path = None
        return source_sha256, byte_count, archived_path
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _raw_record(headers: list[str], values: list[str]) -> dict[str, Any]:
    record: dict[str, Any] = {
        header: values[index] if index < len(values) else None
        for index, header in enumerate(headers)
    }
    if len(values) > len(headers):
        record["__extra_columns__"] = values[len(headers) :]
    return record


def _rows(source_path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    with source_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source, strict=True)
        headers = next(reader, None)
        if not headers:
            raise PipelineInputError("CSV is empty or has no header row")

        normalized_headers = [header.strip() for header in headers]
        if any(not header for header in normalized_headers):
            raise PipelineInputError("CSV header contains an empty column name")
        if len(set(normalized_headers)) != len(normalized_headers):
            raise PipelineInputError("CSV header contains duplicate column names")
        missing = sorted(set(REQUIRED_COLUMNS) - set(normalized_headers))
        if missing:
            raise PipelineInputError(
                "CSV is missing required columns: " + ", ".join(missing)
            )

        for row_number, values in enumerate(reader, start=2):
            if not values:
                continue
            yield row_number, _raw_record(normalized_headers, values)


def _normalize_timestamp(value: str) -> tuple[str, bool]:
    candidate = value.strip()
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("measured_at must be an ISO 8601 date or timestamp") from error

    if parsed.tzinfo is None:
        return parsed.isoformat(), False
    return (
        parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        True,
    )


def _validate_record(
    raw_record: dict[str, Any],
) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    values = {key: raw_record.get(key) for key in REQUIRED_COLUMNS}

    for field in REQUIRED_COLUMNS:
        value = values[field]
        if value is None or not str(value).strip():
            errors.append(f"{field} is required")

    normalized_value: str | None = None
    normalized_unit: str | None = None
    normalized_timestamp: str | None = None
    timezone_known = False

    raw_value = values["value"]
    if raw_value is not None and str(raw_value).strip():
        try:
            decimal_value = Decimal(str(raw_value).strip())
            if not decimal_value.is_finite():
                raise InvalidOperation
            normalized_value = format(decimal_value, "f")
        except InvalidOperation:
            errors.append("value must be a finite decimal number")

    raw_unit = values["unit"]
    if raw_unit is not None and str(raw_unit).strip():
        unit_key = str(raw_unit).strip().lower()
        normalized_unit = UNIT_ALIASES.get(unit_key)
        if normalized_unit is None:
            errors.append(f"unit is not in the supported alias list: {raw_unit}")

    raw_timestamp = values["measured_at"]
    if raw_timestamp is not None and str(raw_timestamp).strip():
        try:
            normalized_timestamp, timezone_known = _normalize_timestamp(
                str(raw_timestamp)
            )
        except ValueError as error:
            errors.append(str(error))

    if errors:
        return None, errors

    return (
        {
            "sample_id": str(values["sample_id"]),
            "batch_id": str(values["batch_id"]),
            "analyte": str(values["analyte"]),
            "value": normalized_value,
            "unit": normalized_unit,
            "original_value": str(values["value"]),
            "original_unit": str(values["unit"]),
            "measured_at": normalized_timestamp,
            "timestamp_timezone_known": int(timezone_known),
            "method": str(values["method"]),
            "instrument": str(values["instrument"]),
        },
        [],
    )


def ingest_csv(
    source_path: Path, database_path: Path, raw_dir: Path
) -> IngestResult:
    source_sha256, byte_count, archived_path = _archive_source(source_path, raw_dir)
    connection = connect(database_path)
    try:
        existing = connection.execute(
            "SELECT total_rows, accepted_rows, rejected_rows "
            "FROM ingestion_files WHERE source_sha256 = ?",
            (source_sha256,),
        ).fetchone()
        if existing is not None:
            return IngestResult(
                source_sha256,
                "skipped_duplicate",
                existing["total_rows"],
                existing["accepted_rows"],
                existing["rejected_rows"],
            )

        accepted_count = 0
        rejected_count = 0
        total_count = 0
        ingested_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        with connection:
            connection.execute(
                """
                INSERT INTO ingestion_files
                    (source_sha256, source_filename, ingested_at, byte_count)
                VALUES (?, ?, ?, ?)
                """,
                (source_sha256, source_path.name, ingested_at, byte_count),
            )

            for row_number, raw_record in _rows(archived_path):
                total_count += 1
                record_id = hashlib.sha256(
                    f"{source_sha256}:{row_number}".encode("utf-8")
                ).hexdigest()
                raw_json = json.dumps(
                    raw_record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                normalized, errors = _validate_record(raw_record)
                if normalized is None:
                    rejected_count += 1
                    connection.execute(
                        """
                        INSERT INTO quarantined_rows
                            (record_id, source_sha256, row_number, errors_json, raw_record_json)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            record_id,
                            source_sha256,
                            row_number,
                            json.dumps(errors, ensure_ascii=False),
                            raw_json,
                        ),
                    )
                    continue

                accepted_count += 1
                connection.execute(
                    """
                    INSERT INTO lab_results (
                        record_id, source_sha256, row_number, sample_id, batch_id,
                        analyte, value, unit, original_value, original_unit,
                        measured_at, timestamp_timezone_known, method, instrument,
                        raw_record_json
                    ) VALUES (
                        :record_id, :source_sha256, :row_number, :sample_id, :batch_id,
                        :analyte, :value, :unit, :original_value, :original_unit,
                        :measured_at, :timestamp_timezone_known, :method, :instrument,
                        :raw_record_json
                    )
                    """,
                    {
                        **normalized,
                        "record_id": record_id,
                        "source_sha256": source_sha256,
                        "row_number": row_number,
                        "raw_record_json": raw_json,
                    },
                )

            connection.execute(
                """
                UPDATE ingestion_files
                SET total_rows = ?, accepted_rows = ?, rejected_rows = ?
                WHERE source_sha256 = ?
                """,
                (total_count, accepted_count, rejected_count, source_sha256),
            )

        return IngestResult(
            source_sha256, "ingested", total_count, accepted_count, rejected_count
        )
    finally:
        connection.close()


def export_curated(database_path: Path, output_path: Path) -> int:
    if not database_path.is_file():
        raise FileNotFoundError(f"pipeline database does not exist: {database_path}")
    connection = connect(database_path)
    try:
        rows = connection.execute(
            """
            SELECT record_id, source_sha256, row_number, sample_id, batch_id,
                   analyte, value, unit, original_value, original_unit,
                   measured_at, timestamp_timezone_known, method, instrument
            FROM lab_results
            JOIN ingestion_files USING (source_sha256)
            ORDER BY ingested_at, source_sha256, row_number
            """
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with output_path.open("w", encoding="utf-8", newline="\n") as output:
            for row in rows:
                output.write(
                    json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n"
                )
                count += 1
        return count
    finally:
        connection.close()
