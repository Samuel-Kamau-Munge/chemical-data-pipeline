from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from chemical_pipeline.pipeline import (
    PipelineInputError,
    _archive_source,
    _rows,
    _validate_record,
)


class StreamingInputError(PipelineInputError):
    def __init__(self, source_sha256: str, message: str):
        super().__init__(message)
        self.source_sha256 = source_sha256


def _event_id(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _event(
    source_sha256: str, suffix: str, event_type: str, **fields: Any
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "event_id": _event_id(f"{source_sha256}:{suffix}"),
        "event_type": event_type,
        "source_sha256": source_sha256,
        "occurred_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        **fields,
    }


def _send(producer: Any, topic: str, source_sha256: str, event: dict[str, Any]) -> None:
    from kafka.errors import KafkaError

    try:
        producer.send(
            topic,
            key=source_sha256,
            value=event,
        ).get(timeout=30)
    except (KafkaError, TimeoutError) as error:
        raise RuntimeError(
            f"Kafka publish failed for event {event['event_id']}"
        ) from error


def publish_csv(
    source_path: Path,
    raw_dir: Path,
    bootstrap_servers: str,
    topic: str = "lab-results.v1",
) -> dict[str, Any]:
    try:
        from kafka import KafkaProducer
    except ImportError as error:
        raise RuntimeError(
            "Kafka publishing requires the 'streaming' extra; install "
            "the project with `pip install -e '.[streaming]'`"
        ) from error

    source_sha256, byte_count, archived_path = _archive_source(source_path, raw_dir)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        request_timeout_ms=30_000,
    )
    accepted_count = 0
    rejected_count = 0
    total_count = 0

    try:
        started = _event(
            source_sha256,
            "file:started",
            "file.started",
            file_name=source_path.name,
            byte_count=byte_count,
        )
        _send(producer, topic, source_sha256, started)

        try:
            for row_number, raw_record in _rows(archived_path):
                total_count += 1
                normalized, errors = _validate_record(raw_record)
                accepted = normalized is not None
                accepted_count += int(accepted)
                rejected_count += int(not accepted)
                row_event = _event(
                    source_sha256,
                    f"row:{row_number}",
                    "lab_result",
                    row_number=row_number,
                    status="accepted" if accepted else "quarantined",
                    record=normalized if accepted else raw_record,
                    raw_record=raw_record,
                    validation_errors=errors,
                )
                _send(producer, topic, source_sha256, row_event)
        except (OSError, UnicodeError, csv.Error, PipelineInputError) as error:
            failed = _event(
                source_sha256,
                "file:failed",
                "file.failed",
                file_name=source_path.name,
                error=str(error)[:2000],
            )
            _send(producer, topic, source_sha256, failed)
            raise StreamingInputError(source_sha256, str(error)) from error

        completed = _event(
            source_sha256,
            "file:completed",
            "file.completed",
            file_name=source_path.name,
            total_rows=total_count,
            accepted_rows=accepted_count,
            rejected_rows=rejected_count,
        )
        _send(producer, topic, source_sha256, completed)
        producer.flush(timeout=30)
        return {
            "source_sha256": source_sha256,
            "status": "published",
            "total_rows": total_count,
            "accepted_rows": accepted_count,
            "rejected_rows": rejected_count,
        }
    finally:
        producer.close(timeout=30)
