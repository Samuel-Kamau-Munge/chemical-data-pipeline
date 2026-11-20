from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

from chemical_pipeline.pipeline import _validate_record


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_EVENT_TYPES = {
    "file.started",
    "file.completed",
    "file.failed",
    "lab_result",
}
_ACCEPTED_FIELDS = {
    "sample_id",
    "batch_id",
    "analyte",
    "value",
    "unit",
    "original_value",
    "original_unit",
    "measured_at",
    "timestamp_timezone_known",
    "method",
    "instrument",
}


def _validate_event(event: Any) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise ValueError("Kafka event must be a JSON object")
    if event.get("schema_version") != 1:
        raise ValueError("unsupported event schema version")
    event_type = event.get("event_type")
    source_sha256 = event.get("source_sha256")
    event_id = event.get("event_id")
    if event_type not in _EVENT_TYPES:
        raise ValueError("unsupported event type")
    if not isinstance(source_sha256, str) or not _SHA256.fullmatch(source_sha256):
        raise ValueError("source_sha256 must be a lowercase SHA-256 digest")
    if not isinstance(event_id, str) or not _SHA256.fullmatch(event_id):
        raise ValueError("event_id must be a lowercase SHA-256 digest")

    if event_type == "lab_result":
        row_number = event.get("row_number")
        if not isinstance(row_number, int) or isinstance(row_number, bool) or row_number < 2:
            raise ValueError("lab_result row_number must be an integer >= 2")
        expected_id = hashlib.sha256(
            f"{source_sha256}:row:{row_number}".encode("utf-8")
        ).hexdigest()
        if event_id != expected_id:
            raise ValueError("lab_result event_id does not match its source row")
        if event.get("status") not in {"accepted", "quarantined"}:
            raise ValueError("lab_result status must be accepted or quarantined")
        if not isinstance(event.get("record"), dict):
            raise ValueError("lab_result record must be a JSON object")
        if not isinstance(event.get("raw_record"), dict):
            raise ValueError("lab_result raw_record must be a JSON object")
        if not isinstance(event.get("validation_errors"), list):
            raise ValueError("lab_result validation_errors must be a JSON array")
        if event["status"] == "accepted":
            missing = _ACCEPTED_FIELDS - set(event["record"])
            if missing:
                raise ValueError(
                    "accepted lab_result is missing fields: "
                    + ", ".join(sorted(missing))
                )
            if event["validation_errors"]:
                raise ValueError("accepted lab_result cannot include validation errors")
            _, validation_errors = _validate_record(event["record"])
            if validation_errors:
                raise ValueError(
                    "accepted lab_result fails validation: "
                    + "; ".join(validation_errors)
                )
        elif not event["validation_errors"]:
            raise ValueError("quarantined lab_result must include validation errors")
        if any(not isinstance(error, str) or not error for error in event["validation_errors"]):
            raise ValueError("validation_errors must contain non-empty strings")
    else:
        suffix = {
            "file.started": "file:started",
            "file.completed": "file:completed",
            "file.failed": "file:failed",
        }[event_type]
        expected_id = hashlib.sha256(
            f"{source_sha256}:{suffix}".encode("utf-8")
        ).hexdigest()
        if event_id != expected_id:
            raise ValueError(f"{event_type} event_id does not match its source file")
        if not isinstance(event.get("file_name"), str) or not event["file_name"]:
            raise ValueError(f"{event_type} requires file_name")
        if "/" in event["file_name"] or "\\" in event["file_name"]:
            raise ValueError("file_name must be a basename")
        if event_type == "file.started":
            byte_count = event.get("byte_count")
            if (
                not isinstance(byte_count, int)
                or isinstance(byte_count, bool)
                or byte_count < 0
            ):
                raise ValueError("file.started byte_count must be a non-negative integer")
        elif event_type == "file.completed":
            counts = [
                event.get("total_rows"),
                event.get("accepted_rows"),
                event.get("rejected_rows"),
            ]
            if any(
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
                for value in counts
            ):
                raise ValueError("file.completed row counts must be non-negative integers")
            if counts[0] != counts[1] + counts[2]:
                raise ValueError("file.completed row counts do not reconcile")
        elif not isinstance(event.get("error"), str) or not event["error"]:
            raise ValueError("file.failed requires an error message")

    return event


def _store_event(connection: Any, event: dict[str, Any]) -> None:
    event_type = event["event_type"]
    source_sha256 = event["source_sha256"]

    with connection.transaction():
        if event_type == "file.started":
            connection.execute(
                """
                INSERT INTO raw.ingestion_files
                    (source_sha256, source_filename, byte_count, ingested_at, status)
                VALUES (%s, %s, %s, %s, 'processing')
                ON CONFLICT (source_sha256) DO NOTHING
                """,
                (
                    source_sha256,
                    event["file_name"],
                    event["byte_count"],
                    event["occurred_at"],
                ),
            )
        elif event_type == "lab_result":
            inserted = connection.execute(
                """
                INSERT INTO raw.lab_result_events (
                    event_id, source_sha256, row_number, status, record,
                    raw_record, validation_errors, occurred_at
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                (
                    event["event_id"],
                    source_sha256,
                    event["row_number"],
                    event["status"],
                    json.dumps(event["record"], ensure_ascii=False),
                    json.dumps(event["raw_record"], ensure_ascii=False),
                    json.dumps(event["validation_errors"], ensure_ascii=False),
                    event["occurred_at"],
                ),
            ).fetchone()
            if inserted:
                accepted = event["status"] == "accepted"
                connection.execute(
                    """
                    UPDATE raw.ingestion_files
                    SET total_rows = total_rows + 1,
                        accepted_rows = accepted_rows + %s,
                        rejected_rows = rejected_rows + %s
                    WHERE source_sha256 = %s
                    """,
                    (int(accepted), int(not accepted), source_sha256),
                )
        elif event_type == "file.completed":
            counts = connection.execute(
                """
                SELECT total_rows, accepted_rows, rejected_rows
                FROM raw.ingestion_files
                WHERE source_sha256 = %s
                """,
                (source_sha256,),
            ).fetchone()
            expected_counts = (
                event["total_rows"],
                event["accepted_rows"],
                event["rejected_rows"],
            )
            if counts is None or tuple(counts) != expected_counts:
                raise ValueError(
                    "file completion counts do not match persisted row events"
                )
            connection.execute(
                """
                UPDATE raw.ingestion_files
                SET status = 'completed', completed_at = %s,
                    total_rows = %s, accepted_rows = %s, rejected_rows = %s,
                    last_error = NULL
                WHERE source_sha256 = %s
                """,
                (
                    event["occurred_at"],
                    event["total_rows"],
                    event["accepted_rows"],
                    event["rejected_rows"],
                    source_sha256,
                ),
            )
        else:
            connection.execute(
                """
                UPDATE raw.ingestion_files
                SET status = 'failed', completed_at = %s, last_error = %s
                WHERE source_sha256 = %s
                """,
                (event["occurred_at"], event["error"], source_sha256),
            )


def consume(
    bootstrap_servers: str,
    database_url: str,
    topic: str = "lab-results.v1",
    group_id: str = "chemical-postgres-writer",
    max_messages: int | None = None,
) -> int:
    if max_messages is not None and max_messages < 1:
        raise ValueError("max_messages must be a positive integer")
    try:
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError
        from kafka.structs import OffsetAndMetadata, TopicPartition
        import psycopg
    except ImportError as error:
        raise RuntimeError(
            "The Kafka/PostgreSQL consumer requires the 'streaming' extra; "
            "install the project with `pip install -e '.[streaming]'`"
        ) from error

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=1,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            request_timeout_ms=30_000,
            session_timeout_ms=15_000,
        )
        consumer.subscribe([topic])
    except KafkaError as error:
        raise RuntimeError("Unable to initialize the Kafka consumer") from error
    processed = 0
    try:
        with psycopg.connect(database_url) as connection:
            while max_messages is None or processed < max_messages:
                polled = consumer.poll(timeout_ms=1000, max_records=1)
                for topic_partition, messages in polled.items():
                    for message in messages:
                        event = _validate_event(message.value)
                        _store_event(connection, event)
                        consumer.commit(
                            offsets={
                                TopicPartition(message.topic, message.partition):
                                    OffsetAndMetadata(message.offset + 1, "", -1)
                            },
                            timeout_ms=10_000,
                        )
                        processed += 1
                        logging.info(
                            "Committed event %s (%s)",
                            event["event_id"],
                            event["event_type"],
                        )
                        if max_messages is not None and processed >= max_messages:
                            break
        return processed
    except (KafkaError, psycopg.Error, json.JSONDecodeError) as error:
        raise RuntimeError(
            "Kafka/PostgreSQL consumption stopped before the current offset was committed"
        ) from error
    finally:
        consumer.close()


def check_dependencies(bootstrap_servers: str, database_url: str) -> None:
    try:
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError
        import psycopg
    except ImportError as error:
        raise RuntimeError(
            "The dependency check requires the 'streaming' extra; "
            "install the project with `pip install -e '.[streaming]'`"
        ) from error

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers.split(","),
            enable_auto_commit=False,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        try:
            consumer.topics()
        finally:
            consumer.close()

        with psycopg.connect(database_url, connect_timeout=5) as connection:
            connection.execute("SELECT 1")
    except (KafkaError, psycopg.Error) as error:
        raise RuntimeError("Kafka or PostgreSQL connectivity check failed") from error
