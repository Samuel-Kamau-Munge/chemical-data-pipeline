from __future__ import annotations

from typing import Any


def completed_source_hashes(connection: Any, source_hashes: list[str]) -> set[str]:
    hashes = sorted(set(source_hashes))
    if not hashes:
        return set()

    rows = connection.execute(
        """
        SELECT source_sha256
        FROM raw.ingestion_files
        WHERE source_sha256 = ANY(%s)
          AND status = 'completed'
        """,
        (hashes,),
    ).fetchall()
    return {row[0].strip() for row in rows}


def reconcile_source_files(connection: Any, source_hashes: list[str]) -> int:
    hashes = sorted(set(source_hashes))
    if not hashes:
        return 0

    rows = connection.execute(
        """
        SELECT
            files.source_sha256,
            files.status,
            files.total_rows,
            files.accepted_rows,
            files.rejected_rows,
            COUNT(events.event_id)::INTEGER AS actual_total,
            COUNT(events.event_id) FILTER (
                WHERE events.status = 'accepted'
            )::INTEGER AS actual_accepted,
            COUNT(events.event_id) FILTER (
                WHERE events.status = 'quarantined'
            )::INTEGER AS actual_rejected
        FROM raw.ingestion_files AS files
        LEFT JOIN raw.lab_result_events AS events
            ON events.source_sha256 = files.source_sha256
        WHERE files.source_sha256 = ANY(%s)
        GROUP BY files.source_sha256
        """,
        (hashes,),
    ).fetchall()
    files_by_hash = {row[0].strip(): row for row in rows}

    missing = sorted(set(hashes) - files_by_hash.keys())
    if missing:
        raise RuntimeError(
            "PostgreSQL reconciliation is missing file manifests: "
            + ", ".join(value[:12] for value in missing)
        )

    for source_hash in hashes:
        row = files_by_hash[source_hash]
        _, status, total, accepted, rejected, actual_total, actual_accepted, actual_rejected = row
        if status != "completed":
            raise RuntimeError(
                f"PostgreSQL reconciliation found non-completed file "
                f"{source_hash[:12]} (status={status})"
            )
        expected = (total, accepted, rejected)
        actual = (actual_total, actual_accepted, actual_rejected)
        if expected != actual:
            raise RuntimeError(
                f"PostgreSQL reconciliation count mismatch for "
                f"{source_hash[:12]}: expected {expected}, found {actual}"
            )
    return len(files_by_hash)
