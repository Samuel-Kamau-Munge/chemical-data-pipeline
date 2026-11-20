from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path

from chemical_pipeline.pipeline import (
    PipelineInputError,
    export_curated,
    ingest_csv,
)
from chemical_pipeline.migrations import apply_migrations
from chemical_pipeline.postgres_consumer import check_dependencies, consume
from chemical_pipeline.reconciliation import reconcile_source_files
from chemical_pipeline.streaming import publish_csv


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chemical-lab-pipeline",
        description="Ingest, validate, quarantine, and export chemical lab CSV results.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    ingest = commands.add_parser("ingest", help="ingest one laboratory CSV export")
    ingest.add_argument("source", type=Path, help="path to the source CSV")
    ingest.add_argument("--db", type=Path, default=Path("data/pipeline.sqlite"))
    ingest.add_argument("--raw-dir", type=Path, default=Path("data/raw"))

    export = commands.add_parser("export", help="export validated results as JSONL")
    export.add_argument("--db", type=Path, default=Path("data/pipeline.sqlite"))
    export.add_argument(
        "--output", type=Path, default=Path("data/curated.jsonl")
    )

    publish = commands.add_parser(
        "publish", help="validate a CSV and publish row events to Kafka"
    )
    publish.add_argument("source", type=Path, help="path to the source CSV")
    publish.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    publish.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("PIPELINE_KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    publish.add_argument("--topic", default="lab-results.v1")

    consume_command = commands.add_parser(
        "consume", help="consume Kafka result events into PostgreSQL"
    )
    consume_command.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("PIPELINE_KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    consume_command.add_argument("--topic", default="lab-results.v1")
    consume_command.add_argument(
        "--group-id", default="chemical-postgres-writer"
    )
    consume_command.add_argument(
        "--max-messages",
        type=int,
        help="stop after committing this many messages (for smoke tests)",
    )

    healthcheck = commands.add_parser(
        "healthcheck", help="verify Kafka and PostgreSQL connectivity"
    )
    healthcheck.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("PIPELINE_KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    commands.add_parser("migrate", help="apply pending PostgreSQL schema migrations")

    reconcile = commands.add_parser(
        "reconcile", help="compare PostgreSQL file manifests with persisted rows"
    )
    reconcile.add_argument(
        "source_sha256",
        nargs="+",
        help="one or more source SHA-256 hashes to reconcile",
    )
    return parser


def main() -> None:
    parser = _parser()
    args = parser.parse_args()

    try:
        if args.command == "ingest":
            result = ingest_csv(args.source, args.db, args.raw_dir)
            print(
                f"{result.status}: sha256={result.source_sha256} "
                f"rows={result.total_rows} accepted={result.accepted_rows} "
                f"rejected={result.rejected_rows}"
            )
        else:
            if args.command == "export":
                count = export_curated(args.db, args.output)
                print(f"exported {count} validated rows to {args.output}")
            elif args.command == "publish":
                result = publish_csv(
                    args.source, args.raw_dir, args.bootstrap_servers, args.topic
                )
                print(
                    f"{result['status']}: sha256={result['source_sha256']} "
                    f"rows={result['total_rows']} "
                    f"accepted={result['accepted_rows']} "
                    f"rejected={result['rejected_rows']}"
                )
            elif args.command == "consume":
                database_url = os.environ.get("PIPELINE_DB_URL")
                if not database_url:
                    parser.error("PIPELINE_DB_URL is required")
                count = consume(
                    args.bootstrap_servers,
                    database_url,
                    args.topic,
                    args.group_id,
                    args.max_messages,
                )
                print(f"committed {count} Kafka events")
            elif args.command == "migrate":
                database_url = os.environ.get("PIPELINE_ADMIN_DB_URL")
                if not database_url:
                    parser.error("PIPELINE_ADMIN_DB_URL is required")
                versions = apply_migrations(database_url)
                print(
                    "applied migrations: "
                    + (", ".join(versions) if versions else "none")
                )
            elif args.command == "reconcile":
                database_url = os.environ.get("PIPELINE_READER_DB_URL")
                if not database_url:
                    parser.error("PIPELINE_READER_DB_URL is required")
                try:
                    import psycopg
                except ImportError as error:
                    raise RuntimeError(
                        "PostgreSQL reconciliation requires the 'streaming' extra"
                    ) from error
                with psycopg.connect(database_url) as connection:
                    count = reconcile_source_files(
                        connection, args.source_sha256
                    )
                print(f"reconciled {count} completed ingestion files")
            else:
                database_url = os.environ.get("PIPELINE_DB_URL")
                if not database_url:
                    parser.error("PIPELINE_DB_URL is required")
                check_dependencies(args.bootstrap_servers, database_url)
                print("Kafka and PostgreSQL are reachable")
    except (OSError, UnicodeError, sqlite3.Error, PipelineInputError) as error:
        parser.exit(2, f"error: {error}\n")
    except (RuntimeError, ValueError) as error:
        parser.exit(2, f"error: {error}\n")


if __name__ == "__main__":
    main()
