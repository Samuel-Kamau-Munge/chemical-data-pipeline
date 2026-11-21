from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor

PROJECT_ROOT = Path(
    os.environ.get("CHEMICAL_PIPELINE_HOME", "/opt/chemical-pipeline")
)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from chemical_pipeline.alerting import notify_failure  # noqa: E402
from chemical_pipeline.pipeline import _file_sha256  # noqa: E402
from chemical_pipeline.reconciliation import (  # noqa: E402
    completed_source_hashes,
    reconcile_source_files,
)
from chemical_pipeline.streaming import StreamingInputError, publish_csv  # noqa: E402


DATA_DIR = Path(os.environ.get("CHEMICAL_PIPELINE_DATA", "/data"))
INCOMING_DIR = DATA_DIR / "incoming"
RAW_DIR = DATA_DIR / "raw"
BOOTSTRAP_SERVERS = os.environ.get("PIPELINE_KAFKA_BOOTSTRAP", "kafka:9092")


def publish_incoming_files() -> dict[str, object]:
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    sources = sorted(
        path
        for path in INCOMING_DIR.iterdir()
        if path.is_file() and path.suffix.lower() == ".csv"
    )
    if not sources:
        raise AirflowSkipException("No CSV files are waiting for ingestion")

    results: list[dict[str, object]] = []
    errors: list[str] = []
    source_hashes: dict[Path, str] = {}
    for source in sources:
        try:
            source_hashes[source] = _file_sha256(source)
        except OSError as error:
            logging.exception("Unable to hash %s", source.name)
            errors.append(f"{source.name}: {error}")

    completed_hashes: set[str] = set()
    if source_hashes:
        try:
            import psycopg
        except ImportError as error:
            raise RuntimeError("The Airflow worker requires psycopg") from error

        with psycopg.connect(
            os.environ["PIPELINE_READER_DB_URL"], connect_timeout=5
        ) as connection:
            completed_hashes = completed_source_hashes(
                connection, list(source_hashes.values())
            )

    skipped_files = 0
    for source, source_sha256 in source_hashes.items():
        if source_sha256 in completed_hashes:
            skipped_files += 1
            logging.info(
                "Skipping already completed CSV %s (sha256=%s)",
                source.name,
                source_sha256,
            )
            continue
        try:
            results.append(publish_csv(source, RAW_DIR, BOOTSTRAP_SERVERS))
        except StreamingInputError as error:
            results.append(
                {
                    "source_sha256": error.source_sha256,
                    "status": "failed",
                    "total_rows": 0,
                    "accepted_rows": 0,
                    "rejected_rows": 0,
                }
            )
            logging.exception("Invalid CSV %s", source.name)
            errors.append(f"{source.name}: {error}")
        except (OSError, UnicodeError, ValueError, RuntimeError) as error:
            logging.exception("Unable to publish %s", source.name)
            errors.append(f"{source.name}: {error}")
    if not results and not errors:
        raise AirflowSkipException("No new CSV files require ingestion")

    summary = {
        "files_found": len(sources),
        "files_skipped": skipped_files,
        "results": results,
        "errors": errors,
    }
    logging.info(
        "Published %d CSV files; skipped %d completed files; errors=%d",
        len(results),
        skipped_files,
        len(errors),
    )
    return summary


def kafka_events_persisted(ti, **_) -> bool:
    ingestion = ti.xcom_pull(task_ids="publish_incoming_files") or {}
    source_hashes = [
        result["source_sha256"] for result in ingestion.get("results", [])
    ]
    if not source_hashes:
        return True

    try:
        import psycopg
    except ImportError as error:
        raise RuntimeError("The Airflow worker requires psycopg") from error

    with psycopg.connect(
        os.environ["PIPELINE_READER_DB_URL"], connect_timeout=5
    ) as connection:
        states = connection.execute(
            """
            SELECT source_sha256, status
            FROM raw.ingestion_files
            WHERE source_sha256 = ANY(%s)
            """,
            (source_hashes,),
        ).fetchall()
    terminal = {row[0] for row in states if row[1] in {"completed", "failed"}}
    return len(terminal) == len(set(source_hashes))


def reconcile_persisted_files(ti, **_) -> int:
    ingestion = ti.xcom_pull(task_ids="publish_incoming_files") or {}
    source_hashes = [
        result["source_sha256"] for result in ingestion.get("results", [])
    ]
    if not source_hashes:
        return 0

    try:
        import psycopg
    except ImportError as error:
        raise RuntimeError("The Airflow worker requires psycopg") from error

    with psycopg.connect(
        os.environ["PIPELINE_READER_DB_URL"], connect_timeout=5
    ) as connection:
        count = reconcile_source_files(connection, source_hashes)
    logging.info("Reconciled %d completed ingestion files", count)
    return count


def report_run(**context) -> None:
    task_instance = context["ti"]
    ingestion = task_instance.xcom_pull(task_ids="publish_incoming_files") or {}
    errors = ingestion.get("errors", [])
    summary = {
        "files_found": ingestion.get("files_found", 0),
        "files_published": sum(
            result["status"] == "published"
            for result in ingestion.get("results", [])
        ),
        "accepted_rows": sum(
            result["accepted_rows"] for result in ingestion.get("results", [])
        ),
        "quarantined_rows": sum(
            result["rejected_rows"] for result in ingestion.get("results", [])
        ),
        "file_errors": errors,
        "upstream_states": "visible in Airflow task state",
    }
    logging.info("Chemical lab streaming run summary: %s", summary)
    if errors:
        raise AirflowException(
            "The chemical lab streaming pipeline did not complete successfully."
        )


with DAG(
    dag_id="chemical_lab_csv_pipeline",
    description="Publish validated lab CSV rows through Kafka into PostgreSQL and dbt",
    schedule="* * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["chemical", "kafka", "postgres", "dbt", "data-quality"],
    default_args={"on_failure_callback": notify_failure},
) as dag:
    publish_task = PythonOperator(
        task_id="publish_incoming_files",
        python_callable=publish_incoming_files,
    )
    wait_task = PythonSensor(
        task_id="wait_for_postgres",
        python_callable=kafka_events_persisted,
        mode="reschedule",
        poke_interval=5,
        timeout=300,
    )
    reconcile_task = PythonOperator(
        task_id="reconcile_postgres",
        python_callable=reconcile_persisted_files,
    )
    dbt_task = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "dbt build --project-dir /opt/chemical-pipeline/dbt "
            "--profiles-dir /opt/chemical-pipeline/dbt"
        ),
    )
    report_task = PythonOperator(
        task_id="report_run",
        python_callable=report_run,
        trigger_rule="all_done",
    )

    publish_task >> wait_task >> reconcile_task >> dbt_task >> report_task
