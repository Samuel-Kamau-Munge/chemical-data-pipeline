from __future__ import annotations

import hashlib
import re
from pathlib import Path


_MIGRATION_FILE = re.compile(r"^(?P<version>[0-9]{4})_[a-z0-9_]+\.sql$")
_ADVISORY_LOCK_ID = 7_214_095_103
DEFAULT_MIGRATIONS_DIR = (
    Path(__file__).resolve().parent.parent / "infra" / "postgres" / "migrations"
)


def migration_files(migrations_dir: Path) -> list[tuple[str, Path]]:
    files: list[tuple[str, Path]] = []
    seen_versions: set[str] = set()
    for path in sorted(migrations_dir.glob("*.sql")):
        match = _MIGRATION_FILE.fullmatch(path.name)
        if match is None:
            raise ValueError(f"invalid migration filename: {path.name}")
        version = match.group("version")
        if version in seen_versions:
            raise ValueError(f"duplicate migration version: {version}")
        seen_versions.add(version)
        files.append((version, path))
    if not files:
        raise ValueError(f"no SQL migrations found in {migrations_dir}")
    return files


def apply_migrations(
    database_url: str, migrations_dir: Path = DEFAULT_MIGRATIONS_DIR
) -> list[str]:
    try:
        import psycopg
    except ImportError as error:
        raise RuntimeError(
            "PostgreSQL migrations require the 'streaming' extra; install "
            "the project with `pip install -e '.[streaming]'`"
        ) from error

    files = migration_files(migrations_dir)
    applied: list[str] = []
    try:
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute("SELECT pg_advisory_lock(%s)", (_ADVISORY_LOCK_ID,))
            try:
                with connection.transaction():
                    connection.execute("CREATE SCHEMA IF NOT EXISTS pipeline_meta")
                    connection.execute(
                        """
                        CREATE TABLE IF NOT EXISTS pipeline_meta.schema_migrations (
                            version CHAR(4) PRIMARY KEY,
                            checksum CHAR(64) NOT NULL,
                            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                        )
                        """
                    )

                for version, path in files:
                    content = path.read_text(encoding="utf-8")
                    checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
                    existing = connection.execute(
                        """
                        SELECT checksum
                        FROM pipeline_meta.schema_migrations
                        WHERE version = %s
                        """,
                        (version,),
                    ).fetchone()
                    if existing:
                        if existing[0].strip() != checksum:
                            raise RuntimeError(
                                f"applied migration {version} was modified"
                            )
                        continue

                    with connection.transaction():
                        connection.execute(content, prepare=False)
                        connection.execute(
                            """
                            INSERT INTO pipeline_meta.schema_migrations
                                (version, checksum)
                            VALUES (%s, %s)
                            """,
                            (version, checksum),
                        )
                    applied.append(version)
            finally:
                connection.execute(
                    "SELECT pg_advisory_unlock(%s)", (_ADVISORY_LOCK_ID,)
                )
    except psycopg.Error as error:
        raise RuntimeError("PostgreSQL schema migration failed") from error
    return applied
