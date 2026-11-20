#!/usr/bin/env bash
set -euo pipefail

if [[ -e .env ]]; then
    printf '%s\n' ".env already exists; leaving it unchanged." >&2
    exit 1
fi

secret() {
    python3 -c 'import secrets; print(secrets.token_hex(32))'
}

umask 077
cat > .env <<EOF
POSTGRES_ADMIN_PASSWORD=$(secret)
PIPELINE_DB_PASSWORD=$(secret)
DBT_DB_PASSWORD=$(secret)
PIPELINE_READER_PASSWORD=$(secret)
AIRFLOW_UID=$(id -u)
EOF

mkdir -p data/incoming data/raw
chmod 700 .env data data/incoming data/raw
printf '%s\n' "Created .env with unique local credentials and prepared data folders."
