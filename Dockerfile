FROM python:3.12.8-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY chemical_pipeline ./chemical_pipeline

RUN python -m pip install --no-cache-dir '.[streaming]' \
    && groupadd --system --gid 10001 pipeline \
    && useradd --system --uid 10001 --gid pipeline --home-dir /nonexistent pipeline

USER 10001:10001

ENTRYPOINT ["python", "-m", "chemical_pipeline"]
