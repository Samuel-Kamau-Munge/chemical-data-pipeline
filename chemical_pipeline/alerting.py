from __future__ import annotations

import json
import logging
import os
import ssl
from http.client import HTTPException
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlsplit


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def _validate_webhook_url(url: str) -> None:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "PIPELINE_ALERT_WEBHOOK_URL must be an HTTPS URL without userinfo, "
            "query parameters, or a fragment"
        )


def _failure_payload(context: dict[str, Any]) -> dict[str, str]:
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    return {
        "event": "airflow_task_failure",
        "dag_id": str(getattr(task_instance, "dag_id", "unknown")),
        "task_id": str(getattr(task_instance, "task_id", "unknown")),
        "run_id": str(getattr(dag_run, "run_id", "unknown")),
        "try_number": str(getattr(task_instance, "try_number", "unknown")),
    }


def notify_failure(context: dict[str, Any]) -> None:
    logger = logging.getLogger(__name__)
    url = os.environ.get("PIPELINE_ALERT_WEBHOOK_URL", "")
    if not url:
        logger.warning(
            "Pipeline failure alert not delivered: "
            "PIPELINE_ALERT_WEBHOOK_URL is not configured"
        )
        return

    try:
        _validate_webhook_url(url)
        headers = {"Content-Type": "application/json"}
        token = os.environ.get("PIPELINE_ALERT_WEBHOOK_TOKEN", "")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(
            url,
            data=json.dumps(_failure_payload(context)).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=ssl.create_default_context()),
            _NoRedirect(),
        )
        with opener.open(request, timeout=10) as response:
            if not 200 <= response.status < 300:
                raise RuntimeError(
                    f"alert endpoint returned HTTP {response.status}"
                )
    except (HTTPException, OSError, RuntimeError, ValueError):
        logger.exception("Pipeline failure alert could not be delivered")
