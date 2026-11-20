from __future__ import annotations

import unittest

from chemical_pipeline.alerting import _failure_payload, _validate_webhook_url


class AlertingTests(unittest.TestCase):
    def test_requires_https_without_embedded_credentials(self) -> None:
        _validate_webhook_url("https://alerts.example.test/hook")
        for url in (
            "http://alerts.example.test/hook",
            "https://user:password@alerts.example.test/hook",
            "https://alerts.example.test/hook?token=secret",
            "https://alerts.example.test/hook#fragment",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                _validate_webhook_url(url)

    def test_failure_payload_contains_only_run_identifiers(self) -> None:
        class TaskInstance:
            dag_id = "chemical_lab_csv_pipeline"
            task_id = "dbt_build"
            try_number = 2

        class DagRun:
            run_id = "scheduled__2026-11-20"

        payload = _failure_payload(
            {"task_instance": TaskInstance(), "dag_run": DagRun()}
        )
        self.assertEqual(payload["event"], "airflow_task_failure")
        self.assertEqual(payload["task_id"], "dbt_build")
        self.assertNotIn("exception", payload)
        self.assertNotIn("data", payload)


if __name__ == "__main__":
    unittest.main()
