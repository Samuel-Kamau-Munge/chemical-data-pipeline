from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from chemical_pipeline.kafka_config import kafka_security_config


class KafkaSecurityConfigTests(unittest.TestCase):
    def test_defaults_to_local_plaintext_without_credentials(self) -> None:
        self.assertEqual(kafka_security_config({}), {})

    def test_rejects_credentials_over_plaintext(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot be used with PLAINTEXT"):
            kafka_security_config(
                {"PIPELINE_KAFKA_SASL_USERNAME": "producer"}
            )

    def test_requires_complete_sasl_tls_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ca_file = Path(directory) / "ca.pem"
            ca_file.write_text("test certificate", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "requires"):
                kafka_security_config(
                    {
                        "PIPELINE_KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                        "PIPELINE_KAFKA_SSL_CA_FILE": str(ca_file),
                        "PIPELINE_KAFKA_SASL_USERNAME": "producer",
                    }
                )

    def test_builds_sasl_tls_settings_and_requires_a_ca(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ca_file = Path(directory) / "ca.pem"
            ca_file.write_text("test certificate", encoding="utf-8")
            config = kafka_security_config(
                {
                    "PIPELINE_KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                    "PIPELINE_KAFKA_SSL_CA_FILE": str(ca_file),
                    "PIPELINE_KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
                    "PIPELINE_KAFKA_SASL_USERNAME": "producer",
                    "PIPELINE_KAFKA_SASL_PASSWORD": "not-a-real-secret",
                }
            )
        self.assertEqual(config["security_protocol"], "SASL_SSL")
        self.assertEqual(config["sasl_mechanism"], "SCRAM-SHA-512")
        self.assertEqual(config["ssl_cafile"], str(ca_file))


if __name__ == "__main__":
    unittest.main()
