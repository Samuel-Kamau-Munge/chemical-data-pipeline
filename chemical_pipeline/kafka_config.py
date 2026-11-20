from __future__ import annotations

import os
from pathlib import Path


_SASL_MECHANISMS = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}


def kafka_security_config(environ: dict[str, str] | None = None) -> dict[str, str]:
    values = os.environ if environ is None else environ
    protocol = values.get("PIPELINE_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    if protocol not in {"PLAINTEXT", "SSL", "SASL_SSL"}:
        raise ValueError("Kafka security protocol must be PLAINTEXT, SSL, or SASL_SSL")

    ca_file = values.get("PIPELINE_KAFKA_SSL_CA_FILE")
    username = values.get("PIPELINE_KAFKA_SASL_USERNAME")
    password = values.get("PIPELINE_KAFKA_SASL_PASSWORD")
    mechanism = values.get("PIPELINE_KAFKA_SASL_MECHANISM", "").upper()

    if protocol == "PLAINTEXT":
        if ca_file or username or password or mechanism:
            raise ValueError(
                "Kafka TLS/SASL settings cannot be used with PLAINTEXT"
            )
        return {}

    if not ca_file or not Path(ca_file).is_file():
        raise ValueError(
            "PIPELINE_KAFKA_SSL_CA_FILE must point to an existing CA certificate"
        )
    config = {"security_protocol": protocol, "ssl_cafile": ca_file}

    if protocol == "SASL_SSL":
        if not username or not password:
            raise ValueError(
                "SASL_SSL requires PIPELINE_KAFKA_SASL_USERNAME and "
                "PIPELINE_KAFKA_SASL_PASSWORD"
            )
        if mechanism not in _SASL_MECHANISMS:
            raise ValueError(
                "PIPELINE_KAFKA_SASL_MECHANISM must be PLAIN, "
                "SCRAM-SHA-256, or SCRAM-SHA-512"
            )
        config.update(
            {
                "sasl_mechanism": mechanism,
                "sasl_plain_username": username,
                "sasl_plain_password": password,
            }
        )
    elif username or password or mechanism:
        raise ValueError("SASL credentials require SASL_SSL")

    return config
