"""Register the trade Avro schema with Schema Registry.

The producer runs with ``auto.register.schemas=False``, so schema changes only reach the
registry through this explicit step (and, later, through CI) rather than as a side effect
of deploying a producer.

Idempotent: re-registering an identical schema returns the existing id.

Run:  python -m pipeline.ingestion.register_schema
"""

from __future__ import annotations

import logging
import sys

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient, SchemaRegistryError

from pipeline.ingestion.config import ProducerSettings

logger = logging.getLogger(__name__)

SUBJECT_NOT_FOUND = 40401


def register(settings: ProducerSettings) -> int:
    """Register the schema and return its global schema id."""
    client = SchemaRegistryClient({"url": settings.schema_registry_url})
    subject = settings.trade_value_subject
    schema = Schema(settings.trade_schema_path.read_text(encoding="utf-8"), "AVRO")

    try:
        compatible = client.test_compatibility(subject, schema)
    except SchemaRegistryError as exc:
        if exc.error_code != SUBJECT_NOT_FOUND:
            raise
        logger.info("Subject %s does not exist yet; this will be version 1", subject)
    else:
        if not compatible:
            raise SystemExit(
                f"{settings.trade_schema_path.name} is not compatible with the latest version "
                f"of '{subject}' under the registry's compatibility level. Refusing to register."
            )

    schema_id = client.register_schema(subject, schema)
    version = client.get_latest_version(subject).version
    logger.info(
        "Registered %s as schema id=%s (subject %s, version %s)",
        settings.trade_schema_path.name,
        schema_id,
        subject,
        version,
    )
    return schema_id


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # Schema Registry client request logs
    settings = ProducerSettings()
    try:
        register(settings)
    except SchemaRegistryError as exc:
        logger.error(
            "Schema Registry at %s rejected the request: %s", settings.schema_registry_url, exc
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
