"""SparkSession construction shared by the streaming job and the tests."""

from __future__ import annotations

import pyspark
from pyspark.sql import SparkSession

# Spark 4.x is built for Scala 2.13. Connector jars must match the Spark version exactly,
# so the version comes from the installed pyspark instead of being hardcoded.
SCALA_BINARY_VERSION = "2.13"
AVRO_PACKAGE = "spark-avro"
KAFKA_PACKAGE = "spark-sql-kafka-0-10"


def maven_coordinates(*artifacts: str) -> str:
    """e.g. ``org.apache.spark:spark-avro_2.13:4.2.0`` for ``spark.jars.packages``."""
    return ",".join(
        f"org.apache.spark:{artifact}_{SCALA_BINARY_VERSION}:{pyspark.__version__}"
        for artifact in artifacts
    )


def build_session(
    app_name: str,
    *,
    master: str | None = None,
    packages: tuple[str, ...] = (AVRO_PACKAGE,),
    shuffle_partitions: int = 6,
) -> SparkSession:
    """Build (or reuse) a SparkSession with the given connector packages.

    The session time zone is pinned to UTC so window boundaries and timestamp output
    don't depend on the machine's local time zone.
    """
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    return (
        builder.config("spark.jars.packages", maven_coordinates(*packages))
        .config("spark.sql.session.timeZone", "UTC")
        # Default is 200, far too many for a few symbols; one per Kafka partition fits here.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
