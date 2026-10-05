"""Streaming job: raw trades (Kafka, Avro) -> windowed features (Kafka, JSON).

    market.trades.raw -> decode_trades -> compute_features -> market.features.1m

Runs in the Spark container (see docker/spark/Dockerfile):
    docker compose up -d streaming-job
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener
from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
)

from pipeline.streaming.config import StreamingSettings
from pipeline.streaming.decode import decode_trades, load_avro_schema
from pipeline.streaming.features import compute_features
from pipeline.streaming.session import AVRO_PACKAGE, KAFKA_PACKAGE, build_session

logger = logging.getLogger(__name__)


def read_raw_trades(spark: SparkSession, settings: StreamingSettings) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic_raw)
        .option("startingOffsets", settings.starting_offsets)
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .load()
    )


def to_kafka_records(features: DataFrame) -> DataFrame:
    """Shape feature rows for the Kafka sink: ``key`` = symbol, ``value`` = JSON of the row.

    Keying by symbol keeps each symbol's features in one partition, in window order,
    the same way the raw topic is keyed.
    """
    return features.select(
        F.col("symbol").alias("key"),
        F.to_json(F.struct(*features.columns)).alias("value"),
    )


def build_query(spark: SparkSession, settings: StreamingSettings) -> StreamingQuery:
    trades = decode_trades(
        read_raw_trades(spark, settings), load_avro_schema(settings.trade_schema_path)
    )
    features = compute_features(
        trades,
        window=settings.feature_window,
        slide=settings.feature_slide,
        watermark=settings.feature_watermark,
    )
    return (
        to_kafka_records(features)
        .writeStream.format("kafka")
        .queryName("features_1m")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("topic", settings.kafka_topic_features)
        # Kafka offsets read and window state live here, so a restart resumes where it left off.
        .option("checkpointLocation", settings.checkpoint_dir)
        # Append: each window is written once, when the watermark passes its end.
        .outputMode("append")
        .trigger(processingTime=settings.trigger_interval)
        .start()
    )


class ProgressLogger(StreamingQueryListener):
    """Logs one line per micro-batch: throughput, latency and the current watermark.

    Method names are camelCase because Spark calls them by these exact names.
    """

    def onQueryStarted(self, event: QueryStartedEvent) -> None:  # noqa: N802
        logger.info("Query started id=%s", event.id)

    def onQueryProgress(self, event: QueryProgressEvent) -> None:  # noqa: N802
        p = event.progress
        logger.info(
            "batch=%d input_rows=%d input_rate=%.1f/s processed_rate=%.1f/s "
            "batch_ms=%s watermark=%s",
            p.batchId,
            p.numInputRows,
            p.inputRowsPerSecond,
            p.processedRowsPerSecond,
            p.durationMs.get("triggerExecution"),
            p.eventTime.get("watermark"),
        )

    def onQueryIdle(self, event: QueryIdleEvent) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:  # noqa: N802
        if event.exception:
            logger.error("Query terminated with error: %s", event.exception)
        else:
            logger.info("Query terminated")


def main() -> None:
    settings = StreamingSettings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)  # Python<->JVM bridge chatter

    spark = build_session("features-1m", packages=(AVRO_PACKAGE, KAFKA_PACKAGE))
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger())

    query = build_query(spark, settings)
    logger.info(
        "Streaming %s -> %s (window=%s slide=%s watermark=%s trigger=%s)",
        settings.kafka_topic_raw,
        settings.kafka_topic_features,
        settings.feature_window,
        settings.feature_slide,
        settings.feature_watermark,
        settings.trigger_interval,
    )
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping query")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
