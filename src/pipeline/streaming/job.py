"""Streaming job: raw trades (Kafka, Avro) -> windowed features (Kafka, JSON).

    market.trades.raw -> decode_trades -> compute_features -> market.features.1m
                      -> undecodable_trades -> market.trades.dlq

Two streaming queries share one SparkSession. The features query is a stateful windowed
aggregation, so bad rows can't be split off inside it; each query reads the raw topic with
its own checkpoint instead. If either stops, the job exits rather than run half-blind.

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

from pipeline.ingestion.dlq import ERROR_TYPE_HEADER
from pipeline.streaming.config import StreamingSettings
from pipeline.streaming.decode import decode_trades, load_avro_schema, undecodable_trades
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


def to_dlq_records(dead_letters: DataFrame, source_topic: str) -> DataFrame:
    """Shape undecodable rows like the producer's DLQ records (see ingestion/dlq.py).

    Same JSON envelope and ``error_type`` header, so one consumer can read both. Two
    differences: the payload is binary Avro, so ``raw_payload`` is base64 and says so in
    ``payload_encoding``; and ``source`` points at the exact record, for replay.
    The original key is kept, so dead letters land in the partition matching their symbol.
    """
    envelope = F.struct(
        F.base64("value").alias("raw_payload"),
        F.lit("base64").alias("payload_encoding"),
        F.col("error_type"),
        F.col("error_message"),
        F.format_string(
            "kafka://%s/%d/%d", F.lit(source_topic), "kafka_partition", "kafka_offset"
        ).alias("source"),
        F.unix_millis(F.current_timestamp()).alias("failed_at"),
    )
    return dead_letters.select(
        "key",
        F.to_json(envelope).alias("value"),
        F.array(
            F.struct(
                F.lit(ERROR_TYPE_HEADER).alias("key"),
                F.col("error_type").cast("binary").alias("value"),
            )
        ).alias("headers"),
    )


def build_dlq_query(spark: SparkSession, settings: StreamingSettings) -> StreamingQuery:
    dead_letters = undecodable_trades(
        read_raw_trades(spark, settings), load_avro_schema(settings.trade_schema_path)
    )
    return (
        to_dlq_records(dead_letters, settings.kafka_topic_raw)
        .writeStream.format("kafka")
        .queryName("trades_dlq")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("topic", settings.kafka_topic_dlq)
        .option("checkpointLocation", settings.dlq_checkpoint_dir)
        # Stateless, so append just forwards each bad row once.
        .outputMode("append")
        .trigger(processingTime=settings.trigger_interval)
        .start()
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
    """Logs one line per micro-batch and query: throughput, latency and the watermark.

    Method names are camelCase because Spark calls them by these exact names.
    """

    def onQueryStarted(self, event: QueryStartedEvent) -> None:  # noqa: N802
        logger.info("Query started name=%s id=%s", event.name, event.id)

    def onQueryProgress(self, event: QueryProgressEvent) -> None:  # noqa: N802
        p = event.progress
        logger.info(
            "query=%s batch=%d input_rows=%d input_rate=%.1f/s processed_rate=%.1f/s "
            "batch_ms=%s watermark=%s",
            p.name,
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

    queries = [build_query(spark, settings), build_dlq_query(spark, settings)]
    logger.info(
        "Streaming %s -> %s (window=%s slide=%s watermark=%s trigger=%s)",
        settings.kafka_topic_raw,
        settings.kafka_topic_features,
        settings.feature_window,
        settings.feature_slide,
        settings.feature_watermark,
        settings.trigger_interval,
    )
    logger.info("Undecodable trades -> %s", settings.kafka_topic_dlq)
    try:
        # Returns (or raises) as soon as either query stops.
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Stopping queries")
    finally:
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
