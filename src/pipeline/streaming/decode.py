"""Decode Confluent-framed Avro trades from a Kafka source DataFrame.

Confluent's serializer (used by the producer) writes each value as:

    byte 0      magic byte, always 0
    bytes 1-4   schema id, big-endian int (the id assigned by Schema Registry)
    bytes 5..   Avro binary payload

Spark's ``from_avro`` only understands the payload, so the 5-byte header is split off first.

Limitation: the payload is decoded with ``schemas/trade.avsc`` from this repo, assuming the
writer used a compatible schema. ``schema_id`` is kept as a column so the job can report
records written with an unexpected schema instead of silently mis-decoding them.
"""

from __future__ import annotations

from functools import reduce
from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

CONFLUENT_MAGIC_BYTE = 0
HEADER_LENGTH = 5

TRADE_COLUMNS = (
    "exchange",
    "symbol",
    "trade_id",
    "price",
    "quantity",
    "is_buyer_maker",
    "event_time",
    "ingest_time",
)

# Stable labels, matching the producer's DLQ ``error_type`` style.
ERROR_NOT_FRAMED = "not_confluent_framed"
ERROR_UNDECODABLE = "avro_decode_failed"
ERROR_MESSAGES = {
    ERROR_NOT_FRAMED: "value is empty or lacks the Confluent magic byte and schema id header",
    ERROR_UNDECODABLE: "Avro payload could not be decoded with schemas/trade.avsc",
}


def load_avro_schema(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _schema_id(value: Column) -> Column:
    # substring() on binary is 1-indexed: bytes 2..5 hold the big-endian schema id.
    return F.conv(F.hex(F.substring(value, 2, 4)), 16, 10).cast("int")


def _classify(kafka_df: DataFrame, avro_schema: str) -> DataFrame:
    """Every Kafka row, decoded where possible, with ``error_type`` set where not.

    ``mode=PERMISSIVE`` turns undecodable payloads into nulls instead of failing the whole
    streaming query on one bad record. In that mode Spark 4 returns a struct whose *fields*
    are all null, not a null struct, so validity is checked per field. Every Trade field is
    required in the schema, so any null field means the record didn't decode.
    """
    value = F.col("value")
    payload = F.expr(f"substring(value, {HEADER_LENGTH + 1}, length(value) - {HEADER_LENGTH})")
    # coalesce: a null value (Kafka tombstone) makes the comparison null, not False.
    framed = F.coalesce(
        (F.length(value) > HEADER_LENGTH)
        & (F.hex(F.substring(value, 1, 1)) == F.lit(f"{CONFLUENT_MAGIC_BYTE:02X}")),
        F.lit(False),
    )
    decoded = kafka_df.select(
        "key",
        "value",
        framed.alias("framed"),
        # Only decode framed rows; the rest are already known to be bad.
        F.when(framed, from_avro(payload, avro_schema, {"mode": "PERMISSIVE"})).alias("trade"),
        _schema_id(value).alias("schema_id"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    all_fields_decoded = reduce(
        lambda acc, name: acc & F.col(f"trade.{name}").isNotNull(),
        TRADE_COLUMNS,
        F.col("trade").isNotNull(),
    )
    return decoded.withColumn(
        "error_type",
        F.when(~F.col("framed"), F.lit(ERROR_NOT_FRAMED)).when(
            ~all_fields_decoded, F.lit(ERROR_UNDECODABLE)
        ),
    ).drop("framed")


def decode_trades(kafka_df: DataFrame, avro_schema: str) -> DataFrame:
    """Kafka rows (binary ``value``) -> one typed row per trade.

    Output columns: the Trade fields (``event_time``/``ingest_time`` as timestamps),
    plus ``schema_id``, ``kafka_partition``, ``kafka_offset``.

    Rows that aren't Confluent-framed, or whose payload can't be decoded, are left out
    here; ``undecodable_trades`` returns them for the dead-letter queue.
    """
    return (
        _classify(kafka_df, avro_schema)
        .where(F.col("error_type").isNull())
        .select(
            *(F.col(f"trade.{name}").alias(name) for name in TRADE_COLUMNS),
            "schema_id",
            "kafka_partition",
            "kafka_offset",
        )
    )


def undecodable_trades(kafka_df: DataFrame, avro_schema: str) -> DataFrame:
    """The Kafka rows ``decode_trades`` leaves out, with the original bytes and the reason.

    Output columns: ``key``, ``value`` (as read), ``error_type``, ``error_message``,
    ``kafka_partition``, ``kafka_offset``.
    """
    error_type = F.col("error_type")
    return (
        _classify(kafka_df, avro_schema)
        .where(error_type.isNotNull())
        .select(
            "key",
            "value",
            error_type,
            F.when(error_type == ERROR_NOT_FRAMED, F.lit(ERROR_MESSAGES[ERROR_NOT_FRAMED]))
            .otherwise(F.lit(ERROR_MESSAGES[ERROR_UNDECODABLE]))
            .alias("error_message"),
            "kafka_partition",
            "kafka_offset",
        )
    )
