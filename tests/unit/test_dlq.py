import json

from pipeline.ingestion.dlq import ERROR_TYPE_HEADER, build_dlq_record


def test_envelope_carries_payload_and_context() -> None:
    record = build_dlq_record(
        '{"e":"trade","p":"abc"}',
        error_type="validation_error",
        error_message="p: Input should be a valid number",
        source="wss://example/stream",
        failed_at_ms=1_727_600_000_000,
    )

    assert json.loads(record.value) == {
        "raw_payload": '{"e":"trade","p":"abc"}',
        "error_type": "validation_error",
        "error_message": "p: Input should be a valid number",
        "source": "wss://example/stream",
        "failed_at": 1_727_600_000_000,
    }
    assert record.headers == [(ERROR_TYPE_HEADER, b"validation_error")]


def test_non_utf8_bytes_are_preserved_not_dropped() -> None:
    record = build_dlq_record(
        b"\xff\xfeabc",
        error_type="invalid_json",
        error_message="bad bytes",
        source="s",
        failed_at_ms=0,
    )

    assert json.loads(record.value)["raw_payload"] == "\\xff\\xfeabc"
