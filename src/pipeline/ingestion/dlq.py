"""Dead-letter queue record format for messages that could not be produced as Trades.

DLQ records are plain JSON, not Avro: they are malformed by definition, so forcing them
through the Trade schema would lose exactly the information needed to debug them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

ERROR_TYPE_HEADER = "error_type"


@dataclass(frozen=True, slots=True)
class DlqRecord:
    value: bytes
    headers: list[tuple[str, bytes]]


def build_dlq_record(
    raw: str | bytes,
    *,
    error_type: str,
    error_message: str,
    source: str,
    failed_at_ms: int,
) -> DlqRecord:
    """Wrap a rejected message with enough context to diagnose and replay it."""
    raw_text = raw.decode("utf-8", errors="backslashreplace") if isinstance(raw, bytes) else raw
    envelope = {
        "raw_payload": raw_text,
        "error_type": error_type,
        "error_message": error_message,
        "source": source,
        "failed_at": failed_at_ms,
    }
    return DlqRecord(
        value=json.dumps(envelope, separators=(",", ":")).encode("utf-8"),
        headers=[(ERROR_TYPE_HEADER, error_type.encode("utf-8"))],
    )
