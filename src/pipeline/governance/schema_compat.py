"""Check that a candidate Avro schema is compatible with a base version, using Schema Registry.

CI runs this on every pull request: the base is the schema file on the target branch, the
candidate is the PR's version. Both go into a throwaway subject on an ephemeral registry and
the registry itself decides, so CI applies exactly the rules production enforces instead of
a reimplementation of them.

Why BACKWARD: consumers (the streaming job) are upgraded before or with producers, and
must still read records written with the previous schema that are sitting in Kafka
(retention is 24 h). BACKWARD guarantees the new schema can read data written with the
last one: adding a field needs a default, and field types can only widen.

Run:  python -m pipeline.governance.schema_compat --base base.avsc --candidate schemas/trade.avsc
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Single source of truth: register_schema pins the production subject to this level too.
COMPATIBILITY_LEVEL = "BACKWARD"
LEVELS = (
    "BACKWARD",
    "BACKWARD_TRANSITIVE",
    "FORWARD",
    "FORWARD_TRANSITIVE",
    "FULL",
    "FULL_TRANSITIVE",
    "NONE",
)
CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


@dataclass(frozen=True)
class CompatibilityResult:
    compatible: bool
    messages: list[str] = field(default_factory=list)


class SchemaRegistryHTTPError(RuntimeError):
    pass


def _request(method: str, url: str, body: dict[str, Any] | None = None) -> Any:
    # Plain urllib rather than confluent-kafka's client: its test_compatibility() returns
    # only a bool and drops the registry's explanation, which is what CI needs to show.
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": CONTENT_TYPE}
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise SchemaRegistryHTTPError(f"{method} {url} -> HTTP {exc.code}: {detail}") from exc


def check_compatibility(
    registry_url: str,
    base_schema: str,
    candidate_schema: str,
    level: str = COMPATIBILITY_LEVEL,
) -> CompatibilityResult:
    """Ask the registry whether ``candidate_schema`` is compatible with ``base_schema``.

    Uses a uniquely named scratch subject, so it never touches real subjects and is safe
    to run against a shared registry. The subject is hard-deleted afterwards.
    """
    base = registry_url.rstrip("/")
    subject = f"compat-check-{uuid.uuid4().hex}"
    subject_url = f"{base}/subjects/{subject}"
    try:
        # Set the level on the subject itself so the result doesn't depend on the
        # registry's global default.
        _request("PUT", f"{base}/config/{subject}", {"compatibility": level})
        _request("POST", f"{subject_url}/versions", {"schema": base_schema})
        response = _request(
            "POST",
            f"{base}/compatibility/subjects/{subject}/versions/latest?verbose=true",
            {"schema": candidate_schema},
        )
        return CompatibilityResult(
            compatible=bool(response["is_compatible"]),
            messages=list(response.get("messages", [])),
        )
    finally:
        _delete_subject(base, subject)


def _delete_subject(base: str, subject: str) -> None:
    # Hard delete requires a soft delete first. Best effort: a leftover scratch subject
    # is harmless, and a cleanup failure shouldn't mask the real result.
    for suffix in ("", "?permanent=true"):
        try:
            _request("DELETE", f"{base}/subjects/{subject}{suffix}")
        except (SchemaRegistryHTTPError, urllib.error.URLError) as exc:
            logger.debug("Could not delete scratch subject %s: %s", subject, exc)
            return


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", type=Path, required=True, help="schema on the target branch")
    parser.add_argument("--candidate", type=Path, required=True, help="proposed schema")
    parser.add_argument("--level", choices=LEVELS, default=COMPATIBILITY_LEVEL)
    parser.add_argument(
        "--registry-url",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)

    # A missing or empty base means the schema is new on this branch: nothing to break.
    if not args.base.is_file() or not args.base.read_text(encoding="utf-8").strip():
        logger.info("No base schema at %s; %s is new, skipping", args.base, args.candidate)
        return 0

    result = check_compatibility(
        args.registry_url,
        args.base.read_text(encoding="utf-8"),
        args.candidate.read_text(encoding="utf-8"),
        args.level,
    )
    if result.compatible:
        logger.info("%s is %s compatible with %s", args.candidate, args.level, args.base)
        return 0

    logger.error("%s is NOT %s compatible with %s:", args.candidate, args.level, args.base)
    for message in result.messages:
        # The registry echoes the whole base schema back; it's noise in a CI log.
        if not message.startswith("{oldSchema:"):
            logger.error("  %s", message)
    return 1


if __name__ == "__main__":
    sys.exit(main())
