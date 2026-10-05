"""Access to the Feast feature store with the environment it expects.

``feature_store.yaml`` and ``definitions.py`` read ``${VARS}`` from the process
environment, and the Feast CLI doesn't load ``.env``. These helpers export the values
from ``FeatureStoreSettings`` first, so Feast works the same on Windows and in Docker.

    python -m pipeline.features.store plan     # show what `apply` would change
    python -m pipeline.features.store apply    # register definitions, set up Redis
"""

from __future__ import annotations

import os
import sys

from feast import FeatureStore

from pipeline.features.config import FeatureStoreSettings
from pipeline.features.iceberg import export_feast_catalog_env


def export_feast_env(settings: FeatureStoreSettings) -> None:
    os.environ["REDIS_CONNECTION_STRING"] = settings.redis_connection_string
    os.environ["ICEBERG_CATALOG_URI"] = settings.iceberg_catalog_uri
    os.environ["ICEBERG_WAREHOUSE"] = settings.iceberg_warehouse
    export_feast_catalog_env(settings)


def get_store(settings: FeatureStoreSettings) -> FeatureStore:
    export_feast_env(settings)
    return FeatureStore(repo_path=str(settings.feature_repo_path))


def main() -> None:
    """Run a Feast CLI command against this repo, e.g. ``plan`` or ``apply``."""
    from feast.cli.cli import cli

    settings = FeatureStoreSettings()  # type: ignore[call-arg]  # S3 credentials come from env
    export_feast_env(settings)
    cli(["--chdir", str(settings.feature_repo_path), *sys.argv[1:]])


if __name__ == "__main__":
    main()
