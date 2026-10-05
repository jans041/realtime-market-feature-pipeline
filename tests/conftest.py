from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

DOCKER_HINT = "run Spark tests in Docker: docker compose run --rm spark pytest"


def _windows_without_winutils() -> bool:
    hadoop_home = os.environ.get("HADOOP_HOME")
    return sys.platform == "win32" and not (
        hadoop_home and (Path(hadoop_home) / "bin" / "winutils.exe").exists()
    )


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One local SparkSession for the whole test run (startup takes ~15-20 s).

    Skipped when the optional ``spark`` extra isn't installed, and on native Windows,
    where loading Spark connector packages requires Hadoop's winutils.exe.
    """
    pytest.importorskip("pyspark")
    if _windows_without_winutils():
        pytest.skip(DOCKER_HINT)

    from pipeline.streaming.session import build_session

    # Python workers must use this interpreter, not whatever "python" is on PATH.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = build_session("unit-tests", master="local[2]", shuffle_partitions=2)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
