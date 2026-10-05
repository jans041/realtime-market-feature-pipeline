# Linux runtime for everything Spark: unit tests and the streaming job.
# Spark's Hadoop layer needs native winutils.exe on Windows; running it here avoids that
# and matches the Linux CI environment.
#
# Build:  docker compose build spark
# Tests:  docker compose run --rm spark pytest
FROM python:3.12-slim-bookworm

# Spark 4.x runs on Java 17+. procps provides `ps`, which Spark's launch scripts use.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies first, so this layer is cached until pyproject.toml changes.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[dev,spark]"

# Download the Kafka and Avro connector jars at build time, so runs need no network
# and always use the same jars.
RUN python -c "from pipeline.streaming.session import AVRO_PACKAGE, KAFKA_PACKAGE, build_session; \
build_session('warm-ivy-cache', master='local[1]', packages=(AVRO_PACKAGE, KAFKA_PACKAGE)).stop()"

# At runtime the repo is bind-mounted at /app; PYTHONPATH makes the mounted source
# take precedence over the copy installed above, so code edits need no rebuild.
ENV PYTHONPATH=/app/src \
    PYTHONDONTWRITEBYTECODE=1
