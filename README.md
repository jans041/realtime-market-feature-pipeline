# Real-Time Market Data Feature Pipeline

Streaming market events through Kafka and PySpark Structured Streaming into a Feast feature store (Redis online, Iceberg offline), with Great Expectations data contracts and GitHub Actions CI.

> Status: Phase 2 — streaming features (PySpark Structured Streaming, sliding windows, watermarks).

## Repository layout

```
.
├── .github/workflows/      # CI (Phase 2)
├── docs/                   # architecture diagrams, ADRs
├── feature_repo/           # Feast definitions (Phase 3)
├── schemas/                # Avro schemas registered in Schema Registry
├── src/pipeline/
│   ├── ingestion/          # WebSocket -> Kafka producer
│   ├── streaming/          # PySpark Structured Streaming jobs
│   ├── features/           # feature transforms
│   └── quality/            # Great Expectations suites (Phase 4)
├── tests/unit/
├── docker-compose.yml
└── pyproject.toml
```

## Local setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install --install-hooks -t pre-commit -t commit-msg
cp .env.example .env

docker compose up -d
docker compose ps
```

| Service         | Address                 |
|-----------------|-------------------------|
| Kafka (host)    | `localhost:9092`        |
| Kafka (Docker)  | `kafka:29092`           |
| Schema Registry | `http://localhost:8081` |
| Redis           | `localhost:6379`        |
| Console UI      | `http://localhost:8080` |

Topics created on startup: `market.trades.raw`, `market.trades.dlq`, `market.features.1m`.

## Ingestion: trade producer

`src/pipeline/ingestion/producer.py` subscribes to the public Coinbase Advanced Trade
`market_trades` channel and produces each trade to Kafka.

```bash
python -m pipeline.ingestion.register_schema   # once, and after any schema change
python -m pipeline.ingestion.producer          # Ctrl+C to stop (flushes pending messages)
```

| Output | Format | Key | Contents |
|---|---|---|---|
| `market.trades.raw` | Avro, `schemas/trade.avsc` | symbol, e.g. `BTC-USD` | validated trades |
| `market.trades.dlq` | JSON envelope | none | rejected payload + `error_type`, `error_message`, `source`, `failed_at` |

Design notes:

- **Explicit schema registration.** The producer runs with `auto.register.schemas=false` and
  checks at startup that `trade.avsc` is registered, so schema changes go through review,
  not a deploy. The registry enforces `BACKWARD` compatibility.
- **Keyed by symbol** so all trades for a symbol land on one partition, in order.
- **Durable, ordered delivery:** `acks=all` + `enable.idempotence=true`; `linger.ms=20` and
  `lz4` compression for batching.
- **Per-trade validation.** A Coinbase message can carry several trades; each is validated
  on its own, so one bad trade goes to the DLQ without dropping its neighbours.
- **Reconnects** use exponential backoff with full jitter (1s base, 60s cap) and re-subscribe
  on every connection. Subscribe snapshots (the last ~50 trades) are skipped by default
  (`INCLUDE_SNAPSHOTS=false`) so a reconnect doesn't replay old trades as late events.
- **Two timestamps:** `event_time` (exchange) drives streaming windows; `ingest_time`
  (producer) makes ingestion lag measurable. Keep the host clock NTP-synced, or the lag
  is meaningless.

Configuration is via environment variables (see `.env.example`).

## Streaming: windowed features

`src/pipeline/streaming/job.py` reads raw trades, computes per-symbol features over sliding
windows, and writes them to `market.features.1m`.

```bash
docker compose up -d streaming-job        # builds the Spark image on first run
docker compose logs -f streaming-job      # one progress line per micro-batch
docker compose stop streaming-job
```

Spark UI while it runs: http://localhost:4040

| Feature | Definition |
|---|---|
| `trade_count` | trades in the window |
| `volume` | total quantity traded |
| `vwap` | volume-weighted average price, `sum(price * qty) / sum(qty)` |
| `price_min`, `price_max` | price range |
| `buy_volume`, `sell_volume` | quantity where the buyer / seller was the aggressor (crossed the spread) |

Each output message is keyed by symbol; the value is JSON with `symbol`, `window_start`,
`window_end` (UTC) and the features.

Design notes:

- **Sliding windows:** 1 minute long, every 10 seconds, on `event_time` (exchange time), so
  results don't depend on when trades happen to arrive.
- **Watermark: 10 seconds.** Trades arriving more than 10 s behind the newest event time are
  dropped. Measured exchange-to-producer lag is p99 ~234 ms, so this leaves ample headroom.
- **Append output mode:** each window is written once, when the watermark passes its end.
  Results are final, at the cost of ~10-20 s extra latency after the window closes.
- **Decoding:** the producer writes Confluent-framed Avro (magic byte + 4-byte schema id +
  payload); the job strips the 5-byte header and decodes with `schemas/trade.avsc`.
  Undecodable records are dropped rather than failing the query.
- **Checkpointing** in the `spark-checkpoints` Docker volume: Kafka offsets and window state
  survive restarts. `maxOffsetsPerTrigger` caps batch size when catching up on a backlog.
- **Known limitation:** windows that straddle the job's (or producer's) start contain only
  partial data.

Spark runs in Docker (`docker/spark/Dockerfile`, Python 3.12 + Java 17): on native Windows,
Spark's Hadoop layer needs `winutils.exe` to load connector packages.

## Running tests

```bash
pytest                                    # everything except Spark tests (they skip on Windows)
docker compose run --rm spark pytest      # full suite, including Spark, on Linux
```

CI runs ruff, mypy and the full test suite on every pull request.

## Contributing

- Branch from `main`: `feature/<scope>`, `fix/<scope>`, `ci/<scope>`
- Commits follow [Conventional Commits](https://www.conventionalcommits.org) (enforced by a `commit-msg` hook)
- All changes land via pull request
