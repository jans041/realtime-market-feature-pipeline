# Real-Time Market Data Feature Pipeline

Streaming market events through Kafka and PySpark Structured Streaming into a Feast feature store (Redis online, Iceberg offline), with Great Expectations data contracts and GitHub Actions CI.

> Status: Phase 1 — ingestion (Coinbase WebSocket -> Kafka, Avro + Schema Registry, DLQ).

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

## Contributing

- Branch from `main`: `feature/<scope>`, `fix/<scope>`, `ci/<scope>`
- Commits follow [Conventional Commits](https://www.conventionalcommits.org) (enforced by a `commit-msg` hook)
- All changes land via pull request
