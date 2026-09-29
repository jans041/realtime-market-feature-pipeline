# Real-Time Market Data Feature Pipeline

Streaming market events through Kafka and PySpark Structured Streaming into a Feast feature store (Redis online, Iceberg offline), with Great Expectations data contracts and GitHub Actions CI.

> Status: Phase 1 — local infrastructure and repo tooling.

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

## Contributing

- Branch from `main`: `feature/<scope>`, `fix/<scope>`, `ci/<scope>`
- Commits follow [Conventional Commits](https://www.conventionalcommits.org) (enforced by a `commit-msg` hook)
- All changes land via pull request
