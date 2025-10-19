# Quant Signal Streaming Platform

This document lays out a complete plan for a PyFlink-based quantitative signal platform that ingests live Coinbase market data, orchestrates analytics in Apache Flink on Docker Compose, persists state in PostgreSQL, moves data through Kafka, and visualizes strategy metrics such as Sharpe, Sortino, and returns in Grafana. The sections below provide everything needed to take the project from zero to a working baseline before we begin detailed implementation.

## 1. System Overview
- **Objective** build a modular quant research and production stack for digital assets that performs streaming analytics and publishes performance telemetry in real time.
- **Core services** Coinbase price producer → Kafka (broker) → Flink (PyFlink jobs) → PostgreSQL (state & reference data) → Grafana (dashboards via PostgreSQL / Prometheus adapters).
- **Strategy layer** encapsulated inside PyFlink jobs (`flink_jobs/strategies`) operating on normalized Kafka streams; outputs trades, signals, and rolling performance metrics back to Kafka and PostgreSQL via asynchronous sinks to keep processing non-blocking. The baseline module is a simple moving-average crossover (`sma_cross`) that we will fine tune through automated rolling backtests.
- **Observability** Grafana dashboards consume metrics from PostgreSQL views and optional Prometheus exporters attached to Flink / Kafka for cluster health.

## 2. Target Architecture
- Price feed microservice authenticates to Coinbase WebSocket API, enriches ticks, and publishes to Kafka topic `prices.raw` (and optionally mirrors to long-retention `prices.replay`).
- Kafka Connect (optional) or custom PyFlink source normalizes ticks and pushes to topic `prices.normalized`.
- Flink job graph:
  - **Ingestion operator** consumes `prices.normalized`, applies schema validation.
  - **Feature stage** computes returns, volatility measures, rolling windows; pushes to `signals.features`.
  - **Strategy stage** (pluggable Python module; default `sma_cross`) emits strategy signals & positions to `signals.decisions`.
  - **Performance stage** computes Sharpe/Sortino/returns using keyed state & tumbling windows; writes metrics into PostgreSQL table `strategy_metrics` via asynchronous sink operators and Kafka topic `metrics.performance`.
  - **Rolling backtest stage** (on-demand) replays historical messages through the same strategy pipeline to fine tune SMA window parameters and generate performance diagnostics.
- Downstream Grafana dashboards read `strategy_metrics` using a PostgreSQL data source; additional panels driven by Kafka (via Kafka REST proxy) or Prometheus exporters.

## 3. Proposed Repository Layout
```
flink-signals/
├── docker/
│   ├── flink/                  # Custom Flink image definitions, jobmanager/taskmanager configs
│   ├── kafka/                  # Broker + schema registry settings
│   ├── postgres/               # Initialization scripts, migrations
│   └── grafana/                # Provisioning: datasources + dashboards JSON
├── docker-compose.yml          # Orchestrates entire stack
├── env/                        # Environment templates (.env, secrets)
├── flink_jobs/
│   ├── strategies/             # Strategy implementations (PyFlink Table API / DataStream API, default sma_cross.py)
│   ├── metrics/                # Metric computation utilities (Sharpe, Sortino, drawdowns)
│   ├── schemas/                # Avro/JSON schemas, dataclasses
│   ├── replay/                 # Pipelines/utilities for Kafka-based backtest & replay flows
│   ├── __main__.py             # Entry point for job submission
│   └── requirements.txt        # PyFlink job dependencies
├── producer/
│   ├── coinbase_client.py      # WebSocket client handling feed connection & heartbeats
│   ├── kafka_producer.py       # Async producer w/ batching, compression
│   ├── config.py
│   └── tests/
├── grafana/
│   ├── dashboards/             # Domain dashboards
│   └── provisioning/
├── notebooks/                  # Research notebooks for off-line backtests
├── scripts/
│   ├── submit_flink_job.sh     # Wrapper to deploy jobs via REST API / CLI
│   ├── bootstrap_data.py       # Seed database, create topics
│   └── replay_prices.py        # Trigger Kafka offset replays for strategy backtests
└── README.md                   # (this plan)
```

## 4. Data Contracts & Flow
- **Kafka topics**
  - `prices.raw`: JSON payload from Coinbase (symbol, bid/ask, timestamp, sequence).
  - `prices.normalized`: Flattened schema with converted UTC timestamps, mid-price, microstructure features.
  - `signals.features`: Features enriched by PyFlink (returns, volatility, rolling stats).
  - `signals.decisions`: Strategy outputs (signal, position, confidence, rationale).
  - `metrics.performance`: Rolling Sharpe, Sortino, cumulative returns, drawdowns.
  - `prices.replay`: High-retention topic for storing historical ticks that can be replayed through strategies under test or algorithm refinements.
- **PostgreSQL tables**
  - `instruments` (static metadata), `strategy_positions`, `strategy_metrics`, `latency_metrics`.
  - Utilize TimescaleDB extension (optional) for efficient time-series storage and rely on `asyncpg` in supporting services for non-blocking read/write paths.
- **Grafana visualization**
  - Dashboards: Realtime metrics (Sharpe, Sortino, returns), strategy exposure, latency overview, Kafka/Flink health.

## 5. Strategy Layer Placement
- Strategy logic resides under `flink_jobs/strategies/`. Each strategy implements an interface such as `StrategyNode` with methods:
  - `prepare_environment(table_env)` for registering UDFs and asynchronous connectors (e.g., `asyncpg` metadata fetchers, async sinks).
  - `build_pipeline(prices_table, *, historical_source=None)` returning the Table/DataStream transformations while optionally consuming replay streams for backtests.
- Strategies are modular, discoverable units (via registry or Python entry points), enabling interchangeable trading logic without changing the orchestration code.
- **Example strategy (`sma_cross`)** serves as the initial implementation: computes fast/slow simple moving averages on normalized mid-price, emits long/short/flat signals on crossovers, and writes signal metadata (window lengths, crossover direction, confidence). Parameters (fast/slow window, confirmation periods) are fine tuned using the rolling backtest harness that replays recent history from `prices.replay`.
- Metrics functions live in `flink_jobs/metrics/`, enabling reuse across strategies.
- Strategy selection handled via environment variable or job config (`STRATEGY_MODULE=sma_cross` by default), enabling dynamic deployment and experimentation.
- Backtest harness uses the same strategies with `historical_source` bound to the Kafka `prices.replay` topic to replay historic sessions deterministically.

## 6. Sample `docker-compose.yml`
```yaml
version: "3.9"

services:
  kafka:
    image: bitnami/kafka:3.7
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "false"
    depends_on:
      - zookeeper

  zookeeper:
    image: bitnami/zookeeper:3.9
    ports:
      - "2181:2181"

  postgres:
    image: timescale/timescaledb-ha:pg16
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./docker/postgres/init:/docker-entrypoint-initdb.d

  flink-jobmanager:
    build:
      context: ./docker/flink
      dockerfile: Dockerfile
    command: jobmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
      - FLINK_PROPERTIES=
          jobmanager.rpc.address: flink-jobmanager
          parallelism.default: 2
    ports:
      - "8081:8081"
    volumes:
      - ./flink_jobs:/opt/flink/usrlib
    depends_on:
      - kafka
      - postgres

  flink-taskmanager:
    build:
      context: ./docker/flink
      dockerfile: Dockerfile
    command: taskmanager
    scale: 2
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
    volumes:
      - ./flink_jobs:/opt/flink/usrlib
    depends_on:
      - flink-jobmanager

  producer:
    build: ./producer
    command: python -m producer.run
    environment:
      - COINBASE_API_KEY=${COINBASE_API_KEY}
      - COINBASE_API_SECRET=${COINBASE_API_SECRET}
      - COINBASE_PRODUCT_IDS=${COINBASE_PRODUCT_IDS}
      - KAFKA_BROKER=kafka:9092
    depends_on:
      - kafka

  grafana:
    image: grafana/grafana:10.4.2
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_PASSWORD}
    volumes:
      - grafana_storage:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning
    depends_on:
      - postgres

volumes:
  postgres_data:
  grafana_storage:
```

## 7. Sample `.env` Template (`env/.env.example`)
```
POSTGRES_USER=flink
POSTGRES_PASSWORD=flink_password
POSTGRES_DB=signals

COINBASE_API_KEY=your_key
COINBASE_API_SECRET=your_secret
COINBASE_PRODUCT_IDS=BTC-USD,ETH-USD

GRAFANA_PASSWORD=admin

FLINK_PARALLELISM=2
STRATEGY_MODULE=sma_cross
```

## 8. Operational Considerations
- **High-throughput ingestion** use Kafka with compression (lz4/snappy), configure producer batching, and ensure Flink task slots align with Kafka partitions.
- **State management** leverage incremental checkpoints (RocksDB state backend) stored to local `./state` volume initially; extend to S3/GCS for production.
- **Backtesting via replay** configure long-lived Kafka retention policies and companion tooling to replay historical message batches through strategies for regression testing and research.
- **Metrics efficiency** compute Sharpe/Sortino using streaming windows (e.g., 5m, 1h) with keyed state to avoid recomputation; consider using Flink SQL `TABLE`/`VIEW` for direct PostgreSQL sink with upsert semantics.
- **Schema governance** maintain Avro/JSON schemas in `flink_jobs/schemas` and enforce through schema registry (Confluent/Apicurio) if scaling.
- **Async database access** standardize on `asyncpg` for auxiliary services/sidecars that interact with PostgreSQL to avoid blocking event loops and to maximize throughput in streaming contexts.
- **Security** manage secrets via `.env` (development) and Docker secrets (production). Segregate Grafana credentials and API keys.

## 9. End-to-End Implementation Roadmap (0 → 100)
1. **Bootstrap repository**
   - Initialize Python tooling (pyproject for producer, requirements for PyFlink).
   - Set up pre-commit hooks (lint, formatting, mypy).
2. **Container images**
   - Create Dockerfiles for Flink (install PyFlink dependencies, copy jars for Kafka, JDBC) and producer.
   - Script to build/push images for dev/prod.
3. **Kafka & PostgreSQL setup**
   - Provision topics via `scripts/bootstrap_data.py`.
   - Add Flyway/Alembic migrations for tables (`strategy_metrics`, etc.).
4. **Coinbase producer**
   - Implement resilient WebSocket client (auto-reconnect, subscriptions, heartbeats).
   - Serialize to Kafka with schema validation; include integration tests using `pytest` + mock WebSocket server.
   - Optionally dual-write to `prices.replay` with extended retention for future replays/backtests.
5. **PyFlink job scaffolding**
   - Configure execution environment, connectors, and Table/SQL API.
   - Implement base pipeline with pass-through to ensure infrastructure works, including async JDBC sinks using `asyncpg` wrappers.
6. **Feature engineering layer**
   - Add rolling window computations (returns, volatility).
   - Validate via dedicated unit tests and local Flink mini-cluster runs.
7. **Strategy layer**
   - Implement first strategy (`sma_cross.py`) in `flink_jobs/strategies/`, parameterized for fast/slow windows and crossover confirmation.
   - Provide interface for multiple strategies, dynamic loading, and optional historical replay connectors.
   - Build rolling backtest harness to automatically replay recent `prices.replay` slices and fine tune SMA parameters (grid search or Bayesian optimization).
8. **Performance metrics**
   - Implement streaming Sharpe/Sortino/returns calculators with keyed state and windowed aggregations.
   - Sink to PostgreSQL using asynchronous pathways (e.g., `asyncpg` sidecars or Flink Async I/O) and to Kafka for downstream consumers.
9. **Grafana dashboards**
   - Provision PostgreSQL data source, create panels for metrics & strategy KPIs.
   - Add alerting rules (e.g., low Sharpe, high latency).
10. **Replay & backtest harness**
    - Build tooling to snapshot Kafka offsets, manage `prices.replay` retention, and trigger deterministic strategy replays.
    - Provide CLI/notebook integration to run backtests through the live pipeline with configurable time windows and capture results.
11. **Observability stack (optional enhancement)**
    - Integrate Prometheus/JMX exporters for Kafka & Flink, add Grafana panels.
12. **Resilience & scaling**
    - Enable Flink checkpoints, savepoints, configure HA JobManager.
    - Add schema compatibility checks, CI pipelines, container image scans.
13. **Production hardening**
    - Introduce secrets manager integration, TLS for Kafka/PostgreSQL, infra-as-code (Terraform/Kubernetes if migrating off Docker Compose).

## 10. Next Steps for Implementation
- Validate directory structure and create placeholder modules aligning with this plan.
- Draft detailed tickets per roadmap item, prioritize ingestion → metrics → visualization flow.
- Finalize initial SMA window/confirmation settings and design experiments for rolling backtest tuning.

## 11. Testing & CI Strategy
- **Goals** ensure deterministic validation of strategy logic, data transformations, and system wiring without relying on flakey external dependencies. Balance fast feedback (unit tests) with realistic end-to-end coverage (integration tests) executable inside GitHub Actions without Docker-in-Docker.

### Unit Testing
- **Scope** pure Python components: Coinbase producer utilities, PyFlink UDFs/UDAFs, metrics computations (Sharpe, Sortino, drawdown), configuration loaders, Kafka serialization helpers.
- **Frameworks**
  - `pytest` with `pytest-asyncio` for async producer code.
  - `unittest.mock` or `pytest-mock` to isolate external systems (WebSocket, Kafka producers, JDBC sinks).
  - `hypothesis` for property-based testing of metrics functions.
- **PyFlink specifics**
  - Use `StreamExecutionEnvironment.get_execution_environment().from_collection(...)` or `TableEnvironment` with `from_elements` to exercise transformations locally.
  - Mock Kafka connectors via test harnesses (`TestAppenderSink`, `TestHarness`) to avoid runtime connectors.
  - Assert strategy registry/discovery logic by loading interchangeable modules and ensuring they satisfy the `StrategyNode` contract.
  - Cover asynchronous database utilities with `pytest-asyncio`, using ephemeral Postgres instances to validate `asyncpg` interactions.
- **Fixtures**
  - Synthetic price streams covering edge cases (gaps, out-of-order events).
  - Metrics golden values computed offline to assert numerical stability.
  - Scenario datasets designed to exercise SMA crossover conditions (flat, chop, trending) ensuring correct signal transitions and confidence scoring.
- **Fast feedback**
  - Run with `pytest -m "not integration"` in CI to prioritize unit tests.
  - Enforce coverage thresholds (e.g., 85%) using `coverage.py`.

### Integration Testing
- **Objective** validate cross-service contracts: Kafka topics, Flink jobs, PostgreSQL sinks, and Grafana provisioning.
- **Environment**
  - Use `docker compose -f docker-compose.test.yml up` triggered from CI job with `services` instead of Docker-in-Docker. GitHub Actions allows service containers via `services:` block (Kafka, Postgres, JobManager, TaskManager).
  - Provide a dedicated compose override disabling Grafana UI to reduce resource footprint, or run Grafana in headless mode for datasource provisioning validation.
  - Include an ephemeral `backtest-runner` container that submits the rolling SMA backtest against `prices.replay` slices to ensure replay tooling works end-to-end.
- **Flink job testing**
  - Submit prebuilt JAR/Python job via REST API to the test JobManager.
  - Seed Kafka topics with fixture data using `kafkacat` or Python scripts in `scripts/`, including writing to `prices.replay` and validating consumer offset seek/replay behavior.
  - Assert downstream results by querying PostgreSQL (`psycopg2`/`asyncpg`) and reading Kafka topics (consumer with timeout).
- **Service instrumentation**
  - Leverage Testcontainers for Python where possible, but note GitHub Actions does not allow nested virtualization; rely on Action services instead.
  - Integrate ephemeral schema registry if schema enforcement is required using Confluent images started as actions services.
- **Assertions**
  - Validate schema compatibility (Avro/JSON decode).
  - Confirm strategy metrics align with expected fixtures (Sharpe, Sortino values within tolerance).
  - Verify asynchronous metric sinks complete successfully without blocking (e.g., `asyncpg` writes acknowledged within SLA).
  - Ensure Flink checkpoints succeed by verifying state backend directory creation (mounted volume in CI artifact).
  - Validate rolling backtest job can seek Kafka offsets and produce tuning summaries for `sma_cross`.
- **Cleanup**
  - Use `pytest` integration markers and fixtures to tear down topics/tables; guard against leftover topics by deleting them post-test.
- **Activation**
  - Export `RUN_INTEGRATION_TESTS=1` before running `pytest -m integration` to opt into service-backed scenarios; otherwise, integration tests are skipped.

### Continuous Integration Implementation
- **Workflow layout (`.github/workflows/ci.yml`)**
  - Job 1: `lint-and-unit-tests`
    - Matrix over Python versions if required.
    - Install dependencies via `pip install -r requirements.txt`.
    - Run `pytest -m "not integration" --cov`.
  - Job 2: `integration-tests`
    - Needs job 1.
    - Define services:
      ```yaml
      services:
        kafka:
          image: bitnami/kafka:3.7
          env:
            KAFKA_CFG_ZOOKEEPER_CONNECT: zookeeper:2181
            KAFKA_CFG_LISTENERS: PLAINTEXT://:9092
            KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
          ports: ["9092:9092"]
        zookeeper:
          image: bitnami/zookeeper:3.9
          ports: ["2181:2181"]
        postgres:
          image: timescale/timescaledb-ha:pg16
          env:
            POSTGRES_USER: test
            POSTGRES_PASSWORD: test
            POSTGRES_DB: signals
          ports: ["5432:5432"]
        flink-jobmanager:
          image: custom/flink:latest
          ports: ["8081:8081"]
        flink-taskmanager:
          image: custom/flink:latest
      ```
    - Preload Docker images via `actions/cache` to reduce pull times.
    - Run integration suite with `pytest -m integration`.
- *Status:* Current workflow (`.github/workflows/ci.yml`) executes unit tests and linting; integration-marked scenarios should be enabled once service containers are provisioned.
- **Artifacts & diagnostics**
  - Upload Flink logs (`/opt/flink/log`) and Kafka/Postgres logs on failure for debugging.
  - Store generated Grafana dashboards JSON or provisioning logs to validate templates.
- **Resource constraints**
  - Limit parallelism/partitions for CI (e.g., environment vars `FLINK_PARALLELISM=1`, reduced topic partitions) to stay within GitHub-hosted runner limits.
  - Timebox integration tests (<15 min) by reducing fixture sizes and leveraging deterministic datasets.

### Local Testing Parity
- Provide `make test`, `make test-unit`, and `make test-integration` targets mirroring CI commands.
- Document usage of `docker compose -f docker-compose.yml -f docker-compose.test.yml` for developers to reproduce integration environment without GitHub Actions.
- Run Python test suites from the managed virtualenv: `. .venv/bin/activate && pytest` (and append markers/flags as needed).

This plan positions us to iterate efficiently: we now have an agreed structure, architectural blueprint, sample orchestration configuration, and a staged pathway to a fully operational quant signaling platform. Subsequent work will translate each roadmap item into code, tests, and documentation updates.

## 12. Implementation Progress
- **Developer tooling** `requirements-dev.txt` and the GitHub Actions workflow (`.github/workflows/ci.yml`) now validate bootstrap/replay CLIs in dry-run mode and enforce formatting, providing an initial CI safety net.
- **Container orchestration** `docker-compose.yml` wires Bitnami Kafka/Zookeeper, TimescaleDB (hypertables + aggregates), Grafana provisioning, a custom PyFlink image (with JDBC driver), and the Coinbase producer container.
- **Config & code structure** `.env.example` mirrors runtime variables (including transaction/slippage bps); `flink_jobs/config.py` centralises Kafka/Postgres/strategy parameters used across the job stack.
- **Streaming job** `flink_jobs/__main__.py` prepares the TableEnvironment, registers Kafka sources/sinks (raw, normalized, signals, metrics, JDBC), and executes the SMA crossover pipeline via statement sets.
- **Normalization & signals** `flink_jobs/strategies/sma_cross.py` normalizes Coinbase ticks, writes `prices.normalized`, computes fast/slow SMAs, detects crossovers, and emits enriched decision messages.
- **Metric outputs** `flink_jobs/metrics/performance.py` adds rolling Sharpe/Sortino/returns/drawdown calculations, fan-out to Kafka (`metrics.performance`) and TimescaleDB (`strategy_metrics`).
- **Replay & bootstrap** Dataclass schemas, Kafka replay service/CLI, and bootstrap script support topic provisioning, dry-run checks, and deterministic backtest streaming.
- **Async ingestion** Coinbase producer (client, schema validation, aiokafka publisher) now streams sanitized tick data into Kafka with reconnect/backoff logic.
- **TimescaleDB & Grafana** Database migrations declare hypertables (including `strategy_positions_stream`), compression policies, and continuous aggregates; Grafana provisioning seeds a datasource and placeholder dashboard.
- **Strategy run control plane** Added `scripts/strategy_runs.py` for CLI-driven creation/listing of `strategy_runs`, enabling orchestration metadata to live in TimescaleDB.
- **Position snapshots & transaction costs** SMA pipeline now forward-fills positions, persists trade transitions into `strategy_positions_stream`, and subtracts configurable transaction-cost bps from realized P&L.
- **Replay tooling** `scripts/replay_prices.py` + `ReplayService` support dry runs, timestamp-bounded replays, and speed controls for targeted backtests.
- **Testing scaffolding** Introduced lightweight pytest coverage for producer payload validation, config parsing, replay timestamp parsing, and strategy-run CLI parsing under `tests/`.
- **Runbooks** Added `docs/runbooks/strategy-run-operations.md` describing launch, replay tuning, and rollback procedures.
- **Integration hooks** Added `tests/integration/` harness with `pytest.mark.integration`; run by exporting `RUN_INTEGRATION_TESTS=1` before invoking `pytest -m integration`.

- **Strategy P&L analytics** Model execution slippage/latency more realistically (current fill latency is constant) and visualize exposures & cumulative trade costs.
- **Producer hardening** Finalize Coinbase producer (auth, reconnection, batching, schema validation) and add ingest telemetry.
- **Replay automation** Build end-to-end replay tests using Kafka/Flink services, seed fixture data, and assert Timescale metrics.
- **Strategy management** Automate run parameter templating/rollouts (beyond CLI) and add health/alert hooks.
- **Observability** Integrate Prometheus exporters and finish Grafana dashboards (cluster health, ingestion latency, P&L panels).
- **Testing & CI** Expand unit coverage (metric math, async sinks) and enable integration workflows in GitHub Actions.
- **Security & operations** Introduce secrets management, credential rotation docs, and runbooks for scaling/recovery.
