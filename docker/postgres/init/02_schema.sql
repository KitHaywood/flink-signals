-- Core schema for quant signal platform leveraging TimescaleDB hypertables.

SET timescaledb.restoring = 'off';

CREATE TABLE IF NOT EXISTS instruments (
    instrument_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol            TEXT NOT NULL UNIQUE,
    base_asset        TEXT NOT NULL,
    quote_asset       TEXT NOT NULL,
    tick_size         NUMERIC(18, 8) DEFAULT 0.01,
    lot_size          NUMERIC(18, 8) DEFAULT 0.0001,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata          JSONB DEFAULT '{}'::JSONB
);

CREATE TABLE IF NOT EXISTS strategies (
    strategy_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name              TEXT NOT NULL UNIQUE,
    description       TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_runs (
    strategy_run_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id       UUID NOT NULL REFERENCES strategies(strategy_id) ON DELETE CASCADE,
    run_type          TEXT NOT NULL CHECK (run_type IN ('LIVE', 'REPLAY', 'BACKTEST')),
    parameters        JSONB NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at          TIMESTAMPTZ,
    created_by        TEXT DEFAULT 'system'
);

CREATE TABLE IF NOT EXISTS market_prices (
    instrument_id     UUID NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    event_time        TIMESTAMPTZ NOT NULL,
    sequence_id       BIGINT NOT NULL,
    bid_price         NUMERIC(18, 8),
    ask_price         NUMERIC(18, 8),
    mid_price         NUMERIC(18, 8),
    last_price        NUMERIC(18, 8),
    last_quantity     NUMERIC(18, 8),
    source            TEXT DEFAULT 'coinbase',
    ingestion_time    TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata          JSONB DEFAULT '{}'::JSONB,
    PRIMARY KEY (instrument_id, event_time, sequence_id)
);

SELECT create_hypertable(
    'market_prices',
    'event_time',
    partitioning_column => 'instrument_id',
    number_partitions => 4,
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

ALTER TABLE market_prices
    SET (timescaledb.compress = true,
         timescaledb.compress_segmentby = 'instrument_id',
         timescaledb.compress_orderby = 'event_time');

SELECT add_compression_policy('market_prices', INTERVAL '12 hours');
SELECT add_retention_policy('market_prices', INTERVAL '90 days');

CREATE INDEX IF NOT EXISTS market_prices_event_time_idx
    ON market_prices (event_time DESC);

CREATE TABLE IF NOT EXISTS strategy_signals (
    strategy_run_id   UUID NOT NULL REFERENCES strategy_runs(strategy_run_id) ON DELETE CASCADE,
    instrument_id     UUID NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    signal_time       TIMESTAMPTZ NOT NULL,
    signal_type       TEXT NOT NULL,
    position          NUMERIC(18, 8) NOT NULL,
    confidence        NUMERIC(6, 3),
    execution_price   NUMERIC(18, 8),
    execution_latency INTERVAL,
    metadata          JSONB DEFAULT '{}'::JSONB,
    PRIMARY KEY (strategy_run_id, instrument_id, signal_time)
);

SELECT create_hypertable(
    'strategy_signals',
    'signal_time',
    partitioning_column => 'strategy_run_id',
    number_partitions => 4,
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS strategy_signals_by_instrument
    ON strategy_signals (instrument_id, signal_time DESC);

CREATE TABLE IF NOT EXISTS strategy_metrics (
    strategy_run_id   UUID NOT NULL REFERENCES strategy_runs(strategy_run_id) ON DELETE CASCADE,
    metric_time       TIMESTAMPTZ NOT NULL,
    window_label      TEXT NOT NULL,
    sharpe_ratio      NUMERIC(12, 6),
    sortino_ratio     NUMERIC(12, 6),
    cumulative_return NUMERIC(18, 8),
    drawdown          NUMERIC(18, 8),
    volatility        NUMERIC(18, 8),
    trades_executed   BIGINT DEFAULT 0,
    metadata          JSONB DEFAULT '{}'::JSONB,
    PRIMARY KEY (strategy_run_id, window_label, metric_time)
);

SELECT create_hypertable(
    'strategy_metrics',
    'metric_time',
    partitioning_column => 'strategy_run_id',
    number_partitions => 4,
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS strategy_metrics_window_idx
    ON strategy_metrics (window_label, metric_time DESC);

CREATE TABLE IF NOT EXISTS latency_metrics (
    latency_time      TIMESTAMPTZ NOT NULL,
    component         TEXT NOT NULL,
    value_ms          NUMERIC(18, 6) NOT NULL,
    strategy_run_id   UUID REFERENCES strategy_runs(strategy_run_id) ON DELETE CASCADE,
    metadata          JSONB DEFAULT '{}'::JSONB,
    PRIMARY KEY (component, latency_time)
);

SELECT create_hypertable(
    'latency_metrics',
    'latency_time',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS latency_metrics_component_idx
    ON latency_metrics (component, latency_time DESC);

CREATE TABLE IF NOT EXISTS strategy_positions_stream (
    strategy_run_id   UUID NOT NULL REFERENCES strategy_runs(strategy_run_id) ON DELETE CASCADE,
    product_id        TEXT NOT NULL,
    event_time        TIMESTAMPTZ NOT NULL,
    position          NUMERIC(18, 8) NOT NULL,
    position_change   NUMERIC(18, 8),
    transaction_cost  NUMERIC(18, 8),
    slippage_cost     NUMERIC(18, 8),
    trade_cost        NUMERIC(18, 8),
    mid_price         NUMERIC(18, 8),
    metadata          JSONB DEFAULT '{}'::JSONB,
    PRIMARY KEY (strategy_run_id, product_id, event_time)
);

SELECT create_hypertable(
    'strategy_positions_stream',
    'event_time',
    partitioning_column => 'strategy_run_id',
    number_partitions => 4,
    chunk_time_interval => INTERVAL '12 hours',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS strategy_positions_stream_product_idx
    ON strategy_positions_stream (product_id, event_time DESC);
