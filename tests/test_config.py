import pytest

from flink_jobs.config import JobConfig


def test_job_config_from_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BROKER", "localhost:9093")
    monkeypatch.setenv("KAFKA_TOPIC_PRICES_RAW", "raw")
    monkeypatch.setenv("KAFKA_TOPIC_PRICES_NORMALIZED", "normalized")
    monkeypatch.setenv("SMA_FAST_WINDOW", "10")
    monkeypatch.setenv("SMA_SLOW_WINDOW", "30")
    monkeypatch.setenv("STRATEGY_RUN_ID", "test-run")
    monkeypatch.setenv("TRANSACTION_COST_BPS", "12")
    monkeypatch.setenv("SLIPPAGE_BPS", "3")
    monkeypatch.setenv("SLIPPAGE_MAX_BPS", "25")
    monkeypatch.setenv("SLIPPAGE_VOLATILITY_MULTIPLIER", "0.4")
    monkeypatch.setenv("SLIPPAGE_SPREAD_MULTIPLIER", "0.8")
    monkeypatch.setenv("FILL_LATENCY_MS", "350")
    monkeypatch.setenv("FILL_LATENCY_JITTER_MS", "650")
    monkeypatch.setenv("FILL_LATENCY_VOLATILITY_MS", "900")

    cfg = JobConfig.from_env()

    assert cfg.kafka_broker == "localhost:9093"
    assert cfg.topic_prices_raw == "raw"
    assert cfg.topic_prices_normalized == "normalized"
    assert cfg.sma_fast_window == 10
    assert cfg.sma_slow_window == 30
    assert cfg.strategy_run_id == "test-run"
    assert cfg.transaction_cost_bps == 12
    assert cfg.transaction_cost_rate == 0.0012
    assert cfg.slippage_bps == 3
    assert cfg.slippage_rate == 0.0003
    assert cfg.slippage_max_bps == 25
    assert cfg.slippage_max_rate == 0.0025
    assert cfg.slippage_volatility_multiplier == pytest.approx(0.4)
    assert cfg.slippage_spread_multiplier == pytest.approx(0.8)
    assert cfg.total_trade_cost_rate == pytest.approx(
        cfg.transaction_cost_rate + cfg.slippage_rate
    )
    assert cfg.fill_latency_ms == 350
    assert cfg.fill_latency_jitter_ms == 650
    assert cfg.fill_latency_volatility_ms == 900
