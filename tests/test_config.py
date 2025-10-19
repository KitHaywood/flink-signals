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
    assert cfg.total_trade_cost_rate == pytest.approx(
        cfg.transaction_cost_rate + cfg.slippage_rate
    )
