"""Configuration helpers for PyFlink jobs."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class JobConfig:
    kafka_broker: str
    topic_prices_raw: str
    topic_prices_normalized: str
    topic_signals_decisions: str
    topic_metrics_performance: str
    sma_fast_window: int
    sma_slow_window: int
    sma_confirmation_window: int
    strategy_run_id: str
    postgres_host: str
    postgres_port: str
    postgres_db: str
    postgres_user: str
    postgres_password: str
    transaction_cost_bps: int
    transaction_cost_rate: float
    slippage_bps: int
    slippage_rate: float
    total_trade_cost_rate: float
    fill_latency_ms: int

    @classmethod
    def from_env(cls) -> "JobConfig":
        return cls(
            kafka_broker=os.getenv("KAFKA_BROKER", "kafka:9092"),
            topic_prices_raw=os.getenv("KAFKA_TOPIC_PRICES_RAW", "prices.raw"),
            topic_prices_normalized=os.getenv("KAFKA_TOPIC_PRICES_NORMALIZED", "prices.normalized"),
            topic_signals_decisions=os.getenv("KAFKA_TOPIC_SIGNALS_DECISIONS", "signals.decisions"),
            topic_metrics_performance=os.getenv(
                "KAFKA_TOPIC_METRICS_PERFORMANCE", "metrics.performance"
            ),
            sma_fast_window=int(os.getenv("SMA_FAST_WINDOW", "20")),
            sma_slow_window=int(os.getenv("SMA_SLOW_WINDOW", "60")),
            sma_confirmation_window=int(os.getenv("SMA_CONFIRMATION_WINDOW", "3")),
            strategy_run_id=os.getenv("STRATEGY_RUN_ID", "sma-cross-live"),
            postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
            postgres_port=os.getenv("POSTGRES_PORT", "5432"),
            postgres_db=os.getenv("POSTGRES_DB", "signals"),
            postgres_user=os.getenv("POSTGRES_USER", "flink"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "flink_password"),
            transaction_cost_bps=int(os.getenv("TRANSACTION_COST_BPS", "0")),
            transaction_cost_rate=float(os.getenv("TRANSACTION_COST_BPS", "0")) / 10_000.0,
            slippage_bps=int(os.getenv("SLIPPAGE_BPS", "0")),
            slippage_rate=float(os.getenv("SLIPPAGE_BPS", "0")) / 10_000.0,
            total_trade_cost_rate=(
                float(os.getenv("TRANSACTION_COST_BPS", "0"))
                + float(os.getenv("SLIPPAGE_BPS", "0"))
            )
            / 10_000.0,
            fill_latency_ms=int(os.getenv("FILL_LATENCY_MS", "250")),
        )
