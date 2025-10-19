"""Configuration helpers for PyFlink jobs."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class JobConfig:
    kafka_broker: str
    topic_prices_raw: str
    topic_signals_decisions: str
    topic_metrics_performance: str
    sma_fast_window: int
    sma_slow_window: int
    sma_confirmation_window: int
    strategy_run_id: str

    @classmethod
    def from_env(cls) -> "JobConfig":
        return cls(
            kafka_broker=os.getenv("KAFKA_BROKER", "kafka:9092"),
            topic_prices_raw=os.getenv("KAFKA_TOPIC_PRICES_RAW", "prices.raw"),
            topic_signals_decisions=os.getenv("KAFKA_TOPIC_SIGNALS_DECISIONS", "signals.decisions"),
            topic_metrics_performance=os.getenv(
                "KAFKA_TOPIC_METRICS_PERFORMANCE", "metrics.performance"
            ),
            sma_fast_window=int(os.getenv("SMA_FAST_WINDOW", "20")),
            sma_slow_window=int(os.getenv("SMA_SLOW_WINDOW", "60")),
            sma_confirmation_window=int(os.getenv("SMA_CONFIRMATION_WINDOW", "3")),
            strategy_run_id=os.getenv("STRATEGY_RUN_ID", "sma-cross-live"),
        )
