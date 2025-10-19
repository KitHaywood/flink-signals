"""Schemas for strategy signals and metrics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class StrategySignal:
    strategy_run_id: str
    instrument_id: str
    signal_time: datetime
    signal_type: str
    position: float
    confidence: Optional[float] = None
    metadata: Optional[dict] = None


@dataclass
class StrategyMetric:
    strategy_run_id: str
    metric_time: datetime
    window_label: str
    sharpe_ratio: Optional[float]
    sortino_ratio: Optional[float]
    cumulative_return: Optional[float]
    drawdown: Optional[float]
    volatility: Optional[float]
    trades_executed: int = 0
