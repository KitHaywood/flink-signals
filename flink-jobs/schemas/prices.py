"""Schemas for price-related Kafka topics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class RawPrice:
    """Represents a raw ticker update from Coinbase."""

    product_id: str
    price: float
    best_bid: Optional[float]
    best_ask: Optional[float]
    volume_24h: Optional[float]
    sequence: Optional[int]
    side: Optional[str]
    event_time: datetime
    source: str = "coinbase"


@dataclass
class NormalizedPrice:
    """Price payload after feature engineering/normalization."""

    instrument_id: str
    event_time: datetime
    sequence: int
    mid_price: float
    best_bid: Optional[float]
    best_ask: Optional[float]
    returns: Optional[float]
    volatility: Optional[float]
