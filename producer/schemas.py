"""Producer-specific schema helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class RawPricePayload:
    """Validated payload emitted by the Coinbase producer."""

    product_id: str
    price: float
    best_bid: Optional[float]
    best_ask: Optional[float]
    volume_24h: Optional[float]
    sequence: Optional[int]
    side: Optional[str]
    event_time: datetime
    source: str = "coinbase"
