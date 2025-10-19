"""Configuration helpers for the Coinbase price producer."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List


@dataclass
class ProducerConfig:
    """Runtime configuration for the Coinbase producer."""

    kafka_bootstrap_servers: str
    kafka_topic_raw: str
    coinbase_product_ids: List[str]
    coinbase_ws_url: str
    coinbase_api_key: str | None = None
    coinbase_api_secret: str | None = None
    reconnect_delay_seconds: int = 5

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        product_ids = os.getenv("COINBASE_PRODUCT_IDS", "BTC-USD")
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            kafka_topic_raw=os.getenv("KAFKA_TOPIC_PRICES_RAW", "prices.raw"),
            coinbase_product_ids=[p.strip() for p in product_ids.split(",") if p.strip()],
            coinbase_ws_url=os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com"),
            coinbase_api_key=os.getenv("COINBASE_API_KEY"),
            coinbase_api_secret=os.getenv("COINBASE_API_SECRET"),
            reconnect_delay_seconds=int(os.getenv("COINBASE_RECONNECT_DELAY", "5")),
        )
