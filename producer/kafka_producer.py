"""Kafka producer utilities for streaming price data."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)


class KafkaPriceProducer:
    """Thin wrapper around ``AIOKafkaProducer`` with JSON serialization."""

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self._producer: Optional[AIOKafkaProducer] = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if self._producer is not None:
                return
            logger.info("Starting Kafka producer targeting %s", self.bootstrap_servers)
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                compression_type="snappy",
            )
            await producer.start()
            self._producer = producer

    async def stop(self) -> None:
        async with self._lock:
            if self._producer is None:
                return
            logger.info("Stopping Kafka producer.")
            await self._producer.stop()
            self._producer = None

    async def send(self, topic: str, payload: Dict[str, Any]) -> None:
        """Send a single message to Kafka."""
        if self._producer is None:
            await self.start()
        assert self._producer is not None  # mypy
        try:
            await self._producer.send_and_wait(topic, payload)
        except Exception as exc:  # pragma: no cover - network error
            logger.exception("Failed to publish message to Kafka: %s", exc)
            raise
