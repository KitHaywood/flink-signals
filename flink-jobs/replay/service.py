"""Utilities for replaying Kafka price streams."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

logger = logging.getLogger(__name__)


@dataclass
class ReplayConfig:
    bootstrap_servers: str
    source_topic: str
    target_topic: str
    consumer_group: str = "replay-runner"
    speedup_factor: float = 1.0
    start_offset: Optional[int] = None


class ReplayService:
    """Replays historical Kafka messages into a target topic at controlled speed."""

    def __init__(self, config: ReplayConfig) -> None:
        if config.speedup_factor <= 0:
            raise ValueError("speedup_factor must be positive.")
        self.config = config
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self.config.source_topic,
            bootstrap_servers=self.config.bootstrap_servers,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            group_id=self.config.consumer_group,
        )
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
        )
        await self._consumer.start()
        await self._producer.start()
        if self.config.start_offset is not None:
            partitions = await self._consumer.partitions_for_topic(self.config.source_topic)
            if not partitions:
                raise RuntimeError(f"No partitions found for topic {self.config.source_topic}")
            topic_partitions = [TopicPartition(self.config.source_topic, p) for p in partitions]
            await self._consumer.assign(topic_partitions)
            for tp in topic_partitions:
                await self._consumer.seek(tp, self.config.start_offset)

    async def stop(self) -> None:
        tasks = []
        if self._consumer:
            tasks.append(self._consumer.stop())
        if self._producer:
            tasks.append(self._producer.stop())
        if tasks:
            await asyncio.gather(*tasks)

    async def run(self) -> None:
        """Begin replaying messages until the source topic is exhausted."""
        if self._consumer is None or self._producer is None:
            await self.start()

        assert self._consumer is not None
        assert self._producer is not None

        start_wall = time.monotonic()
        first_event_ts: Optional[int] = None

        try:
            async for record in self._consumer:
                if first_event_ts is None:
                    first_event_ts = record.timestamp
                    start_wall = time.monotonic()

                wait_seconds = 0.0
                if first_event_ts is not None:
                    elapsed_event = (record.timestamp - first_event_ts) / 1000.0
                    wait_seconds = max(
                        0.0, (elapsed_event / self.config.speedup_factor) - (time.monotonic() - start_wall)
                    )
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)

                await self._producer.send_and_wait(
                    self.config.target_topic,
                    value=record.value,
                    key=record.key,
                    headers=record.headers,
                )
        finally:
            await self.stop()
