"""Entrypoint for the Coinbase price producer service."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any, Dict

from .coinbase_client import CoinbaseClient
from .config import ProducerConfig
from .kafka_producer import KafkaPriceProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Kick off the producer service."""
    config = ProducerConfig.from_env()
    coinbase = CoinbaseClient(
        product_ids=config.coinbase_product_ids,
        reconnect_delay_seconds=config.reconnect_delay_seconds,
    )
    kafka_producer = KafkaPriceProducer(config.kafka_bootstrap_servers)

    stop_event = asyncio.Event()

    def _handle_shutdown(*_: Any) -> None:
        logger.info("Shutdown signal received.")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_shutdown)

    await coinbase.connect()

    try:
        async for message in coinbase.stream_prices():
            await send_to_kafka(kafka_producer, config.kafka_topic_raw, message)
            if stop_event.is_set():
                break
    finally:
        await coinbase.close()
        await kafka_producer.stop()


async def send_to_kafka(
    producer: KafkaPriceProducer, topic: str, message: Dict[str, Any]
) -> None:
    """Forward Coinbase messages into Kafka."""
    await producer.send(topic=topic, payload=message)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
