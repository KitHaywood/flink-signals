"""Entrypoint for the Coinbase price producer service."""

from __future__ import annotations

import asyncio
import logging
import signal
from datetime import datetime
from typing import Any, Dict, Optional

from .coinbase_client import CoinbaseClient
from .config import ProducerConfig
from .kafka_producer import KafkaPriceProducer
from .schemas import RawPricePayload

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
            payload = prepare_payload(message)
            if payload is None:
                continue
            await send_to_kafka(kafka_producer, config.kafka_topic_raw, payload)
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


def prepare_payload(message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Validate and normalize raw Coinbase payload prior to publishing."""
    try:
        event_time = parse_iso8601(message["event_time"])
        raw_price = RawPricePayload(
            product_id=message["product_id"],
            price=float(message["price"]),
            best_bid=message.get("best_bid"),
            best_ask=message.get("best_ask"),
            volume_24h=message.get("volume_24h"),
            sequence=message.get("sequence"),
            side=message.get("side"),
            event_time=event_time,
            source=message.get("source", "coinbase"),
        )
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("Dropping malformed message: %s; payload=%s", exc, message)
        return None

    return {
        "product_id": raw_price.product_id,
        "price": raw_price.price,
        "best_bid": raw_price.best_bid,
        "best_ask": raw_price.best_ask,
        "volume_24h": raw_price.volume_24h,
        "sequence": raw_price.sequence,
        "side": raw_price.side,
        "event_time": raw_price.event_time.isoformat(),
        "source": raw_price.source,
    }


def parse_iso8601(value: str) -> datetime:
    """Parse ISO-8601 timestamps while supporting trailing 'Z'."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
