"""Async Coinbase WebSocket client."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import AsyncGenerator, Dict, Iterable, Optional

import websockets
from websockets.client import WebSocketClientProtocol

logger = logging.getLogger(__name__)

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"


@dataclass
class CoinbaseClient:
    """
    Encapsulates connection logic to Coinbase WebSocket feeds.

    The client yields normalized ticker messages which can be forwarded to Kafka.
    """

    product_ids: Iterable[str]
    reconnect_delay_seconds: int = 5
    ws_url: str = COINBASE_WS_URL

    _ws: Optional[WebSocketClientProtocol] = None
    _shutdown: bool = False

    async def connect(self) -> None:
        """Establish the WebSocket connection and subscribe to ticker updates."""
        await self._ensure_connection()

    async def stream_prices(self) -> AsyncGenerator[Dict[str, str], None]:
        """
        Yield price updates from the Coinbase feed.

        Automatically reconnects on transient failures with a backoff defined
        by ``reconnect_delay_seconds``.
        """
        while not self._shutdown:
            try:
                await self._ensure_connection()
                assert self._ws is not None  # for type checkers
                message = await asyncio.wait_for(self._ws.recv(), timeout=30)
            except (asyncio.TimeoutError, websockets.ConnectionClosedError) as exc:
                logger.warning("Coinbase stream error: %s. Reconnecting...", exc)
                await self._reconnect()
                continue
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Unexpected error from Coinbase stream: %s", exc)
                await self._reconnect()
                continue

            data = json.loads(message)
            if data.get("type") != "ticker":
                continue

            normalized = self._normalize_ticker(data)
            if normalized is not None:
                yield normalized

    async def close(self) -> None:
        """Close the WebSocket connection."""
        self._shutdown = True
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def _ensure_connection(self) -> None:
        if self._ws and not self._ws.closed:
            return
        logger.info("Connecting to Coinbase WebSocket feed.")
        self._ws = await websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10)
        await self._subscribe()

    async def _subscribe(self) -> None:
        if self._ws is None:
            return
        payload = {
            "type": "subscribe",
            "product_ids": list(self.product_ids),
            "channels": ["ticker"],
        }
        await self._ws.send(json.dumps(payload))

    async def _reconnect(self) -> None:
        if self._ws:
            await self._ws.close()
        await asyncio.sleep(self.reconnect_delay_seconds)
        await self._ensure_connection()

    @staticmethod
    def _normalize_ticker(message: Dict[str, str]) -> Optional[Dict[str, str]]:
        """Normalize Coinbase ticker payload into schema expected downstream."""
        product_id = message.get("product_id")
        price = message.get("price")
        time = message.get("time")
        if not product_id or not price or not time:
            return None

        return {
            "product_id": product_id,
            "price": CoinbaseClient._to_float(price),
            "best_bid": CoinbaseClient._to_float(message.get("best_bid")),
            "best_ask": CoinbaseClient._to_float(message.get("best_ask")),
            "volume_24h": CoinbaseClient._to_float(message.get("volume_24h")),
            "sequence": CoinbaseClient._to_int(message.get("sequence")),
            "side": message.get("side"),
            "event_time": time,
            "source": "coinbase",
        }

    @staticmethod
    def _to_float(value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):  # pragma: no cover - validation guard
            return None

    @staticmethod
    def _to_int(value: Optional[str]) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):  # pragma: no cover - validation guard
            return None
