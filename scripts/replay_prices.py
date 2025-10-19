"""Utilities to replay historical Kafka price data for backtesting."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime

from flink_jobs.replay.service import ReplayConfig, ReplayService

logging.basicConfig(level=logging.INFO)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay historical Kafka prices.")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BROKER", "kafka:9092"))
    parser.add_argument("--source-topic", default=os.getenv("KAFKA_TOPIC_PRICES_REPLAY", "prices.replay"))
    parser.add_argument("--target-topic", default=os.getenv("KAFKA_TOPIC_PRICES_RAW", "prices.raw"))
    parser.add_argument("--speedup", type=float, default=float(os.getenv("REPLAY_SPEEDUP", "1.0")))
    parser.add_argument("--start-offset", type=int, default=None)
    parser.add_argument("--start-ts", help="ISO8601 timestamp for replay start (overrides offset)")
    parser.add_argument("--end-ts", help="ISO8601 timestamp to stop replay")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without connecting to Kafka.")
    return parser.parse_args()


async def run_async(config: ReplayConfig) -> None:
    service = ReplayService(config)
    await service.run()


def main() -> None:
    """Entry point for replay CLI."""
    args = parse_args()
    start_ts_ms = parse_timestamp_ms(args.start_ts) if args.start_ts else None
    end_ts_ms = parse_timestamp_ms(args.end_ts) if args.end_ts else None
    config = ReplayConfig(
        bootstrap_servers=args.bootstrap,
        source_topic=args.source_topic,
        target_topic=args.target_topic,
        speedup_factor=args.speedup,
        start_offset=args.start_offset,
        start_timestamp_ms=start_ts_ms,
        end_timestamp_ms=end_ts_ms,
    )
    if args.dry_run:
        logging.info("Replay dry run successful with config: %s", config)
        return
    asyncio.run(run_async(config))


def parse_timestamp_ms(value: str) -> int:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


if __name__ == "__main__":  # pragma: no cover
    main()
