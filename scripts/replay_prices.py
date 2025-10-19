"""Utilities to replay historical Kafka price data for backtesting."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

from flink_jobs.replay.service import ReplayConfig, ReplayService

logging.basicConfig(level=logging.INFO)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay historical Kafka prices.")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BROKER", "kafka:9092"))
    parser.add_argument("--source-topic", default=os.getenv("KAFKA_TOPIC_PRICES_REPLAY", "prices.replay"))
    parser.add_argument("--target-topic", default=os.getenv("KAFKA_TOPIC_PRICES_RAW", "prices.raw"))
    parser.add_argument("--speedup", type=float, default=float(os.getenv("REPLAY_SPEEDUP", "1.0")))
    parser.add_argument("--start-offset", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without connecting to Kafka.")
    return parser.parse_args()


async def run_async(args: argparse.Namespace) -> None:
    config = ReplayConfig(
        bootstrap_servers=args.bootstrap,
        source_topic=args.source_topic,
        target_topic=args.target_topic,
        speedup_factor=args.speedup,
        start_offset=args.start_offset,
    )
    service = ReplayService(config)
    await service.run()


def main() -> None:
    """Entry point for replay CLI."""
    args = parse_args()
    if args.dry_run:
        config = ReplayConfig(
            bootstrap_servers=args.bootstrap,
            source_topic=args.source_topic,
            target_topic=args.target_topic,
            speedup_factor=args.speedup,
            start_offset=args.start_offset,
        )
        logging.info("Replay dry run successful with config: %s", config)
        return
    asyncio.run(run_async(args))


if __name__ == "__main__":  # pragma: no cover
    main()
