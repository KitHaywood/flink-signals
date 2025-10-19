"""Bootstrap script to initialise Kafka topics and database schema."""

from __future__ import annotations

import argparse
import logging
import os
from typing import Iterable, Mapping

from kafka.admin import KafkaAdminClient, NewTopic
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_TOPICS: Mapping[str, int] = {
    "prices.raw": 3,
    "prices.normalized": 3,
    "signals.features": 3,
    "signals.decisions": 3,
    "metrics.performance": 3,
    "prices.replay": 6,
}


def create_topics(bootstrap_servers: str, topics: Mapping[str, int]) -> None:
    """Create Kafka topics with the desired partition count."""
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="bootstrap")
    new_topics = [
        NewTopic(name=name, num_partitions=partitions, replication_factor=1)
        for name, partitions in topics.items()
    ]
    try:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        logger.info("Created topics: %s", ", ".join(topics.keys()))
    except Exception as exc:
        logger.warning("Topic creation returned: %s (safe to ignore if already exists)", exc)
    finally:
        admin.close()


def verify_database(dsn: str) -> None:
    """Verify the TimescaleDB schema exists by running a simple query."""
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM strategies;")
            count = cur.fetchone()[0]
            logger.info("Database connectivity verified. Registered strategies: %s", count)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap Kafka topics and database state.")
    parser.add_argument("--kafka", default=os.getenv("KAFKA_BROKER", "kafka:9092"))
    parser.add_argument(
        "--topics",
        nargs="+",
        help="Optional list of topic=partitions overrides (e.g. prices.raw=6).",
    )
    parser.add_argument("--db-host", default=os.getenv("POSTGRES_HOST", "postgres"))
    parser.add_argument("--db-port", default=os.getenv("POSTGRES_PORT", "5432"))
    parser.add_argument("--db-name", default=os.getenv("POSTGRES_DB", "signals"))
    parser.add_argument("--db-user", default=os.getenv("POSTGRES_USER", "flink"))
    parser.add_argument("--db-password", default=os.getenv("POSTGRES_PASSWORD", "flink_password"))
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without performing actions.")
    return parser.parse_args()


def build_topic_map(overrides: Iterable[str] | None) -> Mapping[str, int]:
    topics = dict(DEFAULT_TOPICS)
    if not overrides:
        return topics
    for item in overrides:
        name, _, partitions = item.partition("=")
        if not name or not partitions:
            raise ValueError(f"Invalid topic override '{item}'. Expected format name=partitions.")
        topics[name] = int(partitions)
    return topics


def main() -> None:
    """Bootstrap Kafka topics and verify database schema."""
    args = parse_args()
    topics = build_topic_map(args.topics)

    if args.dry_run:
        logger.info("Dry run enabled; skipping Kafka topic creation and DB verification.")
        logger.info("Kafka bootstrap servers: %s", args.kafka)
        logger.info("Topics to ensure: %s", topics)
        logger.info("Database host: %s", args.db_host)
        return

    create_topics(args.kafka, topics)

    dsn = (
        f"host={args.db_host} port={args.db_port} dbname={args.db_name} "
        f"user={args.db_user} password={args.db_password}"
    )
    verify_database(dsn)


if __name__ == "__main__":  # pragma: no cover
    main()
