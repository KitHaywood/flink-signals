"""Healthcheck for the Coinbase producer service."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Dict, Tuple
from urllib.error import URLError
from urllib.request import Request, urlopen

DEFAULT_URL = os.getenv("PRODUCER_HEALTH_URL", "http://producer:8080/health")
DEFAULT_MAX_HEARTBEAT = float(os.getenv("PRODUCER_MAX_HEARTBEAT_AGE_SEC", "30"))
DEFAULT_MAX_QUEUE_DEPTH = int(os.getenv("PRODUCER_MAX_QUEUE_DEPTH", "250"))


def fetch_status(url: str, timeout: float) -> Dict[str, object]:
    """Retrieve the producer health payload as JSON."""
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
    return json.loads(body)


def evaluate_payload(
    payload: Dict[str, object],
    max_heartbeat_age: float,
    max_queue_depth: int,
    now: float,
) -> Tuple[bool, str]:
    """Determine whether the payload signals a healthy state."""
    status = str(payload.get("status", "")).lower()
    if status not in {"ok", "healthy", "pass"}:
        return False, f"Producer status not healthy: {payload!r}"

    if "lastHeartbeatMs" in payload:
        last_heartbeat_ms = payload.get("lastHeartbeatMs")
    else:
        last_heartbeat_ms = payload.get("last_heartbeat_ms")
    if last_heartbeat_ms is not None:
        try:
            last_heartbeat_sec = float(last_heartbeat_ms) / 1000.0
        except (TypeError, ValueError):
            return False, f"Invalid heartbeat value: {last_heartbeat_ms}"
        age = now - last_heartbeat_sec
        if age > max_heartbeat_age:
            return False, f"Producer heartbeat stale by {age:.1f}s (threshold {max_heartbeat_age}s)"

    if "queueDepth" in payload:
        queue_depth = payload.get("queueDepth")
    else:
        queue_depth = payload.get("queue_depth")
    if queue_depth is not None:
        try:
            depth = int(queue_depth)
        except (TypeError, ValueError):
            return False, f"Invalid queue depth: {queue_depth}"
        if depth > max_queue_depth:
            return False, f"Producer queue depth {depth} exceeds threshold {max_queue_depth}"

    return True, "Producer healthcheck passed."


def check_producer_health(
    url: str,
    max_heartbeat_age: float,
    max_queue_depth: int,
    timeout: float,
) -> Tuple[bool, str]:
    """Fetch and evaluate the producer health payload."""
    try:
        payload = fetch_status(url, timeout)
    except URLError as exc:
        return False, f"Producer health endpoint unreachable: {exc}"

    now = time.time()
    return evaluate_payload(payload, max_heartbeat_age, max_queue_depth, now)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Coinbase producer health endpoint.")
    parser.add_argument("--url", default=DEFAULT_URL, help="Producer health endpoint URL.")
    parser.add_argument("--timeout", type=float, default=2.0, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--max-heartbeat-age",
        type=float,
        default=DEFAULT_MAX_HEARTBEAT,
        help="Maximum allowed heartbeat staleness in seconds.",
    )
    parser.add_argument(
        "--max-queue-depth",
        type=int,
        default=DEFAULT_MAX_QUEUE_DEPTH,
        help="Maximum allowed in-flight queue depth.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Skip network call and exit cleanly.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.dry_run:
        print("Producer healthcheck dry run: no network calls executed.")
        return 0

    ok, message = check_producer_health(
        url=args.url,
        max_heartbeat_age=args.max_heartbeat_age,
        max_queue_depth=args.max_queue_depth,
        timeout=args.timeout,
    )
    print(message)
    return 0 if ok else 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
