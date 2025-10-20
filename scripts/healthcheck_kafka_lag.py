"""Kafka consumer lag healthcheck."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Any, Dict, Iterable, List, Tuple

DEFAULT_MAX_LAG = int(os.getenv("KAFKA_MAX_LAG", "1000"))


def load_payload_from_command(command: List[str]) -> Any:
    """Execute a command expected to emit JSON payload describing lag metrics."""
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(result.stdout or "null")


def normalize_partitions(payload: Any) -> Iterable[Dict[str, Any]]:
    """
    Normalise the Kafka lag payload into an iterable of partition dictionaries.

    Supported shapes:
      * List of dicts [{...}]
      * {"partitions": [...]}
      * {"groups": [{"group": "...", "lag": [...]}]}
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if "partitions" in payload and isinstance(payload["partitions"], list):
            return payload["partitions"]
        if "groups" in payload and isinstance(payload["groups"], list):
            partitions: List[Dict[str, Any]] = []
            for group in payload["groups"]:
                group_id = group.get("group") or group.get("groupId")
                for item in group.get("partitions", []):
                    item_copy = dict(item)
                    item_copy.setdefault("group", group_id)
                    partitions.append(item_copy)
            return partitions
    raise ValueError("Unrecognised Kafka lag payload format.")


def evaluate_lag(
    partitions: Iterable[Dict[str, Any]],
    max_lag: int,
    group: str | None,
    topic: str | None,
) -> Tuple[bool, str]:
    """Check whether any partition exceeds max_lag for the targeted group/topic."""
    offending: List[Tuple[str, str, int]] = []
    inspected = 0

    for partition in partitions:
        group_id = str(partition.get("group") or partition.get("groupId") or "")
        topic_name = str(partition.get("topic") or "")
        if group and group_id != group:
            continue
        if topic and topic_name != topic:
            continue

        lag_value = partition.get("lag") or partition.get("currentLag")
        if lag_value is None:
            continue

        try:
            lag = int(lag_value)
        except (TypeError, ValueError):
            return False, f"Invalid lag value encountered: {lag_value}"

        inspected += 1
        if lag > max_lag:
            offending.append((group_id or "<unknown>", topic_name or "<unknown>", lag))

    if inspected == 0:
        return False, "No partitions inspected; check group/topic filters or payload source."
    if offending:
        formatted = ", ".join(
            f"group={g} topic={t} lag={lag}" for g, t, lag in offending
        )
        return False, f"Kafka lag exceeds threshold {max_lag}: {formatted}"
    return True, f"Kafka lag within threshold (max {max_lag})."


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Kafka consumer lag thresholds.")
    parser.add_argument("--max-lag", type=int, default=DEFAULT_MAX_LAG, help="Lag threshold (messages).")
    parser.add_argument("--group", help="Optional consumer group to filter by.")
    parser.add_argument("--topic", help="Optional topic to filter by.")
    parser.add_argument(
        "--command",
        nargs=argparse.REMAINDER,
        help="Command that emits Kafka lag JSON (e.g. kafka-consumer-groups ... --format json).",
    )
    parser.add_argument("--input", help="Path to a JSON file with lag metrics.")
    parser.add_argument("--dry-run", action="store_true", help="Skip execution and exit cleanly.")
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)
    if args.dry_run:
        print("Kafka lag healthcheck dry run: no commands executed.")
        return 0

    if args.command:
        payload = load_payload_from_command(args.command)
    elif args.input:
        with open(args.input, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    else:
        payload = json.load(sys.stdin)

    try:
        partitions = normalize_partitions(payload)
    except ValueError as exc:
        print(str(exc))
        return 2

    ok, message = evaluate_lag(partitions, args.max_lag, args.group, args.topic)
    print(message)
    return 0 if ok else 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
