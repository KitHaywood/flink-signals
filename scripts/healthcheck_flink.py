"""Flink JobManager REST healthcheck."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Dict, List, Tuple
from urllib.error import URLError
from urllib.request import Request, urlopen

DEFAULT_HOST = os.getenv("FLINK_JOBMANAGER_HOST", "flink-jobmanager")
DEFAULT_PORT = int(os.getenv("FLINK_JOBMANAGER_PORT", "8081"))
DEFAULT_MAX_CHECKPOINT_AGE = float(os.getenv("FLINK_MAX_CHECKPOINT_AGE_SEC", "180"))


def fetch_job_overview(host: str, port: int, timeout: float) -> Dict[str, object]:
    """Fetch the Flink jobs overview JSON from the JobManager REST API."""
    url = f"http://{host}:{port}/jobs/overview"
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
    return json.loads(body)


def evaluate_jobs(
    payload: Dict[str, object],
    max_checkpoint_age: float,
    now_ms: float,
) -> Tuple[bool, str]:
    """Assess job states and checkpoint freshness."""
    jobs = payload.get("jobs", [])
    if not jobs:
        return False, "No Flink jobs reported; ensure job is submitted."

    unhealthy: List[str] = []
    for job in jobs:
        name = job.get("name", job.get("jid", "<unknown>"))
        state = job.get("state", "")
        if state not in {"RUNNING", "CREATED"}:
            unhealthy.append(f"{name} state={state}")
            continue

        if "lastCheckpointCompleted" in job:
            checkpoint_ts = job.get("lastCheckpointCompleted")
        elif "lastCheckpointCompletionTimestamp" in job:
            checkpoint_ts = job.get("lastCheckpointCompletionTimestamp")
        elif "lastCheckpointTimestamp" in job:
            checkpoint_ts = job.get("lastCheckpointTimestamp")
        else:
            checkpoint_ts = None
        if checkpoint_ts is not None and max_checkpoint_age > 0:
            age_sec = (now_ms - float(checkpoint_ts)) / 1000.0
            if age_sec > max_checkpoint_age:
                unhealthy.append(f"{name} checkpoint stale {age_sec:.1f}s")

    if unhealthy:
        details = ", ".join(unhealthy)
        return False, f"Flink healthcheck failed: {details}"

    return True, "Flink jobs healthy."


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Flink job health over REST.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Flink JobManager host.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Flink JobManager port.")
    parser.add_argument("--timeout", type=float, default=2.0, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--max-checkpoint-age",
        type=float,
        default=DEFAULT_MAX_CHECKPOINT_AGE,
        help="Maximum age (seconds) of last completed checkpoint.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Skip network call and exit cleanly.")
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Treat empty job lists as success (useful before job submission).",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)
    if args.dry_run:
        print("Flink healthcheck dry run: no network calls executed.")
        return 0

    try:
        payload = fetch_job_overview(args.host, args.port, args.timeout)
    except URLError as exc:
        print(f"Unable to reach Flink JobManager: {exc}")
        return 2

    if args.allow_empty and not payload.get("jobs"):
        print("Flink jobs overview empty but allowed by flag.")
        return 0

    now_ms = time.time() * 1000.0
    ok, message = evaluate_jobs(payload, args.max_checkpoint_age, now_ms)
    print(message)
    return 0 if ok else 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
