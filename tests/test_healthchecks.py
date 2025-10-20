import json
from urllib.error import URLError

import pytest

from scripts.healthcheck_flink import evaluate_jobs, main as flink_main
from scripts.healthcheck_kafka_lag import evaluate_lag, main as kafka_main, normalize_partitions
from scripts.healthcheck_producer import check_producer_health, evaluate_payload, main as producer_main
from scripts.replay_prices import ReplayConfig, perform_healthcheck


def test_producer_healthcheck_succeeds(monkeypatch):
    payload = {"status": "ok", "lastHeartbeatMs": 1_000, "queueDepth": 5}
    monkeypatch.setattr("scripts.healthcheck_producer.fetch_status", lambda url, timeout: payload)
    monkeypatch.setattr("scripts.healthcheck_producer.time.time", lambda: 2.0)

    ok, message = check_producer_health("http://test", 5.0, 10, 2.0)
    assert ok
    assert "passed" in message.lower()


def test_producer_healthcheck_detects_stale_heartbeat(monkeypatch):
    payload = {"status": "ok", "lastHeartbeatMs": 0, "queueDepth": 1}
    monkeypatch.setattr("scripts.healthcheck_producer.fetch_status", lambda url, timeout: payload)
    monkeypatch.setattr("scripts.healthcheck_producer.time.time", lambda: 100.0)

    ok, message = check_producer_health("http://test", 5.0, 10, 2.0)
    assert not ok
    assert "heartbeat" in message.lower()


def test_producer_evaluate_payload_validates_queue_depth():
    now = 10.0
    ok, message = evaluate_payload({"status": "ok", "queueDepth": 99}, 5.0, 100, now)
    assert ok

    ok, message = evaluate_payload({"status": "ok", "queueDepth": 250}, 5.0, 100, now)
    assert not ok
    assert "queue depth" in message.lower()


def test_normalize_partitions_accepts_group_payload():
    payload = {
        "groups": [
            {
                "group": "signals",
                "partitions": [
                    {"topic": "metrics.performance", "lag": 10},
                    {"topic": "signals.decisions", "lag": 0},
                ],
            }
        ]
    }
    partitions = list(normalize_partitions(payload))
    assert len(partitions) == 2
    assert partitions[0]["group"] == "signals"


def test_evaluate_lag_flags_excessive_lag():
    partitions = [
        {"group": "signals", "topic": "metrics.performance", "lag": 5},
        {"group": "signals", "topic": "metrics.performance", "lag": 50},
    ]
    ok, _ = evaluate_lag(partitions, max_lag=100, group="signals", topic="metrics.performance")
    assert ok

    ok, message = evaluate_lag(partitions, max_lag=10, group="signals", topic="metrics.performance")
    assert not ok
    assert "exceeds" in message.lower()


def test_evaluate_jobs_checks_state_and_checkpoint():
    now_ms = 1_000_000.0
    payload = {
        "jobs": [
            {"name": "strategy-pipeline", "state": "RUNNING", "lastCheckpointCompleted": now_ms - 30_000},
        ]
    }
    ok, message = evaluate_jobs(payload, max_checkpoint_age=60.0, now_ms=now_ms)
    assert ok

    ok, message = evaluate_jobs(payload, max_checkpoint_age=1.0, now_ms=now_ms)
    assert not ok
    assert "checkpoint" in message.lower()


def test_perform_replay_healthcheck_config_only(monkeypatch):
    config = ReplayConfig(
        bootstrap_servers="kafka:9092",
        source_topic="prices.replay",
        target_topic="prices.raw",
    )
    perform_healthcheck(config, connect=False)


def test_perform_replay_healthcheck_connect(monkeypatch):
    config = ReplayConfig(
        bootstrap_servers="kafka:9092",
        source_topic="prices.replay",
        target_topic="prices.raw",
    )

    events = {"started": False, "stopped": False}

    class DummyService:
        def __init__(self, cfg):
            assert cfg == config

        async def start(self):
            events["started"] = True

        async def stop(self):
            events["stopped"] = True

    monkeypatch.setattr("scripts.replay_prices.ReplayService", DummyService)
    perform_healthcheck(config, connect=True)
    assert events["started"] and events["stopped"]


def test_perform_replay_healthcheck_requires_distinct_topics():
    config = ReplayConfig(
        bootstrap_servers="kafka:9092",
        source_topic="prices.raw",
        target_topic="prices.raw",
    )
    with pytest.raises(RuntimeError):
        perform_healthcheck(config, connect=False)


def test_producer_healthcheck_main_dry_run(capsys):
    assert producer_main(["--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "dry run" in out.lower()


def test_producer_healthcheck_main_failure(monkeypatch):
    def _raise(*args, **kwargs):
        raise URLError("offline")

    monkeypatch.setattr("scripts.healthcheck_producer.fetch_status", _raise)
    code = producer_main(["--url", "http://localhost"])
    assert code == 2


def test_kafka_healthcheck_main(tmp_path):
    payload_path = tmp_path / "lag.json"
    payload_path.write_text(
        json.dumps([
            {"group": "signals", "topic": "metrics.performance", "lag": 0},
            {"group": "signals", "topic": "metrics.performance", "lag": 1},
        ]),
        encoding="utf-8",
    )
    assert kafka_main(["--input", str(payload_path), "--group", "signals", "--topic", "metrics.performance", "--max-lag", "10"]) == 0
    assert kafka_main(["--dry-run"]) == 0
    assert kafka_main(["--input", str(payload_path), "--group", "signals", "--topic", "metrics.performance", "--max-lag", "0"]) == 2


def test_flink_healthcheck_main(monkeypatch):
    payload = {"jobs": [{"name": "job", "state": "RUNNING", "lastCheckpointCompleted": 0}]}
    monkeypatch.setattr("scripts.healthcheck_flink.fetch_job_overview", lambda host, port, timeout: payload)
    monkeypatch.setattr("scripts.healthcheck_flink.time.time", lambda: 1000.0)
    assert flink_main(["--allow-empty", "--dry-run"]) == 0
    assert flink_main(["--host", "localhost", "--max-checkpoint-age", "10"]) == 2
