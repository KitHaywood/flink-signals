import json
from argparse import Namespace
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

import pytest

from scripts.strategy_manager import (
    StrategySpec,
    command_retire,
    command_register,
    command_deploy,
    command_list,
    command_configs,
    create_strategy_run,
    ensure_strategy,
    end_strategy_runs,
    find_flink_job_id,
    load_strategy_spec,
    resolve_config_path,
)


class DummyCursor:
    def __init__(
        self,
        *,
        fetchone_results: Optional[List[Any]] = None,
        fetchall_results: Optional[List[Any]] = None,
        rowcount_sequence: Optional[List[int]] = None,
    ) -> None:
        self.fetchone_results = fetchone_results or []
        self.fetchall_results = fetchall_results or []
        self.fetchone_index = 0
        self.rowcount_sequence = rowcount_sequence or []
        self.rowcount_index = -1
        self.rowcount = 0
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query: str, params: Any = None):
        self.queries.append((query, params))
        if self.rowcount_index + 1 < len(self.rowcount_sequence):
            self.rowcount_index += 1
            self.rowcount = self.rowcount_sequence[self.rowcount_index]

    def fetchone(self):
        result = self.fetchone_results[self.fetchone_index]
        self.fetchone_index += 1
        return result

    def fetchall(self):
        return self.fetchall_results


class DummyConnection:
    def __init__(self, cursor: DummyCursor):
        self.cursor_obj = cursor
        self.commit_called = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commit_called = True


def test_load_strategy_spec_with_defaults(tmp_path: Path):
    config_path = tmp_path / "strategy.json"
    payload = {
        "name": "sma_cross",
        "module": "flink_jobs.strategies.sma_cross",
        "parameters": {"SMA_FAST_WINDOW": 10},
        "env_overrides": {"kafka_topic_prices_raw": "prices.test"},
    }
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    spec = load_strategy_spec(config_path)
    assert spec.name == "sma_cross"
    assert spec.module == "flink_jobs.strategies.sma_cross"
    assert spec.parameters["SMA_FAST_WINDOW"] == 10
    assert spec.env_overrides["kafka_topic_prices_raw"] == "prices.test"


def test_load_strategy_spec_requires_name(tmp_path: Path):
    config_path = tmp_path / "invalid.json"
    config_path.write_text(json.dumps({"parameters": {}}), encoding="utf-8")

    with pytest.raises(ValueError):
        load_strategy_spec(config_path)


def test_strategy_spec_build_env_uppercases_keys():
    spec = StrategySpec(
        name="mock",
        module="pkg.mod",
        description="",
        run_type="PAPER",
        execution_mode="paper",
        parameters={"SMA_FAST_WINDOW": 5, "custom_key": 42},
        env_overrides={"kafka_topic": "prices.mock"},
        created_by="tester",
    )
    env = spec.build_env("run-123")
    assert env["STRATEGY_MODULE"] == "pkg.mod"
    assert env["STRATEGY_RUN_ID"] == "run-123"
    assert env["KAFKA_TOPIC"] == "prices.mock"
    assert env["SMA_FAST_WINDOW"] == "5"
    assert env["CUSTOM_KEY"] == "42"


def test_ensure_strategy_inserts_and_commits(monkeypatch):
    cursor = DummyCursor(fetchone_results=[("strategy-id",)])
    connection = DummyConnection(cursor)
    monkeypatch.setattr("scripts.strategy_manager.get_connection", lambda: connection)

    spec = StrategySpec(
        name="mock",
        module="pkg.mod",
        description="desc",
        run_type="PAPER",
        execution_mode="paper",
        parameters={},
        env_overrides={},
        created_by="tester",
    )

    strategy_id = ensure_strategy(spec)
    assert strategy_id == "strategy-id"
    assert connection.commit_called
    executed_query, params = cursor.queries[0]
    assert "INSERT INTO strategies" in executed_query
    assert params[0] == "mock"


def test_create_strategy_run_inserts_and_commits(monkeypatch):
    cursor = DummyCursor(fetchone_results=[("run-id",)])
    connection = DummyConnection(cursor)
    monkeypatch.setattr("scripts.strategy_manager.get_connection", lambda: connection)

    spec = StrategySpec(
        name="mock",
        module="pkg.mod",
        description="desc",
        run_type="PAPER",
        execution_mode="paper",
        parameters={"foo": "bar"},
        env_overrides={},
        created_by="tester",
    )

    run_id = create_strategy_run(spec, "strategy-id", "config.json")
    assert run_id == "run-id"
    assert connection.commit_called
    query, params = cursor.queries[0]
    assert "INSERT INTO strategy_runs" in query
    assert params[0] == "strategy-id"


def test_end_strategy_runs_marks_rows(monkeypatch):
    cursor = DummyCursor(rowcount_sequence=[2])
    connection = DummyConnection(cursor)
    monkeypatch.setattr("scripts.strategy_manager.get_connection", lambda: connection)

    updated = end_strategy_runs("strategy-id")
    assert updated == 2


def test_find_flink_job_id_returns_running(monkeypatch):
    payload = {
        "jobs": [
            {"jid": "abc", "name": "test-strategy-pipeline", "state": "RUNNING"},
            {"jid": "def", "name": "other", "state": "FAILED"},
        ]
    }

    class DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(payload).encode("utf-8")

    monkeypatch.setattr("scripts.strategy_manager.urlopen", lambda req, timeout=5: DummyResponse())

    job_id = find_flink_job_id("test-strategy", "localhost", 8081)
    assert job_id == "abc"


def test_command_retire_cancels_job(monkeypatch, capsys):
    monkeypatch.setattr("scripts.strategy_manager.find_strategy_id", lambda name: "strategy-id")
    monkeypatch.setattr("scripts.strategy_manager.end_strategy_runs", lambda strategy_id, run_id=None: 1)
    monkeypatch.setattr(
        "scripts.strategy_manager.find_flink_job_id",
        lambda strategy_name, host, port: "flink-job-id",
    )

    cancelled = {}

    def _cancel(job_id, host, port, flink_bin):
        cancelled["job_id"] = job_id
        cancelled["host"] = host
        cancelled["port"] = port

    monkeypatch.setattr("scripts.strategy_manager.cancel_flink_job", _cancel)
    monkeypatch.setattr("scripts.strategy_manager.delete_strategy", lambda strategy_id: True)

    args = Namespace(
        strategy_name="test-strategy",
        run_id=None,
        cancel_job=True,
        job_manager_host="localhost",
        job_manager_port=8081,
        flink_bin=None,
        delete=True,
    )

    command_retire(args)
    assert cancelled["job_id"] == "flink-job-id"
    captured = capsys.readouterr().out
    assert "Closed 1 active run" in captured
    assert "Strategy metadata removed" in captured


def test_command_retire_requires_identifier():
    with pytest.raises(SystemExit):
        command_retire(
            Namespace(
                strategy_name=None,
                run_id=None,
                cancel_job=False,
                job_manager_host="localhost",
                job_manager_port=8081,
                flink_bin=None,
                delete=False,
            )
        )


def test_resolve_config_path_prefers_default(tmp_path, monkeypatch):
    configs_dir = tmp_path / "configs" / "strategies"
    configs_dir.mkdir(parents=True)
    config_path = configs_dir / "example.json"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr("scripts.strategy_manager.DEFAULT_CONFIG_DIR", configs_dir)

    resolved = resolve_config_path("example")
    assert resolved == config_path


def test_resolve_config_path_missing_raises(monkeypatch):
    monkeypatch.setattr("scripts.strategy_manager.DEFAULT_CONFIG_DIR", Path("/non-existent"))
    with pytest.raises(SystemExit):
        resolve_config_path("missing")


def test_strategy_spec_persisted_parameters():
    spec = StrategySpec(
        name="mock",
        module="pkg.mod",
        description="desc",
        run_type="PAPER",
        execution_mode="paper",
        parameters={"alpha": 1},
        env_overrides={"beta": 2},
        created_by="tester",
    )
    persisted = spec.persisted_parameters("config.json")
    assert persisted["config_path"] == "config.json"
    assert persisted["parameters"]["alpha"] == 1
    assert persisted["env_overrides"]["beta"] == 2


def test_command_register_invokes_ensure(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "strategy.json"
    config_path.write_text(
        json.dumps(
            {
                "name": "mock",
                "module": "flink_jobs.strategies.sma_cross",
                "parameters": {},
            }
        ),
        encoding="utf-8",
    )

    recorded = {}

    def _ensure(spec):
        recorded["name"] = spec.name
        return "strategy-id"

    monkeypatch.setattr("scripts.strategy_manager.ensure_strategy", _ensure)

    command_register(
        Namespace(
            config=str(config_path),
            name=None,
            module=None,
            description=None,
            run_type=None,
            execution_mode=None,
            created_by="unit-test",
        )
    )
    assert recorded["name"] == "mock"
    output = capsys.readouterr().out
    assert "registered" in output.lower()


def test_command_deploy_dry_run(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "strategy.json"
    config_path.write_text(
        json.dumps(
            {
                "name": "mock",
                "module": "flink_jobs.strategies.sma_cross",
                "parameters": {"SMA_FAST_WINDOW": 5},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("scripts.strategy_manager.ensure_strategy", lambda spec: "strategy-id")
    monkeypatch.setattr("scripts.strategy_manager.create_strategy_run", lambda spec, strategy_id, config_path: "run-id")
    monkeypatch.setattr("scripts.strategy_manager.end_strategy_runs", lambda strategy_id, run_id=None: 0)

    def _forbid(*args, **kwargs):
        raise RuntimeError("Should not execute")

    monkeypatch.setattr("scripts.strategy_manager.subprocess.run", _forbid)

    command_deploy(
        Namespace(
            config=str(config_path),
            name=None,
            module=None,
            description=None,
            run_type=None,
            execution_mode=None,
            execute=False,
            flink_run_flags="-d",
            end_existing=False,
            created_by="unit-test",
        )
    )

    output = capsys.readouterr().out
    assert "dry run" in output.lower()
    assert "STRATEGY_RUN_ID" in output


def test_command_list_outputs_summary(monkeypatch, capsys):
    cursor = DummyCursor(
        fetchall_results=[
            ("mock", "desc", 1, 2, datetime(2024, 1, 1, 0, 0)),
        ]
    )
    connection = DummyConnection(cursor)
    monkeypatch.setattr("scripts.strategy_manager.get_connection", lambda: connection)

    command_list(Namespace())
    output = capsys.readouterr().out
    assert "mock" in output
    assert "active" in output


def test_command_configs_lists_files(monkeypatch, tmp_path, capsys):
    configs_dir = tmp_path / "configs" / "strategies"
    configs_dir.mkdir(parents=True)
    config_path = configs_dir / "example.json"
    config_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr("scripts.strategy_manager.DEFAULT_CONFIG_DIR", configs_dir)
    monkeypatch.setattr("scripts.strategy_manager.REPO_ROOT", tmp_path)
    command_configs(Namespace())
    output = capsys.readouterr().out
    assert "example.json" in output
