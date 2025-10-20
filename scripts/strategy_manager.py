"""Host-level CLI to register, deploy, and retire strategies without service restarts."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import URLError
from urllib.request import urlopen

from scripts.strategy_runs import get_connection

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_DIR = REPO_ROOT / "configs" / "strategies"


@dataclass
class StrategySpec:
    """Configuration bundle used to provision a strategy."""

    name: str
    module: str
    description: str
    run_type: str
    execution_mode: str
    parameters: Dict[str, Any]
    env_overrides: Dict[str, Any]
    created_by: str

    def build_env(self, strategy_run_id: str) -> Dict[str, str]:
        """Construct environment variables for launching the strategy."""
        env = os.environ.copy()
        env["STRATEGY_MODULE"] = self.module
        env["STRATEGY_RUN_ID"] = strategy_run_id
        env["EXECUTION_MODE"] = self.execution_mode
        for key, value in self.env_overrides.items():
            env[str(key).upper()] = str(value)
        for key, value in self.parameters.items():
            env_key = key if str(key).isupper() else str(key).upper()
            env[env_key] = str(value)
        return env

    def persisted_parameters(self, config_path: str) -> Dict[str, Any]:
        """Shape the configuration payload stored in strategy_runs.parameters."""
        return {
            "config_path": config_path,
            "module": self.module,
            "execution_mode": self.execution_mode,
            "parameters": self.parameters,
            "env_overrides": self.env_overrides,
        }


def load_strategy_spec(
    path: Path,
    *,
    name: Optional[str] = None,
    module: Optional[str] = None,
    description: Optional[str] = None,
    run_type: Optional[str] = None,
    execution_mode: Optional[str] = None,
    created_by: str = "strategy-manager",
) -> StrategySpec:
    with open(path, "r", encoding="utf-8") as fp:
        data = json.load(fp)

    parameters: Dict[str, Any]
    env_overrides: Dict[str, Any] = {}

    if isinstance(data, dict) and "parameters" in data:
        parameters = data["parameters"] or {}
        env_overrides = data.get("env_overrides", data.get("env", {})) or {}
    elif isinstance(data, dict):
        # Backwards-compatibility for parameter-only files.
        parameters = data
    else:
        raise ValueError(f"Unsupported config format in {path}")

    resolved_name = name or data.get("name")
    if not resolved_name:
        raise ValueError(f"Strategy name not provided in config {path} or CLI flag.")

    resolved_module = module or data.get("module") or f"flink_jobs.strategies.{resolved_name}"
    resolved_description = description or data.get("description", "")
    resolved_run_type = run_type or data.get("run_type", "PAPER")
    resolved_execution_mode = execution_mode or data.get("execution_mode", "paper")

    return StrategySpec(
        name=resolved_name,
        module=resolved_module,
        description=resolved_description,
        run_type=resolved_run_type,
        execution_mode=resolved_execution_mode,
        parameters=parameters,
        env_overrides=env_overrides,
        created_by=created_by,
    )


def ensure_strategy(spec: StrategySpec) -> str:
    """Insert or update the strategies table entry and return the strategy_id."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO strategies (name, description)
            VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE
            SET description = EXCLUDED.description
            RETURNING strategy_id
            """,
            (spec.name, spec.description),
        )
        strategy_id = cur.fetchone()[0]
        conn.commit()
        return strategy_id


def create_strategy_run(spec: StrategySpec, strategy_id: str, config_path: str) -> str:
    """Create a new strategy_runs row and return the strategy_run_id."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO strategy_runs (strategy_id, run_type, parameters, started_at, created_by)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING strategy_run_id
            """,
            (
                strategy_id,
                spec.run_type,
                json.dumps(spec.persisted_parameters(config_path)),
                datetime.utcnow(),
                spec.created_by,
            ),
        )
        strategy_run_id = cur.fetchone()[0]
        conn.commit()
        return strategy_run_id


def end_strategy_runs(strategy_id: str, run_id: Optional[str] = None) -> int:
    """Mark active runs as ended. Returns number of runs updated."""
    with get_connection() as conn, conn.cursor() as cur:
        if run_id:
            cur.execute(
                """
                UPDATE strategy_runs
                SET ended_at = %s
                WHERE strategy_run_id = %s AND ended_at IS NULL
                """,
                (datetime.utcnow(), run_id),
            )
        else:
            cur.execute(
                """
                UPDATE strategy_runs
                SET ended_at = %s
                WHERE strategy_id = %s AND ended_at IS NULL
                """,
                (datetime.utcnow(), strategy_id),
            )
        updated = cur.rowcount
        conn.commit()
        return updated


def delete_strategy(strategy_id: str) -> bool:
    """Delete strategy metadata when no runs remain."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM strategies
            WHERE strategy_id = %s
              AND NOT EXISTS (
                  SELECT 1 FROM strategy_runs WHERE strategy_id = %s
              )
            """,
            (strategy_id, strategy_id),
        )
        deleted = cur.rowcount
        conn.commit()
        return deleted > 0


def find_strategy_id(strategy_name: str) -> Optional[str]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT strategy_id FROM strategies WHERE name = %s", (strategy_name,))
        result = cur.fetchone()
        return result[0] if result else None


def find_flink_job_id(strategy_name: str, host: str, port: int) -> Optional[str]:
    """Lookup a running Flink job by its friendly pipeline name."""
    overview_url = f"http://{host}:{port}/jobs/overview"
    try:
        with urlopen(overview_url, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except URLError:
        return None

    target_name = f"{strategy_name}-pipeline"
    for job in payload.get("jobs", []):
        if job.get("name") == target_name and job.get("state") not in {"FAILED", "CANCELED", "FINISHED"}:
            return job.get("jid")
    return None


def cancel_flink_job(job_id: str, host: str, port: int, flink_bin: Optional[str]) -> None:
    """Cancel a Flink job via the CLI."""
    bin_path = flink_bin or str(Path(os.getenv("FLINK_HOME", "/opt/flink")) / "bin" / "flink")
    subprocess.run(
        [bin_path, "cancel", "-m", f"{host}:{port}", job_id],
        check=True,
    )


def command_register(args: argparse.Namespace) -> None:
    config_path = resolve_config_path(args.config)
    spec = load_strategy_spec(
        config_path,
        name=args.name,
        module=args.module,
        description=args.description,
        run_type=args.run_type,
        execution_mode=args.execution_mode,
        created_by=args.created_by,
    )
    strategy_id = ensure_strategy(spec)
    print(f"Strategy '{spec.name}' registered with strategy_id={strategy_id}")


def command_deploy(args: argparse.Namespace) -> None:
    config_path = resolve_config_path(args.config)
    spec = load_strategy_spec(
        config_path,
        name=args.name,
        module=args.module,
        description=args.description,
        run_type=args.run_type,
        execution_mode=args.execution_mode,
        created_by=args.created_by,
    )

    strategy_id = ensure_strategy(spec)
    if args.end_existing:
        ended = end_strategy_runs(strategy_id)
        if ended:
            print(f"Ended {ended} existing run(s) for '{spec.name}'.")

    strategy_run_id = create_strategy_run(spec, strategy_id, str(config_path))
    print(f"Created strategy run {strategy_run_id} for '{spec.name}'.")

    env = spec.build_env(strategy_run_id)
    if args.flink_run_flags is not None:
        env["FLINK_RUN_FLAGS"] = args.flink_run_flags

    command = [
        str(REPO_ROOT / "scripts" / "submit_flink_job.sh"),
        str(REPO_ROOT / "flink_jobs" / "__main__.py"),
    ]

    if args.execute:
        try:
            subprocess.run(
                command,
                env=env,
                cwd=str(REPO_ROOT),
                check=True,
            )
            print("Flink job submission successful (command executed).")
        except subprocess.CalledProcessError as exc:
            # Mark the run as ended to avoid dangling active runs.
            end_strategy_runs(strategy_id, run_id=strategy_run_id)
            raise SystemExit(
                f"Flink submission failed with exit code {exc.returncode}. "
                f"Strategy run {strategy_run_id} marked as ended."
            ) from exc
    else:
        exported = " ".join(f"{key}={value}" for key, value in env.items() if key in {"STRATEGY_MODULE", "STRATEGY_RUN_ID", "EXECUTION_MODE"})
        print("Dry run: not executing Flink submission.")
        print(
            f"Invoke manually with environment (partial): {exported} "
            f"FLINK_RUN_FLAGS='{env.get('FLINK_RUN_FLAGS', '')}' "
            f"{command[0]} {command[1]}"
        )


def command_retire(args: argparse.Namespace) -> None:
    if not args.strategy_name and not args.run_id:
        raise SystemExit("Either --strategy-name or --run-id must be provided for retire.")

    strategy_id: Optional[str] = None

    if args.run_id:
        # Resolve strategy_id from run identifier.
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT strategy_id FROM strategy_runs WHERE strategy_run_id = %s
                """,
                (args.run_id,),
            )
            row = cur.fetchone()
            if row:
                strategy_id = row[0]
    elif args.strategy_name:
        strategy_id = find_strategy_id(args.strategy_name)

    if not strategy_id:
        raise SystemExit("Strategy could not be resolved from provided arguments.")

    updated = end_strategy_runs(strategy_id, run_id=args.run_id)
    print(f"Closed {updated} active run(s).")

    if args.cancel_job and args.strategy_name:
        job_id = find_flink_job_id(args.strategy_name, args.job_manager_host, args.job_manager_port)
        if job_id:
            cancel_flink_job(job_id, args.job_manager_host, args.job_manager_port, args.flink_bin)
            print(f"Cancelled Flink job {job_id} for strategy '{args.strategy_name}'.")
        else:
            print("No running Flink job found to cancel.")

    if args.delete and strategy_id:
        if delete_strategy(strategy_id):
            print("Strategy metadata removed (no remaining runs).")
        else:
            print("Strategy not removed: existing run history still present.")


def command_list(args: argparse.Namespace) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                s.name,
                s.description,
                COUNT(*) FILTER (WHERE sr.ended_at IS NULL) AS active_runs,
                COUNT(sr.*) AS total_runs,
                MIN(sr.started_at) FILTER (WHERE sr.ended_at IS NULL) AS oldest_active
            FROM strategies s
            LEFT JOIN strategy_runs sr ON sr.strategy_id = s.strategy_id
            GROUP BY s.strategy_id
            ORDER BY s.created_at DESC
            """
        )
        rows = cur.fetchall()
    if not rows:
        print("No strategies registered.")
        return
    for name, description, active_runs, total_runs, oldest_active in rows:
        active_info = f"{active_runs} active" if active_runs else "inactive"
        tail = ""
        if oldest_active:
            tail = f", oldest active run started {oldest_active:%Y-%m-%d %H:%M:%S} UTC"
        print(f"{name}: {description} ({active_info}, {total_runs} total runs{tail})")


def command_configs(_: argparse.Namespace) -> None:
    if not DEFAULT_CONFIG_DIR.exists():
        print("No strategy configs directory present.")
        return
    for candidate in sorted(DEFAULT_CONFIG_DIR.glob("*.json")):
        print(candidate.relative_to(REPO_ROOT))


def resolve_config_path(value: str) -> Path:
    candidate = Path(value)
    if candidate.is_file():
        return candidate
    default_path = DEFAULT_CONFIG_DIR / value
    if default_path.is_file():
        return default_path
    if candidate.suffix != ".json":
        candidate_with_ext = candidate.with_suffix(".json")
        if candidate_with_ext.is_file():
            return candidate_with_ext
        default_with_ext = DEFAULT_CONFIG_DIR / candidate_with_ext
        if default_with_ext.is_file():
            return default_with_ext
    raise SystemExit(f"Config file '{value}' not found (searched absolute and {DEFAULT_CONFIG_DIR}).")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage strategy lifecycle without restarting Flink services.")
    parser.add_argument("--created-by", default="strategy-manager", help="Metadata tag stored alongside strategy runs.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    register_cmd = subparsers.add_parser("register", help="Register a strategy definition in TimescaleDB.")
    register_cmd.add_argument("--config", required=True)
    register_cmd.add_argument("--name")
    register_cmd.add_argument("--module")
    register_cmd.add_argument("--description")
    register_cmd.add_argument("--run-type")
    register_cmd.add_argument("--execution-mode")
    register_cmd.set_defaults(func=command_register)

    deploy_cmd = subparsers.add_parser("deploy", help="Register and launch a strategy from config.")
    deploy_cmd.add_argument("--config", required=True)
    deploy_cmd.add_argument("--name")
    deploy_cmd.add_argument("--module")
    deploy_cmd.add_argument("--description")
    deploy_cmd.add_argument("--run-type")
    deploy_cmd.add_argument("--execution-mode")
    deploy_cmd.add_argument("--execute", action="store_true", help="Submit the Flink job immediately.")
    deploy_cmd.add_argument(
        "--flink-run-flags",
        default="-d",
        help="Flags forwarded to the underlying `flink run` command. Use '' to attach.",
    )
    deploy_cmd.add_argument(
        "--end-existing",
        action="store_true",
        help="End any active runs for the same strategy before launching a new one.",
    )
    deploy_cmd.set_defaults(func=command_deploy)

    retire_cmd = subparsers.add_parser("retire", help="Stop active runs and optionally cancel the Flink job.")
    retire_cmd.add_argument("--strategy-name", help="Name of the strategy to retire.")
    retire_cmd.add_argument("--run-id", help="Specific strategy_run_id to end.")
    retire_cmd.add_argument("--cancel-job", action="store_true", help="Cancel the running Flink job as part of retirement.")
    retire_cmd.add_argument("--job-manager-host", default=os.getenv("FLINK_JOBMANAGER_HOST", "flink-jobmanager"))
    retire_cmd.add_argument("--job-manager-port", type=int, default=8081)
    retire_cmd.add_argument("--flink-bin", help="Path to the Flink CLI binary (defaults to $FLINK_HOME/bin/flink).")
    retire_cmd.add_argument("--delete", action="store_true", help="Remove strategy metadata when no runs remain.")
    retire_cmd.set_defaults(func=command_retire)

    list_cmd = subparsers.add_parser("list", help="List registered strategies and run status.")
    list_cmd.set_defaults(func=command_list)

    configs_cmd = subparsers.add_parser("configs", help="List available strategy config files.")
    configs_cmd.set_defaults(func=command_configs)

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main(sys.argv[1:])
