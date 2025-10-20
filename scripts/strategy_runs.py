"""CLI utilities to manage strategy runs in TimescaleDB."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Any, Dict

import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5434"),
        dbname=os.getenv("POSTGRES_DB", "signals"),
        user=os.getenv("POSTGRES_USER", "flink"),
        password=os.getenv("POSTGRES_PASSWORD", "flink_password"),
    )


def list_runs(args: argparse.Namespace) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        query = (
            """
            SELECT sr.strategy_run_id, s.name, sr.run_type, sr.started_at, sr.ended_at, sr.parameters
            FROM strategy_runs sr
            JOIN strategies s ON sr.strategy_id = s.strategy_id
            {where_clause}
            ORDER BY sr.started_at DESC
            LIMIT %s
            """
        )
        where_clause = "WHERE sr.ended_at IS NULL" if args.active_only else ""
        cur.execute(query.format(where_clause=where_clause), (args.limit,))
        rows = cur.fetchall()
        for row in rows:
            run_id, strategy_name, run_type, started_at, ended_at, params = row
            ended = ended_at.strftime("%Y-%m-%d %H:%M:%S") if ended_at else "ACTIVE"
            print(
                f"{run_id} | {strategy_name} | {run_type} | "
                f"{started_at:%Y-%m-%d %H:%M:%S} -> {ended} | {params}"
            )


def create_run(args: argparse.Namespace) -> None:
    parameters: Dict[str, Any]
    if args.param_file:
        with open(args.param_file, "r", encoding="utf-8") as fp:
            parameters = json.load(fp)
    else:
        parameters = json.loads(args.parameters)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT strategy_id FROM strategies WHERE name = %s", (args.strategy_name,))
        result = cur.fetchone()
        if result is None:
            raise RuntimeError(f"Strategy '{args.strategy_name}' not found. Seed strategies table first.")
        strategy_id = result[0]

        cur.execute(
            """
            INSERT INTO strategy_runs (strategy_id, run_type, parameters, started_at, created_by)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING strategy_run_id
            """,
            (
                strategy_id,
                args.run_type,
                json.dumps(parameters),
                datetime.utcnow(),
                args.created_by,
            ),
        )
        run_id = cur.fetchone()[0]
        conn.commit()
        print(f"Created strategy_run_id={run_id}")


def update_run(args: argparse.Namespace) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        if args.parameters is not None:
            if args.param_file:
                with open(args.param_file, "r", encoding="utf-8") as fp:
                    parameters = json.load(fp)
            else:
                parameters = json.loads(args.parameters)
            cur.execute(
                "UPDATE strategy_runs SET parameters = %s WHERE strategy_run_id = %s",
                (json.dumps(parameters), args.run_id),
            )
        if args.end:
            cur.execute(
                "UPDATE strategy_runs SET ended_at = %s WHERE strategy_run_id = %s AND ended_at IS NULL",
                (datetime.utcnow(), args.run_id),
            )
        conn.commit()
        print(f"Updated strategy_run_id={args.run_id}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage strategy runs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List recent strategy runs")
    list_parser.add_argument("--limit", type=int, default=20)
    list_parser.add_argument("--active-only", action="store_true")
    list_parser.set_defaults(func=list_runs)

    create_parser = subparsers.add_parser("create", help="Create a new strategy run entry")
    create_parser.add_argument("strategy_name", help="Name of the strategy (e.g., sma_cross)")
    create_parser.add_argument(
        "--run-type",
        choices=["LIVE", "REPLAY", "BACKTEST", "PAPER"],
        default="LIVE",
    )
    create_parser.add_argument(
        "--parameters",
        default="{}",
        help="JSON string of strategy parameters",
    )
    create_parser.add_argument("--param-file", help="Path to JSON parameter file")
    create_parser.add_argument("--created-by", default="cli")
    create_parser.set_defaults(func=create_run)

    update_parser = subparsers.add_parser("update", help="Update parameters or mark a run ended")
    update_parser.add_argument("run_id", help="UUID of the strategy run to update")
    update_parser.add_argument(
        "--parameters",
        help="JSON string of strategy parameters to replace",
    )
    update_parser.add_argument("--param-file", help="Path to JSON parameter file")
    update_parser.add_argument(
        "--end",
        action="store_true",
        help="Mark the run as ended (sets ended_at to current UTC time)",
    )
    update_parser.set_defaults(func=update_run)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main()
