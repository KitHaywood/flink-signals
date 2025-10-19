"""
Entry point for submitting PyFlink jobs.

This module resolves the strategy module to load, prepares any shared
configuration, and invokes the strategy-specific pipeline builder. The
actual Flink execution environment wiring will be implemented in later
iterations.
"""

from __future__ import annotations

import importlib
import os
from typing import Callable

from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, StatementSet

from .config import JobConfig

DEFAULT_STRATEGY = "sma_cross"


def resolve_strategy(name: str) -> Callable[..., None]:
    """Dynamically import the requested strategy module."""
    module_path = f"flink_jobs.strategies.{name}"
    module = importlib.import_module(module_path)
    if not hasattr(module, "build_pipeline"):
        raise AttributeError(
            f"Strategy module '{module_path}' must expose a 'build_pipeline' callable."
        )
    return module.build_pipeline


def create_table_environment(parallelism: int) -> StreamTableEnvironment:
    """Create a streaming TableEnvironment with sensible defaults."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
    env.enable_checkpointing(60_000)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10_000))

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)
    table_env.get_config().set("pipeline.name", "quant-signals")
    table_env.get_config().set("table.exec.state.ttl", "PT6H")
    return table_env


def register_common_tables(table_env: StreamTableEnvironment, config: JobConfig) -> None:
    """Register shared Kafka sources and sinks used by strategies."""
    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS prices_raw (
            product_id STRING,
            price DOUBLE,
            best_bid DOUBLE,
            best_ask DOUBLE,
            volume_24h DOUBLE,
            sequence BIGINT,
            side STRING,
            event_time TIMESTAMP_LTZ(3),
            source STRING,
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.topic_prices_raw}',
            'properties.bootstrap.servers' = '{config.kafka_broker}',
            'properties.group.id' = 'flink-sma-consumer',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS prices_normalized (
            product_id STRING,
            event_time TIMESTAMP_LTZ(3),
            sequence BIGINT,
            mid_price DOUBLE,
            best_bid DOUBLE,
            best_ask DOUBLE,
            returns DOUBLE,
            volatility DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.topic_prices_normalized}',
            'properties.bootstrap.servers' = '{config.kafka_broker}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS signals_decisions (
            strategy_run_id STRING,
            instrument_id STRING,
            signal_time TIMESTAMP_LTZ(3),
            signal_type STRING,
            position DOUBLE,
            confidence DOUBLE,
            metadata STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.topic_signals_decisions}',
            'properties.bootstrap.servers' = '{config.kafka_broker}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS metrics_performance (
            strategy_run_id STRING,
            metric_time TIMESTAMP_LTZ(3),
            window_label STRING,
            sharpe_ratio DOUBLE,
            sortino_ratio DOUBLE,
            cumulative_return DOUBLE,
            drawdown DOUBLE,
            volatility DOUBLE,
            trades_executed BIGINT,
            metadata STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.topic_metrics_performance}',
            'properties.bootstrap.servers' = '{config.kafka_broker}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS metrics_performance_pg (
            strategy_run_id STRING,
            metric_time TIMESTAMP_LTZ(3),
            window_label STRING,
            sharpe_ratio DOUBLE,
            sortino_ratio DOUBLE,
            cumulative_return DOUBLE,
            drawdown DOUBLE,
            volatility DOUBLE,
            trades_executed BIGINT,
            metadata STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{config.postgres_host}:{config.postgres_port}/{config.postgres_db}',
            'table-name' = 'strategy_metrics',
            'username' = '{config.postgres_user}',
            'password' = '{config.postgres_password}',
            'driver' = 'org.postgresql.Driver',
            'sink.buffer-flush.max-rows' = '100',
            'sink.buffer-flush.interval' = '1s',
            'sink.max-retries' = '5'
        )
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS strategy_positions_pg (
            strategy_run_id STRING,
            product_id STRING,
            event_time TIMESTAMP_LTZ(3),
            position DOUBLE,
            position_change DOUBLE,
            transaction_cost DOUBLE,
            slippage_cost DOUBLE,
            trade_cost DOUBLE,
            mid_price DOUBLE,
            metadata STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{config.postgres_host}:{config.postgres_port}/{config.postgres_db}',
            'table-name' = 'strategy_positions_stream',
            'username' = '{config.postgres_user}',
            'password' = '{config.postgres_password}',
            'driver' = 'org.postgresql.Driver',
            'sink.buffer-flush.max-rows' = '200',
            'sink.buffer-flush.interval' = '1s',
            'sink.max-retries' = '5'
        )
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS strategy_executions_pg (
            strategy_run_id STRING,
            product_id STRING,
            signal_time TIMESTAMP_LTZ(3),
            execution_time TIMESTAMP_LTZ(3),
            position_change DOUBLE,
            execution_price DOUBLE,
            base_price DOUBLE,
            transaction_cost DOUBLE,
            slippage_cost DOUBLE,
            metadata STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{config.postgres_host}:{config.postgres_port}/{config.postgres_db}',
            'table-name' = 'strategy_executions_stream',
            'username' = '{config.postgres_user}',
            'password' = '{config.postgres_password}',
            'driver' = 'org.postgresql.Driver',
            'sink.buffer-flush.max-rows' = '200',
            'sink.buffer-flush.interval' = '1s',
            'sink.max-retries' = '5'
        )
        """
    )


def main() -> None:
    """Resolve and execute the selected strategy."""
    config = JobConfig.from_env()
    strategy_name = os.getenv("STRATEGY_MODULE", DEFAULT_STRATEGY)
    build_pipeline = resolve_strategy(strategy_name)

    table_env = create_table_environment(parallelism=int(os.getenv("FLINK_PARALLELISM", "2")))
    register_common_tables(table_env, config)

    statement_set = table_env.create_statement_set()
    build_pipeline(table_env=table_env, config=config, statement_set=statement_set)
    statement_set.execute(f"{strategy_name}-pipeline")


if __name__ == "__main__":  # pragma: no cover
    main()
