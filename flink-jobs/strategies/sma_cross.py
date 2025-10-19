"""
Simple Moving Average Crossover strategy implementation.

The pipeline reads raw Coinbase ticker events, computes fast/slow moving
averages, detects crossover events, and emits trading signals to a Kafka sink.
"""

from __future__ import annotations

from typing import Optional

from pyflink.table import StatementSet, StreamTableEnvironment

from ..config import JobConfig


def build_pipeline(
    *,
    table_env: StreamTableEnvironment,
    config: JobConfig,
    statement_set: StatementSet,
    historical_source: Optional[str] = None,
) -> None:
    """
    Build the SMA crossover PyFlink pipeline.

    Parameters
    ----------
    table_env:
        Flink TableEnvironment configured by the job entrypoint.
    config:
        Runtime configuration (topics, parameters, strategy run id).
    historical_source:
        Optional name of a table representing replayed historical data. When
        provided, the pipeline unions the live and replayed streams for
        backtesting scenarios.
    """
    if config.sma_fast_window >= config.sma_slow_window:
        raise ValueError("SMA_FAST_WINDOW must be smaller than SMA_SLOW_WINDOW.")

    source_table = "prices_raw"
    if historical_source:
        table_env.execute_sql(
            f"""
            CREATE TEMPORARY VIEW combined_prices AS
            SELECT * FROM {source_table}
            UNION ALL
            SELECT * FROM {historical_source}
            """
        )
        source_table = "combined_prices"

    confirmation = max(1, config.sma_confirmation_window)

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW sma_enriched AS
        SELECT
            product_id,
            event_time,
            price,
            AVG(price) OVER (
                PARTITION BY product_id
                ORDER BY event_time
                ROWS BETWEEN {config.sma_fast_window - 1} PRECEDING AND CURRENT ROW
            ) AS fast_sma,
            AVG(price) OVER (
                PARTITION BY product_id
                ORDER BY event_time
                ROWS BETWEEN {config.sma_slow_window - 1} PRECEDING AND CURRENT ROW
            ) AS slow_sma
        FROM {source_table}
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW crossover_signals AS
        SELECT
            '{config.strategy_run_id}' AS strategy_run_id,
            product_id AS instrument_id,
            event_time AS signal_time,
            CASE
                WHEN spread > 0 AND prev_spread <= 0 THEN 'LONG'
                WHEN spread < 0 AND prev_spread >= 0 THEN 'SHORT'
                ELSE 'HOLD'
            END AS signal_type,
            CASE
                WHEN spread > 0 AND prev_spread <= 0 THEN 1.0
                WHEN spread < 0 AND prev_spread >= 0 THEN -1.0
                ELSE 0.0
            END AS position,
            ABS(spread) AS confidence,
            JSON_OBJECT(
                KEY 'fast_sma' VALUE CAST(fast_sma AS STRING),
                KEY 'slow_sma' VALUE CAST(slow_sma AS STRING),
                KEY 'spread' VALUE CAST(spread AS STRING),
                KEY 'confirmation_window' VALUE CAST({confirmation} AS STRING)
            ) AS metadata
        FROM (
            SELECT
                *,
                fast_sma - slow_sma AS spread,
                LAG(fast_sma - slow_sma, {confirmation}) OVER (
                    PARTITION BY product_id
                    ORDER BY event_time
                ) AS prev_spread
            FROM sma_enriched
        )
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO signals_decisions
        SELECT
            strategy_run_id,
            instrument_id,
            signal_time,
            signal_type,
            position,
            confidence,
            metadata
        FROM crossover_signals
        WHERE signal_type <> 'HOLD'
        """
    )
