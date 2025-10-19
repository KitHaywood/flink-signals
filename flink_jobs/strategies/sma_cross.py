"""
Simple Moving Average Crossover strategy implementation.

The pipeline reads raw Coinbase ticker events, computes fast/slow moving
averages, detects crossover events, and emits trading signals to a Kafka sink.
"""

from __future__ import annotations

from typing import Optional

from pyflink.table import StatementSet, StreamTableEnvironment

from ..config import JobConfig
from ..metrics import register_performance_metrics


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
        CREATE TEMPORARY VIEW normalized_base AS
        SELECT
            product_id,
            event_time,
            CAST(sequence AS BIGINT) AS sequence,
            CAST(price AS DOUBLE) AS price,
            CAST(best_bid AS DOUBLE) AS best_bid,
            CAST(best_ask AS DOUBLE) AS best_ask,
            CASE
                WHEN best_bid IS NOT NULL AND best_ask IS NOT NULL THEN (CAST(best_bid AS DOUBLE) + CAST(best_ask AS DOUBLE)) / 2
                ELSE CAST(price AS DOUBLE)
            END AS mid_price
        FROM {source_table}
        """
    )

    table_env.execute_sql(
        """
        CREATE TEMPORARY VIEW normalized_prices AS
        SELECT
            enriched.product_id,
            enriched.event_time,
            enriched.sequence,
            enriched.mid_price,
            enriched.best_bid,
            enriched.best_ask,
            CASE
                WHEN enriched.prev_mid_price IS NULL OR enriched.prev_mid_price = 0 THEN NULL
                ELSE (enriched.mid_price - enriched.prev_mid_price) / enriched.prev_mid_price
            END AS returns,
            STDDEV_POP(enriched.mid_price) OVER (
                PARTITION BY enriched.product_id
                ORDER BY enriched.event_time
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS volatility
        FROM (
            SELECT
                nb.product_id,
                nb.event_time,
                nb.sequence,
                nb.mid_price,
                nb.best_bid,
                nb.best_ask,
                LAG(nb.mid_price) OVER (
                    PARTITION BY nb.product_id
                    ORDER BY nb.event_time
                ) AS prev_mid_price
            FROM normalized_base nb
        ) AS enriched
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO prices_normalized
        SELECT
            product_id,
            event_time,
            sequence,
            mid_price,
            best_bid,
            best_ask,
            returns,
            volatility
        FROM normalized_prices
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW sma_enriched AS
        SELECT
            product_id,
            event_time,
            mid_price AS price,
            AVG(mid_price) OVER (
                PARTITION BY product_id
                ORDER BY event_time
                ROWS BETWEEN {config.sma_fast_window - 1} PRECEDING AND CURRENT ROW
            ) AS fast_sma,
            AVG(mid_price) OVER (
                PARTITION BY product_id
                ORDER BY event_time
                ROWS BETWEEN {config.sma_slow_window - 1} PRECEDING AND CURRENT ROW
            ) AS slow_sma
        FROM normalized_prices
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

    table_env.execute_sql(
        """
        CREATE TEMPORARY VIEW positions_base AS
        SELECT
            np.product_id,
            np.event_time,
            np.sequence,
            np.mid_price,
            np.returns,
            np.volatility,
            cs.position AS signal_position
        FROM normalized_prices np
        LEFT JOIN crossover_signals cs
            ON np.product_id = cs.instrument_id
           AND np.event_time = cs.signal_time
        """
    )

    table_env.execute_sql(
        """
        CREATE TEMPORARY VIEW positions_stream AS
        SELECT
            product_id,
            event_time,
            sequence,
            mid_price,
            returns,
            volatility,
            COALESCE(
                LAST_VALUE(signal_position, TRUE) OVER (
                    PARTITION BY product_id
                    ORDER BY event_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
                0.0
            ) AS position
        FROM positions_base
        """
    )

    table_env.execute_sql(
        """
        CREATE TEMPORARY VIEW positions_enriched AS
        SELECT
            product_id,
            event_time,
            sequence,
            mid_price,
            returns,
            volatility,
            position,
            LAG(position) OVER (
                PARTITION BY product_id
                ORDER BY event_time
            ) AS prev_position
        FROM positions_stream
        """
    )

    register_performance_metrics(table_env, config, statement_set)

    statement_set.add_insert_sql(
        f"""
        INSERT INTO strategy_positions_pg
        SELECT
            '{config.strategy_run_id}' AS strategy_run_id,
            product_id,
            event_time,
            position,
            (position - COALESCE(prev_position, 0.0)) AS position_change,
            ABS(position - COALESCE(prev_position, 0.0)) * mid_price * {config.transaction_cost_rate} AS transaction_cost,
            ABS(position - COALESCE(prev_position, 0.0)) * mid_price * {config.slippage_rate} AS slippage_cost,
            ABS(position - COALESCE(prev_position, 0.0)) * mid_price * {config.total_trade_cost_rate} AS trade_cost,
            mid_price,
            JSON_OBJECT(
                KEY 'prev_position' VALUE CAST(COALESCE(prev_position, 0.0) AS STRING),
                KEY 'transaction_cost_bps' VALUE CAST({config.transaction_cost_bps} AS STRING),
                KEY 'slippage_bps' VALUE CAST({config.slippage_bps} AS STRING)
            ) AS metadata
        FROM positions_enriched
        WHERE prev_position IS NULL OR position <> prev_position
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
