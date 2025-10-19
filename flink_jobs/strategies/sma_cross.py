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
    execution_mode_literal = config.execution_mode.replace("'", "''")

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
                KEY 'confirmation_window' VALUE CAST({confirmation} AS STRING),
                KEY 'execution_mode' VALUE '{execution_mode_literal}'
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
            np.best_bid,
            np.best_ask,
            CASE
                WHEN np.best_bid IS NOT NULL AND np.best_ask IS NOT NULL THEN np.best_ask - np.best_bid
                ELSE NULL
            END AS spread,
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
            best_bid,
            best_ask,
            spread,
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
            best_bid,
            best_ask,
            spread,
            position,
            LAG(position) OVER (
                PARTITION BY product_id
                ORDER BY event_time
            ) AS prev_position,
            CASE
                WHEN mid_price IS NULL OR mid_price = 0 THEN 0.0
                ELSE COALESCE(volatility, 0.0) / mid_price
            END AS volatility_ratio,
            CASE
                WHEN mid_price IS NULL OR mid_price = 0 OR spread IS NULL THEN 0.0
                ELSE spread / mid_price
            END AS spread_ratio
        FROM positions_stream
        """
    )

    slippage_components = (
        f"{config.slippage_rate}"
        + f" + volatility_ratio * {config.slippage_volatility_multiplier}"
        + f" + spread_ratio * {config.slippage_spread_multiplier}"
    )
    dynamic_slippage_expr = (
        f"CASE "
        f"WHEN {slippage_components} < 0 THEN 0.0 "
        f"WHEN {slippage_components} > {config.slippage_max_rate} THEN {config.slippage_max_rate} "
        f"ELSE {slippage_components} END"
    )
    latency_increment_expr = (
        f"CAST({config.fill_latency_volatility_ms} * volatility_ratio AS BIGINT)"
    )
    dynamic_latency_expr = (
        f"CASE "
        f"WHEN {config.fill_latency_ms} + {latency_increment_expr} < {config.fill_latency_ms} "
        f"THEN {config.fill_latency_ms} "
        f"WHEN {config.fill_latency_ms} + {latency_increment_expr} > "
        f"{config.fill_latency_ms + config.fill_latency_jitter_ms} "
        f"THEN {config.fill_latency_ms + config.fill_latency_jitter_ms} "
        f"ELSE {config.fill_latency_ms} + {latency_increment_expr} "
        f"END"
    )

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW positions_costs AS
        SELECT
            product_id,
            event_time,
            sequence,
            mid_price,
            returns,
            volatility,
            best_bid,
            best_ask,
            spread,
            position,
            prev_position,
            position - COALESCE(prev_position, 0.0) AS position_change,
            volatility_ratio,
            spread_ratio,
            {dynamic_slippage_expr} AS slippage_rate,
            {config.transaction_cost_rate} AS transaction_cost_rate,
            ({dynamic_slippage_expr}) + {config.transaction_cost_rate} AS trade_cost_rate,
            {dynamic_latency_expr} AS fill_latency_ms
        FROM positions_enriched
        """
    )

    register_performance_metrics(table_env, config, statement_set)

    timestamp_add_expression = "TIMESTAMPADD(MILLISECOND, CAST(fill_latency_ms AS BIGINT), event_time)"

    statement_set.add_insert_sql(
        f"""
        INSERT INTO strategy_executions_pg
        SELECT
            '{config.strategy_run_id}' AS strategy_run_id,
            product_id,
            event_time AS signal_time,
            {timestamp_add_expression} AS execution_time,
            position_change,
            CASE
                WHEN position_change > 0 THEN mid_price * (1 + slippage_rate)
                WHEN position_change < 0 THEN mid_price * (1 - slippage_rate)
                ELSE mid_price
            END AS execution_price,
            mid_price AS base_price,
            ABS(position_change) * mid_price * {config.transaction_cost_rate} AS transaction_cost,
            ABS(position_change) * mid_price * slippage_rate AS slippage_cost,
            JSON_OBJECT(
                KEY 'fill_latency_ms' VALUE CAST(fill_latency_ms AS STRING),
                KEY 'slippage_rate' VALUE CAST(slippage_rate AS STRING),
                KEY 'execution_mode' VALUE '{execution_mode_literal}'
            ) AS metadata
        FROM (
            SELECT
                product_id,
                event_time,
                position_change,
                mid_price,
                slippage_rate,
                fill_latency_ms
            FROM positions_costs
        )
        WHERE position_change <> 0
        """
    )

    statement_set.add_insert_sql(
        f"""
        INSERT INTO strategy_positions_pg
        SELECT
            '{config.strategy_run_id}' AS strategy_run_id,
            product_id,
            event_time,
            position,
            position_change,
            ABS(position_change) * mid_price * {config.transaction_cost_rate} AS transaction_cost,
            ABS(position_change) * mid_price * slippage_rate AS slippage_cost,
            ABS(position_change) * mid_price * trade_cost_rate AS trade_cost,
            mid_price,
            JSON_OBJECT(
                KEY 'prev_position' VALUE CAST(COALESCE(prev_position, 0.0) AS STRING),
                KEY 'transaction_cost_bps' VALUE CAST({config.transaction_cost_bps} AS STRING),
                KEY 'slippage_bps' VALUE CAST({config.slippage_bps} AS STRING),
                KEY 'effective_slippage_rate' VALUE CAST(slippage_rate AS STRING),
                KEY 'execution_mode' VALUE '{execution_mode_literal}'
            ) AS metadata
        FROM positions_costs
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
