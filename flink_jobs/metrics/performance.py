"""Performance metric utilities for PyFlink pipelines."""

from __future__ import annotations

from pyflink.table import StatementSet, StreamTableEnvironment

from ..config import JobConfig


def register_performance_metrics(
    table_env: StreamTableEnvironment,
    config: JobConfig,
    statement_set: StatementSet,
    window_interval_sql: str = "INTERVAL '5' MINUTE",
    window_label: str = "5m",
) -> None:
    """
    Register metric computation views and sinks for strategy performance.

    Parameters
    ----------
    table_env:
        Active TableEnvironment containing the pricing/signal views.
    config:
        Shared job configuration for topics and TimescaleDB connection.
    statement_set:
        StatementSet to which sink insertions will be appended.
    window_interval_sql:
        SQL literal representing the tumbling window duration (default 5 minutes).
    window_label:
        Friendly label for the metrics window (e.g., "5m").
    """

    transaction_rate = config.transaction_cost_rate
    slippage_rate = config.slippage_rate
    total_cost_rate = config.total_trade_cost_rate

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW position_returns AS
        SELECT
            product_id,
            event_time,
            sequence,
            mid_price,
            COALESCE(returns, 0.0) AS asset_return,
            position,
            prev_position,
            ABS(position - COALESCE(prev_position, 0.0)) AS position_change,
            ABS(position - COALESCE(prev_position, 0.0)) * mid_price * {transaction_rate} AS transaction_cost,
            ABS(position - COALESCE(prev_position, 0.0)) * mid_price * {slippage_rate} AS slippage_cost,
            ABS(position - COALESCE(prev_position, 0.0)) * mid_price * {total_cost_rate} AS trade_cost,
            COALESCE(prev_position, 0.0) * COALESCE(returns, 0.0)
                - ABS(position - COALESCE(prev_position, 0.0)) * mid_price * {total_cost_rate} AS realized_pnl
        FROM positions_enriched
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW performance_windows AS
        SELECT
            window_start,
            window_end,
            AVG(realized_pnl) AS avg_return,
            STDDEV_POP(realized_pnl) AS volatility,
            SUM(realized_pnl) AS cumulative_return,
            SUM(CASE WHEN realized_pnl < 0 THEN realized_pnl * realized_pnl ELSE 0 END) AS downside_sum,
            COUNT(realized_pnl) AS sample_size,
            COUNT(CASE WHEN realized_pnl < 0 THEN 1 END) AS negative_samples,
            MIN(realized_pnl) AS min_return,
            AVG(ABS(position)) AS avg_exposure,
            SUM(trade_cost) AS total_trade_cost,
            SUM(transaction_cost) AS total_transaction_cost,
            SUM(slippage_cost) AS total_slippage_cost
        FROM TABLE(
            TUMBLE(
                TABLE (
                    SELECT
                        product_id,
                        event_time,
                        sequence,
                        position,
                        realized_pnl,
                        trade_cost
                    FROM position_returns
                ),
                DESCRIPTOR(event_time),
                {window_interval_sql}
            )
        )
        GROUP BY window_start, window_end
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW signal_counts AS
        SELECT
            window_start,
            window_end,
            COUNT(*) AS trades_executed
        FROM TABLE(
            TUMBLE(
                TABLE (SELECT * FROM crossover_signals WHERE signal_type <> 'HOLD'),
                DESCRIPTOR(signal_time),
                {window_interval_sql}
            )
        )
        GROUP BY window_start, window_end
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW metrics_enriched AS
        SELECT
            '{config.strategy_run_id}' AS strategy_run_id,
            pw.window_end AS metric_time,
            '{window_label}' AS window_label,
            CASE
                WHEN pw.volatility IS NULL OR pw.volatility = 0 THEN NULL
                ELSE (pw.avg_return / pw.volatility) * SQRT(12.0)
            END AS sharpe_ratio,
            CASE
                WHEN pw.downside_sum IS NULL OR pw.negative_samples = 0 THEN NULL
                ELSE (pw.avg_return / NULLIF(SQRT(pw.downside_sum / pw.negative_samples), 0))
                     * SQRT(12.0)
            END AS sortino_ratio,
            pw.cumulative_return AS cumulative_return,
            pw.min_return AS drawdown,
            pw.volatility AS volatility,
            COALESCE(sc.trades_executed, 0) AS trades_executed,
            JSON_OBJECT(
                KEY 'sample_size' VALUE CAST(pw.sample_size AS STRING),
                KEY 'negative_samples' VALUE CAST(pw.negative_samples AS STRING),
                KEY 'average_exposure' VALUE CAST(pw.avg_exposure AS STRING),
                KEY 'total_trade_cost' VALUE CAST(pw.total_trade_cost AS STRING),
                KEY 'total_transaction_cost' VALUE CAST(pw.total_transaction_cost AS STRING),
                KEY 'total_slippage_cost' VALUE CAST(pw.total_slippage_cost AS STRING)
            ) AS metadata
        FROM performance_windows pw
        LEFT JOIN signal_counts sc
            ON pw.window_start = sc.window_start AND pw.window_end = sc.window_end
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO metrics_performance
        SELECT
            strategy_run_id,
            metric_time,
            window_label,
            sharpe_ratio,
            sortino_ratio,
            cumulative_return,
            drawdown,
            volatility,
            trades_executed,
            metadata
        FROM metrics_enriched
        """
    )

    statement_set.add_insert_sql(
        """
        INSERT INTO metrics_performance_pg
        SELECT
            strategy_run_id,
            metric_time,
            window_label,
            sharpe_ratio,
            sortino_ratio,
            cumulative_return,
            drawdown,
            volatility,
            trades_executed,
            metadata
        FROM metrics_enriched
        """
    )
