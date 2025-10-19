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

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY VIEW performance_windows AS
        SELECT
            window_start,
            window_end,
            AVG(returns) AS avg_return,
            STDDEV_POP(returns) AS volatility,
            SUM(returns) AS cumulative_return,
            SUM(CASE WHEN returns < 0 THEN returns * returns ELSE 0 END) AS downside_sum,
            COUNT(returns) AS sample_size,
            COUNT(CASE WHEN returns < 0 THEN 1 END) AS negative_samples,
            MIN(returns) AS min_return
        FROM TABLE(
            TUMBLE(
                TABLE (SELECT * FROM normalized_prices WHERE returns IS NOT NULL),
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
                KEY 'negative_samples' VALUE CAST(pw.negative_samples AS STRING)
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
