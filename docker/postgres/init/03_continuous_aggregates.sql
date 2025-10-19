-- Continuous aggregates to support Grafana dashboards and analytics.

CREATE MATERIALIZED VIEW IF NOT EXISTS strategy_metrics_hourly
WITH (timescaledb.continuous) AS
SELECT
    strategy_run_id,
    window_label,
    time_bucket('1 hour', metric_time) AS bucket,
    AVG(sharpe_ratio)      AS sharpe_avg,
    AVG(sortino_ratio)     AS sortino_avg,
    LAST(cumulative_return, metric_time) AS cumulative_return_last,
    MAX(drawdown)          AS max_drawdown,
    SUM(trades_executed)   AS trades_executed_sum
FROM strategy_metrics
GROUP BY strategy_run_id, window_label, bucket;

SELECT add_continuous_aggregate_policy(
    'strategy_metrics_hourly',
    start_offset => INTERVAL '7 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '15 minutes'
);
