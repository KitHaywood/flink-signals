# Strategy Run Operations Runbook

This runbook outlines the standard operating procedures for launching, tuning, and retiring strategy runs in the quant signals platform.

## Prerequisites
- Docker Compose stack running (`docker-compose up -d`).
- TimescaleDB migrations applied via container init scripts.
- Access to the `strategy_runs` CLI (`python scripts/strategy_runs.py`).

## Launching a New Strategy Run
1. Create a JSON parameter template (e.g. `configs/sma_cross_live.json`) containing:
   ```json
   {
     "sma_fast_window": 20,
     "sma_slow_window": 60,
     "sma_confirmation_window": 3,
     "transaction_cost_bps": 5,
     "slippage_bps": 12
   }
   ```
2. Register the run:
   ```bash
   python scripts/strategy_runs.py create sma_cross --param-file configs/sma_cross_live.json --run-type LIVE --created-by "alice"
   ```
   The command prints the generated `strategy_run_id`. Export it before submitting the PyFlink job:
   ```bash
   export STRATEGY_RUN_ID=<printed-id>
   ```
3. Update `.env` or deployment manifests with `STRATEGY_RUN_ID`, `TRANSACTION_COST_BPS`, and `SLIPPAGE_BPS`.
4. Submit the Flink job (within the `flink_jobs` container or host):
   ```bash
   ./scripts/submit_flink_job.sh flink_jobs/__main__.py
   ```

## Monitoring
- Grafana dashboard **Quant Signals Overview** (http://localhost:3000) displays cumulative returns, recent position transitions, and trade cost time series filtered by `strategy_run` variable.
- Validate raw metrics via SQL:
  ```sql
  SELECT * FROM strategy_metrics ORDER BY metric_time DESC LIMIT 20;
  SELECT * FROM strategy_positions_stream ORDER BY event_time DESC LIMIT 20;
  ```

## Updating Parameters Mid-Run
1. Edit the JSON parameter file with new values.
2. Push parameters to TimescaleDB:
   ```bash
   python scripts/strategy_runs.py update <run-id> --param-file configs/sma_cross_live.json
   ```
3. Restart or rescale Flink jobs as necessary to pick up configuration changes.

## Rolling Back / Ending a Run
1. Mark the run ended:
   ```bash
   python scripts/strategy_runs.py update <run-id> --end
   ```
2. Stop the Flink job or redeploy with a new `STRATEGY_RUN_ID`.
3. Archive dashboards and snapshots if required for compliance.

## Replay-Based Tuning Workflow
1. Capture a time window to replay (timestamps in UTC):
   ```bash
   python scripts/replay_prices.py --start-ts 2024-06-01T12:00:00Z --end-ts 2024-06-01T14:00:00Z --speedup 10
   ```
2. Inspect outputs in TimescaleDB or Grafana to evaluate SMA parameters.
3. Iterate on parameter templates and re-run the replay until satisfied, then promote to LIVE.

## Incident Response Checklist
- Validate Kafka ingestion via `kafka-console-consumer` and ensure topic lag is stable.
- Check Flink UI (http://localhost:8081) for failing tasks or checkpoint errors.
- Review TimescaleDB logs and verify hypertable compression policies are active.
- Confirm Grafana dashboards are updating; if not, inspect datasource credentials.
- Use `strategy_runs.py list --active-only` to ensure only intended runs remain active.
