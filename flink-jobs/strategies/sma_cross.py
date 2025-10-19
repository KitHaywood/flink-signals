"""
Simple Moving Average Crossover strategy placeholder.

The real implementation will construct a PyFlink Table/DataStream pipeline
that consumes normalized price data, computes fast/slow moving averages,
produces position signals, and leverages asynchronous sinks for metrics.
"""

from __future__ import annotations

from typing import Any, Optional


def build_pipeline(env: Optional[Any]) -> None:
    """
    Build the SMA crossover PyFlink pipeline.

    Parameters
    ----------
    env:
        Placeholder for the Flink execution environment. The concrete type
        (TableEnvironment or StreamExecutionEnvironment) will be supplied once
        the streaming job is implemented.
    """
    # TODO: Implement pipeline wiring once sources/sinks are defined.
    return None
