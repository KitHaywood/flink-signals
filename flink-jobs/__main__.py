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
from typing import Any, Callable

DEFAULT_STRATEGY = "sma_cross"


def resolve_strategy(name: str) -> Callable[..., Any]:
    """Dynamically import the requested strategy module."""
    module_path = f"flink_jobs.strategies.{name}"
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:  # pragma: no cover - implementation detail
        raise ModuleNotFoundError(
            f"Strategy module '{module_path}' not found. "
            "Ensure the strategy exists and is listed in project configuration."
        ) from exc

    if not hasattr(module, "build_pipeline"):
        raise AttributeError(
            f"Strategy module '{module_path}' must expose a 'build_pipeline' callable."
        )
    return module.build_pipeline


def main() -> None:
    """Resolve and execute the selected strategy."""
    strategy_name = os.getenv("STRATEGY_MODULE", DEFAULT_STRATEGY)
    build_pipeline = resolve_strategy(strategy_name)

    # Placeholder execution context: the returned callable will accept the Table
    # or DataStream environment once the job scaffolding is implemented.
    build_pipeline(env=None)  # type: ignore[arg-type]


if __name__ == "__main__":  # pragma: no cover
    main()
