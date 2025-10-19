"""Strategy registry and helper utilities."""

from importlib import import_module
from typing import Dict

STRATEGY_REGISTRY: Dict[str, str] = {
    "sma_cross": "flink_jobs.strategies.sma_cross",
}


def get_strategy_module(name: str):
    """Return the Python module object associated with the given strategy name."""
    path = STRATEGY_REGISTRY.get(name, name)
    return import_module(path)
