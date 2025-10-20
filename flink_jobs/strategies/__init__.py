"""Strategy registry and helper utilities."""

from importlib import import_module
from typing import Dict

STRATEGY_REGISTRY: Dict[str, str] = {
    "sma_cross": "flink_jobs.strategies.sma_cross",
}


def get_strategy_module(name: str):
    """Return the Python module object associated with the given strategy name."""
    path = STRATEGY_REGISTRY.get(name, name)
    if "." not in path:
        path = f"flink_jobs.strategies.{path}"
    return import_module(path)


def register_strategy(name: str, module_path: str) -> None:
    """
    Register or update a strategy alias at runtime.

    This enables dynamic strategy binding without restarting services.
    """
    STRATEGY_REGISTRY[name] = module_path
