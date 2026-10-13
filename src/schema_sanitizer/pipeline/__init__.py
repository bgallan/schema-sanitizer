"""Configured public API for partitioned Parquet pipelines.

It exposes the configured Hive and modified-time facades plus an advanced namespace,
without importing private orchestration details into user code.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from . import advanced
    from .high_level import HivePartitions, ModifiedTimePartitions, ParquetPipeline
    from .partition_execution import PartitionPipelineResult
    from .types import PartitionRunPlan, PartitionRunResult, SchemaRegistryState


_LAZY: dict[str, tuple[str, str]] = {
    "HivePartitions": (".high_level", "HivePartitions"),
    "ModifiedTimePartitions": (".high_level", "ModifiedTimePartitions"),
    "ParquetPipeline": (".high_level", "ParquetPipeline"),
    "PartitionPipelineResult": (".partition_execution", "PartitionPipelineResult"),
    "PartitionRunPlan": (".types", "PartitionRunPlan"),
    "PartitionRunResult": (".types", "PartitionRunResult"),
    "SchemaRegistryState": (".types", "SchemaRegistryState"),
}


def __getattr__(name: str) -> Any:
    """Load one public pipeline symbol only when callers request it."""
    if name == "advanced":
        value = import_module(f"{__name__}.advanced")
    else:
        spec = _LAZY.get(name)
        if spec is None:
            raise AttributeError(name)
        module_name, attribute = spec
        value = getattr(import_module(module_name, __name__), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Include deferred public names in interactive module discovery."""
    return sorted({*globals(), *__all__})


__all__ = [
    "HivePartitions",
    "ModifiedTimePartitions",
    "ParquetPipeline",
    "PartitionPipelineResult",
    "PartitionRunPlan",
    "PartitionRunResult",
    "SchemaRegistryState",
    "advanced",
]
