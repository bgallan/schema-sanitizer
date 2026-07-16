"""Reusable partition-pipeline data types."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from ..core_impl.schema_registry import _normalize_registry_json


@dataclass(frozen=True, init=False)
class SchemaRegistryState:
    """Durable registry JSON plus optional native compiled state."""

    schema_registry_json: str
    native_registry_state: Any | None = field(default=None, compare=False, repr=False)
    _schema_registry_cache: dict[str, Any] | None = field(default=None, compare=False, repr=False)

    def __init__(
        self,
        schema_registry: dict[str, Any] | str | None = None,
        *,
        schema_registry_json: str | None = None,
        native_registry_state: Any | None = None,
    ) -> None:
        """Create a pipeline registry state from mapping or JSON input."""
        registry_json = (
            _normalize_registry_json(schema_registry)
            if schema_registry_json is None
            else _normalize_registry_json(schema_registry_json)
        )
        object.__setattr__(self, "schema_registry_json", registry_json)
        object.__setattr__(self, "native_registry_state", native_registry_state)
        object.__setattr__(
            self,
            "_schema_registry_cache",
            dict(schema_registry) if isinstance(schema_registry, dict) else None,
        )

    @property
    def schema_registry(self) -> dict[str, Any]:
        """Return the parsed registry, parsing JSON only when requested."""
        cached = self._schema_registry_cache
        if cached is None:
            cached = json.loads(self.schema_registry_json or "{}")
            object.__setattr__(self, "_schema_registry_cache", cached)
        return cached


@dataclass(frozen=True, init=False)
class PartitionRunPlan:
    """Source and output URI for one logical partition run."""

    logical_date: date | None
    source_uri: str
    output_uri: str
    logical_hour: int | None = None
    discovered_input: Any | None = field(default=None, compare=False, repr=False)

    def __init__(
        self,
        logical_date: date | None,
        source_uri: str | None = None,
        output_uri: str | None = None,
        logical_hour: int | None = None,
        *,
        discovered_input: Any | None = None,
    ):
        """Initialize a partition run plan."""
        if source_uri is None:
            raise TypeError("source_uri is required")
        if output_uri is None:
            raise TypeError("output_uri is required")
        object.__setattr__(self, "logical_date", logical_date)
        object.__setattr__(self, "source_uri", source_uri)
        object.__setattr__(self, "output_uri", output_uri)
        object.__setattr__(self, "logical_hour", logical_hour)
        object.__setattr__(self, "discovered_input", discovered_input)

    def with_discovered_input(self, discovered_input: Any | None) -> PartitionRunPlan:
        """Return the same partition plan with internal discovered source metadata."""
        return PartitionRunPlan(
            self.logical_date,
            self.source_uri,
            self.output_uri,
            self.logical_hour,
            discovered_input=discovered_input,
        )

    @property
    def label(self) -> str:
        """Return a compact display label for the logical partition."""
        if self.logical_date is None:
            return "single-file"
        if self.logical_hour is not None:
            return f"{self.logical_date.isoformat()}/hour={self.logical_hour:02d}"
        return self.logical_date.isoformat()


@dataclass(frozen=True)
class PartitionRunResult:
    """Result metadata for one completed logical partition."""

    plan: PartitionRunPlan
    output_schema: Any | None
    stats: Any
    schema_registry: dict[str, Any] | None = None
    schema_drifts: list[dict[str, Any]] | None = None
    schema_registry_json: str | None = None
    schema_drifts_json: str | None = None
    wall_seconds: float | None = None
    cpu_seconds: float | None = None
    io_wait_seconds: float | None = None
    native_registry_state: Any | None = field(default=None, compare=False, repr=False)


@dataclass(frozen=True)
class SourcePlanDiscovery:
    """Existing and skipped source plans after source discovery."""

    existing_plans: list[PartitionRunPlan]
    skipped_plans: list[PartitionRunPlan]
