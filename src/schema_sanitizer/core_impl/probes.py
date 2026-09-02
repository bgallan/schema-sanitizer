"""Expose native schema and registry probes through the execution context.

Probe-safe options are prepared before Arrow streams, path sources, and registry operations are
submitted through the owned native execution capsule.
"""

from __future__ import annotations

from typing import Any

from .native_options import _options_capsule
from .native_results import RegistryProbeResult, SchemaProbeResult
from .native_runtime import native_core as _native


def options_for_schema_probe(options: dict[str, Any]) -> dict[str, Any]:
    """Return converter options that infer the current input schema."""
    out = dict(options)
    out["schema_contract"] = None
    out["schema_mode"] = "additive"
    return out


def options_for_registry_operation(
    options: dict[str, Any],
    *,
    registry_json: str,
    schema_mode: str,
) -> dict[str, Any]:
    """Infer incoming shape before applying registry-backed evolution rules."""
    del registry_json, schema_mode
    # Registry-backed ``strict`` means that a canonical registry must already
    # exist. The registry still owns compatible promotions and version-family
    # creation, so probing the incoming source itself must remain additive.
    return options_for_schema_probe(options)


class _ExecutionCapsuleOwner:
    """Type contract shared by execution-context mixins."""

    _capsule: Any


class _ExecutionSchemaProbeMethods(_ExecutionCapsuleOwner):
    """Schema probe routes exposed by ``ExecutionContext``."""

    def schema_probe_from_source(
        self, frontend: str, source: str, payload: Any, options: Any = None
    ) -> SchemaProbeResult:
        """Infer schema from a source-selected input without materializing a sink."""
        return SchemaProbeResult.from_native(
            _native.context_schema_probe_from_source(
                self._capsule,
                frontend,
                source,
                payload,
                _options_capsule(options),
            )
        )

    def schema_probe_paths(
        self,
        frontend: str,
        paths: list[str],
        options: Any = None,
        *,
        separator: str = "\n",
    ) -> SchemaProbeResult:
        """Infer schema from multiple local files as one logical input."""
        return SchemaProbeResult.from_native(
            _native.context_schema_probe_from_paths(
                self._capsule,
                frontend,
                paths,
                _options_capsule(options),
                separator,
            )
        )


class _ExecutionRegistryInputProbeMethods(_ExecutionCapsuleOwner):
    """Registry probe routes for direct, path, and Arrow inputs."""

    def registry_probe_arrow_sources(
        self,
        sources: list[tuple[Any, str]],
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        native_registry_state: Any = None,
    ) -> RegistryProbeResult:
        """Infer and merge registry state from multiple Arrow stream sources."""
        prepared = _options_capsule(options)
        if native_registry_state is not None:
            raw = _native.context_registry_probe_from_arrow_sources_registry_state(
                self._capsule,
                sources,
                prepared,
                native_registry_state,
                field_name_policy,
                schema_mode,
            )
        else:
            raw = _native.context_registry_probe_from_arrow_sources(
                self._capsule,
                sources,
                prepared,
                registry_json,
                field_name_policy,
                schema_mode,
            )
        return RegistryProbeResult.from_native(raw)

    def registry_probe_from_source(
        self,
        frontend: str,
        source: str,
        payload: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
    ) -> RegistryProbeResult:
        """Infer and merge registry state without materializing a sink."""
        return RegistryProbeResult.from_native(
            _native.context_registry_probe_from_source(
                self._capsule,
                frontend,
                source,
                payload,
                _options_capsule(options),
                registry_json,
                field_name_policy,
                schema_mode,
            )
        )

    def registry_probe_paths(
        self,
        frontend: str,
        paths: list[str],
        options: Any = None,
        *,
        separator: str = "\n",
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
    ) -> RegistryProbeResult:
        """Infer and merge registry state from multiple local files."""
        return RegistryProbeResult.from_native(
            _native.context_registry_probe_from_paths(
                self._capsule,
                frontend,
                paths,
                _options_capsule(options),
                separator,
                registry_json,
                field_name_policy,
                schema_mode,
            )
        )


class _ExecutionRegistryPathSourceProbeMethods(_ExecutionCapsuleOwner):
    """Registry probe routes for path-source collections and providers."""

    def registry_probe_path_sources(
        self,
        sources: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        native_registry_state: Any = None,
    ) -> RegistryProbeResult:
        """Infer and merge registry state from native path-source inputs."""
        prepared = _options_capsule(options)
        if native_registry_state is not None:
            raw = _native.context_registry_probe_from_path_sources_registry_state(
                self._capsule,
                sources,
                prepared,
                native_registry_state,
                field_name_policy,
                schema_mode,
            )
        else:
            raw = _native.context_registry_probe_from_path_sources(
                self._capsule,
                sources,
                prepared,
                registry_json,
                field_name_policy,
                schema_mode,
            )
        return RegistryProbeResult.from_native(raw)

    def registry_probe_path_sources_best_effort(
        self,
        sources: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        native_registry_state: Any = None,
    ) -> RegistryProbeResult:
        """Infer registry state from path sources, skipping JSON parse failures."""
        prepared = _options_capsule(options)
        if native_registry_state is not None:
            raw = _native.context_registry_probe_from_path_sources_best_effort_registry_state(
                self._capsule,
                sources,
                prepared,
                native_registry_state,
                field_name_policy,
                schema_mode,
            )
        else:
            raw = _native.context_registry_probe_from_path_sources_best_effort(
                self._capsule,
                sources,
                prepared,
                registry_json,
                field_name_policy,
                schema_mode,
            )
        return RegistryProbeResult.from_native(raw)

    def registry_probe_path_source_chunk_provider(
        self,
        provider: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        native_registry_state: Any = None,
        skip_invalid_json_sources: bool = True,
    ) -> RegistryProbeResult:
        """Infer registry state from lazily provided path-source chunks."""
        prepared = _options_capsule(options)
        skip_invalid = bool(skip_invalid_json_sources)
        if native_registry_state is not None:
            raw = _native.context_registry_probe_from_path_source_chunk_provider_registry_state(
                self._capsule,
                provider,
                prepared,
                native_registry_state,
                field_name_policy,
                schema_mode,
                skip_invalid,
            )
        else:
            raw = _native.context_registry_probe_from_path_source_chunk_provider(
                self._capsule,
                provider,
                prepared,
                registry_json,
                field_name_policy,
                schema_mode,
                skip_invalid,
            )
        return RegistryProbeResult.from_native(raw)
