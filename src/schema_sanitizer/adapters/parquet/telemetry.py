"""Parquet reader route, fallback, and diagnostic telemetry."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _default_native_reader_diagnostics() -> dict[str, Any]:
    """Return a fresh default diagnostics record."""
    return {
        "attempted": False,
        "ready": False,
        "reason": "none",
        "blockers": [],
        "fallback_expected": False,
        "fallback_attempted": False,
        "fallback_succeeded": False,
        "fallback_route": None,
        "fallback_error": None,
        "fallback_attempt_history": [],
        "pipeline_contract_satisfied": False,
        "pipeline_contract_route": None,
        "pipeline_contract_error": None,
        "native_reader_contract_satisfied": False,
        "safe_fallback_contract_satisfied": False,
        "created_by": None,
        "native_writer_detected": False,
        "native_writer_contract_satisfied": False,
        "native_nested_contract_applicable": False,
        "native_nested_contract_satisfied": False,
        "native_nested_contract_issues": [],
    }


def _copy_diagnostic_value(value: Any) -> Any:
    """Copy supported diagnostic containers without generic deepcopy machinery."""
    if isinstance(value, dict):
        return {key: _copy_diagnostic_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_diagnostic_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_copy_diagnostic_value(item) for item in value)
    if isinstance(value, set):
        return {_copy_diagnostic_value(item) for item in value}
    return value


def _diagnostics_snapshot(diagnostics: dict[str, Any]) -> dict[str, Any]:
    """Return a recursive defensive copy of one diagnostics record."""
    return {key: _copy_diagnostic_value(value) for key, value in diagnostics.items()}


_PYARROW_PARQUET_FALLBACK_ROUTES = frozenset(
    {"pyarrow_dataset_scanner", "pyarrow_parquetfile_iter_batches"}
)


def _parquet_pipeline_contract_status_from_diagnostics(
    diagnostics: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return a compact yes/no status for the last Parquet pipeline outcome."""
    diagnostics = _diagnostics_snapshot(diagnostics or {})
    route = diagnostics.get("pipeline_contract_route") or diagnostics.get("fallback_route")
    route = None if route is None else str(route)
    issues: list[str] = []

    if diagnostics.get("pipeline_contract_satisfied") is not True:
        error = diagnostics.get("pipeline_contract_error") or diagnostics.get("fallback_error")
        if error:
            issues.append(f"pipeline contract failed: {error}")
        else:
            issues.append("pipeline contract is not satisfied")

    if route == "native_parquet_stream":
        if diagnostics.get("native_reader_contract_satisfied") is not True:
            issues.append("native reader contract was not marked satisfied")
        if diagnostics.get("fallback_attempted") is True:
            issues.append("native success should not also attempt PyArrow fallback")
    elif route in _PYARROW_PARQUET_FALLBACK_ROUTES:
        if diagnostics.get("safe_fallback_contract_satisfied") is not True:
            issues.append("safe PyArrow fallback contract was not marked satisfied")
        if diagnostics.get("fallback_attempted") is not True:
            issues.append("PyArrow fallback route did not record an attempt")
        if diagnostics.get("fallback_succeeded") is not True:
            issues.append("PyArrow fallback route did not record success")
    else:
        issues.append(f"unknown or missing pipeline route: {route!r}")

    history = list(diagnostics.get("fallback_attempt_history") or [])
    return {
        "satisfied": not issues,
        "route": route,
        "issues": list(dict.fromkeys(issues)),
        "pipeline_contract_satisfied": diagnostics.get("pipeline_contract_satisfied") is True,
        "native_reader_contract_satisfied": (
            diagnostics.get("native_reader_contract_satisfied") is True
        ),
        "safe_fallback_contract_satisfied": (
            diagnostics.get("safe_fallback_contract_satisfied") is True
        ),
        "fallback_attempted": diagnostics.get("fallback_attempted") is True,
        "fallback_succeeded": diagnostics.get("fallback_succeeded") is True,
        "fallback_route": diagnostics.get("fallback_route"),
        "fallback_attempt_history": history,
        "pipeline_contract_error": diagnostics.get("pipeline_contract_error"),
        "fallback_error": diagnostics.get("fallback_error"),
    }


@dataclass(slots=True)
class ParquetReaderTelemetryState:
    """Own the latest route, diagnostics, and route counters."""

    last_route: str = "none"
    last_native_reader_diagnostics: dict[str, Any] = field(
        default_factory=_default_native_reader_diagnostics
    )
    route_counts: dict[str, int] = field(default_factory=dict)
    native_reader_reason_counts: dict[str, int] = field(default_factory=dict)
    fallback_attempt_counts: dict[str, int] = field(default_factory=dict)
    fallback_success_counts: dict[str, int] = field(default_factory=dict)
    fallback_failure_counts: dict[str, int] = field(default_factory=dict)

    def reset(self) -> None:
        """Return all telemetry to its initial state."""
        self.last_route = "none"
        self.last_native_reader_diagnostics = _default_native_reader_diagnostics()
        self.route_counts.clear()
        self.native_reader_reason_counts.clear()
        self.fallback_attempt_counts.clear()
        self.fallback_success_counts.clear()
        self.fallback_failure_counts.clear()

    def diagnostics_snapshot(self) -> dict[str, Any]:
        """Return a defensive copy of the latest native diagnostics."""
        return _diagnostics_snapshot(self.last_native_reader_diagnostics)

    def normalized_diagnostics(self, updates: dict[str, Any]) -> dict[str, Any]:
        """Build a fresh diagnostics record from defaults and updates."""
        diagnostics = _default_native_reader_diagnostics()
        diagnostics.update(updates)
        return _diagnostics_snapshot(diagnostics)


_STATE = ParquetReaderTelemetryState()


def _increment(counter: dict[str, int], route: str) -> None:
    """Increment one route counter."""
    counter[route] = counter.get(route, 0) + 1


def update_parquet_native_reader_diagnostics(**updates: Any) -> None:
    """Update the latest diagnostics without incrementing reason counters."""
    diagnostics = _STATE.diagnostics_snapshot()
    diagnostics.update(updates)
    _STATE.last_native_reader_diagnostics = _diagnostics_snapshot(diagnostics)


def _fallback_history_with(
    route: str,
    status: str,
    *,
    error: str | None = None,
) -> list[dict[str, Any]]:
    """Return fallback history with one appended immutable-style event."""
    history = [
        dict(event)
        for event in _STATE.last_native_reader_diagnostics.get("fallback_attempt_history", [])
    ]
    event: dict[str, Any] = {"route": route, "status": status}
    if error is not None:
        event["error"] = error
    history.append(event)
    return history


def set_parquet_stream_factory_route(route: str) -> None:
    """Record the route used by the most recent Parquet stream factory."""
    _STATE.last_route = route
    _increment(_STATE.route_counts, route)


def set_parquet_native_reader_diagnostics(**diagnostics: Any) -> None:
    """Replace diagnostics for the most recent native reader attempt."""
    normalized = _STATE.normalized_diagnostics(diagnostics)
    _STATE.last_native_reader_diagnostics = normalized
    reason = str(normalized.get("reason") or "none")
    _increment(_STATE.native_reader_reason_counts, reason)


def record_parquet_fallback_attempt(route: str) -> None:
    """Record that a PyArrow fallback route is being attempted."""
    _increment(_STATE.fallback_attempt_counts, route)
    diagnostics = _STATE.last_native_reader_diagnostics
    if diagnostics.get("ready") is not True or diagnostics.get("reason") != "native_stream":
        update_parquet_native_reader_diagnostics(
            fallback_attempted=True,
            fallback_succeeded=False,
            fallback_route=route,
            fallback_attempt_history=_fallback_history_with(route, "attempted"),
        )


def record_parquet_fallback_success(route: str) -> None:
    """Record a successful PyArrow fallback route."""
    _increment(_STATE.fallback_success_counts, route)
    set_parquet_stream_factory_route(route)
    diagnostics = _STATE.last_native_reader_diagnostics
    if diagnostics.get("ready") is not True or diagnostics.get("reason") != "native_stream":
        update_parquet_native_reader_diagnostics(
            fallback_attempted=True,
            fallback_succeeded=True,
            fallback_route=route,
            fallback_error=None,
            fallback_attempt_history=_fallback_history_with(route, "succeeded"),
            pipeline_contract_satisfied=True,
            pipeline_contract_route=route,
            pipeline_contract_error=None,
            safe_fallback_contract_satisfied=True,
            native_reader_contract_satisfied=False,
        )


def record_parquet_fallback_failure(route: str, exc: BaseException) -> None:
    """Record a failed PyArrow fallback attempt before it is re-raised."""
    _increment(_STATE.fallback_failure_counts, route)
    diagnostics = _STATE.last_native_reader_diagnostics
    if diagnostics.get("ready") is not True or diagnostics.get("reason") != "native_stream":
        error = f"{type(exc).__name__}: {exc}"
        update_parquet_native_reader_diagnostics(
            fallback_attempted=True,
            fallback_succeeded=False,
            fallback_route=route,
            fallback_error=error,
            fallback_attempt_history=_fallback_history_with(route, "failed", error=error),
            pipeline_contract_satisfied=False,
            pipeline_contract_route=route,
            pipeline_contract_error=error,
            safe_fallback_contract_satisfied=False,
        )


def last_parquet_stream_factory_route() -> str:
    """Return the route used by the most recent Parquet stream factory."""
    return _STATE.last_route


def last_parquet_native_reader_diagnostics() -> dict[str, Any]:
    """Return diagnostics for the most recent native Parquet reader attempt."""
    return _STATE.diagnostics_snapshot()


def parquet_stream_factory_observability() -> dict[str, Any]:
    """Return a defensive snapshot of route and fallback telemetry."""
    return {
        "last_route": _STATE.last_route,
        "route_counts": dict(_STATE.route_counts),
        "last_native_reader_diagnostics": _STATE.diagnostics_snapshot(),
        "native_reader_reason_counts": dict(_STATE.native_reader_reason_counts),
        "fallback_attempt_counts": dict(_STATE.fallback_attempt_counts),
        "fallback_success_counts": dict(_STATE.fallback_success_counts),
        "fallback_failure_counts": dict(_STATE.fallback_failure_counts),
    }


def last_parquet_pipeline_contract_status() -> dict[str, Any]:
    """Return a compact gate for the most recent Parquet pipeline read."""
    return _parquet_pipeline_contract_status_from_diagnostics(_STATE.diagnostics_snapshot())


def reset_parquet_stream_factory_observability() -> None:
    """Reset Parquet route/fallback telemetry and latest diagnostics."""
    _STATE.reset()
