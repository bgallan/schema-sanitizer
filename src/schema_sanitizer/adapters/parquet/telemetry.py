"""Parquet reader route, fallback, and diagnostic telemetry."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

from ...core_impl.operation_diagnostics import _bounded_diagnostic_snapshot

_MAX_COUNTER_KEYS = 128
_MAX_FALLBACK_HISTORY = 64
_MAX_LABEL_CHARS = 160
_MAX_ERROR_CHARS = 512
_LABEL_HASH_CHUNK_CHARS = 4096
_OVERFLOW_KEY = "<other>"


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
        "compressed_bytes": 0,
        "decompressed_bytes": 0,
        "decompression_ratio": 0.0,
    }


def _bounded_label(value: object) -> str:
    """Return one stable label without retaining attacker-sized text."""
    if type(value) is str:
        text = value
    else:
        try:
            text = str(value)
        except BaseException:
            value_type = type(value)
            text = f"<{value_type.__module__}.{value_type.__qualname__}>"
    if len(text) <= _MAX_LABEL_CHARS:
        return text
    digest = hashlib.blake2b(digest_size=16)
    for offset in range(0, len(text), _LABEL_HASH_CHUNK_CHARS):
        digest.update(
            text[offset : offset + _LABEL_HASH_CHUNK_CHARS].encode("utf-8", errors="surrogatepass")
        )
    return f"long-label:{len(text)}:{digest.hexdigest()}"


def _bounded_exception_text(exc: BaseException) -> str:
    """Return bounded exception telemetry without trusting ``__str__``."""
    error_type = type(exc)
    name = f"{error_type.__module__}.{error_type.__qualname__}"
    if error_type.__module__ == "builtins":
        name = error_type.__qualname__
    try:
        detail = str(exc)
    except BaseException:
        detail = "<exception text unavailable>"
    if len(detail) > _MAX_ERROR_CHARS:
        detail = f"{detail[:_MAX_ERROR_CHARS]}..."
    return f"{name}: {detail}"


def _diagnostics_snapshot(diagnostics: dict[str, Any]) -> dict[str, Any]:
    """Return a bounded recursive defensive copy of diagnostics."""
    return _bounded_diagnostic_snapshot(diagnostics)


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
    """Own the latest route, diagnostics, and bounded route counters."""

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
        """Build a fresh bounded diagnostics record from defaults and updates."""
        diagnostics = _default_native_reader_diagnostics()
        diagnostics.update(updates)
        return _diagnostics_snapshot(diagnostics)


_LOCK = Lock()
_STATE = ParquetReaderTelemetryState()


def _increment_locked(counter: dict[str, int], raw_key: object) -> None:
    """Increment one bounded counter while ``_LOCK`` is held."""
    key = _bounded_label(raw_key)
    if key not in counter and len(counter) >= _MAX_COUNTER_KEYS - 1:
        key = _OVERFLOW_KEY
    counter[key] = counter.get(key, 0) + 1


def _update_diagnostics_locked(updates: dict[str, Any]) -> None:
    """Merge bounded diagnostics while ``_LOCK`` is held."""
    diagnostics = _STATE.diagnostics_snapshot()
    diagnostics.update(updates)
    _STATE.last_native_reader_diagnostics = _diagnostics_snapshot(diagnostics)


def _fallback_history_with_locked(
    route: str,
    status: str,
    *,
    error: str | None = None,
) -> list[dict[str, Any]]:
    """Return bounded fallback history while ``_LOCK`` is held."""
    raw_history = _STATE.last_native_reader_diagnostics.get("fallback_attempt_history", [])
    history_items = raw_history if isinstance(raw_history, list) else []
    history = [
        dict(event)
        for event in history_items[-(_MAX_FALLBACK_HISTORY - 1) :]
        if isinstance(event, dict)
    ]
    event: dict[str, Any] = {"route": route, "status": status}
    if error is not None:
        event["error"] = error
    history.append(event)
    return history


def update_parquet_native_reader_diagnostics(**updates: Any) -> None:
    """Update the latest diagnostics without incrementing reason counters."""
    with _LOCK:
        _update_diagnostics_locked(updates)


def set_parquet_stream_factory_route(route: str) -> None:
    """Record the route used by the most recent Parquet stream factory."""
    bounded_route = _bounded_label(route)
    with _LOCK:
        _STATE.last_route = bounded_route
        _increment_locked(_STATE.route_counts, bounded_route)


def set_parquet_native_reader_diagnostics(**diagnostics: Any) -> None:
    """Replace diagnostics for the most recent native reader attempt."""
    with _LOCK:
        normalized = _STATE.normalized_diagnostics(diagnostics)
        reason = _bounded_label(normalized.get("reason") or "none")
        normalized["reason"] = reason
        _STATE.last_native_reader_diagnostics = normalized
        _increment_locked(_STATE.native_reader_reason_counts, reason)


def record_parquet_fallback_attempt(route: str) -> None:
    """Record that a PyArrow fallback route is being attempted."""
    bounded_route = _bounded_label(route)
    with _LOCK:
        _increment_locked(_STATE.fallback_attempt_counts, bounded_route)
        diagnostics = _STATE.last_native_reader_diagnostics
        if diagnostics.get("ready") is not True or diagnostics.get("reason") != "native_stream":
            _update_diagnostics_locked(
                {
                    "fallback_attempted": True,
                    "fallback_succeeded": False,
                    "fallback_route": bounded_route,
                    "fallback_attempt_history": _fallback_history_with_locked(
                        bounded_route, "attempted"
                    ),
                }
            )


def record_parquet_fallback_success(route: str) -> None:
    """Record a successful PyArrow fallback route."""
    bounded_route = _bounded_label(route)
    with _LOCK:
        _increment_locked(_STATE.fallback_success_counts, bounded_route)
        _STATE.last_route = bounded_route
        _increment_locked(_STATE.route_counts, bounded_route)
        diagnostics = _STATE.last_native_reader_diagnostics
        if diagnostics.get("ready") is not True or diagnostics.get("reason") != "native_stream":
            _update_diagnostics_locked(
                {
                    "fallback_attempted": True,
                    "fallback_succeeded": True,
                    "fallback_route": bounded_route,
                    "fallback_error": None,
                    "fallback_attempt_history": _fallback_history_with_locked(
                        bounded_route, "succeeded"
                    ),
                    "pipeline_contract_satisfied": True,
                    "pipeline_contract_route": bounded_route,
                    "pipeline_contract_error": None,
                    "safe_fallback_contract_satisfied": True,
                    "native_reader_contract_satisfied": False,
                }
            )


def record_parquet_fallback_failure(route: str, exc: BaseException) -> None:
    """Record a failed PyArrow fallback attempt before it is re-raised."""
    bounded_route = _bounded_label(route)
    error = _bounded_exception_text(exc)
    with _LOCK:
        _increment_locked(_STATE.fallback_failure_counts, bounded_route)
        diagnostics = _STATE.last_native_reader_diagnostics
        if diagnostics.get("ready") is not True or diagnostics.get("reason") != "native_stream":
            _update_diagnostics_locked(
                {
                    "fallback_attempted": True,
                    "fallback_succeeded": False,
                    "fallback_route": bounded_route,
                    "fallback_error": error,
                    "fallback_attempt_history": _fallback_history_with_locked(
                        bounded_route, "failed", error=error
                    ),
                    "pipeline_contract_satisfied": False,
                    "pipeline_contract_route": bounded_route,
                    "pipeline_contract_error": error,
                    "safe_fallback_contract_satisfied": False,
                }
            )


def last_parquet_stream_factory_route() -> str:
    """Return the route used by the most recent Parquet stream factory."""
    with _LOCK:
        return _STATE.last_route


def last_parquet_native_reader_diagnostics() -> dict[str, Any]:
    """Return diagnostics for the most recent native Parquet reader attempt."""
    with _LOCK:
        return _STATE.diagnostics_snapshot()


def parquet_stream_factory_observability() -> dict[str, Any]:
    """Return a defensive snapshot of route and fallback telemetry."""
    with _LOCK:
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
    with _LOCK:
        diagnostics = _STATE.diagnostics_snapshot()
    return _parquet_pipeline_contract_status_from_diagnostics(diagnostics)


def reset_parquet_stream_factory_observability() -> None:
    """Reset Parquet route/fallback telemetry and latest diagnostics."""
    with _LOCK:
        _STATE.reset()


def _reset_after_fork() -> None:
    """Discard inherited telemetry locks and parent-process observations."""
    global _LOCK, _STATE
    _LOCK = Lock()
    _STATE = ParquetReaderTelemetryState()


from ...core_impl.fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler("parquet-telemetry", mode="quarantine_only")
