"""Private diagnostics helpers for ingest runtime result wrappers."""

from __future__ import annotations

import json
from typing import Any

from ..core_impl.json_payloads import json_object_loads

_DIAGNOSTIC_INT_KEYS = (
    "inferred_rows",
    "inferred_bytes",
    "arrow_schema_depth",
    "parquet_schema_depth",
    "materialized_rows",
    "batches",
    "flattened_fields",
    "scalar_wrappings",
    "direct_arrow_input",
    "skipped_rows",
    "warnings",
    "errors",
    "soft_errors",
)


def _diagnostics_raw_json(raw: Any) -> str:
    """Return a JSON representation of raw diagnostics."""
    fn = getattr(raw, "to_json", None)
    if callable(fn):
        return str(fn())

    payload: dict[str, Any] = {}
    missing = object()
    for key in _DIAGNOSTIC_INT_KEYS:
        value = getattr(raw, key, missing)
        if value is not missing:
            payload[key] = value
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def _diagnostics_payload(raw: Any) -> dict[str, Any]:
    """Parse raw diagnostics into a dictionary."""
    diag = _diagnostics_target(raw)
    if diag is not None:
        try:
            cached = object.__getattribute__(diag, "_obj")
        except AttributeError:
            cached = None
        if isinstance(cached, dict):
            return cached
    try:
        return json_object_loads(_diagnostics_raw_json(raw))
    except Exception:
        return {}


def _diagnostics_target(raw: Any) -> Any:
    """Return the diagnostics object associated with a wrapper."""
    if raw is None:
        return None
    try:
        return object.__getattribute__(raw, "diagnostics")
    except AttributeError:
        return raw


def _patch_diagnostics_values(raw: Any, values: dict[str, Any]) -> None:
    """Patch live and serialized diagnostics values."""
    diag = _diagnostics_target(raw)
    if diag is None:
        return

    ensure_obj = getattr(diag, "_ensure_obj", None)
    if callable(ensure_obj):
        obj = ensure_obj()
    else:
        try:
            obj = object.__getattribute__(diag, "_obj")
        except AttributeError:
            for key, value in values.items():
                setattr(diag, key, value)
            return
    if not isinstance(obj, dict):
        for key, value in values.items():
            setattr(diag, key, value)
        return

    for key, value in values.items():
        setattr(diag, key, value)

    obj.update(values)
    diag._diag_json = json.dumps(obj, separators=(",", ":"), sort_keys=True, default=str)


def _increment_diagnostics_counter(raw: Any, key: str, delta: int) -> None:
    """Increment a diagnostics counter when available."""
    diag = _diagnostics_target(raw)
    if diag is None:
        return
    try:
        current = int(getattr(diag, key, 0) or 0)
    except Exception:
        current = 0
    _patch_diagnostics_values(diag, {key: current + delta})


def _diagnostics_stats(raw: Any) -> dict[str, Any]:
    """Return normalized integer diagnostics statistics."""
    payload = _diagnostics_payload(raw)
    out: dict[str, Any] = {}
    for key in _DIAGNOSTIC_INT_KEYS:
        # Prefer live raw attributes because wrapper-level finalization may patch
        # counters after sink materialization.
        value = getattr(raw, key, payload.get(key, 0))
        try:
            out[key] = int(value)
        except Exception:
            out[key] = 0
    return out
