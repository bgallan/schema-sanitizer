"""Implements `schema_sanitizer.options_impl.options`."""

from __future__ import annotations

from enum import Enum
from typing import Any

from ..core_impl.options_bytes import Options as _RawOptions
from ..core_impl.options_bytes import validate_options as _validate_options
from ..core_impl.options_logical_schema import LogicalSchemaPayload
from .options_groups import (
    _ALLOWED_BY_GROUP,
    _GROUP_BY_OPTION_NAME,
    _GROUP_NAMES,
    _coerce_enum_if_needed,
    _coerce_option_value_if_needed,
    _Proxy,
)

_OPTION_NAMES = frozenset(_GROUP_BY_OPTION_NAME)
_READ_ONLY_OPTION_ATTRS = frozenset({"_raw"}) | _GROUP_NAMES


class Options:
    """Internal wrapper for the native C++ options object.

    The underlying C++ options struct is intentionally flat. This wrapper provides
    grouped accessors (`schema`, `inference`, `io`, ...) for per-call option
    normalization before C++ runtime dispatch.

    Construction supports grouped initialization:

        >>> Options(schema={"schema_evolution": "STRICT"},
        ...         errors={"on_error": "STOP"})

    Each group value must be a `dict` mapping canonical option names to values.
    Canonical names are the ones in ``cpp/src/sanitize/options/options_catalog.def``.

    Unknown keys raise `TypeError`.
    """

    _raw: _RawOptions

    def _bind_groups(self, raw: _RawOptions) -> None:
        """Bind this wrapper to a raw options instance."""

        object.__setattr__(self, "_raw", raw)

        # High-level, grouped proxies (catalog-driven; canonical names only).
        for group_name, allowed in _ALLOWED_BY_GROUP.items():
            object.__setattr__(self, group_name, _Proxy(self._raw, allowed=allowed))

    def __init__(self, **kwargs: Any) -> None:
        """Create grouped options from canonical option dictionaries."""
        self._bind_groups(_RawOptions())

        if kwargs:
            self._apply_kwargs(kwargs)

    def _apply_kwargs(self, kwargs: dict[str, Any]) -> None:
        """Apply grouped constructor keyword arguments."""
        for k, v in kwargs.items():
            if k in _GROUP_NAMES:
                if not isinstance(v, dict):
                    raise TypeError(f"Options.{k} must be a dict of group fields")
                proxy = getattr(self, k)
                for subk, subv in v.items():
                    setattr(proxy, subk, subv)
                continue

            raise TypeError(
                f"Unknown option {k!r}. Use grouped kwargs (schema=..., inference=..., ...)."
            )

    @property
    def raw(self) -> _RawOptions:
        """The backing native Options instance."""
        return self._raw

    def __getattr__(self, name: str) -> Any:
        """Return a canonical raw option value."""
        # Convenience: allow accessing raw fields directly (advanced usage).
        if name not in _OPTION_NAMES:
            raise AttributeError(f"Unknown option attribute {name!r}")
        return getattr(self._raw, name)

    def __setattr__(self, name: str, value: Any) -> None:
        """Set and normalize a canonical raw option value."""
        # Keep proxies immutable once created.
        if name in _READ_ONLY_OPTION_ATTRS:
            raise AttributeError(f"'{type(self).__name__}' attribute '{name}' is read-only")
        if name not in _OPTION_NAMES:
            raise AttributeError(f"Unknown option attribute {name!r}")
        value = _coerce_option_value_if_needed(name, value)
        setattr(self._raw, name, _coerce_enum_if_needed(name, value))

    def __repr__(self) -> str:
        """Return a compact grouped options representation."""
        return "Options(schema=..., inference=..., io=..., performance=...)"

    # ---- Serialization -------------------------------------------------

    @staticmethod
    def _encode_value_for_json(v: Any) -> Any:
        """Convert an option value to a JSON-friendly representation."""
        if isinstance(v, Enum):
            return v.name

        if isinstance(v, (list, tuple)):
            return [Options._encode_value_for_json(x) for x in v]
        if isinstance(v, dict):
            return {str(k): Options._encode_value_for_json(val) for k, val in v.items()}

        if isinstance(v, LogicalSchemaPayload):
            return v

        # Arrow schema is not JSON-serializable in a stable way.
        if v is not None and hasattr(v, "__arrow_c_schema__"):
            raise TypeError(
                "arrow_schema_contract cannot be serialized to JSON. "
                "Pass it programmatically (pyarrow.Schema) instead."
            )
        return v

    def to_flat_dict(self, *, include_defaults: bool = False) -> dict[str, Any]:
        """Serialize the underlying C++ options as a flat dict.

        Values are JSON-friendly (enums are rendered as their name).
        """

        default = _RawOptions() if not include_defaults else None
        out: dict[str, Any] = {}
        for name in sorted(_OPTION_NAMES):
            v = getattr(self._raw, name)
            if default is not None and v == getattr(default, name):
                continue
            out[name] = self._encode_value_for_json(v)
        return out

    def to_dict(self, *, include_defaults: bool = False) -> dict[str, Any]:
        """Serialize options into the grouped kwargs form accepted by Options(...)."""

        grouped: dict[str, dict[str, Any]] = {}

        # Group by catalog metadata. Import-time validation guarantees coverage.
        for k, v in self.to_flat_dict(include_defaults=include_defaults).items():
            grouped.setdefault(_GROUP_BY_OPTION_NAME[k], {})[k] = v

        return {group: grouped[group] for group in sorted(_GROUP_NAMES) if group in grouped}

    # ---------------------------------------------------------------------
    # Native validation / compilation
    # ---------------------------------------------------------------------

    def validate_native(self) -> None:
        """Validate options against the native engine (raises on error)."""

        _validate_options(self.raw)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Options:
        """Create Options from a grouped dict."""

        if not isinstance(d, dict):
            raise TypeError("Options.from_dict expects a dict")
        return cls(**d)


__all__ = ["Options"]
