"""Define stable public errors and structured diagnostic details.

The exception hierarchy covers cancellation, resource exhaustion, integrity, validation, and
native failures while preserving machine-readable error codes and context.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    """Stable error codes (public API).

    These are intended for programmatic handling and are stable across releases.

    Error messages may change; prefer `code` for branching.
    """

    LOADER_IMPORT_FAILED = "E_LOADER_IMPORT_FAILED"
    RUNTIME_MISMATCH = "E_RUNTIME_MISMATCH"
    INTEGRITY = "E_INTEGRITY"
    INVALID_ARGUMENT = "E_INVALID_ARGUMENT"
    RESOURCE_LIMIT = "E_RESOURCE_LIMIT"
    OUT_OF_MEMORY = "E_OUT_OF_MEMORY"
    CANCELLED = "E_CANCELLED"
    RUNTIME = "E_RUNTIME"


@dataclass(slots=True)
class ErrorDetail:
    """Structured detail payload for exceptions (JSON-serializable)."""

    code: ErrorCode
    message: str
    detail: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable error detail mapping."""
        out: dict[str, Any] = {"code": self.code.value, "message": self.message}
        if self.detail is not None:
            out["detail"] = self.detail
        return out


class SchemaSanitizerError(RuntimeError):
    """Base error for schema-sanitizer."""

    code: ErrorCode
    detail: dict[str, Any] | None

    def __init__(
        self,
        message: str,
        *,
        code: ErrorCode = ErrorCode.RUNTIME,
        detail: dict[str, Any] | None = None,
    ):
        """Create an error with a stable code and optional detail."""
        super().__init__(message)
        self.code = code
        self.detail = detail

    def to_detail(self) -> ErrorDetail:
        """Return this exception as structured error detail."""
        return ErrorDetail(self.code, str(self), self.detail)


class SchemaSanitizerImportError(SchemaSanitizerError, ImportError):
    """Raised when the native extension cannot be imported."""

    def __init__(self, message: str, *, detail: dict[str, Any] | None = None):
        """Create a native import error."""
        SchemaSanitizerError.__init__(
            self, message, code=ErrorCode.LOADER_IMPORT_FAILED, detail=detail
        )


class SchemaSanitizerRuntimeMismatchError(SchemaSanitizerError):
    """Raised when runtime components do not match the expected ABI."""

    def __init__(self, message: str, *, detail: dict[str, Any] | None = None):
        """Create a runtime mismatch error."""
        super().__init__(message, code=ErrorCode.RUNTIME_MISMATCH, detail=detail)


class SchemaSanitizerCancelledError(SchemaSanitizerError):
    """Raised when native execution reports cancellation."""

    def __init__(self, message: str, *, detail: dict[str, Any] | None = None):
        """Create a cancellation error."""
        super().__init__(message, code=ErrorCode.CANCELLED, detail=detail)


class SchemaSanitizerResourceError(SchemaSanitizerError):
    """Raised when a resource limit (e.g. memory_limit_bytes) is exceeded."""

    def __init__(self, message: str, *, detail: dict[str, Any] | None = None):
        """Create a resource limit error."""
        super().__init__(message, code=ErrorCode.RESOURCE_LIMIT, detail=detail)


class SchemaSanitizerInvalidArgumentError(SchemaSanitizerError, ValueError):
    """Raised when inputs/options are invalid."""

    def __init__(self, message: str, *, detail: dict[str, Any] | None = None):
        """Create an invalid argument error."""
        SchemaSanitizerError.__init__(self, message, code=ErrorCode.INVALID_ARGUMENT, detail=detail)


class SchemaSanitizerIntegrityError(SchemaSanitizerError):
    """Raised when a runtime integrity check fails."""

    def __init__(self, message: str, *, detail: dict[str, Any] | None = None):
        """Create an integrity error."""
        super().__init__(message, code=ErrorCode.INTEGRITY, detail=detail)


class SchemaSanitizerOutOfMemoryError(SchemaSanitizerError, MemoryError):
    """Raised on true out-of-memory (distinct from a configured resource limit)."""

    def __init__(self, message: str, *, detail: dict[str, Any] | None = None):
        """Create an out-of-memory error."""
        SchemaSanitizerError.__init__(self, message, code=ErrorCode.OUT_OF_MEMORY, detail=detail)


__all__ = [
    "ErrorCode",
    "ErrorDetail",
    "SchemaSanitizerCancelledError",
    "SchemaSanitizerError",
    "SchemaSanitizerImportError",
    "SchemaSanitizerIntegrityError",
    "SchemaSanitizerInvalidArgumentError",
    "SchemaSanitizerOutOfMemoryError",
    "SchemaSanitizerResourceError",
    "SchemaSanitizerRuntimeMismatchError",
]
