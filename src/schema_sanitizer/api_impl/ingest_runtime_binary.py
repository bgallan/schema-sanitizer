"""Binary input routing helpers for ingest runtime APIs."""

from __future__ import annotations

from typing import Any, Literal, TypeAlias

from .parquet_errors import unsupported_direct_parquet_ingestion

_PARQUET_MAGIC = b"PAR1"
_ARROW_IPC_MAGIC = b"ARROW1"
_Source: TypeAlias = Literal["auto", "text", "path", "python", "uri", "stream"]


def _sniff_bytes_format(buf: bytes | bytearray | memoryview) -> str:
    """Infer an input format from a byte buffer prefix."""
    mv = memoryview(buf)
    n = len(mv)
    i = 0
    while i < n and mv[i] in b" \t\r\n":
        i += 1
    if i >= n:
        return "json"

    if n - i >= len(_PARQUET_MAGIC) and bytes(mv[i : i + len(_PARQUET_MAGIC)]) == _PARQUET_MAGIC:
        return "parquet"
    if (
        n - i >= len(_ARROW_IPC_MAGIC)
        and bytes(mv[i : i + len(_ARROW_IPC_MAGIC)]) == _ARROW_IPC_MAGIC
    ):
        return "ipc"

    first = mv[i]
    if first in {ord("["), ord("{")}:
        return "json"
    if first == ord("<"):
        return "xml"
    return "csv"


def reject_unsupported_binary_direct_input(
    data: Any,
    *,
    source: _Source,
    format: str,
    memory_limit_bytes: int | None = None,
) -> tuple[Any, _Source, str]:
    """Reject binary formats that no longer have direct native routes."""
    if format != "parquet":
        return data, source, format

    del data, source, memory_limit_bytes
    raise unsupported_direct_parquet_ingestion()
