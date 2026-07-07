"""Shared input preparation helpers for native ingestion paths."""

from __future__ import annotations

from typing import Any

from ..core_impl.transcoding_reader import TranscodingPathByteReader
from .ingest_runtime_selectors import (
    _decode_text_bytes,
    _Format,
    _normalize_input_text_encoding,
    _Source,
)
from .native_folder_common import native_text_encoding_supported


def prepare_native_text_data(
    data: Any,
    *,
    src: _Source,
    fmt: _Format,
    input_text_encoding: str,
) -> tuple[Any, _Source]:
    """Prepare encoded text input for native ingestion."""
    if fmt not in {"csv", "json", "json_array", "xml"}:
        return data, src

    enc = _normalize_input_text_encoding(input_text_encoding)
    if src == "path" and enc != "utf-8":
        if native_text_encoding_supported(enc):
            return data, src
        return TranscodingPathByteReader(data, encoding=enc), "stream"
    if src != "path" and isinstance(data, (bytes, bytearray, memoryview)):
        return _decode_text_bytes(data, encoding=enc), src
    return data, src
