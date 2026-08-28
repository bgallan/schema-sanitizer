"""Shared fixtures/imports for public input contract tests."""

from __future__ import annotations

import asyncio
import inspect
import io
import json
import signal
import threading
from pathlib import Path

import pytest
from conftest import (
    read_test_csv,
    read_test_json,
    read_test_json_folder,
    read_test_jsonl,
    read_test_parquet,
    read_test_python,
    read_test_xml,
    read_test_xml_folder,
    require_native,
)

import schema_sanitizer as ss
from schema_sanitizer.core_impl.execution import PythonRowsJsonlByteReader


class _TrackingByteReader:
    """Seekable byte reader used to verify native streaming reads."""

    def __init__(self, data: bytes):
        """Store the byte payload and reset read tracking."""
        self._data = data
        self._pos = 0
        self.requests: list[int] = []

    def read(self, max_bytes: int) -> bytes:
        """Return at most the requested bytes and record the requested size."""
        self.requests.append(max_bytes)
        if self._pos >= len(self._data):
            return b""
        end = min(len(self._data), self._pos + max_bytes)
        chunk = self._data[self._pos : end]
        self._pos = end
        return chunk

    def seek(self, offset: int) -> int:
        """Reset the reader to the start of the byte payload."""
        if offset != 0:
            raise ValueError("test reader only supports seek(0)")
        self._pos = 0
        return self._pos

    def close(self) -> None:
        """Match the pyarrow reader close API."""
        pass


class _OversizedByteReader:
    """Byte reader that verifies bounded folder reads stop at the memory limit."""

    def __init__(self, size: int):
        """Store the virtual byte size and reset read tracking."""
        self._remaining = size
        self.requests: list[int] = []
        self.bytes_returned = 0

    def read(self, max_bytes: int) -> bytes:
        """Return virtual bytes up to the requested size."""
        self.requests.append(max_bytes)
        if self._remaining <= 0:
            return b""
        size = min(max_bytes, self._remaining)
        self._remaining -= size
        self.bytes_returned += size
        return b"x" * size

    def close(self) -> None:
        """Match the pyarrow reader close API."""
        pass


__all__ = [
    "Path",
    "PythonRowsJsonlByteReader",
    "_OversizedByteReader",
    "_TrackingByteReader",
    "asyncio",
    "inspect",
    "io",
    "json",
    "pytest",
    "read_test_csv",
    "read_test_json",
    "read_test_json_folder",
    "read_test_jsonl",
    "read_test_parquet",
    "read_test_python",
    "read_test_xml",
    "read_test_xml_folder",
    "require_native",
    "signal",
    "ss",
    "threading",
]
