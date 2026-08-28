"""Encode Python row iterables once into replayable native JSONL input.

Native batches are recorded in a bounded memory-or-disk spool that can be replayed safely and
whose buffers and temporary storage are securely released on close.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterable, Sequence
from tempfile import SpooledTemporaryFile
from typing import Any

from .generated_bytes import BufferedGeneratedBytesReader
from .memory_budget import memory_budget
from .native_symbols import PYTHON_ITER_ROWS_JSONL_BYTES, PYTHON_ROWS_JSONL_BYTES


def ensure_replay_spool_capacity(
    directory: str | None,
    *,
    payload_bytes: int,
    next_size: int,
    memory_bytes: int,
    minimum_free_bytes: int,
) -> None:
    """Reject disk-backed replay growth before exhausting temporary storage."""
    if next_size <= memory_bytes or payload_bytes <= 0:
        return
    target = directory or tempfile.gettempdir()
    try:
        free_bytes = shutil.disk_usage(target).free
    except OSError as exc:
        raise OSError(f"Unable to inspect replay spool filesystem {target!r}") from exc
    required = payload_bytes + minimum_free_bytes
    if free_bytes < required:
        raise OSError(
            "Replay spool filesystem has insufficient free space: "
            f"{free_bytes} bytes available, {required} bytes required"
        )


def close_replay_spool(spool: SpooledTemporaryFile[bytes], spool_bytes: int) -> None:
    """Best-effort overwrite replay bytes, then close the spool."""
    if spool_bytes > 0:
        try:
            spool.seek(0)
            zero_chunk = b"\x00" * min(1024 * 1024, spool_bytes)
            remaining = spool_bytes
            while remaining > 0:
                size = min(len(zero_chunk), remaining)
                written = spool.write(zero_chunk[:size])
                if written != size:
                    break
                remaining -= written
            spool.flush()
        except OSError:
            # Closing still removes the temporary file. Secure overwrite is
            # best-effort because filesystems may be copy-on-write.
            pass
    spool.close()


class PythonRowsJsonlByteReader(BufferedGeneratedBytesReader):
    """Seekable byte reader that serializes Python rows as JSON Lines once."""

    _MAX_ITERABLE_ROWS_PER_BATCH = 4_096
    _MIN_FREE_DISK_BYTES = 16 * 1024 * 1024

    def __init__(self, rows: Iterable[Any], *, memory_limit_bytes: int | None = None):
        """Retain the source and progressively record one encoded replay stream."""
        budget = memory_budget(memory_limit_bytes)
        self._spool_memory_bytes = min(8 * 1024 * 1024, budget.total_bytes)
        self._max_spool_bytes = budget.replay_spool_bytes
        self._spool_dir: str | None = None
        self._rows: Sequence[Any] | None = rows if isinstance(rows, Sequence) else None
        self._iterable = None if self._rows is not None else iter(rows)
        self._iterable_index = 0
        self._sequence_index = 0
        self._source_complete = False
        self._spool: SpooledTemporaryFile[bytes] | None = SpooledTemporaryFile(
            max_size=self._spool_memory_bytes,
            mode="w+b",
            dir=self._spool_dir,
        )
        self._spool_bytes = 0
        self._replay_spool = False
        self._source_limit_bytes = budget.total_bytes if memory_limit_bytes is not None else None
        self._accounted_sequence_bytes = 0
        self._native_batch = PYTHON_ROWS_JSONL_BYTES
        self._native_iter_batch = PYTHON_ITER_ROWS_JSONL_BYTES
        super().__init__("PythonRowsJsonlByteReader", default_chunk_bytes=budget.io_chunk_bytes)

    def _next_iterable_payload(self, target_bytes: int) -> bytes:
        """Consume and encode one bounded iterator batch in one native call."""
        if self._source_complete:
            return b""
        assert self._iterable is not None
        try:
            payload, next_index, exhausted = self._native_iter_batch(
                self._iterable,
                self._iterable_index,
                max(1, target_bytes),
                self._MAX_ITERABLE_ROWS_PER_BATCH,
            )
        except (RuntimeError, ValueError) as exc:
            raise RuntimeError("Native Python row JSONL encoding failed") from exc
        if next_index < self._iterable_index:
            raise RuntimeError("Native Python row JSONL encoder did not make progress")
        if not payload and not exhausted and next_index == self._iterable_index:
            raise RuntimeError("Native Python row JSONL encoder did not make progress")
        self._iterable_index = next_index
        if exhausted:
            self._iterable = None
            self._source_complete = True
        return payload

    def _next_sequence_payload(self, target_bytes: int) -> bytes:
        """Encode the next sequence segment exactly once in source order."""
        assert self._rows is not None
        if self._source_complete:
            return b""
        row_count = len(self._rows)
        if self._sequence_index >= row_count:
            self._source_complete = True
            return b""
        start_index = self._sequence_index
        try:
            payload, next_index = self._native_batch(
                self._rows,
                start_index,
                max(1, target_bytes),
            )
        except (RuntimeError, ValueError) as exc:
            raise RuntimeError("Native Python row JSONL encoding failed") from exc
        if next_index <= start_index:
            raise RuntimeError("Native Python row JSONL encoder did not make progress")
        self._sequence_index = next_index
        self._accounted_sequence_bytes += len(payload)
        if (
            self._source_limit_bytes is not None
            and self._accounted_sequence_bytes > self._source_limit_bytes
        ):
            raise RuntimeError(
                "memory_limit_bytes limit exceeded: "
                f"{self._accounted_sequence_bytes} bytes > {self._source_limit_bytes} bytes"
            )
        if self._sequence_index >= row_count:
            self._source_complete = True
        return payload

    def _next_source_payload(self, target_bytes: int) -> bytes:
        """Encode one new source payload from a sequence or one-shot iterator."""
        if self._rows is not None:
            return self._next_sequence_payload(target_bytes)
        return self._next_iterable_payload(target_bytes)

    def _ensure_spool_disk_capacity(self, payload_bytes: int, next_size: int) -> None:
        """Reject disk-backed replay growth before exhausting temporary storage."""
        ensure_replay_spool_capacity(
            self._spool_dir,
            payload_bytes=payload_bytes,
            next_size=next_size,
            memory_bytes=self._spool_memory_bytes,
            minimum_free_bytes=self._MIN_FREE_DISK_BYTES,
        )

    def _record_payload(self, payload: bytes) -> None:
        """Append newly encoded bytes to the bounded replay stream."""
        if not payload:
            return
        next_size = self._spool_bytes + len(payload)
        if next_size > self._max_spool_bytes:
            raise RuntimeError(
                "max_replay_spool_bytes limit exceeded: "
                f"{next_size} bytes > {self._max_spool_bytes} bytes"
            )
        self._ensure_spool_disk_capacity(len(payload), next_size)
        assert self._spool is not None
        self._spool.seek(0, 2)
        written = self._spool.write(payload)
        if written != len(payload):
            raise OSError("Replay spool short write")
        self._spool_bytes = next_size

    def _produce_and_record(self, target_bytes: int) -> bytes:
        """Encode one source payload and retain it for current or future replay."""
        payload = self._next_source_payload(target_bytes)
        self._record_payload(payload)
        return payload

    def _append_next(self, target_bytes: int) -> bool:
        """Replay recorded bytes, then progressively extend the same stream."""
        assert self._spool is not None
        if self._replay_spool:
            payload = self._spool.read(max(1, target_bytes))
            if payload:
                self._buffer.extend(payload)
                return True
        payload = self._produce_and_record(target_bytes)
        if not payload:
            return False
        self._buffer.extend(payload)
        return True

    def _reset_reader(self) -> None:
        """Replay encoded bytes without draining or re-encoding the source."""
        assert self._spool is not None
        self._spool.seek(0)
        self._replay_spool = True

    def close(self) -> None:
        """Release generated bytes and the bounded replay spool."""
        if self._spool is not None:
            close_replay_spool(self._spool, self._spool_bytes)
            self._spool = None
        self._iterable = None
        self._iterable_index = 0
        self._sequence_index = 0
        self._spool_bytes = 0
        super().close()
