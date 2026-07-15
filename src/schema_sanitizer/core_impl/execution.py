"""Package-owned ABI3 execution context and process-local lifecycle."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from tempfile import SpooledTemporaryFile
from typing import Any

from .generated_bytes import BufferedGeneratedBytesReader
from .json_payloads import json_object_loads
from .memory_budget import memory_budget
from .native_options import _options_capsule
from .native_results import SinkOutput, _registry_sink_output
from .native_runtime import native_core as _native
from .native_symbols import PYTHON_ROWS_JSONL_BYTES
from .probes import (
    _ExecutionRegistryInputProbeMethods,
    _ExecutionRegistryPathSourceProbeMethods,
    _ExecutionSchemaProbeMethods,
)
from .registry_sinks import (
    _RegistryArrowSinkMethods,
    _RegistryPathProviderSinkMethods,
    _RegistryPathSourceSinkMethods,
)
from .replay_spool import close_replay_spool, ensure_replay_spool_capacity

_LAST_SOURCE_ROUTE = "none"
_LAST_PYTHON_ROWS_ROUTE = "none"
_DEFAULT_CONTEXT: ExecutionContext | None = None


def last_python_rows_route() -> str:
    """Return the route used by the most recent Python rows read."""
    return _LAST_PYTHON_ROWS_ROUTE


class PythonRowsJsonlByteReader(BufferedGeneratedBytesReader):
    """Seekable byte reader that serializes Python rows as JSON Lines."""

    _ENCODE_ROWS_PER_CHUNK = 1
    _MIN_FREE_DISK_BYTES = 16 * 1024 * 1024

    def __init__(self, rows: Iterable[Any], *, memory_limit_bytes: int | None = None):
        """Retain sequences directly and spool one-shot iterables as JSONL."""
        budget = memory_budget(memory_limit_bytes)
        self._spool_memory_bytes = min(8 * 1024 * 1024, budget.total_bytes)
        self._max_spool_bytes = budget.replay_spool_bytes
        self._spool_dir: str | None = None
        self._rows: Sequence[Any] | None = rows if isinstance(rows, Sequence) else None
        self._iterable = None if self._rows is not None else iter(rows)
        self._iterable_chunk: list[Any] = []
        self._iterable_chunk_index = 0
        self._spool: SpooledTemporaryFile[bytes] | None = None
        if self._rows is None:
            self._spool = SpooledTemporaryFile(
                max_size=self._spool_memory_bytes,
                mode="w+b",
                dir=self._spool_dir,
            )
        self._spool_bytes = 0
        self._spool_complete = False
        self._replay_spool = False
        self._index = 0
        self._native_batch = PYTHON_ROWS_JSONL_BYTES
        super().__init__("PythonRowsJsonlByteReader", default_chunk_bytes=budget.io_chunk_bytes)

    def _next_iterable_payload(self, target_bytes: int) -> bytes:
        """Encode one bounded part of a one-shot iterable without concatenating rows."""
        global _LAST_PYTHON_ROWS_ROUTE
        if self._spool_complete:
            return b""
        assert self._iterable is not None
        if self._iterable_chunk_index >= len(self._iterable_chunk):
            try:
                self._iterable_chunk = [next(self._iterable)]
            except StopIteration:
                self._iterable = None
                self._spool_complete = True
                assert self._spool is not None
                self._spool.flush()
                return b""
            self._iterable_chunk_index = 0
        try:
            payload, next_index = self._native_batch(
                self._iterable_chunk,
                self._iterable_chunk_index,
                max(1, target_bytes),
            )
        except (RuntimeError, ValueError) as exc:
            raise RuntimeError("Native Python row JSONL encoding failed") from exc
        if next_index <= self._iterable_chunk_index:
            raise RuntimeError("Native Python row JSONL encoder did not make progress")
        self._iterable_chunk_index = next_index
        if self._iterable_chunk_index >= len(self._iterable_chunk):
            self._iterable_chunk.clear()
            self._iterable_chunk_index = 0
        _LAST_PYTHON_ROWS_ROUTE = "native_batch"
        return payload

    def _ensure_spool_disk_capacity(self, payload_bytes: int, next_size: int) -> None:
        """Reject disk-backed replay growth before exhausting temporary storage."""
        ensure_replay_spool_capacity(
            self._spool_dir,
            payload_bytes=payload_bytes,
            next_size=next_size,
            memory_bytes=self._spool_memory_bytes,
            minimum_free_bytes=self._MIN_FREE_DISK_BYTES,
        )

    def _spool_next_iterable_chunk(self, target_bytes: int) -> bytes:
        """Serialize one bounded iterable chunk and append it to the replay spool."""
        if self._spool_complete:
            return b""
        assert self._spool is not None
        payload = self._next_iterable_payload(target_bytes)
        if not payload:
            return b""
        next_size = self._spool_bytes + len(payload)
        if next_size > self._max_spool_bytes:
            raise RuntimeError(
                "max_replay_spool_bytes limit exceeded: "
                f"{next_size} bytes > {self._max_spool_bytes} bytes"
            )
        self._ensure_spool_disk_capacity(len(payload), next_size)
        written = self._spool.write(payload)
        if written != len(payload):
            raise OSError("Replay spool short write")
        self._spool_bytes = next_size
        return payload

    def _finish_spool(self, target_bytes: int | None = None) -> None:
        """Consume the remainder of a one-shot iterable into bounded replay storage."""
        if self._spool is None or self._spool_complete:
            return
        chunk_bytes = target_bytes or self._default_chunk_bytes
        while self._spool_next_iterable_chunk(chunk_bytes):
            pass

    def _append_native_rows(self, target_bytes: int) -> bool:
        """Append a native-encoded batch of sequence rows to the byte buffer."""
        if self._rows is None or self._index >= len(self._rows):
            return False
        try:
            payload, next_index = self._native_batch(
                self._rows,
                self._index,
                max(1, target_bytes),
            )
        except (RuntimeError, ValueError) as exc:
            raise RuntimeError("Native Python row JSONL encoding failed") from exc
        if next_index <= self._index:
            raise RuntimeError("Native Python row JSONL encoder did not make progress")
        self._buffer.extend(payload)
        self._index = next_index
        global _LAST_PYTHON_ROWS_ROUTE
        _LAST_PYTHON_ROWS_ROUTE = "native_batch"
        return True

    def _append_next(self, target_bytes: int) -> bool:
        """Append sequence bytes, stream a generator, or replay its spool."""
        if self._rows is not None:
            return self._append_native_rows(target_bytes)
        assert self._spool is not None
        if not self._replay_spool:
            payload = self._spool_next_iterable_chunk(target_bytes)
        else:
            payload = self._spool.read(max(1, target_bytes))
        if not payload:
            return False
        self._buffer.extend(payload)
        return True

    def _reset_reader(self) -> None:
        """Reset the row stream to the beginning."""
        self._index = 0
        if self._spool is not None:
            self._finish_spool()
            self._spool.seek(0)
            self._replay_spool = True

    def close(self) -> None:
        """Release generated bytes and any disk-backed replay spool."""
        if self._spool is not None:
            close_replay_spool(self._spool, self._spool_bytes)
            self._spool = None
        self._iterable = None
        self._iterable_chunk.clear()
        self._iterable_chunk_index = 0
        self._spool_bytes = 0
        super().close()


def last_sink_source_route() -> str:
    """Return the route used by the most recent plain native sink call."""
    return _LAST_SOURCE_ROUTE


def _record_sink_source_route(route: str) -> None:
    """Record the route used by a plain native sink call."""
    global _LAST_SOURCE_ROUTE
    _LAST_SOURCE_ROUTE = route


class ExecutionContext(
    _ExecutionSchemaProbeMethods,
    _ExecutionRegistryInputProbeMethods,
    _ExecutionRegistryPathSourceProbeMethods,
    _RegistryPathSourceSinkMethods,
    _RegistryPathProviderSinkMethods,
    _RegistryArrowSinkMethods,
):
    """Low-level ABI3 ingestion execution context."""

    def __init__(self) -> None:
        """Create a native execution context."""
        self._capsule = _native.context_new()

    def memory_stats(self) -> dict[str, Any]:
        """Return native context memory statistics."""
        return json_object_loads(_native.context_memory_stats_json(self._capsule))

    @staticmethod
    def _sink_output(sink: str, native_result: tuple[Any, Any]) -> SinkOutput:
        """Wrap a native sink result."""
        main, diagnostics = native_result
        return SinkOutput(sink=sink, main_stream_capsule=main, diagnostics_capsule=diagnostics)

    def _call_native_sink_from_source(
        self, sink: str, frontend: str, source: str, payload: Any, options: Any
    ) -> SinkOutput:
        """Prepare options and invoke the source-selected native sink."""
        _record_sink_source_route(source)
        return self._sink_output(
            sink,
            _native.context_to_sink_from_source(
                self._capsule,
                sink,
                frontend,
                source,
                payload,
                _options_capsule(options),
            ),
        )

    def _call_native_registry_sink_from_source(
        self,
        sink: str,
        frontend: str,
        source: str,
        payload: Any,
        options: Any,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any] | None = None,
        all_row_columns: dict[str, Any] | None = None,
        row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
        timestamp_columns: tuple[str, ...] = (),
    ) -> SinkOutput:
        """Prepare options and invoke a source-selected registry sink."""
        args = [
            self._capsule,
            sink,
            frontend,
            source,
            payload,
            _options_capsule(options),
            registry_json,
            field_name_policy,
            schema_mode,
        ]
        if (
            first_row_columns is not None
            or all_row_columns is not None
            or row_span_columns is not None
            or timestamp_columns
        ):
            args.extend(
                [
                    first_row_columns or {},
                    all_row_columns or {},
                    row_span_columns or {},
                    timestamp_columns,
                ]
            )
        native_result = _native.context_to_registry_sink_from_source(*args)
        return _registry_sink_output(sink, native_result)

    def to_registry_sink_text(
        self,
        sink: str,
        frontend: str,
        text: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
    ) -> SinkOutput:
        """Send text input to a native registry-backed sink."""
        return self.to_registry_sink_from_source(
            sink,
            frontend,
            "text",
            text,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )

    def to_registry_sink_path(
        self,
        sink: str,
        frontend: str,
        path: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
    ) -> SinkOutput:
        """Send path input to a native registry-backed sink."""
        return self.to_registry_sink_from_source(
            sink,
            frontend,
            "path",
            path,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )

    def to_registry_sink_reader(
        self,
        sink: str,
        frontend: str,
        reader: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
    ) -> SinkOutput:
        """Send a seekable reader to a native registry-backed sink."""
        return self.to_registry_sink_from_source(
            sink,
            frontend,
            "stream",
            reader,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )

    def to_registry_sink_from_source(
        self,
        sink: str,
        frontend: str,
        source: str,
        payload: Any,
        options: Any = None,
        *,
        registry_json: str,
        field_name_policy: str,
        schema_mode: str,
        first_row_columns: dict[str, Any] | None = None,
        all_row_columns: dict[str, Any] | None = None,
        row_span_columns: dict[str, list[tuple[int, str | None]]] | None = None,
        timestamp_columns: tuple[str, ...] = (),
    ) -> SinkOutput:
        """Send source-selected input to a native registry-backed sink."""
        return self._call_native_registry_sink_from_source(
            sink,
            frontend,
            source,
            payload,
            options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            all_row_columns=all_row_columns,
            row_span_columns=row_span_columns,
            timestamp_columns=timestamp_columns,
        )

    def to_sink_text(self, sink: str, frontend: str, text: Any, options: Any = None) -> SinkOutput:
        """Send text input to a native sink."""
        return self.to_sink_from_source(sink, frontend, "text", text, options)

    def to_sink_path(self, sink: str, frontend: str, path: Any, options: Any = None) -> SinkOutput:
        """Send path input to a native sink."""
        return self.to_sink_from_source(sink, frontend, "path", path, options)

    def to_sink_reader(
        self, sink: str, frontend: str, reader: Any, options: Any = None
    ) -> SinkOutput:
        """Send a seekable byte reader to a native sink."""
        return self.to_sink_from_source(sink, frontend, "stream", reader, options)

    def to_sink_from_source(
        self, sink: str, frontend: str, source: str, payload: Any, options: Any = None
    ) -> SinkOutput:
        """Send source-selected input to a native sink."""
        return self._call_native_sink_from_source(sink, frontend, source, payload, options)

    def to_sink_python(self, sink: str, data: Any, options: Any = None) -> SinkOutput:
        """Serialize Python rows and send them to a native JSON sink."""
        memory_limit = getattr(options, "memory_limit_bytes", None) if options is not None else None
        reader = PythonRowsJsonlByteReader(data, memory_limit_bytes=memory_limit)
        return self.to_sink_reader(sink, "json", reader, options)

    def to_sink_path_sources(
        self,
        sink: str,
        sources: Any,
        options: Any = None,
        *,
        include_source_file: bool,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
    ) -> SinkOutput:
        """Send multiple local path sources to a native sink."""
        _record_sink_source_route("path_sources")
        return self._sink_output(
            sink,
            _native.context_to_sink_from_path_sources(
                self._capsule,
                sink,
                sources,
                _options_capsule(options),
                include_source_file,
                first_row_columns,
                timestamp_columns,
            ),
        )

    def to_sink_path_source_chunk_provider(
        self,
        sink: str,
        provider: Any,
        options: Any = None,
        *,
        include_source_file: bool,
        first_row_columns: dict[str, Any],
        timestamp_columns: tuple[str, ...],
    ) -> SinkOutput:
        """Send lazily provided path-source chunks to a native sink."""
        _record_sink_source_route("path_source_chunk_provider")
        return self._sink_output(
            sink,
            _native.context_to_sink_from_path_source_chunk_provider(
                self._capsule,
                sink,
                provider,
                _options_capsule(options),
                include_source_file,
                first_row_columns,
                timestamp_columns,
            ),
        )

    def to_sink_arrow_stream(
        self, sink: str, frontend: str, stream: Any, options: Any = None
    ) -> SinkOutput:
        """Send an Arrow C stream to a native sink."""
        _record_sink_source_route("arrow")
        return self._sink_output(
            sink,
            _native.context_to_sink_arrow_stream(
                self._capsule,
                sink,
                frontend,
                stream,
                _options_capsule(options),
            ),
        )


def default_execution_context() -> ExecutionContext:
    """Return the shared low-level execution context, creating it when needed."""
    global _DEFAULT_CONTEXT
    if _DEFAULT_CONTEXT is None:
        _DEFAULT_CONTEXT = ExecutionContext()
    return _DEFAULT_CONTEXT


def reset_default_execution_context() -> None:
    """Discard the shared low-level execution context."""
    global _DEFAULT_CONTEXT
    _DEFAULT_CONTEXT = None
