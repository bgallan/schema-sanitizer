"""Validate and prepare public file/directory ingestion inputs."""

from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..core_impl.native_functions import XML_FOLDER_EFFECTIVE_ROW_TAG
from ..core_impl.transcoding_reader import TranscodingPathByteReader
from .async_remote_io import (
    RemoteFile,
    list_remote_directory_files,
    local_path_from_file_uri,
    looks_like_file_uri,
    looks_like_remote_uri,
    remote_directory_stage_chunk_size,
    stage_remote_files_to_directory,
    stage_remote_parquet_directory,
    stage_remote_single_file,
)
from .async_remote_scheduler import read_int_env
from .folder_listing import FolderFile, check_document_size, folder_files
from .ingest_runtime_selectors import _normalize_input_text_encoding
from .native_directory_errors import unsupported_native_directory_ingestion
from .native_folder_common import is_utf8_encoding, native_text_encoding_supported
from .native_ingest_plan import source_for_file_input
from .parquet_folder_reader import ParquetDirectoryInput
from .parquet_multisource import ParquetDirectorySourceFile, ParquetDirectorySourceManifest
from .source_batch import PreparedSourceBatch, SourceDescriptor, source_kind_for_format

PUBLIC_INPUT_FORMATS = frozenset({"csv", "json", "json_array", "jsonl", "ndjson", "xml", "parquet"})
PUBLIC_INPUT_MODES = frozenset({"single_file", "directory"})
FORMAT_SUFFIXES: dict[str, tuple[str, ...]] = {
    "csv": (".csv",),
    "json": (".json",),
    "json_array": (".json",),
    "jsonl": (".jsonl",),
    "ndjson": (".ndjson",),
    "xml": (".xml",),
    "parquet": (".parquet", ".pq"),
}
_DISCOVERED_DIRECTORY_INPUTS: ContextVar[Mapping[str, "DiscoveredDirectoryInput"] | None] = (
    ContextVar("schema_sanitizer_discovered_directory_inputs", default=None)
)


@dataclass(slots=True)
class PreparedPublicInput:
    """Resolved public input payload and native selectors."""

    data: Any
    format: str
    source: str
    keepalive: Any = None
    xml_row_tag: str | None = None
    source_file: str | None = None
    source_file_spans: Any = None

    def close(self) -> None:
        """Close any generated reader."""
        close = getattr(self.keepalive, "close", None)
        if callable(close):
            close()
        self.keepalive = None


@dataclass(frozen=True, slots=True)
class DiscoveredDirectoryInput:
    """Directory child files already found by pipeline source discovery."""

    input_format: str
    local_files: tuple[FolderFile, ...] = ()
    remote_files: tuple[RemoteFile, ...] = ()


@dataclass(frozen=True, slots=True)
class NativeDirectorySourceFile:
    """One local child source for native multi-source directory conversion."""

    path: str
    source_file: str


@dataclass(frozen=True, slots=True)
class NativeDirectorySourceManifest:
    """Local directory sources with shared public input options."""

    files: list[NativeDirectorySourceFile]
    input_format: str
    csv_delimiter: str = ","
    csv_has_header: bool = True
    xml_row_tag: str | None = None
    memory_limit_bytes: int | None = None
    source_batch: PreparedSourceBatch | None = None


class StagedNativeDirectoryManifest:
    """Own one locally staged chunk of a remote native directory manifest."""

    def __init__(self, manifest: NativeDirectorySourceManifest, keepalive: Any):
        """Store the native manifest and its staged temporary files."""
        self.manifest = manifest
        self.keepalive = keepalive

    def close(self) -> None:
        """Remove the staged files for this chunk."""
        close = getattr(self.keepalive, "close", None)
        if callable(close):
            close()


@dataclass(slots=True)
class RemoteNativeDirectorySourceManifest:
    """Remote directory sources staged lazily into bounded native path-source chunks."""

    files: list[RemoteFile]
    input_format: str
    input_text_encoding: str = "utf-8"
    csv_delimiter: str = ","
    csv_has_header: bool = True
    xml_row_tag: str | None = None
    memory_limit_bytes: int | None = None
    chunk_size: int = 64

    def _chunk(self, start: int) -> list[RemoteFile]:
        """Return one bounded remote file chunk."""
        return self.files[start : start + max(1, self.chunk_size)]

    def stage_chunk(self, start: int) -> StagedNativeDirectoryManifest | None:
        """Stage one remote chunk and return its local native manifest."""
        chunk = self._chunk(start)
        if not chunk:
            return None
        staged = stage_remote_files_to_directory(
            chunk,
            memory_limit_bytes=self.memory_limit_bytes,
        )
        try:
            local_files = _directory_files_for_format(staged.path, self.input_format)
            effective_xml_row_tag = self.xml_row_tag
            if self.input_format == "xml":
                effective_xml_row_tag = _detect_native_xml_row_tag(
                    local_files,
                    input_text_encoding=self.input_text_encoding,
                    xml_row_tag=self.xml_row_tag,
                    memory_limit_bytes=self.memory_limit_bytes,
                )
                if self.xml_row_tag is None:
                    self.xml_row_tag = effective_xml_row_tag
            manifest = _native_directory_manifest_or_none(
                local_files,
                input_format=self.input_format,
                input_text_encoding=self.input_text_encoding,
                csv_delimiter=self.csv_delimiter,
                csv_has_header=self.csv_has_header,
                xml_row_tag=effective_xml_row_tag,
                memory_limit_bytes=self.memory_limit_bytes,
                source_file_by_name=staged.source_file_by_name,
            )
            if manifest is None:
                raise unsupported_native_directory_ingestion()
            return StagedNativeDirectoryManifest(manifest, staged)
        except Exception:
            staged.close()
            raise

    def close(self) -> None:
        """Satisfy PreparedPublicInput keepalive cleanup."""


def remote_chunk_prefetch_count() -> int:
    """Return how many remote directory chunks to stage ahead."""
    return read_int_env("SCHEMA_SANITIZER_REMOTE_CHUNK_PREFETCH_CHUNKS", 1)


class RemoteChunkPrefetchIterator:
    """Iterate staged remote chunks while prefetching bounded lookahead."""

    def __init__(
        self,
        manifest: RemoteNativeDirectorySourceManifest,
        *,
        prefetch_chunks: int | None = None,
    ):
        """Store manifest and initialize bounded prefetch state."""
        self._manifest = manifest
        self._chunk_size = max(1, manifest.chunk_size)
        self._starts = list(range(0, len(manifest.files), self._chunk_size))
        self._next_start_index = 0
        self._prefetch_chunks = (
            remote_chunk_prefetch_count() if prefetch_chunks is None else prefetch_chunks
        )
        if manifest.input_format == "xml":
            self._prefetch_chunks = min(self._prefetch_chunks, 1)
        self._executor: ThreadPoolExecutor | None = None
        self._futures: list[Future[StagedNativeDirectoryManifest | None]] = []
        self._closed = False
        if self._prefetch_chunks > 0 and self._starts:
            self._executor = ThreadPoolExecutor(
                max_workers=max(1, self._prefetch_chunks),
                thread_name_prefix="schema-sanitizer-remote-stage",
            )
            self._fill_prefetch_window()

    def __enter__(self) -> RemoteChunkPrefetchIterator:
        """Return this iterator for context-manager use."""
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Close pending staged chunks on context exit."""
        self.close()

    def __iter__(self) -> RemoteChunkPrefetchIterator:
        """Return this iterator."""
        return self

    def __next__(self) -> StagedNativeDirectoryManifest:
        """Return the next staged chunk, skipping empty chunks."""
        if self._closed:
            raise StopIteration
        while True:
            if self._executor is None:
                if self._next_start_index >= len(self._starts):
                    raise StopIteration
                start = self._starts[self._next_start_index]
                self._next_start_index += 1
                staged = self._manifest.stage_chunk(start)
            else:
                if not self._futures:
                    raise StopIteration
                future = self._futures.pop(0)
                staged = future.result()
                self._fill_prefetch_window()
            if staged is not None:
                return staged

    def _fill_prefetch_window(self) -> None:
        """Schedule staged chunk downloads up to the configured lookahead."""
        if self._executor is None:
            return
        while self._next_start_index < len(self._starts) and len(self._futures) < max(
            1, self._prefetch_chunks
        ):
            start = self._starts[self._next_start_index]
            self._next_start_index += 1
            self._futures.append(self._executor.submit(self._manifest.stage_chunk, start))

    def close(self) -> None:
        """Close prefetched chunks that were not consumed."""
        if self._closed:
            return
        self._closed = True
        pending = self._futures
        self._futures = []
        for future in pending:
            if future.cancel():
                continue
            with contextlib.suppress(Exception):
                staged = future.result()
                if staged is not None:
                    staged.close()
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None


def iter_staged_remote_chunks(
    manifest: RemoteNativeDirectorySourceManifest,
    *,
    prefetch_chunks: int | None = None,
) -> RemoteChunkPrefetchIterator:
    """Return a bounded prefetch iterator over staged remote chunks."""
    return RemoteChunkPrefetchIterator(manifest, prefetch_chunks=prefetch_chunks)


class _ChainedKeepalive:
    """Close multiple keepalive resources in order."""

    def __init__(self, *items: Any):
        """Store resources that may expose close()."""
        self._items = list(items)

    def close(self) -> None:
        """Close every retained resource."""
        while self._items:
            item = self._items.pop()
            close = getattr(item, "close", None)
            if callable(close):
                close()


class _NativeDirectoryManifestCarrier:
    """Minimal object used only to carry native directory manifest metadata."""

    def close(self) -> None:
        """Satisfy PreparedPublicInput keepalive cleanup."""


@contextlib.contextmanager
def discovered_directory_inputs(
    inputs: Mapping[str, DiscoveredDirectoryInput],
) -> Iterator[None]:
    """Temporarily provide pre-discovered directory files to public input prep."""
    token = _DISCOVERED_DIRECTORY_INPUTS.set(inputs)
    try:
        yield
    finally:
        _DISCOVERED_DIRECTORY_INPUTS.reset(token)


def _attach_remote_native_directory_manifest(
    reader: Any,
    manifest: RemoteNativeDirectorySourceManifest,
) -> None:
    """Attach lazy remote directory conversion metadata to a carrier."""
    setattr(reader, "remote_native_multisource_manifest", manifest)


def _validate_remote_native_directory_supported(
    input_format: str,
    *,
    input_text_encoding: str,
    csv_delimiter: str,
) -> None:
    """Reject remote directory settings that cannot use native chunked staging."""
    if input_format == "parquet":
        raise ValueError("Parquet remote directories use the Parquet Arrow-source path")

    can_stage_native_directory = native_text_encoding_supported(input_text_encoding) and not (
        input_format == "csv" and len(csv_delimiter.encode("utf-8")) != 1
    )
    if not can_stage_native_directory:
        raise unsupported_native_directory_ingestion()


def _remote_native_directory_prepared_from_files(
    files: list[RemoteFile],
    input_format: str,
    *,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
) -> PreparedPublicInput:
    """Prepare one remote directory as a lazy bounded native source-plan manifest."""
    _validate_remote_native_directory_supported(
        input_format,
        input_text_encoding=input_text_encoding,
        csv_delimiter=csv_delimiter,
    )

    remote_manifest = RemoteNativeDirectorySourceManifest(
        files,
        input_format=input_format,
        input_text_encoding=input_text_encoding,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        xml_row_tag=xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
        chunk_size=remote_directory_stage_chunk_size(),
    )
    if input_format == "xml" and xml_row_tag is None:
        staged_probe = remote_manifest.stage_chunk(0)
        if staged_probe is not None:
            staged_probe.close()

    carrier = _NativeDirectoryManifestCarrier()
    _attach_remote_native_directory_manifest(carrier, remote_manifest)
    prepared_format = "json" if input_format in {"json", "jsonl", "ndjson"} else input_format
    return PreparedPublicInput(
        carrier,
        prepared_format,
        "stream",
        carrier,
        xml_row_tag=remote_manifest.xml_row_tag,
    )


def normalize_public_input_format(input_format: str | None) -> str:
    """Normalize an explicit public input format."""
    if input_format is None:
        raise ValueError(
            "input_format is required; select csv, json, json_array, jsonl, ndjson, xml, or parquet"
        )
    if not isinstance(input_format, str):
        raise TypeError("input_format must be a string")
    value = input_format.strip().lower()
    if value == "auto":
        raise ValueError("input_format='auto' is not supported; select an explicit format")
    if value not in PUBLIC_INPUT_FORMATS:
        accepted = ", ".join(sorted(PUBLIC_INPUT_FORMATS))
        raise ValueError(f"input_format must be one of: {accepted}")
    return value


def normalize_public_input_mode(input_mode: str) -> str:
    """Normalize a public path mode."""
    if not isinstance(input_mode, str):
        raise TypeError("input_mode must be a string")
    value = input_mode.strip().lower()
    if value not in PUBLIC_INPUT_MODES:
        raise ValueError("input_mode must be 'single_file' or 'directory'")
    return value


def _path_suffix(value: str | os.PathLike[str]) -> str:
    """Return a local or URI path suffix."""
    raw = os.fspath(value)
    if looks_like_file_uri(raw):
        path = local_path_from_file_uri(raw)
    elif looks_like_remote_uri(raw):
        path = urlparse(raw).path
    else:
        path = raw
    return Path(path).suffix.lower()


def _validate_suffix(path: str | os.PathLike[str], input_format: str) -> None:
    """Require the exact extension associated with an input format."""
    suffix = _path_suffix(path)
    accepted = FORMAT_SUFFIXES[input_format]
    if suffix not in accepted:
        expected = " or ".join(accepted)
        raise ValueError(
            f"input_format={input_format!r} requires extension {expected}; got {suffix!r}"
        )


def _validate_local_path_mode(path: str | os.PathLike[str], input_mode: str) -> None:
    """Require a local file or directory matching input_mode."""
    local = Path(os.fspath(path))
    if input_mode == "single_file":
        if not local.is_file():
            raise FileNotFoundError(f"single_file input requires a file: {local}")
    elif not local.is_dir():
        raise NotADirectoryError(f"directory input requires a directory: {local}")


def _single_file_descriptor(path: str | os.PathLike[str]) -> FolderFile:
    """Return a reopenable descriptor for one local or URI file."""
    raw = os.fspath(path)
    if looks_like_file_uri(raw):
        raw = local_path_from_file_uri(raw)
    local = Path(raw)
    return FolderFile(
        display_name=str(local),
        name=local.name,
        size=local.stat().st_size,
        open_binary=lambda: local.open("rb"),
        native_path=str(local),
    )


def _display_source_file(path: str | os.PathLike[str]) -> str:
    """Return the full displayed source path for generated ETL metadata."""
    raw = os.fspath(path)
    if looks_like_remote_uri(raw) or looks_like_file_uri(raw):
        return raw
    return str(Path(raw).resolve())


def _folder_file_source(
    file: FolderFile,
    source_file_by_name: dict[str, str] | None = None,
) -> str:
    """Return the full displayed path for one folder child."""
    if source_file_by_name is not None:
        source_file = source_file_by_name.get(file.name)
        if source_file is not None:
            return source_file
    raw = file.native_path or file.display_name
    if looks_like_remote_uri(raw):
        return raw
    return str(Path(raw).resolve())


def _native_directory_manifest_or_none(
    files: list[FolderFile],
    *,
    input_format: str,
    input_text_encoding: str,
    csv_delimiter: str,
    csv_has_header: bool,
    xml_row_tag: str | None,
    memory_limit_bytes: int | None,
    source_file_by_name: dict[str, str] | None = None,
) -> NativeDirectorySourceManifest | None:
    """Return a native multi-source manifest for local directory inputs."""
    if not native_text_encoding_supported(input_text_encoding):
        return None
    if input_format == "csv" and len(csv_delimiter.encode("utf-8")) != 1:
        return None
    source_kind = source_kind_for_format(input_format)
    if source_kind is None:
        return None
    files_out: list[NativeDirectorySourceFile] = []
    source_descriptors: list[SourceDescriptor] = []
    for file in files:
        if file.native_path is None:
            return None
        source_file = _folder_file_source(file, source_file_by_name)
        files_out.append(
            NativeDirectorySourceFile(
                path=file.native_path,
                source_file=source_file,
            )
        )
        source_descriptors.append(
            SourceDescriptor(
                kind=source_kind,
                path=file.native_path,
                source_file=source_file,
            )
        )
    source_batch = PreparedSourceBatch(
        sources=tuple(source_descriptors),
        input_format=input_format,
        input_mode="directory",
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        xml_row_tag=xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
    )
    return NativeDirectorySourceManifest(
        files_out,
        input_format=input_format,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        xml_row_tag=xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
        source_batch=source_batch,
    )


def _attach_native_directory_manifest(
    reader: Any,
    files: list[FolderFile],
    *,
    input_format: str,
    input_text_encoding: str,
    csv_delimiter: str,
    csv_has_header: bool,
    xml_row_tag: str | None,
    memory_limit_bytes: int | None,
    source_file_by_name: dict[str, str] | None = None,
) -> NativeDirectorySourceManifest | None:
    """Attach native directory conversion metadata to a reader when possible."""
    manifest = _native_directory_manifest_or_none(
        files,
        input_format=input_format,
        input_text_encoding=input_text_encoding,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        xml_row_tag=xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
        source_file_by_name=source_file_by_name,
    )
    if manifest is not None:
        setattr(reader, "native_multisource_manifest", manifest)
    return manifest


def _directory_files_for_format(
    path: str | os.PathLike[str],
    input_format: str,
) -> list[FolderFile]:
    """Return deterministic direct child files for one public directory format."""
    suffix = FORMAT_SUFFIXES[input_format][0]
    return folder_files(path, suffix=suffix, reader_name=f"{input_format} directory input")


def _detect_native_xml_row_tag(
    files: list[FolderFile],
    *,
    input_text_encoding: str,
    xml_row_tag: str | None,
    memory_limit_bytes: int | None,
) -> str:
    """Validate XML child roots and return the effective row tag."""
    from .native_folder_common import all_local_files, is_utf8_encoding, memory_limit_arg

    requested = xml_row_tag or ""
    native_effective = XML_FOLDER_EFFECTIVE_ROW_TAG.get()
    if (
        native_effective is None
        or not is_utf8_encoding(input_text_encoding)
        or not all_local_files(files)
    ):
        raise unsupported_native_directory_ingestion(
            "XML directory row-tag detection requires the native XML folder helper."
        )

    for file in files:
        check_document_size(
            file.display_name,
            file.size,
            memory_limit_bytes=memory_limit_bytes,
            stage="xml_parse",
        )
    return native_effective(
        [file.native_path or file.display_name for file in files],
        requested,
        memory_limit_arg(memory_limit_bytes),
    )


def _native_directory_prepared_or_none(
    path: str | os.PathLike[str],
    input_format: str,
    *,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
    source_file_by_name: dict[str, str] | None = None,
) -> PreparedPublicInput | None:
    """Prepare native-compatible directories without constructing fallback readers."""
    if input_format == "parquet":
        return None
    files = _directory_files_for_format(path, input_format)
    return _native_directory_prepared_from_files_or_none(
        files,
        input_format,
        input_text_encoding=input_text_encoding,
        xml_row_tag=xml_row_tag,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        memory_limit_bytes=memory_limit_bytes,
        source_file_by_name=source_file_by_name,
    )


def _native_directory_prepared_from_files_or_none(
    files: list[FolderFile],
    input_format: str,
    *,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
    source_file_by_name: dict[str, str] | None = None,
) -> PreparedPublicInput | None:
    """Prepare native-compatible directory files without relisting the directory."""
    if input_format == "parquet":
        return None
    if not native_text_encoding_supported(input_text_encoding):
        return None
    if input_format == "csv" and len(csv_delimiter.encode("utf-8")) != 1:
        return None
    effective_xml_row_tag = None
    if input_format == "xml":
        effective_xml_row_tag = xml_row_tag
        if not effective_xml_row_tag:
            effective_xml_row_tag = _detect_native_xml_row_tag(
                files,
                input_text_encoding=input_text_encoding,
                xml_row_tag=xml_row_tag,
                memory_limit_bytes=memory_limit_bytes,
            )

    carrier = _NativeDirectoryManifestCarrier()
    manifest = _attach_native_directory_manifest(
        carrier,
        files,
        input_format=input_format,
        input_text_encoding=input_text_encoding,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        xml_row_tag=effective_xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
        source_file_by_name=source_file_by_name,
    )
    if manifest is None:
        return None
    prepared_format = "json" if input_format in {"json", "jsonl", "ndjson"} else input_format
    return PreparedPublicInput(
        carrier,
        prepared_format,
        "stream",
        carrier,
        xml_row_tag=effective_xml_row_tag,
    )


def _prepare_directory_from_files(
    files: list[FolderFile],
    input_format: str,
    *,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
    source_file_by_name: dict[str, str] | None = None,
) -> PreparedPublicInput:
    """Prepare one already-listed local directory source."""
    native_prepared = _native_directory_prepared_from_files_or_none(
        files,
        input_format,
        input_text_encoding=input_text_encoding,
        xml_row_tag=xml_row_tag,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        memory_limit_bytes=memory_limit_bytes,
        source_file_by_name=source_file_by_name,
    )
    if native_prepared is not None:
        return native_prepared

    if input_format != "parquet":
        raise unsupported_native_directory_ingestion()
    carrier = _NativeDirectoryManifestCarrier()
    setattr(
        carrier,
        "native_parquet_multisource_manifest",
        ParquetDirectorySourceManifest(
            [
                ParquetDirectorySourceFile(
                    path=file.native_path or file.display_name,
                    source_file=_folder_file_source(file, source_file_by_name),
                )
                for file in files
            ],
            memory_limit_bytes=memory_limit_bytes,
        ),
    )
    return PreparedPublicInput(carrier, "parquet", "stream", carrier)


def _prepare_directory(
    path: str | os.PathLike[str],
    input_format: str,
    *,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
    source_file_by_name: dict[str, str] | None = None,
) -> PreparedPublicInput:
    """Prepare one deterministic, non-recursive directory source."""
    native_prepared = _native_directory_prepared_or_none(
        path,
        input_format,
        input_text_encoding=input_text_encoding,
        xml_row_tag=xml_row_tag,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        memory_limit_bytes=memory_limit_bytes,
        source_file_by_name=source_file_by_name,
    )
    if native_prepared is not None:
        return native_prepared

    if input_format != "parquet":
        raise unsupported_native_directory_ingestion()
    parquet_input = ParquetDirectoryInput(path, memory_limit_bytes=memory_limit_bytes)
    setattr(
        parquet_input,
        "native_parquet_multisource_manifest",
        ParquetDirectorySourceManifest(
            [
                ParquetDirectorySourceFile(
                    path=file.native_path or file.display_name,
                    source_file=_folder_file_source(file, source_file_by_name),
                )
                for file in parquet_input.files
            ],
            memory_limit_bytes=memory_limit_bytes,
        ),
    )
    return PreparedPublicInput(
        parquet_input,
        "parquet",
        "stream",
        parquet_input,
    )


def _discovered_directory_input_for(
    path: str | os.PathLike[str],
    input_format: str,
) -> DiscoveredDirectoryInput | None:
    """Return a matching pre-discovered directory input for this path and format."""
    inputs = _DISCOVERED_DIRECTORY_INPUTS.get()
    if not inputs:
        return None
    raw = os.fspath(path)
    discovered = inputs.get(raw)
    if discovered is None and isinstance(path, str) and looks_like_file_uri(path):
        discovered = inputs.get(local_path_from_file_uri(path))
    if discovered is None:
        return None
    if normalize_public_input_format(discovered.input_format) != input_format:
        return None
    return discovered


def prepare_public_input(
    path: str | os.PathLike[str],
    *,
    input_format: str | None,
    input_mode: str,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
) -> PreparedPublicInput:
    """Validate a public input target and prepare its native payload."""
    if not isinstance(path, (str, os.PathLike)):
        raise TypeError("input_path must be a local path or supported remote URI")
    mode = normalize_public_input_mode(input_mode)
    fmt = normalize_public_input_format(input_format)
    original_source_file = _display_source_file(path)
    discovered_directory_input = (
        _discovered_directory_input_for(path, fmt) if mode == "directory" else None
    )
    keepalive: Any = None
    source_file_by_name: dict[str, str] | None = None
    suffix_validated = False
    if discovered_directory_input is not None:
        if discovered_directory_input.remote_files:
            files = list(discovered_directory_input.remote_files)
            if fmt == "parquet":
                staged = stage_remote_files_to_directory(
                    files,
                    memory_limit_bytes=memory_limit_bytes,
                )
                try:
                    prepared = _prepare_directory(
                        staged.path,
                        fmt,
                        input_text_encoding=input_text_encoding,
                        xml_row_tag=xml_row_tag,
                        csv_delimiter=csv_delimiter,
                        csv_has_header=csv_has_header,
                        memory_limit_bytes=memory_limit_bytes,
                        source_file_by_name=staged.source_file_by_name,
                    )
                    prepared.keepalive = _ChainedKeepalive(prepared.keepalive, staged)
                    if prepared.source_file is None and prepared.source_file_spans is None:
                        prepared.source_file = original_source_file
                    return prepared
                except Exception:
                    staged.close()
                    raise
            return _remote_native_directory_prepared_from_files(
                files,
                fmt,
                input_text_encoding=input_text_encoding,
                xml_row_tag=xml_row_tag,
                csv_delimiter=csv_delimiter,
                csv_has_header=csv_has_header,
                memory_limit_bytes=memory_limit_bytes,
            )
        if discovered_directory_input.local_files:
            prepared = _prepare_directory_from_files(
                list(discovered_directory_input.local_files),
                fmt,
                input_text_encoding=input_text_encoding,
                xml_row_tag=xml_row_tag,
                csv_delimiter=csv_delimiter,
                csv_has_header=csv_has_header,
                memory_limit_bytes=memory_limit_bytes,
            )
            if prepared.source_file is None and prepared.source_file_spans is None:
                prepared.source_file = original_source_file
            return prepared
    if isinstance(path, str) and looks_like_file_uri(path):
        path = local_path_from_file_uri(path)
    elif isinstance(path, str) and looks_like_remote_uri(path):
        if mode == "single_file":
            _validate_suffix(path, fmt)
            suffix_validated = True
            staged = stage_remote_single_file(
                path,
                memory_limit_bytes=memory_limit_bytes,
            )
            keepalive = staged
            path = staged.path
        elif fmt == "parquet":
            staged = stage_remote_parquet_directory(
                path,
                suffixes=FORMAT_SUFFIXES["parquet"],
                memory_limit_bytes=memory_limit_bytes,
            )
            keepalive = staged
            source_file_by_name = staged.source_file_by_name
            path = staged.path
        else:
            _validate_remote_native_directory_supported(
                fmt,
                input_text_encoding=input_text_encoding,
                csv_delimiter=csv_delimiter,
            )
            files = list_remote_directory_files(path, FORMAT_SUFFIXES[fmt])
            if not files:
                expected = " or ".join(FORMAT_SUFFIXES[fmt])
                raise ValueError(f"remote directory input found no {expected} files in: {path}")
            return _remote_native_directory_prepared_from_files(
                files,
                fmt,
                input_text_encoding=input_text_encoding,
                xml_row_tag=xml_row_tag,
                csv_delimiter=csv_delimiter,
                csv_has_header=csv_has_header,
                memory_limit_bytes=memory_limit_bytes,
            )
    _validate_local_path_mode(path, mode)
    if mode == "directory":
        prepared = _prepare_directory(
            path,
            fmt,
            input_text_encoding=input_text_encoding,
            xml_row_tag=xml_row_tag,
            csv_delimiter=csv_delimiter,
            csv_has_header=csv_has_header,
            memory_limit_bytes=memory_limit_bytes,
            source_file_by_name=source_file_by_name,
        )
        if keepalive is not None:
            prepared.keepalive = _ChainedKeepalive(prepared.keepalive, keepalive)
        if prepared.source_file is None and prepared.source_file_spans is None:
            prepared.source_file = original_source_file
        return prepared

    if not suffix_validated:
        _validate_suffix(path, fmt)
    input_text_encoding = _normalize_input_text_encoding(input_text_encoding)
    is_utf8_input = is_utf8_encoding(str(input_text_encoding))
    if (
        not is_utf8_input
        and fmt != "parquet"
        and not native_text_encoding_supported(str(input_text_encoding))
    ):
        reader = TranscodingPathByteReader(path, encoding=str(input_text_encoding))
        reader_keepalive = reader
        if keepalive is not None:
            reader_keepalive = _ChainedKeepalive(reader, keepalive)
        return PreparedPublicInput(
            reader,
            "json" if fmt in {"jsonl", "ndjson"} else fmt,
            "stream",
            reader_keepalive,
            source_file=original_source_file,
        )
    if fmt == "json_array":
        return PreparedPublicInput(
            os.fspath(path),
            "json_array",
            source_for_file_input(path),
            keepalive=keepalive,
            source_file=original_source_file,
        )
    native_format = "json" if fmt in {"jsonl", "ndjson"} else fmt
    return PreparedPublicInput(
        os.fspath(path),
        native_format,
        source_for_file_input(path),
        keepalive=keepalive,
        source_file=original_source_file,
    )
