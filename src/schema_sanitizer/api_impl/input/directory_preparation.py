"""Local and remote public directory-input preparation."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING, Any

from ...core_impl.native_options import optional_memory_limit_arg
from ...core_impl.native_symbols import XML_FOLDER_EFFECTIVE_ROW_TAG
from ...core_impl.resource_lifecycle import _close_sequence_with_error
from ...core_impl.safe_errors import add_bounded_note
from ...input_impl.directory_inputs import FolderFile, check_document_size, folder_files
from ...input_impl.prepared import (
    ChainedKeepalive,
    NativeDirectoryManifestCarrier,
    NativeDirectorySourceManifest,
    PreparedPublicInput,
    StagedNativeDirectoryManifest,
)
from ...input_impl.selection import (
    FORMAT_SUFFIXES,
    folder_file_source,
    is_utf8_encoding,
    native_input_format,
    native_text_encoding_supported,
    unsupported_native_directory_ingestion,
)
from ...input_impl.source_plan import (
    PreparedSourceBatch,
    SourceDescriptor,
    source_kind_for_format,
)
from ...remote_impl import directory_downloads as remote_downloads
from ...remote_impl import staging as remote_staging
from ...remote_impl.packetization import (
    remote_file_packet,
    remote_file_packet_estimated_bytes,
)
from ...sources.models import RemoteFile

if TYPE_CHECKING:
    from ..operation_context import OperationExecutionContext
from ..parquet.multisource import (
    ParquetDirectorySourceFile,
    ParquetDirectorySourceManifest,
)


def _all_files_have_native_paths(files: list[FolderFile]) -> bool:
    """Return whether every listed file exposes a native local path."""
    return all(file.native_path is not None for file in files)


def _native_directory_supported(
    input_format: str,
    *,
    input_text_encoding: str,
    csv_delimiter: str,
) -> bool:
    """Return whether one directory can use the native path-source frontend."""
    return native_text_encoding_supported(input_text_encoding) and not (
        input_format == "csv" and len(csv_delimiter.encode("utf-8")) != 1
    )


def _directory_files_for_format(
    path: str | os.PathLike[str],
    input_format: str,
    *,
    memory_limit_bytes: int | None,
) -> list[FolderFile]:
    """Return deterministic direct children accepted for one public format."""
    return folder_files(
        path,
        suffix=FORMAT_SUFFIXES[input_format],
        reader_name=f"{input_format} directory input",
        memory_limit_bytes=memory_limit_bytes,
    )


def _detect_native_xml_row_tag(
    files: list[FolderFile],
    *,
    input_text_encoding: str,
    xml_row_tag: str | None,
    memory_limit_bytes: int | None,
) -> str:
    """Validate XML child roots and return the effective row tag."""
    requested = xml_row_tag or ""
    if not is_utf8_encoding(input_text_encoding) or not _all_files_have_native_paths(files):
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
    return XML_FOLDER_EFFECTIVE_ROW_TAG(
        [file.native_path or file.display_name for file in files],
        requested,
        optional_memory_limit_arg(memory_limit_bytes),
    )


def _native_directory_manifest(
    files: list[FolderFile],
    *,
    input_format: str,
    csv_delimiter: str,
    csv_has_header: bool,
    xml_row_tag: str | None,
    memory_limit_bytes: int | None,
    source_file_by_name: dict[str, str] | None = None,
) -> NativeDirectorySourceManifest | None:
    """Build a native manifest after format and text settings were validated."""
    source_kind = source_kind_for_format(input_format)
    if source_kind is None:
        return None
    sources: list[SourceDescriptor] = []
    for file in files:
        if file.native_path is None:
            return None
        sources.append(
            SourceDescriptor(
                kind=source_kind,
                path=file.native_path,
                source_file=folder_file_source(file, source_file_by_name),
            )
        )
    return NativeDirectorySourceManifest(
        PreparedSourceBatch(
            sources=tuple(sources),
            input_format=input_format,
            input_mode="directory",
            csv_delimiter=csv_delimiter,
            csv_has_header=csv_has_header,
            xml_row_tag=xml_row_tag,
            memory_limit_bytes=memory_limit_bytes,
        )
    )


def _effective_native_xml_row_tag(
    files: list[FolderFile],
    *,
    input_format: str,
    input_text_encoding: str,
    xml_row_tag: str | None,
    memory_limit_bytes: int | None,
) -> str | None:
    """Resolve the row tag only when the native XML frontend needs detection."""
    if input_format != "xml" or xml_row_tag:
        return xml_row_tag
    return _detect_native_xml_row_tag(
        files,
        input_text_encoding=input_text_encoding,
        xml_row_tag=xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
    )


def native_directory_prepared_from_files_or_none(
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
    if input_format == "parquet" or not _native_directory_supported(
        input_format,
        input_text_encoding=input_text_encoding,
        csv_delimiter=csv_delimiter,
    ):
        return None
    effective_xml_row_tag = _effective_native_xml_row_tag(
        files,
        input_format=input_format,
        input_text_encoding=input_text_encoding,
        xml_row_tag=xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
    )
    manifest = _native_directory_manifest(
        files,
        input_format=input_format,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        xml_row_tag=effective_xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
        source_file_by_name=source_file_by_name,
    )
    if manifest is None:
        return None
    carrier = NativeDirectoryManifestCarrier()
    setattr(carrier, "native_multisource_manifest", manifest)
    return PreparedPublicInput(
        carrier,
        native_input_format(input_format),
        "stream",
        carrier,
        xml_row_tag=effective_xml_row_tag,
    )


def prepare_parquet_directory_from_files(
    files: list[FolderFile],
    *,
    memory_limit_bytes: int | None,
    source_file_by_name: dict[str, str] | None = None,
) -> PreparedPublicInput:
    """Prepare an already-listed local Parquet directory."""
    carrier = NativeDirectoryManifestCarrier()
    setattr(
        carrier,
        "native_parquet_multisource_manifest",
        ParquetDirectorySourceManifest(
            [
                ParquetDirectorySourceFile(
                    path=file.native_path or file.display_name,
                    source_file=folder_file_source(file, source_file_by_name),
                )
                for file in files
            ],
            memory_limit_bytes=memory_limit_bytes,
        ),
    )
    return PreparedPublicInput(carrier, "parquet", "stream", carrier)


def prepare_parquet_directory(
    path: str | os.PathLike[str],
    *,
    memory_limit_bytes: int | None,
    source_file_by_name: dict[str, str] | None = None,
) -> PreparedPublicInput:
    """Prepare one deterministic local Parquet directory."""
    return prepare_parquet_directory_from_files(
        _directory_files_for_format(path, "parquet", memory_limit_bytes=memory_limit_bytes),
        memory_limit_bytes=memory_limit_bytes,
        source_file_by_name=source_file_by_name,
    )


def prepare_single_parquet_file(
    path: str | os.PathLike[str],
    *,
    source_file: str,
    keepalive: Any,
    memory_limit_bytes: int | None,
) -> PreparedPublicInput:
    """Prepare one staged Parquet file through the shared Arrow-source path."""
    carrier = NativeDirectoryManifestCarrier()
    setattr(
        carrier,
        "native_parquet_multisource_manifest",
        ParquetDirectorySourceManifest(
            [ParquetDirectorySourceFile(path=os.fspath(path), source_file=source_file)],
            memory_limit_bytes=memory_limit_bytes,
        ),
    )
    retained: Any = carrier if keepalive is None else ChainedKeepalive(carrier, keepalive)
    return PreparedPublicInput(
        carrier,
        "parquet",
        "stream",
        retained,
        source_file=source_file,
    )


def prepare_directory_from_files(
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
    native_prepared = native_directory_prepared_from_files_or_none(
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
    return prepare_parquet_directory_from_files(
        files,
        memory_limit_bytes=memory_limit_bytes,
        source_file_by_name=source_file_by_name,
    )


def prepare_directory(
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
    if input_format == "parquet":
        return prepare_parquet_directory(
            path,
            memory_limit_bytes=memory_limit_bytes,
            source_file_by_name=source_file_by_name,
        )
    files = _directory_files_for_format(path, input_format, memory_limit_bytes=memory_limit_bytes)
    native_prepared = native_directory_prepared_from_files_or_none(
        files,
        input_format,
        input_text_encoding=input_text_encoding,
        xml_row_tag=xml_row_tag,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        memory_limit_bytes=memory_limit_bytes,
        source_file_by_name=source_file_by_name,
    )
    if native_prepared is None:
        raise unsupported_native_directory_ingestion()
    return native_prepared


@dataclass(slots=True)
class RemoteNativeDirectorySourceManifest:
    """Remote sources staged lazily into bounded native path-source chunks."""

    files: list[RemoteFile]
    input_format: str
    input_text_encoding: str = "utf-8"
    csv_delimiter: str = ","
    csv_has_header: bool = True
    xml_row_tag: str | None = None
    memory_limit_bytes: int | None = None
    threading_mode: str = "single"
    chunk_size: int = 64
    chunk_target_bytes: int = 16 * 1024 * 1024
    operation_context: OperationExecutionContext | None = None
    _temporary_storage_pool: Any = field(default=None, init=False, repr=False)
    _prefetched_chunks: list[StagedNativeDirectoryManifest] = field(
        default_factory=list, init=False, repr=False
    )
    _prefetched_file_count: int = field(default=0, init=False, repr=False)
    _prefetch_lock: Any = field(default_factory=Lock, init=False, repr=False)
    _pid: int = field(default_factory=os.getpid, init=False, repr=False)

    def _ensure_owner_process(self) -> None:
        """Reject inherited manifest state before touching its locks or pools."""
        if os.getpid() != getattr(self, "_pid", os.getpid()):
            raise RuntimeError("remote directory manifest cannot be reused after fork")

    def prefetch_first_chunk(self) -> bool:
        """Stage and retain one immutable prefix for later ordered consumption."""
        self._ensure_owner_process()
        with self._prefetch_lock:
            if self._prefetched_chunks:
                return False
            staged = self.stage_chunk(0)
            if staged is None:
                return False
            self._prefetched_chunks.append(staged)
            self._prefetched_file_count = len(staged.manifest.source_batch.sources)
            return True

    def take_prefetched_chunks(self) -> tuple[list[StagedNativeDirectoryManifest], int]:
        """Transfer ownership of a retained lookahead prefix to one provider."""
        self._ensure_owner_process()
        with self._prefetch_lock:
            chunks, file_count = self._prefetched_chunks, self._prefetched_file_count
            self._prefetched_chunks = []
            self._prefetched_file_count = 0
        return chunks, file_count

    def stage_chunk(self, start: int) -> StagedNativeDirectoryManifest | None:
        """Synchronously stage one bounded remote chunk."""
        self._ensure_owner_process()
        chunk = self._chunk_at(start)
        if not chunk:
            return None
        lease = self._storage_pool().acquire(
            self.estimated_chunk_bytes(start), label=f"remote source packet at file ordinal {start}"
        )
        try:
            staged = remote_staging.stage_remote_files_to_directory(
                chunk,
                memory_limit_bytes=self.memory_limit_bytes,
                threading_mode=self.threading_mode,
                operation_context=self.operation_context,
                storage_lease=lease,
            )
        except BaseException:
            lease.release()
            raise
        return self._prepared_staged_chunk(staged, start=start)

    async def stage_chunk_async(
        self,
        start: int,
        download_session: remote_downloads.RemoteDirectoryDownloadSession,
        storage_lease: Any | None = None,
    ) -> StagedNativeDirectoryManifest | None:
        """Stage one chunk on the operation-owned remote event loop."""
        self._ensure_owner_process()
        chunk = self._chunk_at(start)
        if not chunk:
            return None
        lease = storage_lease or self._storage_pool().acquire(
            self.estimated_chunk_bytes(start),
            label=f"remote source packet at file ordinal {start}",
        )
        try:
            staged = await remote_staging.stage_remote_files_to_directory_async(
                chunk,
                memory_limit_bytes=self.memory_limit_bytes,
                threading_mode=self.threading_mode,
                download_session=download_session,
                storage_lease=lease,
            )
        except BaseException:
            lease.release()
            raise
        return self._prepared_staged_chunk(staged, start=start)

    def open_staging_session(self) -> remote_downloads.RemoteDirectoryDownloadSession:
        """Return the shared provider session for this manifest operation."""
        self._ensure_owner_process()
        return remote_downloads.RemoteDirectoryDownloadSession(
            self.files,
            memory_limit_bytes=self.memory_limit_bytes,
            threading_mode=self.threading_mode,
        )

    def _chunk_at(self, start: int) -> list[RemoteFile]:
        """Return one stable packet bounded by file count and known bytes."""
        return remote_file_packet(
            self.files, start, max_files=self.chunk_size, target_bytes=self.chunk_target_bytes
        )

    def next_chunk_start(self, start: int) -> int:
        """Return the first file ordinal after the packet beginning at ``start``."""
        return start + len(self._chunk_at(start))

    def estimated_chunk_bytes(self, start: int) -> int:
        """Return the deterministic reservation for one remote packet."""
        return remote_file_packet_estimated_bytes(
            self.files, start, max_files=self.chunk_size, target_bytes=self.chunk_target_bytes
        )

    def _storage_pool(self) -> Any:
        """Return the operation-owned temporary storage pool."""
        self._ensure_owner_process()
        if self.operation_context is not None:
            return self.operation_context.temporary_storage
        if self._temporary_storage_pool is None:
            from ...core_impl.temporary_storage import TemporaryStoragePermitPool

            self._temporary_storage_pool = TemporaryStoragePermitPool(self.memory_limit_bytes)
        return self._temporary_storage_pool

    def try_acquire_storage_lease(self, start: int) -> Any | None:
        """Reserve one packet without blocking the source consumer."""
        return self._storage_pool().try_acquire(
            self.estimated_chunk_bytes(start), label=f"remote source packet at file ordinal {start}"
        )

    def _prepared_staged_chunk(
        self, staged: remote_staging.StagedPath, *, start: int
    ) -> StagedNativeDirectoryManifest:
        """Build the native manifest for one fully downloaded local chunk."""
        try:
            local_files = _directory_files_for_format(
                staged.path,
                self.input_format,
                memory_limit_bytes=self.memory_limit_bytes,
            )
            effective_xml_row_tag = _effective_native_xml_row_tag(
                local_files,
                input_format=self.input_format,
                input_text_encoding=self.input_text_encoding,
                xml_row_tag=self.xml_row_tag,
                memory_limit_bytes=self.memory_limit_bytes,
            )
            if self.xml_row_tag is None and start == 0:
                self.xml_row_tag = effective_xml_row_tag
            manifest = _native_directory_manifest(
                local_files,
                input_format=self.input_format,
                csv_delimiter=self.csv_delimiter,
                csv_has_header=self.csv_has_header,
                xml_row_tag=effective_xml_row_tag,
                memory_limit_bytes=self.memory_limit_bytes,
                source_file_by_name=staged.source_file_by_name,
            )
            if manifest is None:
                raise unsupported_native_directory_ingestion()
            return StagedNativeDirectoryManifest(manifest, staged)
        except BaseException:
            staged.close()
            raise

    def close(self) -> None:
        """Close retained lookahead chunks and any manifest-local permit pool."""
        self._ensure_owner_process()
        chunks, _file_count = self.take_prefetched_chunks()
        first_error = _close_sequence_with_error(chunks)
        if chunks:
            with self._prefetch_lock:
                existing_ids = {id(chunk) for chunk in self._prefetched_chunks}
                self._prefetched_chunks[:0] = [
                    chunk for chunk in chunks if id(chunk) not in existing_ids
                ]
                self._prefetched_file_count = sum(
                    len(chunk.manifest.source_batch.sources) for chunk in self._prefetched_chunks
                )

        pool = self._temporary_storage_pool
        pools = [] if pool is None else [pool]
        pool_error = _close_sequence_with_error(pools)
        if not pools and self._temporary_storage_pool is pool:
            self._temporary_storage_pool = None
        if first_error is None:
            first_error = pool_error
        elif pool_error is not None:
            add_bounded_note(first_error, "temporary pool cleanup failure", pool_error)
        if first_error is not None:
            raise first_error
