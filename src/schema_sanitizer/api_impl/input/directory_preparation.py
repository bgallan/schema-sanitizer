"""Local and remote public directory-input preparation."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from ...core_impl.native_options import optional_memory_limit_arg
from ...core_impl.native_symbols import XML_FOLDER_EFFECTIVE_ROW_TAG
from ...input_impl.directory_inputs import FolderFile, RemoteFile, check_document_size, folder_files
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
from ...remote_impl import staging as remote_staging
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
) -> list[FolderFile]:
    """Return deterministic direct children accepted for one public format."""
    return folder_files(
        path,
        suffix=FORMAT_SUFFIXES[input_format],
        reader_name=f"{input_format} directory input",
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
        _directory_files_for_format(path, "parquet"),
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
    return PreparedPublicInput(carrier, "parquet", "stream", retained)


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
    files = _directory_files_for_format(path, input_format)
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
    chunk_size: int = 64

    def stage_chunk(self, start: int) -> StagedNativeDirectoryManifest | None:
        """Stage one bounded remote chunk and return its local native manifest."""
        chunk = self.files[start : start + max(1, self.chunk_size)]
        if not chunk:
            return None
        staged = remote_staging.stage_remote_files_to_directory(
            chunk,
            memory_limit_bytes=self.memory_limit_bytes,
        )
        try:
            local_files = _directory_files_for_format(staged.path, self.input_format)
            effective_xml_row_tag = _effective_native_xml_row_tag(
                local_files,
                input_format=self.input_format,
                input_text_encoding=self.input_text_encoding,
                xml_row_tag=self.xml_row_tag,
                memory_limit_bytes=self.memory_limit_bytes,
            )
            if self.xml_row_tag is None:
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
        except Exception:
            staged.close()
            raise

    def close(self) -> None:
        """Satisfy PreparedPublicInput keepalive cleanup."""


def _validate_remote_native_directory_supported(
    input_format: str,
    *,
    input_text_encoding: str,
    csv_delimiter: str,
) -> None:
    """Reject remote settings that cannot use native chunked staging."""
    if input_format == "parquet":
        raise ValueError("Parquet remote directories use the Parquet Arrow-source path")
    if not _native_directory_supported(
        input_format,
        input_text_encoding=input_text_encoding,
        csv_delimiter=csv_delimiter,
    ):
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
    """Prepare a remote directory as a lazy bounded source-plan manifest."""
    _validate_remote_native_directory_supported(
        input_format,
        input_text_encoding=input_text_encoding,
        csv_delimiter=csv_delimiter,
    )
    manifest = RemoteNativeDirectorySourceManifest(
        files,
        input_format=input_format,
        input_text_encoding=input_text_encoding,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        xml_row_tag=xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
        chunk_size=remote_staging.remote_directory_stage_chunk_size(memory_limit_bytes),
    )
    if input_format == "xml" and xml_row_tag is None:
        staged_probe = manifest.stage_chunk(0)
        if staged_probe is not None:
            staged_probe.close()
    carrier = NativeDirectoryManifestCarrier()
    setattr(carrier, "remote_native_multisource_manifest", manifest)
    return PreparedPublicInput(
        carrier,
        native_input_format(input_format),
        "stream",
        carrier,
        xml_row_tag=manifest.xml_row_tag,
    )
