"""Public SourceManifest validation and staging preparation."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from ...core_impl.resource_lifecycle import _cleanup_with_note
from ...input_impl.prepared import ChainedKeepalive, PreparedPublicInput
from ...input_impl.remote_files import RemoteFile
from ...input_impl.selection import (
    normalize_public_input_format,
    normalize_public_input_mode,
    validate_suffix,
)
from ...input_impl.source_manifest import SourceManifest
from ...remote_impl import staging as remote_staging
from .directory_preparation import prepare_directory
from .remote_directory_preparation import remote_native_directory_prepared_from_files

if TYPE_CHECKING:
    from ..operation_context import OperationExecutionContext


def _staging_files(manifest: SourceManifest) -> list[RemoteFile]:
    """Return exact identities with deterministic collision-free local names."""
    staged: list[RemoteFile] = []
    for index, file in enumerate(manifest.files):
        basename = Path(file.name).name
        staged.append(replace(file, name=f"{index:08d}-{basename}"))
    return staged


def prepare_source_manifest_input(
    manifest: SourceManifest,
    *,
    input_format: str | None,
    input_mode: str,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
    threading_mode: str,
    operation_context: OperationExecutionContext | None,
) -> PreparedPublicInput:
    """Prepare exactly the objects already frozen in a public manifest."""
    normalize_public_input_mode(input_mode)
    fmt = normalize_public_input_format(input_format)
    if fmt == "python":
        raise ValueError("SourceManifest inputs require a file input_format")
    if not manifest.files:
        raise ValueError("SourceManifest input contains no remote objects")
    for file in manifest.files:
        validate_suffix(file.uri, fmt)

    files = _staging_files(manifest)
    if fmt != "parquet":
        prepared = remote_native_directory_prepared_from_files(
            files,
            fmt,
            input_text_encoding=input_text_encoding,
            xml_row_tag=xml_row_tag,
            csv_delimiter=csv_delimiter,
            csv_has_header=csv_has_header,
            memory_limit_bytes=memory_limit_bytes,
            threading_mode=threading_mode,
            operation_context=operation_context,
        )
        prepared.source_manifest = manifest
        return prepared

    staged = remote_staging.stage_remote_files_to_directory(
        files,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
        operation_context=operation_context,
    )
    try:
        prepared = prepare_directory(
            staged.path,
            fmt,
            input_text_encoding=input_text_encoding,
            xml_row_tag=xml_row_tag,
            csv_delimiter=csv_delimiter,
            csv_has_header=csv_has_header,
            memory_limit_bytes=memory_limit_bytes,
            source_file_by_name=staged.source_file_by_name,
        )
        prepared.keepalive = ChainedKeepalive(prepared.keepalive, staged)
        prepared.source_manifest = manifest
        return prepared
    except BaseException as exc:
        _cleanup_with_note(exc, staged, label="source-manifest staging cleanup also failed")
        raise


__all__ = ["prepare_source_manifest_input"]
