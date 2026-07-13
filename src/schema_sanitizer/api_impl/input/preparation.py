"""Public input validation and native payload preparation."""

from __future__ import annotations

import os
from typing import Any

from ...core_impl.uris import local_path_from_file_uri, looks_like_file_uri, looks_like_remote_uri
from ...input_impl.directory_inputs import discovered_directory_input_for
from ...input_impl.prepared import ChainedKeepalive, PreparedPublicInput
from ...input_impl.selection import (
    FORMAT_SUFFIXES,
    display_source_file,
    native_input_format,
    normalize_public_input_format,
    normalize_public_input_mode,
    prepare_native_text_data,
    source_for_file_input,
    validate_local_path_mode,
    validate_suffix,
)
from ...remote_impl import routing
from ...remote_impl import staging as remote_staging
from ...remote_impl.transport import run_sync
from .directory_preparation import (
    _remote_native_directory_prepared_from_files,
    _validate_remote_native_directory_supported,
    prepare_directory,
    prepare_directory_from_files,
    prepare_single_parquet_file,
)


def _prepare_discovered_directory(
    discovered: Any,
    *,
    input_format: str,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
    original_source_file: str,
) -> PreparedPublicInput | None:
    """Prepare an already-listed local or remote directory input."""
    if discovered is None:
        return None
    if discovered.remote_files:
        files = list(discovered.remote_files)
        if input_format != "parquet":
            return _remote_native_directory_prepared_from_files(
                files,
                input_format,
                input_text_encoding=input_text_encoding,
                xml_row_tag=xml_row_tag,
                csv_delimiter=csv_delimiter,
                csv_has_header=csv_has_header,
                memory_limit_bytes=memory_limit_bytes,
            )
        staged = remote_staging.stage_remote_files_to_directory(
            files,
            memory_limit_bytes=memory_limit_bytes,
        )
        try:
            prepared = prepare_directory(
                staged.path,
                input_format,
                input_text_encoding=input_text_encoding,
                xml_row_tag=xml_row_tag,
                csv_delimiter=csv_delimiter,
                csv_has_header=csv_has_header,
                memory_limit_bytes=memory_limit_bytes,
                source_file_by_name=staged.source_file_by_name,
            )
            prepared.keepalive = ChainedKeepalive(prepared.keepalive, staged)
            if prepared.source_file is None and prepared.source_file_spans is None:
                prepared.source_file = original_source_file
            return prepared
        except Exception:
            staged.close()
            raise
    if discovered.local_files:
        prepared = prepare_directory_from_files(
            list(discovered.local_files),
            input_format,
            input_text_encoding=input_text_encoding,
            xml_row_tag=xml_row_tag,
            csv_delimiter=csv_delimiter,
            csv_has_header=csv_has_header,
            memory_limit_bytes=memory_limit_bytes,
        )
        if prepared.source_file is None and prepared.source_file_spans is None:
            prepared.source_file = original_source_file
        return prepared
    return None


def _prepare_input_target(
    path: str | os.PathLike[str],
    *,
    input_format: str,
    input_mode: str,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
    original_source_file: str,
) -> PreparedPublicInput:
    """Prepare a target that was not supplied through discovery context."""
    keepalive: Any = None
    source_file_by_name: dict[str, str] | None = None
    suffix_validated = False
    if isinstance(path, str) and looks_like_file_uri(path):
        path = local_path_from_file_uri(path)
    elif isinstance(path, str) and looks_like_remote_uri(path):
        if input_mode == "single_file":
            validate_suffix(path, input_format)
            suffix_validated = True
            staged = remote_staging.stage_remote_single_file(
                path,
                memory_limit_bytes=memory_limit_bytes,
            )
            keepalive = staged
            path = staged.path
        elif input_format == "parquet":
            staged = remote_staging.stage_remote_parquet_directory(
                path,
                suffixes=FORMAT_SUFFIXES["parquet"],
                memory_limit_bytes=memory_limit_bytes,
            )
            keepalive = staged
            source_file_by_name = staged.source_file_by_name
            path = staged.path
        else:
            _validate_remote_native_directory_supported(
                input_format,
                input_text_encoding=input_text_encoding,
                csv_delimiter=csv_delimiter,
            )
            files = list(
                run_sync(routing.list_remote_directory(path, FORMAT_SUFFIXES[input_format]))
            )
            if not files:
                expected = " or ".join(FORMAT_SUFFIXES[input_format])
                raise ValueError(f"remote directory input found no {expected} files in: {path}")
            return _remote_native_directory_prepared_from_files(
                files,
                input_format,
                input_text_encoding=input_text_encoding,
                xml_row_tag=xml_row_tag,
                csv_delimiter=csv_delimiter,
                csv_has_header=csv_has_header,
                memory_limit_bytes=memory_limit_bytes,
            )

    validate_local_path_mode(path, input_mode)
    if input_mode == "directory":
        prepared = prepare_directory(
            path,
            input_format,
            input_text_encoding=input_text_encoding,
            xml_row_tag=xml_row_tag,
            csv_delimiter=csv_delimiter,
            csv_has_header=csv_has_header,
            memory_limit_bytes=memory_limit_bytes,
            source_file_by_name=source_file_by_name,
        )
        if keepalive is not None:
            prepared.keepalive = ChainedKeepalive(prepared.keepalive, keepalive)
        if prepared.source_file is None and prepared.source_file_spans is None:
            prepared.source_file = original_source_file
        return prepared

    if not suffix_validated:
        validate_suffix(path, input_format)
    if input_format == "parquet" and keepalive is not None:
        return prepare_single_parquet_file(
            path,
            source_file=original_source_file,
            keepalive=keepalive,
            memory_limit_bytes=memory_limit_bytes,
        )
    native_format = native_input_format(input_format)
    native_data, native_source = prepare_native_text_data(
        os.fspath(path),
        source=source_for_file_input(path),
        format_name=native_format,
        input_text_encoding=input_text_encoding,
    )
    if native_source == "stream":
        reader_keepalive: Any = native_data
        if keepalive is not None:
            reader_keepalive = ChainedKeepalive(native_data, keepalive)
        return PreparedPublicInput(
            native_data,
            native_format,
            native_source,
            reader_keepalive,
            source_file=original_source_file,
        )
    if input_format == "json_array":
        return PreparedPublicInput(
            os.fspath(path),
            "json_array",
            source_for_file_input(path),
            keepalive=keepalive,
            source_file=original_source_file,
        )
    return PreparedPublicInput(
        os.fspath(path),
        native_format,
        source_for_file_input(path),
        keepalive=keepalive,
        source_file=original_source_file,
    )


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
    original_source_file = display_source_file(path)
    discovered = (
        discovered_directory_input_for(
            path,
            fmt,
            normalize_input_format=normalize_public_input_format,
        )
        if mode == "directory"
        else None
    )
    prepared = _prepare_discovered_directory(
        discovered,
        input_format=fmt,
        input_text_encoding=input_text_encoding,
        xml_row_tag=xml_row_tag,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        memory_limit_bytes=memory_limit_bytes,
        original_source_file=original_source_file,
    )
    if prepared is not None:
        return prepared
    return _prepare_input_target(
        path,
        input_format=fmt,
        input_mode=mode,
        input_text_encoding=input_text_encoding,
        xml_row_tag=xml_row_tag,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        memory_limit_bytes=memory_limit_bytes,
        original_source_file=original_source_file,
    )
