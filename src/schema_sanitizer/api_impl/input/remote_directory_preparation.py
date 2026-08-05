"""Remote directory validation and prepared-input assembly."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ...input_impl.prepared import NativeDirectoryManifestCarrier, PreparedPublicInput
from ...input_impl.selection import native_input_format, unsupported_native_directory_ingestion
from ...remote_impl.packetization import remote_staging_packet_policy
from ...sources.models import RemoteFile
from .directory_preparation import (
    RemoteNativeDirectorySourceManifest,
    _native_directory_supported,
)

if TYPE_CHECKING:
    from ..operation_context import OperationExecutionContext


def validate_remote_native_directory_supported(
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


def remote_native_directory_prepared_from_files(
    files: list[RemoteFile],
    input_format: str,
    *,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
    threading_mode: str = "single",
    operation_context: OperationExecutionContext | None = None,
) -> PreparedPublicInput:
    """Prepare a remote directory as a lazy bounded source-plan manifest."""
    validate_remote_native_directory_supported(
        input_format,
        input_text_encoding=input_text_encoding,
        csv_delimiter=csv_delimiter,
    )
    packet_policy = remote_staging_packet_policy(memory_limit_bytes)
    manifest = RemoteNativeDirectorySourceManifest(
        files,
        input_format=input_format,
        input_text_encoding=input_text_encoding,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        xml_row_tag=xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
        threading_mode=threading_mode,
        chunk_size=packet_policy.max_files,
        chunk_target_bytes=packet_policy.target_bytes,
        operation_context=operation_context,
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
