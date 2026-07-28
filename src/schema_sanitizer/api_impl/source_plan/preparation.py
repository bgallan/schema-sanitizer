"""Canonical source-plan construction for prepared public inputs."""

from __future__ import annotations

import os
from collections.abc import Sequence

from schema_sanitizer.input_impl.source_plan import (
    SEQUENCE,
    NativeSourcePlan,
    PreparedSourceBatch,
    SourceDescriptor,
    _native_path_source_plan,
    source_kind_for_format,
)

from ...core_impl.native_options import optional_memory_limit_arg
from ...core_impl.native_symbols import XML_FOLDER_EFFECTIVE_ROW_TAG
from ...input_impl.prepared import PreparedPublicInput
from ...input_impl.selection import display_source_file, folder_file_source
from ..parquet.multisource import (
    ParquetDirectorySourceFile,
    ParquetDirectorySourceManifest,
)
from .attached import (
    native_multisource_manifest_from_data,
    remote_native_multisource_manifest_from_data,
    source_plan_from_parquet_manifest,
    source_plan_from_remote_manifest,
)


def _source_batch(
    sources: Sequence[SourceDescriptor],
    *,
    input_format: str,
    input_mode: str | None = None,
    csv_delimiter: str = ",",
    csv_has_header: bool = True,
    xml_row_tag: str | None = None,
    memory_limit_bytes: int | None = None,
) -> PreparedSourceBatch:
    """Build the shared source batch for native path execution."""
    return PreparedSourceBatch(
        sources=tuple(sources),
        input_format=input_format,
        input_mode=input_mode,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        xml_row_tag=xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
    )


def _prepared_native_sources(
    prepared: PreparedPublicInput,
    *,
    input_format: str,
) -> tuple[SourceDescriptor, ...] | None:
    """Return native local descriptors represented by a prepared input."""
    manifest = native_multisource_manifest_from_data(prepared.data)
    if manifest is not None:
        return manifest.source_batch.sources
    if prepared.source == "path":
        source_kind = source_kind_for_format(prepared.format)
        if source_kind is None:
            return None
        path = os.fspath(prepared.data)
        source_file = prepared.source_file or display_source_file(path)
        return (SourceDescriptor(source_kind, path, source_file),)

    source_kind = source_kind_for_format(input_format)
    if source_kind is None:
        return None
    data = prepared.data
    source = getattr(data, "_source", None)
    if source is not None:
        native_path = getattr(source, "native_path", None)
        if native_path is None:
            return None
        path = os.fspath(native_path)
        source_file = prepared.source_file or display_source_file(path)
        return (SourceDescriptor(source_kind, path, source_file),)

    files = getattr(data, "_files", None)
    if files is None:
        return None
    sources: list[SourceDescriptor] = []
    for file in files:
        native_path = getattr(file, "native_path", None)
        if native_path is None:
            return None
        path = os.fspath(native_path)
        sources.append(SourceDescriptor(source_kind, path, folder_file_source(file)))
    return tuple(sources)


def _prepared_parquet_sources(
    prepared: PreparedPublicInput,
) -> Sequence[ParquetDirectorySourceFile] | None:
    """Return Parquet descriptors represented by a prepared input."""
    manifest = getattr(prepared.data, "native_parquet_multisource_manifest", None)
    if isinstance(manifest, ParquetDirectorySourceManifest):
        return manifest.files
    if prepared.format == "parquet" and prepared.source == "path":
        path = os.fspath(prepared.data)
        source_file = prepared.source_file or display_source_file(path)
        return (ParquetDirectorySourceFile(path=path, source_file=source_file),)
    return None


def _remote_or_path_sequence_plan(
    prepared_inputs: list[PreparedPublicInput],
    *,
    input_format: str,
) -> NativeSourcePlan | None:
    """Return a sequence plan when at least one source is remote-native."""
    remote_manifests = tuple(
        remote_native_multisource_manifest_from_data(prepared.data) for prepared in prepared_inputs
    )
    if not any(manifest is not None for manifest in remote_manifests):
        return None

    child_plans: list[NativeSourcePlan] = []
    for prepared, remote_manifest in zip(
        prepared_inputs,
        remote_manifests,
        strict=True,
    ):
        if remote_manifest is not None:
            child_plans.append(source_plan_from_remote_manifest(remote_manifest))
            continue
        sources = _prepared_native_sources(prepared, input_format=input_format)
        if not sources:
            return None
        child_plans.append(
            _native_path_source_plan(
                input_format=input_format,
                route_name="native_manifest_paths",
                source_batch=_source_batch(sources, input_format=input_format),
            )
        )
    return NativeSourcePlan(
        kind=SEQUENCE,
        payload=tuple(child_plans),
        input_format=input_format,
        route_name="remote_native_manifest_chunks",
        xml_row_tag=next(
            (prepared.xml_row_tag for prepared in prepared_inputs if prepared.xml_row_tag),
            None,
        ),
    )


def _parquet_arrow_plan(
    prepared_inputs: list[PreparedPublicInput],
    *,
    memory_limit_bytes: int | None,
) -> NativeSourcePlan | None:
    """Return a lazy Arrow-source Parquet plan when all inputs qualify."""
    sources: list[ParquetDirectorySourceFile] = []
    for prepared in prepared_inputs:
        prepared_sources = _prepared_parquet_sources(prepared)
        if not prepared_sources:
            return None
        sources.extend(prepared_sources)
    if not sources:
        return None
    return source_plan_from_parquet_manifest(
        ParquetDirectorySourceManifest(
            sources,
            memory_limit_bytes=memory_limit_bytes,
        )
    )


def _native_xml_row_tag_or_none(
    sources: Sequence[SourceDescriptor],
    *,
    memory_limit_bytes: int | None,
) -> str | None:
    """Infer a consistent XML row tag through the native XML helper."""
    return XML_FOLDER_EFFECTIVE_ROW_TAG(
        [source.path for source in sources],
        "",
        optional_memory_limit_arg(memory_limit_bytes),
    )


def _local_path_source_plan(
    prepared_inputs: list[PreparedPublicInput],
    *,
    input_format: str,
    input_mode: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
) -> NativeSourcePlan | None:
    """Return a native path-source plan when all inputs qualify."""
    sources: list[SourceDescriptor] = []
    for prepared in prepared_inputs:
        prepared_sources = _prepared_native_sources(
            prepared,
            input_format=input_format,
        )
        if not prepared_sources:
            return None
        sources.extend(prepared_sources)
    if not sources:
        return None

    source_kinds = {source.kind for source in sources}
    if source_kinds == {"csv"} and len(csv_delimiter.encode("utf-8")) != 1:
        return None
    if source_kinds <= {"json", "jsonl", "json_array", "csv"}:
        return _native_path_source_plan(
            input_format=input_format,
            route_name="native_manifest_paths",
            source_batch=_source_batch(
                sources,
                input_format=input_format,
                input_mode=input_mode,
                csv_delimiter=csv_delimiter,
                csv_has_header=csv_has_header,
                memory_limit_bytes=memory_limit_bytes,
            ),
        )
    if source_kinds != {"xml"}:
        return None

    effective_row_tag = xml_row_tag or next(
        (prepared.xml_row_tag for prepared in prepared_inputs if prepared.xml_row_tag),
        None,
    )
    if not effective_row_tag:
        effective_row_tag = _native_xml_row_tag_or_none(
            sources,
            memory_limit_bytes=memory_limit_bytes,
        )
    if not effective_row_tag:
        return None
    return _native_path_source_plan(
        input_format=input_format,
        route_name="native_manifest_paths",
        xml_row_tag=effective_row_tag,
        source_batch=_source_batch(
            sources,
            input_format=input_format,
            input_mode=input_mode,
            csv_delimiter=csv_delimiter,
            csv_has_header=csv_has_header,
            xml_row_tag=effective_row_tag,
            memory_limit_bytes=memory_limit_bytes,
        ),
    )


def source_plan_from_prepared_inputs(
    prepared_inputs: list[PreparedPublicInput],
    *,
    input_format: str,
    input_mode: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
) -> NativeSourcePlan | None:
    """Build the canonical native source plan for prepared warm-up inputs."""
    if input_format == "parquet":
        return _parquet_arrow_plan(
            prepared_inputs,
            memory_limit_bytes=memory_limit_bytes,
        )
    remote_plan = _remote_or_path_sequence_plan(
        prepared_inputs,
        input_format=input_format,
    )
    if remote_plan is not None:
        return remote_plan
    return _local_path_source_plan(
        prepared_inputs,
        input_format=input_format,
        input_mode=input_mode,
        xml_row_tag=xml_row_tag,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        memory_limit_bytes=memory_limit_bytes,
    )
