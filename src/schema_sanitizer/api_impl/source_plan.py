"""Shared native source plans for warm-up and normal registry execution."""

from __future__ import annotations

import contextlib
import json
import os
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

from ..core_impl.native_functions import (
    PATH_SOURCE_PLAN_CREATE,
    XML_FOLDER_EFFECTIVE_ROW_TAG,
)
from ..errors import SchemaSanitizerError
from .async_remote_scheduler import read_int_env
from .file_conversion_metadata import (
    SCHEMA_DRIFTS_COLUMN,
    SCHEMA_REGISTRY_COLUMN,
    SOURCE_FILE_COLUMN,
)
from .folder_listing import check_document_size
from .ingest_lifecycle import _close_suppressing_errors
from .ingest_runtime_types import Result, Stream
from .native_folder_common import memory_limit_arg
from .parquet_multisource import (
    ParquetDirectorySourceFile,
    ParquetDirectorySourceManifest,
    infer_parquet_multisource_registry,
)
from .public_input import (
    NativeDirectorySourceManifest,
    PreparedPublicInput,
    RemoteNativeDirectorySourceManifest,
    _display_source_file,
    _folder_file_source,
    iter_staged_remote_chunks,
)
from .shared import Options, _unwrap_options
from .source_batch import (
    PreparedSourceBatch,
    SourceDescriptor,
    path_source_tuples,
    source_kind_for_format,
)
from .source_plan_registry_output import write_opened_registry_stream_to_file
from .stream_writer_core import write_raw_stream_to_file

PATH_SOURCES = "path_sources"
REMOTE_CHUNKS = "remote_chunks"
PARQUET_ARROW_SOURCES = "parquet_arrow_sources"
SEQUENCE = "sequence"
_LAST_NATIVE_MULTISOURCE_ROUTE = "none"


def last_native_multisource_route() -> str:
    """Return the route used by the most recent native multi-source conversion."""
    return _LAST_NATIVE_MULTISOURCE_ROUTE


def native_multisource_manifest_from_data(data: Any) -> NativeDirectorySourceManifest | None:
    """Return an attached native multi-source manifest from prepared directory data."""
    manifest = getattr(data, "native_multisource_manifest", None)
    return manifest if isinstance(manifest, NativeDirectorySourceManifest) else None


def remote_native_multisource_manifest_from_data(
    data: Any,
) -> RemoteNativeDirectorySourceManifest | None:
    """Return an attached lazy remote native multi-source manifest, if present."""
    manifest = getattr(data, "remote_native_multisource_manifest", None)
    return manifest if isinstance(manifest, RemoteNativeDirectorySourceManifest) else None


@dataclass(slots=True)
class NativeSourcePlan:
    """Canonical native source execution plan."""

    kind: str
    payload: Any
    input_format: str
    route_name: str
    xml_row_tag: str | None = None
    source_batch: PreparedSourceBatch | None = None
    native_payload: Any | None = None
    close_items: list[Any] = field(default_factory=list)

    def close(self) -> None:
        """Close resources owned by this plan."""
        if self.kind == SEQUENCE:
            for item in self.payload:
                _close_suppressing_errors(item)
        while self.close_items:
            _close_suppressing_errors(self.close_items.pop())


@dataclass(slots=True)
class OpenedSourcePlanRegistryStream:
    """Opened registry-backed stream plus registry metadata."""

    stream: Any | None
    schema_registry_json: str
    schema_drifts_json: str
    native_registry_state: Any = None
    diagnostics: Any = None
    raw_stream: Any | None = None
    close_items: list[Any] = field(default_factory=list)

    def output_stream(self) -> Any:
        """Return a Python stream wrapper for file writers."""
        if self.stream is None:
            if self.raw_stream is None:
                raise RuntimeError("opened registry stream has no stream backend")
            self.stream = Stream(self.raw_stream)
        return self.stream

    def materialization_stream(self) -> Any:
        """Return the most direct Arrow C Stream object for table materialization."""
        if self.raw_stream is not None:
            return self.raw_stream
        return self.output_stream()

    def take_raw_output_stream(self) -> Any | None:
        """Transfer the raw stream to a direct file writer when no wrapper exists."""
        if self.raw_stream is None or self.stream is not None:
            return None
        raw = self.raw_stream
        self.raw_stream = None
        self.close_items = [item for item in self.close_items if id(item) != id(raw)]
        return raw

    def close(self) -> None:
        """Close opened stream resources."""
        closed_raw_id: int | None = None
        if self.stream is not None:
            _close_suppressing_errors(self.stream)
            self.stream = None
        elif self.raw_stream is not None:
            _close_suppressing_errors(self.raw_stream)
            closed_raw_id = id(self.raw_stream)
        while self.close_items:
            item = self.close_items.pop()
            if closed_raw_id is not None and id(item) == closed_raw_id:
                continue
            _close_suppressing_errors(item)


@dataclass(frozen=True, slots=True)
class SourcePlanRegistryProbeResult:
    """Registry probe result plus the source-plan route used to produce it."""

    raw: Any
    route_name: str


def _append_json_array(target: list[Any], raw_json: str | None) -> None:
    """Append JSON array items from a native result JSON string."""
    if not raw_json:
        return
    value = json.loads(raw_json)
    if isinstance(value, list):
        target.extend(value)


def _memory_limit_stage(input_format: str) -> str:
    """Return the document-size stage label for native path-source files."""
    if input_format in {"json", "json_array", "jsonl", "ndjson"}:
        return "json_parse"
    if input_format == "xml":
        return "xml_parse"
    if input_format == "csv":
        return "csv_parse"
    return f"{input_format}_parse"


def _path_source_tuples_from_plan(plan: NativeSourcePlan) -> list[tuple[str, str, str]]:
    """Return Python path-source tuples only when the fallback representation is needed."""
    if plan.source_batch is not None:
        return path_source_tuples(plan.source_batch)
    payload = plan.payload
    return list(payload) if payload is not None else []


def _check_path_source_sizes(plan: NativeSourcePlan) -> None:
    """Apply per-child file-size memory limits before native path ingestion."""
    source_batch = plan.source_batch
    if source_batch is None:
        return
    memory_limit_bytes = source_batch.memory_limit_bytes
    if memory_limit_bytes is None or memory_limit_bytes <= 0:
        return
    stage = _memory_limit_stage(source_batch.input_format)
    for source in source_batch.sources:
        try:
            size = os.path.getsize(source.path)
        except OSError:
            size = None
        check_document_size(
            source.source_file,
            size,
            memory_limit_bytes=memory_limit_bytes,
            stage=stage,
        )


def _path_source_native_payload(sources: list[tuple[str, str, str]]) -> Any | None:
    """Return a reusable native path-source plan capsule when available."""
    create = PATH_SOURCE_PLAN_CREATE.get()
    if create is None:
        return None
    return create(sources)


def _native_path_source_plan(
    *,
    payload: list[tuple[str, str, str]] | None,
    input_format: str,
    route_name: str,
    xml_row_tag: str | None = None,
    source_batch: PreparedSourceBatch | None = None,
) -> NativeSourcePlan:
    """Build a path-source plan with both Python and native payloads."""
    native_sources = payload
    if native_sources is None and source_batch is not None:
        native_sources = path_source_tuples(source_batch)
    native_payload = (
        _path_source_native_payload(native_sources) if native_sources is not None else None
    )
    retained_payload = None if native_payload is not None and source_batch is not None else payload
    return NativeSourcePlan(
        kind=PATH_SOURCES,
        payload=retained_payload,
        input_format=input_format,
        route_name=route_name,
        xml_row_tag=xml_row_tag,
        source_batch=source_batch,
        native_payload=native_payload,
    )


def _flatten_path_source_sequence_or_none(plan: NativeSourcePlan) -> NativeSourcePlan | None:
    """Return one native path-source plan for a sequence of path-source children."""
    if plan.kind != SEQUENCE:
        return None
    descriptors: list[SourceDescriptor] = []
    sources: list[tuple[str, str, str]] = []
    for child in plan.payload:
        if not isinstance(child, NativeSourcePlan) or child.kind != PATH_SOURCES:
            return None
        if child.source_batch is not None:
            descriptors.extend(child.source_batch.sources)
        else:
            sources.extend(_path_source_tuples_from_plan(child))
    if not descriptors and not sources:
        return None
    source_batch = None
    if descriptors and not sources:
        source_batch = PreparedSourceBatch(
            sources=tuple(descriptors),
            input_format=plan.input_format,
            input_mode=None,
        )
    elif descriptors:
        sources = [
            *path_source_tuples(descriptors),
            *sources,
        ]
    return _native_path_source_plan(
        payload=None if source_batch is not None else sources,
        input_format=plan.input_format,
        route_name="native_sequence_path_sources",
        xml_row_tag=plan.xml_row_tag,
        source_batch=source_batch,
    )


def _path_sources_for_native(raw_context: Any, plan: NativeSourcePlan) -> Any:
    """Return the reusable native path-source payload when one exists."""
    if plan.native_payload is not None and getattr(
        raw_context, "_accepts_native_path_source_plan", False
    ):
        return plan.native_payload
    return _path_source_tuples_from_plan(plan)


def path_sources_from_native_manifest(
    manifest: NativeDirectorySourceManifest,
) -> list[tuple[str, str, str]] | None:
    """Return C++ ABI path sources for a native local directory manifest."""
    source_batch = getattr(manifest, "source_batch", None)
    if isinstance(source_batch, PreparedSourceBatch):
        return path_source_tuples(source_batch)
    if manifest.input_format in {"json", "jsonl", "ndjson"}:
        frontend = "json"
    elif manifest.input_format == "csv":
        frontend = "csv"
    elif manifest.input_format == "json_array":
        frontend = "json_array"
    elif manifest.input_format == "xml":
        frontend = "xml"
    else:
        return None
    return [(frontend, source.path, source.source_file) for source in manifest.files]


def _source_batch_from_sources(
    sources: list[SourceDescriptor],
    *,
    input_format: str,
    input_mode: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    xml_row_tag: str | None,
    memory_limit_bytes: int | None,
) -> PreparedSourceBatch:
    """Build the shared source-batch object for native path execution."""
    return PreparedSourceBatch(
        sources=tuple(sources),
        input_format=input_format,
        input_mode=input_mode,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        xml_row_tag=xml_row_tag,
        memory_limit_bytes=memory_limit_bytes,
    )


def source_plan_from_native_manifest(
    manifest: NativeDirectorySourceManifest,
) -> NativeSourcePlan | None:
    """Return a native path-source plan from a normal local manifest."""
    source_batch = getattr(manifest, "source_batch", None)
    if isinstance(source_batch, PreparedSourceBatch):
        sources = None
    else:
        sources = path_sources_from_native_manifest(manifest)
        if sources is None:
            return None
    return _native_path_source_plan(
        payload=sources,
        input_format=manifest.input_format,
        route_name="native_manifest_paths",
        xml_row_tag=manifest.xml_row_tag,
        source_batch=source_batch if isinstance(source_batch, PreparedSourceBatch) else None,
    )


def source_plan_from_remote_manifest(
    manifest: RemoteNativeDirectorySourceManifest,
) -> NativeSourcePlan:
    """Return a native remote staged-chunk plan from a normal remote manifest."""
    return NativeSourcePlan(
        kind=REMOTE_CHUNKS,
        payload=manifest,
        input_format=manifest.input_format,
        route_name="remote_native_manifest_chunks",
        xml_row_tag=manifest.xml_row_tag,
    )


def source_plan_from_parquet_manifest(
    manifest: ParquetDirectorySourceManifest,
) -> NativeSourcePlan:
    """Return a native Arrow-source plan from a Parquet manifest."""
    return NativeSourcePlan(
        kind=PARQUET_ARROW_SOURCES,
        payload=manifest,
        input_format="parquet",
        route_name="native_parquet_arrow_sources",
    )


def source_plan_from_data(data: Any) -> NativeSourcePlan | None:
    """Return an attached native source plan from prepared public input data."""
    native_manifest = native_multisource_manifest_from_data(data)
    if native_manifest is not None:
        return source_plan_from_native_manifest(native_manifest)
    remote_manifest = remote_native_multisource_manifest_from_data(data)
    if remote_manifest is not None:
        return source_plan_from_remote_manifest(remote_manifest)
    parquet_manifest = getattr(data, "native_parquet_multisource_manifest", None)
    if isinstance(parquet_manifest, ParquetDirectorySourceManifest):
        return source_plan_from_parquet_manifest(parquet_manifest)
    return None


def prepared_native_sources(
    prepared: PreparedPublicInput,
    *,
    input_format: str,
) -> list[SourceDescriptor] | None:
    """Return native local source descriptors represented by one prepared input."""
    plan = source_plan_from_data(prepared.data)
    if plan is not None and plan.kind == PATH_SOURCES and plan.source_batch is not None:
        return list(plan.source_batch.sources)
    if prepared.source == "path":
        source_kind = source_kind_for_format(prepared.format)
        if source_kind is None:
            return None
        path = os.fspath(prepared.data)
        source_file = prepared.source_file or _display_source_file(path)
        return [SourceDescriptor(source_kind, path, source_file)]
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
        source_file = prepared.source_file or _display_source_file(path)
        return [SourceDescriptor(source_kind, path, source_file)]
    files = getattr(data, "_files", None)
    if files is None:
        return None
    sources: list[SourceDescriptor] = []
    for file in files:
        native_path = getattr(file, "native_path", None)
        if native_path is None:
            return None
        path = os.fspath(native_path)
        sources.append(SourceDescriptor(source_kind, path, _folder_file_source(file)))
    return sources


def _prepared_parquet_sources(
    prepared: PreparedPublicInput,
) -> list[ParquetDirectorySourceFile] | None:
    """Return Parquet source descriptors represented by one prepared input."""
    plan = source_plan_from_data(prepared.data)
    if plan is not None and plan.kind == PARQUET_ARROW_SOURCES:
        return list(plan.payload.files)
    if prepared.format == "parquet" and prepared.source == "path":
        path = os.fspath(prepared.data)
        source_file = prepared.source_file or _display_source_file(path)
        return [ParquetDirectorySourceFile(path=path, source_file=source_file)]
    return None


def _native_xml_row_tag_or_none(
    sources: list[SourceDescriptor],
    *,
    memory_limit_bytes: int | None,
) -> str | None:
    """Infer a consistent XML row tag through the native XML file helper."""
    native_effective = XML_FOLDER_EFFECTIVE_ROW_TAG.get()
    if native_effective is None:
        return None
    return native_effective(
        [source.path for source in sources],
        "",
        memory_limit_arg(memory_limit_bytes),
    )


def local_path_source_plan_from_prepared_inputs(
    prepared_inputs: list[PreparedPublicInput],
    *,
    input_format: str,
    input_mode: str,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
) -> NativeSourcePlan | None:
    """Return a native path-source plan when all prepared inputs qualify."""
    sources: list[SourceDescriptor] = []
    for prepared in prepared_inputs:
        prepared_sources = prepared_native_sources(prepared, input_format=input_format)
        if not prepared_sources:
            return None
        sources.extend(prepared_sources)
    if not sources:
        return None
    source_kinds = {source.kind for source in sources}
    if source_kinds == {"csv"} and len(csv_delimiter.encode("utf-8")) != 1:
        return None
    if source_kinds <= {"json", "json_array", "csv"}:
        source_batch = _source_batch_from_sources(
            sources,
            input_format=input_format,
            input_mode=input_mode,
            csv_delimiter=csv_delimiter,
            csv_has_header=csv_has_header,
            xml_row_tag=None,
            memory_limit_bytes=memory_limit_bytes,
        )
        return _native_path_source_plan(
            payload=None,
            input_format=input_format,
            route_name="native_manifest_paths",
            source_batch=source_batch,
        )
    if source_kinds == {"xml"}:
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
        source_batch = _source_batch_from_sources(
            sources,
            input_format=input_format,
            input_mode=input_mode,
            csv_delimiter=csv_delimiter,
            csv_has_header=csv_has_header,
            xml_row_tag=effective_row_tag,
            memory_limit_bytes=memory_limit_bytes,
        )
        return _native_path_source_plan(
            payload=None,
            input_format=input_format,
            route_name="native_manifest_paths",
            xml_row_tag=effective_row_tag,
            source_batch=source_batch,
        )
    return None


def remote_or_path_sequence_plan_from_prepared_inputs(
    prepared_inputs: list[PreparedPublicInput],
    *,
    input_format: str,
) -> NativeSourcePlan | None:
    """Return a sequence plan when at least one prepared source is remote-native."""
    child_plans: list[NativeSourcePlan] = []
    has_remote = False
    for prepared in prepared_inputs:
        child = source_plan_from_data(prepared.data)
        if child is not None and child.kind == REMOTE_CHUNKS:
            has_remote = True
            child_plans.append(child)
            continue
        sources = prepared_native_sources(prepared, input_format=input_format)
        if not sources:
            return None
        source_batch = PreparedSourceBatch(
            sources=tuple(sources),
            input_format=input_format,
        )
        child_plans.append(
            _native_path_source_plan(
                payload=None,
                input_format=input_format,
                route_name="native_manifest_paths",
                source_batch=source_batch,
            )
        )
    if not has_remote:
        return None
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


def parquet_arrow_plan_from_prepared_inputs(
    prepared_inputs: list[PreparedPublicInput],
    *,
    memory_limit_bytes: int | None,
    call_options: Any,
) -> NativeSourcePlan | None:
    """Return a lazy native Arrow-source Parquet plan when all inputs qualify."""
    sources: list[ParquetDirectorySourceFile] = []
    for prepared in prepared_inputs:
        prepared_sources = _prepared_parquet_sources(prepared)
        if not prepared_sources:
            return None
        sources.extend(prepared_sources)
    if not sources:
        return None
    manifest = ParquetDirectorySourceManifest(
        sources,
        memory_limit_bytes=memory_limit_bytes,
    )
    return source_plan_from_parquet_manifest(manifest)


def source_plan_from_prepared_inputs(
    prepared_inputs: list[PreparedPublicInput],
    *,
    input_format: str,
    input_mode: str,
    input_text_encoding: str,
    xml_row_tag: str | None,
    csv_delimiter: str,
    csv_has_header: bool,
    memory_limit_bytes: int | None,
    call_options: Any,
) -> NativeSourcePlan | None:
    """Build the canonical native source plan for prepared warm-up inputs."""
    if input_format == "parquet":
        return parquet_arrow_plan_from_prepared_inputs(
            prepared_inputs,
            memory_limit_bytes=memory_limit_bytes,
            call_options=call_options,
        )
    remote_plan = remote_or_path_sequence_plan_from_prepared_inputs(
        prepared_inputs,
        input_format=input_format,
    )
    if remote_plan is not None:
        return remote_plan
    return local_path_source_plan_from_prepared_inputs(
        prepared_inputs,
        input_format=input_format,
        input_mode=input_mode,
        input_text_encoding=input_text_encoding,
        xml_row_tag=xml_row_tag,
        csv_delimiter=csv_delimiter,
        csv_has_header=csv_has_header,
        memory_limit_bytes=memory_limit_bytes,
    )


def _probe_path_sources(
    raw_context: Any,
    sources: list[tuple[str, str, str]],
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
) -> Any:
    """Probe one native path-source batch through the best available ABI."""
    kwargs = {
        "registry_json": registry_json,
        "field_name_policy": field_name_policy,
        "schema_mode": schema_mode,
    }
    if native_registry_state is not None:
        kwargs["native_registry_state"] = native_registry_state
    best_effort_probe = getattr(raw_context, "registry_probe_path_sources_best_effort", None)
    if best_effort_probe is not None:
        return best_effort_probe(sources, call_options, **kwargs)
    return raw_context.registry_probe_path_sources(sources, call_options, **kwargs)


def infer_native_multisource_registry(
    raw_context: Any,
    manifest: NativeDirectorySourceManifest,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
) -> Any:
    """Infer and merge one registry across all local manifest child files."""
    plan = source_plan_from_native_manifest(manifest)
    if plan is None:
        from .native_directory_errors import unsupported_native_directory_ingestion

        raise unsupported_native_directory_ingestion()
    return probe_source_plan_registry(
        raw_context,
        plan,
        call_options,
        registry_json=registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        native_registry_state=native_registry_state,
    )


def _remote_registry_probe_chunk_provider_or_none(
    raw_context: Any,
    manifest: RemoteNativeDirectorySourceManifest,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
    retain_chunks: int = 0,
) -> Any | None:
    """Infer a remote registry through the native lazy chunk-provider probe."""
    native_probe_call = getattr(raw_context, "registry_probe_path_source_chunk_provider", None)
    supports_provider = getattr(
        raw_context,
        "supports_registry_probe_path_source_chunk_provider",
        lambda: native_probe_call is not None,
    )
    if native_probe_call is None or not supports_provider():
        return None
    provider = RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=manifest,
        retain_consumed_chunks=retain_chunks,
    )
    try:
        raw = native_probe_call(
            provider,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            native_registry_state=native_registry_state,
            skip_invalid_json_sources=True,
        )
        if retain_chunks <= 0:
            return raw
        retained_file_count = provider.preserved_file_count
        retained_chunks = provider.release_preserved_chunks()
        return SimpleNamespace(
            schema_registry_json=raw.schema_registry_json,
            schema_drifts_json=raw.schema_drifts_json,
            conversion_timestamp=raw.conversion_timestamp,
            field_names=raw.field_names,
            native_registry_state=getattr(raw, "native_registry_state", None),
            retained_chunks=retained_chunks,
            retained_file_count=retained_file_count,
        )
    except Exception:
        provider.close_all()
        raise


def infer_remote_native_multisource_registry(
    raw_context: Any,
    manifest: RemoteNativeDirectorySourceManifest,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
) -> Any:
    """Infer and merge one registry across lazily staged remote chunks."""
    native_probe = _remote_registry_probe_chunk_provider_or_none(
        raw_context,
        manifest,
        call_options,
        registry_json=registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        native_registry_state=native_registry_state,
    )
    if native_probe is not None:
        return native_probe

    current_registry = registry_json
    drifts: list[Any] = []
    conversion_timestamp = ""
    field_names: tuple[str, ...] = ()
    current_native_registry_state: Any = native_registry_state
    with iter_staged_remote_chunks(manifest) as staged_chunks:
        for staged in staged_chunks:
            try:
                raw = infer_native_multisource_registry(
                    raw_context,
                    staged.manifest,
                    call_options,
                    registry_json=current_registry,
                    field_name_policy=field_name_policy,
                    schema_mode=schema_mode,
                    native_registry_state=current_native_registry_state,
                )
                current_registry = raw.schema_registry_json
                conversion_timestamp = raw.conversion_timestamp
                field_names = raw.field_names
                current_native_registry_state = getattr(raw, "native_registry_state", None)
                _append_json_array(drifts, raw.schema_drifts_json)
            finally:
                staged.close()
    return SimpleNamespace(
        schema_registry_json=current_registry,
        schema_drifts_json=json.dumps(drifts, separators=(",", ":")),
        conversion_timestamp=conversion_timestamp,
        field_names=field_names,
        native_registry_state=current_native_registry_state,
    )


def remote_retained_stage_chunks() -> int:
    """Return how many staged remote chunks may be retained between probe and output."""
    return max(0, read_int_env("SCHEMA_SANITIZER_REMOTE_RETAINED_STAGE_CHUNKS", 1))


def infer_bounded_remote_native_multisource_registry(
    raw_context: Any,
    manifest: RemoteNativeDirectorySourceManifest,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    retain_chunks: int | None = None,
    native_registry_state: Any = None,
) -> Any:
    """Infer remote registry while retaining only a bounded staged-chunk window."""
    max_retained = (
        remote_retained_stage_chunks() if retain_chunks is None else max(0, retain_chunks)
    )
    native_probe = _remote_registry_probe_chunk_provider_or_none(
        raw_context,
        manifest,
        call_options,
        registry_json=registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        native_registry_state=native_registry_state,
        retain_chunks=max_retained,
    )
    if native_probe is not None:
        return native_probe

    current_registry = registry_json
    drifts: list[Any] = []
    conversion_timestamp = ""
    field_names: tuple[str, ...] = ()
    current_native_registry_state: Any = native_registry_state
    retained_chunks = []
    retained_file_count = 0
    try:
        with iter_staged_remote_chunks(manifest) as staged_chunks:
            for staged in staged_chunks:
                keep_staged = False
                try:
                    raw = infer_native_multisource_registry(
                        raw_context,
                        staged.manifest,
                        call_options,
                        registry_json=current_registry,
                        field_name_policy=field_name_policy,
                        schema_mode=schema_mode,
                        native_registry_state=current_native_registry_state,
                    )
                    current_registry = raw.schema_registry_json
                    conversion_timestamp = raw.conversion_timestamp
                    field_names = raw.field_names
                    current_native_registry_state = getattr(raw, "native_registry_state", None)
                    _append_json_array(drifts, raw.schema_drifts_json)
                    if len(retained_chunks) < max_retained:
                        retained_chunks.append(staged)
                        retained_file_count += len(staged.manifest.files)
                        keep_staged = True
                finally:
                    if not keep_staged:
                        staged.close()
    except Exception:
        while retained_chunks:
            _close_suppressing_errors(retained_chunks.pop())
        raise
    return SimpleNamespace(
        schema_registry_json=current_registry,
        schema_drifts_json=json.dumps(drifts, separators=(",", ":")),
        conversion_timestamp=conversion_timestamp,
        field_names=field_names,
        native_registry_state=current_native_registry_state,
        retained_chunks=retained_chunks,
        retained_file_count=retained_file_count,
    )


def probe_source_plan_registry(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    native_registry_state: Any = None,
) -> Any:
    """Infer and merge registry state for a native source plan."""
    if plan.kind == PATH_SOURCES:
        return _probe_path_sources(
            raw_context,
            _path_sources_for_native(raw_context, plan),
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            native_registry_state=native_registry_state,
        )
    if plan.kind == REMOTE_CHUNKS:
        return infer_remote_native_multisource_registry(
            raw_context,
            plan.payload,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            native_registry_state=native_registry_state,
        )
    if plan.kind == PARQUET_ARROW_SOURCES:
        return infer_parquet_multisource_registry(
            raw_context,
            plan.payload,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            native_registry_state=native_registry_state,
        )
    if plan.kind == SEQUENCE:
        flattened = _flatten_path_source_sequence_or_none(plan)
        if flattened is not None:
            return probe_source_plan_registry(
                raw_context,
                flattened,
                call_options,
                registry_json=registry_json,
                field_name_policy=field_name_policy,
                schema_mode=schema_mode,
                native_registry_state=native_registry_state,
            )
        current_registry = registry_json
        drifts: list[Any] = []
        conversion_timestamp = ""
        current_native_registry_state: Any = native_registry_state
        for child in plan.payload:
            raw = probe_source_plan_registry(
                raw_context,
                child,
                call_options,
                registry_json=current_registry,
                field_name_policy=field_name_policy,
                schema_mode=schema_mode,
                native_registry_state=current_native_registry_state,
            )
            current_registry = raw.schema_registry_json
            conversion_timestamp = raw.conversion_timestamp
            current_native_registry_state = getattr(raw, "native_registry_state", None)
            _append_json_array(drifts, getattr(raw, "schema_drifts_json", "[]"))
        return _RegistryProbeSummary(
            schema_registry_json=current_registry,
            schema_drifts_json=json.dumps(drifts, separators=(",", ":")),
            conversion_timestamp=conversion_timestamp,
            native_registry_state=current_native_registry_state,
        )
    raise ValueError(f"Unsupported native source plan kind: {plan.kind!r}")


def _probe_source_plan_registry_via_stream_or_none(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
) -> Any | None:
    """Probe a source plan through the same native auto-registry stream as normal runs."""
    if plan.kind != PATH_SOURCES:
        return None
    if getattr(raw_context, "to_registry_sink_path_sources_auto_registry", None) is None:
        return None
    try:
        raw = _path_sources_registry_sink_auto_or_none(
            raw_context,
            plan,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns={},
            timestamp_columns=(),
            skip_invalid_json_sources=True,
        )
    except SchemaSanitizerError as exc:
        if "takes exactly 9 arguments" not in str(exc):
            raise
        return None
    if raw is None:
        return None
    try:
        return SimpleNamespace(
            schema_registry_json=raw.schema_registry_json,
            schema_drifts_json=raw.schema_drifts_json,
            conversion_timestamp=getattr(raw, "conversion_timestamp", ""),
            field_names=getattr(raw, "field_names", ()),
            native_registry_state=getattr(raw, "native_registry_state", None),
        )
    finally:
        _close_suppressing_errors(raw)


def probe_prepared_source_plan_registry(
    raw_context: Any,
    prepared: PreparedPublicInput,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
) -> SourcePlanRegistryProbeResult:
    """Infer and merge registry state from a prepared source-plan input."""
    if prepared.source != "source_plan":
        raise ValueError(f"Unsupported prepared source-plan input: {prepared.source!r}")
    plan = prepared.data
    if not isinstance(plan, NativeSourcePlan):
        raise TypeError("prepared source_plan input must contain a NativeSourcePlan")
    raw = _probe_source_plan_registry_via_stream_or_none(
        raw_context,
        plan,
        call_options,
        registry_json=registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
    )
    if raw is None:
        raw = probe_source_plan_registry(
            raw_context,
            plan,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )
    return SourcePlanRegistryProbeResult(raw=raw, route_name=plan.route_name)


def _write_raw_stream(
    raw: Any,
    out_path: Any,
    *,
    writer: Any,
    feature: str,
) -> Result:
    """Write a native raw stream with native-first file output."""
    result = write_raw_stream_to_file(
        raw,
        out_path,
        writer=writer,
        feature=feature,
        first_row_columns=None,
        all_row_columns=None,
        row_span_columns=None,
        timestamp_columns=(),
    )
    result.schema_registry_json = getattr(raw, "schema_registry_json", None)
    result.schema_drifts_json = getattr(raw, "schema_drifts_json", None)
    result.native_registry_state = getattr(raw, "native_registry_state", None)
    return result


def _write_raw_stream_with_keepalive(
    raw: Any,
    out_path: Any,
    *,
    writer: Any,
    feature: str,
    keepalive: list[Any],
) -> Result:
    """Write a native raw stream while retaining staged resources."""
    try:
        return _write_raw_stream(raw, out_path, writer=writer, feature=feature)
    finally:
        while keepalive:
            _close_suppressing_errors(keepalive.pop())


def _opened_raw_registry_stream(raw: Any) -> OpenedSourcePlanRegistryStream:
    """Wrap a native raw registry stream for source-plan callers."""
    return OpenedSourcePlanRegistryStream(
        stream=None,
        schema_registry_json=raw.schema_registry_json,
        schema_drifts_json=raw.schema_drifts_json,
        native_registry_state=getattr(raw, "native_registry_state", None),
        diagnostics=getattr(raw, "diagnostics", None),
        raw_stream=raw,
        close_items=[raw],
    )


def _remote_manifest_after_files(
    manifest: RemoteNativeDirectorySourceManifest,
    skip_files: int,
) -> RemoteNativeDirectorySourceManifest | None:
    """Return a remote manifest for files after the retained staging window."""
    remaining = list(manifest.files[max(0, skip_files) :])
    if not remaining:
        return None
    return RemoteNativeDirectorySourceManifest(
        remaining,
        input_format=manifest.input_format,
        input_text_encoding=manifest.input_text_encoding,
        csv_delimiter=manifest.csv_delimiter,
        csv_has_header=manifest.csv_has_header,
        xml_row_tag=manifest.xml_row_tag,
        memory_limit_bytes=manifest.memory_limit_bytes,
        chunk_size=manifest.chunk_size,
    )


class RemotePathSourceChunkProvider:
    """Provide staged remote path-source chunks to the native stream at boundaries."""

    def __init__(
        self,
        *,
        retained_chunks: list[Any],
        remaining_manifest: RemoteNativeDirectorySourceManifest | None,
        retain_consumed_chunks: int = 0,
    ) -> None:
        """Store retained chunks and the lazy remaining remote manifest."""
        self._retained_chunks = list(retained_chunks)
        self._remaining_manifest = remaining_manifest
        self._current_staged: Any | None = None
        self._current_staged_preserved = False
        self._remaining_context: Any | None = None
        self._remaining_iter: Any | None = None
        self._retain_consumed_chunks = max(0, int(retain_consumed_chunks))
        self._preserved_chunks: list[Any] = []
        self._preserved_file_count = 0
        self._closed = False

    def next_sources(self) -> Any | None:
        """Return the next staged chunk as a native plan capsule or fallback tuples."""
        if self._closed:
            return None
        self._close_current()
        staged = self._next_staged_chunk()
        if staged is None:
            self.close()
            return None
        try:
            plan = source_plan_from_native_manifest(staged.manifest)
            if plan is None:
                from .native_directory_errors import unsupported_native_directory_ingestion

                raise unsupported_native_directory_ingestion()
            self._current_staged = staged
            self._current_staged_preserved = False
            return plan.native_payload if plan.native_payload is not None else plan.payload
        except Exception:
            _close_suppressing_errors(staged)
            raise

    def close(self) -> None:
        """Close current, retained, and not-yet-opened staged resources."""
        if self._closed:
            return
        self._closed = True
        self._close_current()
        while self._retained_chunks:
            _close_suppressing_errors(self._retained_chunks.pop())
        self._close_remaining_context()

    def __del__(self) -> None:
        """Best-effort cleanup for abandoned providers."""
        with contextlib.suppress(Exception):
            self.close()

    @property
    def preserved_file_count(self) -> int:
        """Return the number of files covered by preserved staged chunks."""
        return self._preserved_file_count

    def release_preserved_chunks(self) -> list[Any]:
        """Transfer preserved staged chunks to the caller."""
        preserved = self._preserved_chunks
        self._preserved_chunks = []
        self._preserved_file_count = 0
        return preserved

    def close_all(self) -> None:
        """Close all provider resources, including preserved chunks."""
        self.close()
        while self._preserved_chunks:
            _close_suppressing_errors(self._preserved_chunks.pop())
        self._preserved_file_count = 0

    def _next_staged_chunk(self) -> Any | None:
        """Return the next retained or newly staged remote chunk."""
        if self._retained_chunks:
            return self._retained_chunks.pop(0)
        if self._remaining_manifest is None:
            return None
        if self._remaining_context is None:
            self._remaining_context = iter_staged_remote_chunks(self._remaining_manifest)
            self._remaining_iter = self._remaining_context.__enter__()
        try:
            return next(self._remaining_iter)
        except StopIteration:
            self._close_remaining_context()
            return None

    def _close_current(self) -> None:
        """Close the currently opened staged chunk."""
        if (
            self._current_staged is not None
            and not self._current_staged_preserved
            and len(self._preserved_chunks) < self._retain_consumed_chunks
        ):
            self._preserved_chunks.append(self._current_staged)
            self._preserved_file_count += len(self._current_staged.manifest.files)
            self._current_staged_preserved = True
        elif not self._current_staged_preserved:
            _close_suppressing_errors(self._current_staged)
        self._current_staged = None
        self._current_staged_preserved = False

    def _close_remaining_context(self) -> None:
        """Close the remaining chunk iterator context."""
        context = self._remaining_context
        self._remaining_context = None
        self._remaining_iter = None
        if context is not None:
            with contextlib.suppress(Exception):
                context.__exit__(None, None, None)


def _remote_registry_sink_bounded_or_none(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    native_registry_state: Any = None,
) -> OpenedSourcePlanRegistryStream | None:
    """Open a remote registry stream with bounded staged chunk retention."""
    registry_probe = infer_bounded_remote_native_multisource_registry(
        raw_context,
        plan.payload,
        call_options,
        registry_json=registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        native_registry_state=native_registry_state,
    )
    registry_out = registry_probe.schema_registry_json
    drifts_out = registry_probe.schema_drifts_json
    merged_first_row_columns = dict(first_row_columns or {})
    merged_first_row_columns.update(
        {
            SCHEMA_REGISTRY_COLUMN: registry_out,
            SCHEMA_DRIFTS_COLUMN: drifts_out,
        }
    )
    retained_chunks = list(registry_probe.retained_chunks)
    try:
        native_provider_call = getattr(
            raw_context,
            "to_registry_sink_path_source_chunk_provider",
            None,
        )
        supports_provider = getattr(
            raw_context,
            "supports_path_source_chunk_provider",
            lambda: native_provider_call is not None,
        )
        provider_state = getattr(registry_probe, "native_registry_state", None)
        if native_provider_call is None or provider_state is None or not supports_provider():
            from .native_directory_errors import unsupported_native_directory_ingestion

            raise unsupported_native_directory_ingestion(
                "Remote registry output requires native path-source chunk-provider support."
            )
        remaining_manifest = _remote_manifest_after_files(
            plan.payload,
            registry_probe.retained_file_count,
        )
        provider = RemotePathSourceChunkProvider(
            retained_chunks=retained_chunks,
            remaining_manifest=remaining_manifest,
        )
        retained_chunks = []
        try:
            raw = native_provider_call(
                "stream",
                provider,
                call_options,
                native_registry_state=provider_state,
                schema_mode=schema_mode,
                first_row_columns=merged_first_row_columns,
                timestamp_columns=timestamp_columns,
            )
        except Exception:
            provider.close()
            raise
        return OpenedSourcePlanRegistryStream(
            stream=None,
            schema_registry_json=registry_out,
            schema_drifts_json=drifts_out,
            native_registry_state=getattr(raw, "native_registry_state", None) or provider_state,
            diagnostics=getattr(raw, "diagnostics", None),
            raw_stream=raw,
            close_items=[raw],
        )
    except Exception:
        while retained_chunks:
            _close_suppressing_errors(retained_chunks.pop())
        raise


def _remote_registry_sink_auto_or_none(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    native_registry_state: Any = None,
) -> OpenedSourcePlanRegistryStream | None:
    """Open a remote registry stream through native paired-provider auto-registry."""
    native_auto_call = getattr(
        raw_context,
        "to_registry_sink_path_source_chunk_provider_auto_registry",
        None,
    )
    supports_auto = getattr(
        raw_context,
        "supports_path_source_chunk_provider_auto_registry",
        lambda: native_auto_call is not None,
    )
    if native_auto_call is None or not supports_auto():
        return None

    probe_provider = RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=plan.payload,
    )
    stream_provider = RemotePathSourceChunkProvider(
        retained_chunks=[],
        remaining_manifest=plan.payload,
    )
    try:
        raw = native_auto_call(
            "stream",
            probe_provider,
            stream_provider,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=dict(first_row_columns or {}),
            timestamp_columns=timestamp_columns,
            native_registry_state=native_registry_state,
            skip_invalid_json_sources=True,
        )
        _mark_native_path_sources_route()
    except Exception:
        probe_provider.close_all()
        stream_provider.close_all()
        raise
    return OpenedSourcePlanRegistryStream(
        stream=None,
        schema_registry_json=raw.schema_registry_json,
        schema_drifts_json=raw.schema_drifts_json,
        native_registry_state=getattr(raw, "native_registry_state", None),
        diagnostics=getattr(raw, "diagnostics", None),
        raw_stream=raw,
        close_items=[raw],
    )


def _path_sources_registry_sink_auto_or_none(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    skip_invalid_json_sources: bool = False,
    native_registry_state: Any = None,
) -> Any | None:
    """Return an auto-registry native stream for a path-source plan when supported."""
    using_native_state = False
    native_call = None
    if native_registry_state is not None:
        native_call = getattr(
            raw_context,
            "to_registry_sink_path_sources_auto_registry_state",
            None,
        )
        using_native_state = native_call is not None
    if native_call is None:
        native_call = getattr(raw_context, "to_registry_sink_path_sources_auto_registry", None)
    if native_call is None:
        return None
    _check_path_source_sizes(plan)
    metadata_first_row_columns = dict(first_row_columns or {})
    metadata_first_row_columns.pop(SCHEMA_REGISTRY_COLUMN, None)
    metadata_first_row_columns.pop(SCHEMA_DRIFTS_COLUMN, None)
    kwargs = {
        "field_name_policy": field_name_policy,
        "schema_mode": schema_mode,
        "first_row_columns": metadata_first_row_columns,
        "timestamp_columns": timestamp_columns,
        "skip_invalid_json_sources": skip_invalid_json_sources,
    }
    if using_native_state:
        kwargs["native_registry_state"] = native_registry_state
    else:
        kwargs["registry_json"] = registry_json
    raw = _call_native(
        native_call,
        "stream",
        _path_sources_for_native(raw_context, plan),
        call_options,
        **kwargs,
    )
    _mark_native_path_sources_route()
    return raw


def _path_sources_registry_sink_or_none(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    drifts_json: str,
    conversion_timestamp: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    output_field_names: tuple[str, ...] | None = None,
    native_registry_state: Any = None,
) -> Any | None:
    """Return a registry stream for a path-source plan."""
    _check_path_source_sizes(plan)
    if output_field_names is not None:
        metadata_names = {
            SOURCE_FILE_COLUMN,
            *first_row_columns,
            *timestamp_columns,
        }
        if set(output_field_names) & metadata_names:
            from .native_directory_errors import unsupported_native_directory_ingestion

            raise unsupported_native_directory_ingestion(
                "The inferred schema collides with reserved metadata columns."
            )
    raw = raw_context.to_registry_sink_path_sources(
        "stream",
        _path_sources_for_native(raw_context, plan),
        call_options,
        registry_json=registry_json,
        drifts_json=drifts_json,
        conversion_timestamp=conversion_timestamp,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        first_row_columns=first_row_columns,
        timestamp_columns=timestamp_columns,
        native_registry_state=native_registry_state,
    )
    _mark_native_path_sources_route()
    return raw


def _path_sources_sink_or_raise(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
) -> Any:
    """Return a strict path-source stream for a source plan."""
    _check_path_source_sizes(plan)
    raw = raw_context.to_sink_path_sources(
        "stream",
        _path_sources_for_native(raw_context, plan),
        call_options,
        include_source_file=True,
        first_row_columns=dict(first_row_columns or {}),
        timestamp_columns=timestamp_columns,
    )
    if raw is None:
        from .native_directory_errors import unsupported_native_directory_ingestion

        raise unsupported_native_directory_ingestion()
    _mark_native_path_sources_route()
    return raw


def open_source_plan_sink_stream_or_none(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    sink: str,
    include_source_file: bool,
    field_name_policy: str,
    feature: str,
) -> Any | None:
    """Open a plain stream sink from the canonical native source plan."""
    if plan.kind == PATH_SOURCES:
        native_supported = getattr(raw_context, "supports_sink_path_sources", None)
        if callable(native_supported) and not native_supported():
            return None
        _check_path_source_sizes(plan)
        raw = raw_context.to_sink_path_sources(
            sink,
            _path_sources_for_native(raw_context, plan),
            call_options,
            include_source_file=include_source_file,
            first_row_columns={},
            timestamp_columns=(),
        )
        _mark_native_path_sources_route()
        return raw
    if plan.kind == REMOTE_CHUNKS:
        native_provider_call = getattr(raw_context, "to_sink_path_source_chunk_provider", None)
        supports_provider = getattr(
            raw_context,
            "supports_sink_path_source_chunk_provider",
            lambda: native_provider_call is not None,
        )
        if native_provider_call is None or not supports_provider():
            return None
        provider = RemotePathSourceChunkProvider(
            retained_chunks=[],
            remaining_manifest=plan.payload,
        )
        try:
            raw = native_provider_call(
                sink,
                provider,
                call_options,
                include_source_file=include_source_file,
                first_row_columns={},
                timestamp_columns=(),
            )
            _mark_native_path_sources_route()
            return raw
        except Exception:
            provider.close()
            raise
    if plan.kind == SEQUENCE:
        flattened = _flatten_path_source_sequence_or_none(plan)
        if flattened is not None:
            return open_source_plan_sink_stream_or_none(
                raw_context,
                flattened,
                call_options,
                sink=sink,
                include_source_file=include_source_file,
                field_name_policy=field_name_policy,
                feature=feature,
            )
    return None


def _call_native(call: Any, *args: Any, **kwargs: Any) -> Any:
    """Call a native ABI function through the shared error translation helper."""
    from .shared import _call_core

    return _call_core(call, *args, **kwargs)


def _mark_native_path_sources_route() -> None:
    """Preserve the native multi-source diagnostic route marker."""
    global _LAST_NATIVE_MULTISOURCE_ROUTE
    _LAST_NATIVE_MULTISOURCE_ROUTE = "cxx_path_sources"


def open_source_plan_registry_stream(
    raw_context: Any,
    plan: NativeSourcePlan,
    call_options: Any,
    *,
    registry_json: str,
    field_name_policy: str,
    schema_mode: str,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    feature: str,
    native_registry_state: Any = None,
) -> OpenedSourcePlanRegistryStream | None:
    """Open a registry-backed stream from the canonical native source plan."""
    if plan.kind == SEQUENCE:
        flattened = _flatten_path_source_sequence_or_none(plan)
        if flattened is not None:
            return open_source_plan_registry_stream(
                raw_context,
                flattened,
                call_options,
                registry_json=registry_json,
                field_name_policy=field_name_policy,
                schema_mode=schema_mode,
                first_row_columns=first_row_columns,
                timestamp_columns=timestamp_columns,
                feature=feature,
                native_registry_state=native_registry_state,
            )

    if plan.kind == PATH_SOURCES:
        raw = _path_sources_registry_sink_auto_or_none(
            raw_context,
            plan,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            native_registry_state=native_registry_state,
        )
        if raw is not None:
            return _opened_raw_registry_stream(raw)

        registry_probe = probe_source_plan_registry(
            raw_context,
            plan,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
        )
        registry_out = registry_probe.schema_registry_json
        drifts_out = registry_probe.schema_drifts_json
        merged_first_row_columns = dict(first_row_columns or {})
        merged_first_row_columns.update(
            {
                SCHEMA_REGISTRY_COLUMN: registry_out,
                SCHEMA_DRIFTS_COLUMN: drifts_out,
            }
        )
        raw = _path_sources_registry_sink_or_none(
            raw_context,
            plan,
            call_options,
            registry_json=registry_out,
            drifts_json=drifts_out,
            conversion_timestamp=registry_probe.conversion_timestamp,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=merged_first_row_columns,
            timestamp_columns=timestamp_columns,
            output_field_names=getattr(registry_probe, "field_names", None),
            native_registry_state=getattr(registry_probe, "native_registry_state", None),
        )
        if raw is None:
            return None
        return _opened_raw_registry_stream(raw)

    if plan.kind == REMOTE_CHUNKS:
        opened = _remote_registry_sink_auto_or_none(
            raw_context,
            plan,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            native_registry_state=native_registry_state,
        )
        if opened is not None:
            return opened
        return _remote_registry_sink_bounded_or_none(
            raw_context,
            plan,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            native_registry_state=native_registry_state,
        )

    if plan.kind == PARQUET_ARROW_SOURCES:
        from .parquet_multisource import parquet_multisource_registry_sink_raw_or_none

        raw = parquet_multisource_registry_sink_raw_or_none(
            raw_context,
            plan.payload,
            call_options,
            registry_json=registry_json,
            field_name_policy=field_name_policy,
            schema_mode=schema_mode,
            first_row_columns=first_row_columns,
            timestamp_columns=timestamp_columns,
            native_registry_state=native_registry_state,
        )
        if raw is None:
            return None
        return _opened_raw_registry_stream(raw)

    return None


def write_source_plan_registry_to_file(
    raw_context: Any,
    plan: NativeSourcePlan,
    out_path: Any,
    *,
    writer: Any,
    feature: str,
    call_options: Options | None,
    first_row_columns: dict[str, Any],
    timestamp_columns: tuple[str, ...],
    schema_registry_json: str,
    schema_mode: str,
    field_name_policy: str,
    native_registry_state: Any = None,
    parquet_compression: str | None = None,
    parquet_gzip_level: int | None = None,
) -> Result | None:
    """Write a registry-backed output from the canonical native source plan."""
    opened = open_source_plan_registry_stream(
        raw_context,
        plan,
        _unwrap_options(call_options),
        registry_json=schema_registry_json,
        field_name_policy=field_name_policy,
        schema_mode=schema_mode,
        first_row_columns=dict(first_row_columns or {}),
        timestamp_columns=timestamp_columns,
        feature=feature,
        native_registry_state=native_registry_state,
    )
    if opened is None:
        return None
    return write_opened_registry_stream_to_file(
        opened,
        out_path,
        writer=writer,
        feature=feature,
        parquet_compression=parquet_compression,
        parquet_gzip_level=parquet_gzip_level,
    )


@dataclass(frozen=True, slots=True)
class _RegistryProbeSummary:
    """Small probe result used when a sequence combines multiple native probes."""

    schema_registry_json: str
    schema_drifts_json: str
    conversion_timestamp: str
    native_registry_state: Any = None
