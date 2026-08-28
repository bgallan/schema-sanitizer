"""Convert attached directory manifests into canonical native source plans.

It converts attached local or remote directory manifests into native source plans
without repeating discovery or losing source identity.
"""

from __future__ import annotations

from typing import Any

from schema_sanitizer.input_impl.source_plan import (
    PARQUET_ARROW_SOURCES,
    REMOTE_CHUNKS,
    NativeSourcePlan,
    _native_path_source_plan,
)

from ...input_impl.prepared import NativeDirectorySourceManifest
from ..input.directory_preparation import RemoteNativeDirectorySourceManifest
from ..parquet.multisource import ParquetDirectorySourceManifest


def native_multisource_manifest_from_data(data: Any) -> NativeDirectorySourceManifest | None:
    """Return an attached local native multi-source manifest."""
    manifest = getattr(data, "native_multisource_manifest", None)
    return manifest if isinstance(manifest, NativeDirectorySourceManifest) else None


def remote_native_multisource_manifest_from_data(
    data: Any,
) -> RemoteNativeDirectorySourceManifest | None:
    """Return an attached lazy remote native multi-source manifest."""
    manifest = getattr(data, "remote_native_multisource_manifest", None)
    return manifest if isinstance(manifest, RemoteNativeDirectorySourceManifest) else None


def source_plan_from_native_manifest(
    manifest: NativeDirectorySourceManifest,
) -> NativeSourcePlan | None:
    """Return a native path-source plan from a local manifest."""
    source_batch = manifest.source_batch
    return _native_path_source_plan(
        source_batch=source_batch,
        input_format=source_batch.input_format,
        route_name="native_manifest_paths",
        xml_row_tag=source_batch.xml_row_tag,
    )


def source_plan_from_remote_manifest(
    manifest: RemoteNativeDirectorySourceManifest,
) -> NativeSourcePlan:
    """Return a staged-chunk plan from a remote native manifest."""
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
    """Return an attached native source plan from prepared input data."""
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
