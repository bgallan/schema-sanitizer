"""Classify transport and lifetime routes for concurrency certification.

Prepared inputs and outputs are reduced to route profiles, then mapped to the shared mechanisms
whose evidence is required for each combination.
"""

from __future__ import annotations

import os
from types import MappingProxyType
from typing import Any

from .uris import looks_like_remote_uri

_COMMON_ROUTE_REQUIREMENTS = (
    "transferable_resident_memory_credit",
    "composite_slot_and_byte_admission",
    "process_control_plane_budget",
    "native_payload_core_call",
    "operation_cancellation_checkpoint",
)

# Transport/lifetime profiles deliberately differ. Release certification must
# prove the mechanisms that are specific to a route instead of allowing a
# global observation from an unrelated route to satisfy it.
_INPUT_ROUTE_REQUIREMENTS = {
    "local_path": (*_COMMON_ROUTE_REQUIREMENTS, "process_file_descriptor_admission"),
    "remote_chunks": (
        *_COMMON_ROUTE_REQUIREMENTS,
        "stage_concurrency_admission",
        "process_file_descriptor_admission",
    ),
    "directory_source_plan": (
        *_COMMON_ROUTE_REQUIREMENTS,
        "stage_concurrency_admission",
        "process_file_descriptor_admission",
    ),
    "materialized_memory": _COMMON_ROUTE_REQUIREMENTS,
    "python_iterator": (*_COMMON_ROUTE_REQUIREMENTS, "stage_concurrency_admission"),
    "staged_remote": (
        *_COMMON_ROUTE_REQUIREMENTS,
        "stage_concurrency_admission",
        "process_file_descriptor_admission",
    ),
}

_OUTPUT_ROUTE_REQUIREMENTS = {
    "local_file": (*_COMMON_ROUTE_REQUIREMENTS, "process_file_descriptor_admission"),
    "remote_staged_commit": (
        *_COMMON_ROUTE_REQUIREMENTS,
        "stage_concurrency_admission",
        "process_file_descriptor_admission",
    ),
    "stream": _COMMON_ROUTE_REQUIREMENTS,
    "analytical_adapter": (*_COMMON_ROUTE_REQUIREMENTS, "external_runtime_pool_claim"),
}

INPUT_ROUTE_PROFILE_REQUIREMENTS = MappingProxyType(_INPUT_ROUTE_REQUIREMENTS)
OUTPUT_ROUTE_PROFILE_REQUIREMENTS = MappingProxyType(_OUTPUT_ROUTE_REQUIREMENTS)


def input_route_profile(prepared_input: Any) -> str:
    """Classify the transport/lifetime path after public input preparation."""
    source = getattr(prepared_input, "source", None)
    data = getattr(prepared_input, "data", None)
    source_file = getattr(prepared_input, "source_file", None)
    if source == "python":
        return "python_iterator"
    if data is not None and hasattr(data, "remote_native_multisource_manifest"):
        return "remote_chunks"
    if data is not None and (
        hasattr(data, "native_multisource_manifest")
        or hasattr(data, "native_parquet_multisource_manifest")
    ):
        if isinstance(source_file, str) and looks_like_remote_uri(source_file):
            return "staged_remote"
        return "directory_source_plan"
    if isinstance(source_file, str) and looks_like_remote_uri(source_file):
        return "staged_remote"
    if source in {"path", "file", "mmap"}:
        return "local_path"
    # Streams/buffers/readers that are already materialized or caller-owned do
    # not require path lifetime, even when the native layer batches them later.
    return "materialized_memory"


def output_file_route_profile(output_path: str | os.PathLike[str]) -> str:
    """Classify a file output as local publication or staged remote commit."""
    value = os.fspath(output_path)
    return "remote_staged_commit" if looks_like_remote_uri(value) else "local_file"


def analytical_output_route_profile(target: str) -> str:
    """Classify an analytical target as a stream or external adapter route."""
    return "stream" if target == "pyarrow_reader" else "analytical_adapter"


__all__ = [
    "INPUT_ROUTE_PROFILE_REQUIREMENTS",
    "OUTPUT_ROUTE_PROFILE_REQUIREMENTS",
    "analytical_output_route_profile",
    "input_route_profile",
    "output_file_route_profile",
]
