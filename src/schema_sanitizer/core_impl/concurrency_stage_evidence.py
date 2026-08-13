"""Format-specific runtime stage evidence for release certification.

Shared admission contracts prove that every public pair participates in the
same bounded runtime.  This module adds a second dimension: one primary stage
that is specific to each input and output route is recorded only after that
route successfully executes.  Release certification can therefore detect a
format path that silently stops traversing the concurrency mechanism advertised
for that format while the generic pair contracts continue to pass.
"""

from __future__ import annotations

from types import MappingProxyType

from .concurrency_contracts import observe_runtime_concurrency_stage_noexcept

_INPUT_PRIMARY_STAGE = {
    "csv": "adaptive_vector_record_framing",
    "json": "worker_authoritative_structural_framing",
    "json_array": "worker_authoritative_structural_framing",
    "jsonl": "row_validation",
    "ndjson": "row_validation",
    "xml": "frontend_row_decode",
    "parquet": "column_decode",
    "python": "native_iterator_batching",
}

_OUTPUT_PRIMARY_STAGE = {
    "csv": "native_parallel_sink",
    "jsonl": "native_parallel_sink",
    "parquet": "ordered_row_group_overlap",
    "pyarrow": "arrow_c_stream_table_materialization",
    "pandas": "threaded_adapter_conversion",
    "polars": "chunk_preserving_no_rechunk_conversion",
    "duckdb": "record_batch_reader_direct_duckdb_handoff",
}

INPUT_PRIMARY_RUNTIME_STAGE = MappingProxyType(_INPUT_PRIMARY_STAGE)
OUTPUT_PRIMARY_RUNTIME_STAGE = MappingProxyType(_OUTPUT_PRIMARY_STAGE)


def observe_successful_input_runtime_stage(input_format: str) -> None:
    """Record the primary input stage after a real pair payload succeeds."""
    stage = INPUT_PRIMARY_RUNTIME_STAGE.get(input_format)
    if stage is not None:
        observe_runtime_concurrency_stage_noexcept(stage)


def observe_successful_output_runtime_stage(output_format: str) -> None:
    """Record the primary output stage after a real pair payload succeeds."""
    stage = OUTPUT_PRIMARY_RUNTIME_STAGE.get(output_format)
    if stage is not None:
        observe_runtime_concurrency_stage_noexcept(stage)


__all__ = [
    "INPUT_PRIMARY_RUNTIME_STAGE",
    "OUTPUT_PRIMARY_RUNTIME_STAGE",
    "observe_successful_input_runtime_stage",
    "observe_successful_output_runtime_stage",
]
