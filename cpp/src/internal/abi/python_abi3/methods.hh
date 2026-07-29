// Declares the complete internal Python ABI3 method catalogue.

#pragma once

#include "internal/abi/python_abi3/base.hh"

namespace core_abi3_internal {

// Context, diagnostics, and prepared options.
PyObject *py_context_new(PyObject *, PyObject *);
PyObject *py_context_memory_stats_json(PyObject *, PyObject *);
PyObject *py_context_performance_stats_json(PyObject *, PyObject *);
PyObject *py_diagnostics_json(PyObject *, PyObject *);
PyObject *py_options_catalog(PyObject *, PyObject *);
PyObject *py_memory_budget(PyObject *, PyObject *);
PyObject *py_process_memory_governor_stats(PyObject *, PyObject *);
PyObject *py_execution_policy(PyObject *, PyObject *);
PyObject *py_ordered_executor_probe(PyObject *, PyObject *);
PyObject *py_ordered_executor_arena_completion_probe(PyObject *, PyObject *);
PyObject *py_operation_task_arena_probe(PyObject *, PyObject *);
PyObject *py_operation_task_arena_stealing_probe(PyObject *, PyObject *);
PyObject *py_operation_task_arena_mixed_lane_probe(PyObject *, PyObject *);
PyObject *py_operation_task_arena_concurrent_submit_probe(PyObject *,
                                                          PyObject *);
PyObject *py_operation_task_arena_wake_coalescing_probe(PyObject *, PyObject *);
PyObject *py_operation_task_arena_output_preference_probe(PyObject *,
                                                          PyObject *);
PyObject *py_operation_task_arena_output_steal_probe(PyObject *, PyObject *);
PyObject *py_output_worker_admission_probe(PyObject *, PyObject *);
PyObject *py_operation_task_arena_cancellation_probe(PyObject *, PyObject *);
PyObject *py_options_prepare_bytes(PyObject *, PyObject *);
PyObject *py_options_with_detected_at(PyObject *, PyObject *);

// Logical-schema payloads.
PyObject *py_logical_schema_payload_validate(PyObject *, PyObject *);
PyObject *py_logical_schema_payload_field_names(PyObject *, PyObject *);
PyObject *py_logical_schema_payload_arrow_c_schema(PyObject *, PyObject *);

// Metadata, stream wrappers, writers, and format tools.
PyObject *py_file_metadata_columns(PyObject *, PyObject *);
PyObject *py_metadata_stream_wrap(PyObject *, PyObject *);
PyObject *py_coalescing_stream_wrap(PyObject *, PyObject *);
PyObject *py_csv_nested_stream_wrap(PyObject *, PyObject *);
PyObject *py_csv_stream_write(PyObject *, PyObject *);
PyObject *py_csv_stream_write_with_metadata(PyObject *, PyObject *);
PyObject *py_csv_schema_supported(PyObject *, PyObject *);
PyObject *py_parquet_stream_write(PyObject *, PyObject *);
PyObject *py_parquet_stream_write_with_metadata(PyObject *, PyObject *);
PyObject *py_parquet_footer_info_json(PyObject *, PyObject *);
PyObject *py_parquet_stream_preflight_json(PyObject *, PyObject *);
PyObject *py_parquet_stream_read(PyObject *, PyObject *);
PyObject *py_jsonl_stream_write(PyObject *, PyObject *);
PyObject *py_jsonl_stream_write_with_metadata(PyObject *, PyObject *);
PyObject *py_jsonl_batch_bytes(PyObject *, PyObject *);
PyObject *py_jsonl_batches_bytes(PyObject *, PyObject *);
PyObject *py_json_compact_bytes(PyObject *, PyObject *);
PyObject *py_json_array_to_jsonl_bytes(PyObject *, PyObject *);
PyObject *py_json_array_files_to_jsonl_bytes(PyObject *, PyObject *);
PyObject *py_xml_folder_effective_row_tag(PyObject *, PyObject *);
PyObject *py_python_row_json_bytes(PyObject *, PyObject *);
PyObject *py_python_rows_jsonl_bytes(PyObject *, PyObject *);
PyObject *py_python_iter_rows_jsonl_bytes(PyObject *, PyObject *);
PyObject *py_jsonl_schema_supported(PyObject *, PyObject *);
PyObject *py_arrow_direct_schema_supported(PyObject *, PyObject *);
PyObject *py_arrow_schema_contract_payload(PyObject *, PyObject *);

// Schema registry and probes.
PyObject *py_schema_registry_merge(PyObject *, PyObject *);
PyObject *py_schema_registry_empty(PyObject *, PyObject *);
PyObject *py_schema_registry_contract_payload(PyObject *, PyObject *);
PyObject *py_registry_state_from_json(PyObject *, PyObject *);
PyObject *py_context_schema_probe_from_source(PyObject *, PyObject *);
PyObject *py_context_registry_probe_from_source(PyObject *, PyObject *);
PyObject *py_context_schema_probe_from_paths(PyObject *, PyObject *);
PyObject *py_context_registry_probe_from_paths(PyObject *, PyObject *);
PyObject *py_context_registry_probe_from_path_sources(PyObject *, PyObject *);
PyObject *
py_context_registry_probe_from_path_sources_registry_state(PyObject *,
                                                           PyObject *);
PyObject *py_context_registry_probe_from_path_sources_best_effort(PyObject *,
                                                                  PyObject *);
PyObject *
py_context_registry_probe_from_path_sources_best_effort_registry_state(
    PyObject *, PyObject *);
PyObject *py_context_registry_probe_from_path_source_chunk_provider(PyObject *,
                                                                    PyObject *);
PyObject *
py_context_registry_probe_from_path_source_chunk_provider_registry_state(
    PyObject *, PyObject *);
PyObject *py_context_registry_probe_from_arrow_sources(PyObject *, PyObject *);
PyObject *
py_context_registry_probe_from_arrow_sources_registry_state(PyObject *,
                                                            PyObject *);
PyObject *py_path_source_plan_create(PyObject *, PyObject *);

// Plain and registry-backed sinks.
PyObject *py_context_to_sink_from_source(PyObject *, PyObject *);
PyObject *py_context_to_sink_from_path_sources(PyObject *, PyObject *);
PyObject *py_context_to_sink_from_path_source_chunk_provider(PyObject *,
                                                             PyObject *);
PyObject *py_context_to_sink_arrow_stream(PyObject *, PyObject *);
PyObject *py_context_to_registry_sink_from_source(PyObject *, PyObject *);
PyObject *py_context_to_registry_sink_from_path_sources(PyObject *, PyObject *);
PyObject *
py_context_to_registry_sink_from_path_sources_registry_state(PyObject *,
                                                             PyObject *);
PyObject *
py_context_to_registry_sink_from_path_source_chunk_provider_registry_state(
    PyObject *, PyObject *);
PyObject *
py_context_to_registry_sink_from_path_source_chunk_provider_auto_registry(
    PyObject *, PyObject *);
PyObject *
py_context_to_registry_sink_from_path_source_chunk_provider_auto_registry_state(
    PyObject *, PyObject *);
PyObject *
py_context_to_registry_sink_from_path_sources_auto_registry(PyObject *,
                                                            PyObject *);
PyObject *
py_context_to_registry_sink_from_path_sources_auto_registry_state(PyObject *,
                                                                  PyObject *);
PyObject *py_context_to_registry_sink_arrow_stream(PyObject *, PyObject *);
PyObject *py_context_to_registry_sink_arrow_sources(PyObject *, PyObject *);
PyObject *py_context_to_registry_sink_arrow_sources_registry_state(PyObject *,
                                                                   PyObject *);
PyObject *py_context_to_registry_sink_arrow_sources_auto_registry(PyObject *,
                                                                  PyObject *);
PyObject *
py_context_to_registry_sink_arrow_sources_auto_registry_state(PyObject *,
                                                              PyObject *);
PyObject *
py_context_to_registry_sink_arrow_source_chunk_provider_registry_state(
    PyObject *, PyObject *);
PyObject *py_context_to_registry_sink_arrow_source_chunk_provider_auto_registry(
    PyObject *, PyObject *);
PyObject *
py_context_to_registry_sink_arrow_source_chunk_provider_auto_registry_state(
    PyObject *, PyObject *);

} // namespace core_abi3_internal
