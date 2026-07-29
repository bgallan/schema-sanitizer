/* ABI3 module initializer, definition, and method table. */
#include "internal/abi/python_abi3/base.hh"

#include "internal/abi/python_abi3/methods.hh"

#include <array>

namespace core_abi3_internal {
namespace {

auto kMethods = std::to_array<PyMethodDef>({
    // Context
    {.ml_name = "context_new",
     .ml_meth = _PyCFunction_CAST(py_context_new),
     .ml_flags = METH_NOARGS,
     .ml_doc = "Create an execution context."},
    {.ml_name = "context_memory_stats_json",
     .ml_meth = _PyCFunction_CAST(py_context_memory_stats_json),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Return memory stats JSON for a context."},
    {.ml_name = "context_performance_stats_json",
     .ml_meth = _PyCFunction_CAST(py_context_performance_stats_json),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Return latest operation performance telemetry as JSON."},
    {.ml_name = "diagnostics_json",
     .ml_meth = _PyCFunction_CAST(py_diagnostics_json),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Return live diagnostics JSON for a diagnostics capsule."},

    // Options
    {.ml_name = "memory_budget",
     .ml_meth = _PyCFunction_CAST(py_memory_budget),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Derive every internal resource budget from one memory limit."},
    {.ml_name = "process_memory_governor_stats",
     .ml_meth = _PyCFunction_CAST(py_process_memory_governor_stats),
     .ml_flags = METH_NOARGS,
     .ml_doc = "Return process-wide operation memory lease diagnostics."},
    {.ml_name = "execution_policy",
     .ml_meth = _PyCFunction_CAST(py_execution_policy),
     .ml_flags = METH_VARARGS,
     .ml_doc =
         "Derive deterministic execution limits from mode, memory, and CPUs."},
    {.ml_name = "ordered_executor_probe",
     .ml_meth = _PyCFunction_CAST(py_ordered_executor_probe),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Exercise bounded ordinal execution for internal tests."},
    {.ml_name = "ordered_executor_arena_completion_probe",
     .ml_meth = _PyCFunction_CAST(py_ordered_executor_arena_completion_probe),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Measure bounded mutex-free arena completion publication."},
    {.ml_name = "operation_task_arena_probe",
     .ml_meth = _PyCFunction_CAST(py_operation_task_arena_probe),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Exercise two ordered stages on one bounded operation arena."},
    {.ml_name = "operation_task_arena_stealing_probe",
     .ml_meth = _PyCFunction_CAST(py_operation_task_arena_stealing_probe),
     .ml_flags = METH_NOARGS,
     .ml_doc = "Verify lane-safe work stealing under a skewed native load."},
    {.ml_name = "operation_task_arena_mixed_lane_probe",
     .ml_meth = _PyCFunction_CAST(py_operation_task_arena_mixed_lane_probe),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Measure mixed-lane stealing on a high-core operation arena."},
    {.ml_name = "operation_task_arena_concurrent_submit_probe",
     .ml_meth =
         _PyCFunction_CAST(py_operation_task_arena_concurrent_submit_probe),
     .ml_flags = METH_VARARGS,
     .ml_doc =
         "Verify exact arena admission totals under concurrent producers."},
    {.ml_name = "operation_task_arena_wake_coalescing_probe",
     .ml_meth =
         _PyCFunction_CAST(py_operation_task_arena_wake_coalescing_probe),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Verify targeted wake epochs and running-worker coalescing."},
    {.ml_name = "operation_task_arena_output_preference_probe",
     .ml_meth =
         _PyCFunction_CAST(py_operation_task_arena_output_preference_probe),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Verify local high-core output-lane queue preference."},
    {.ml_name = "operation_task_arena_output_steal_probe",
     .ml_meth = _PyCFunction_CAST(py_operation_task_arena_output_steal_probe),
     .ml_flags = METH_VARARGS,
     .ml_doc =
         "Verify high-core output preference during compatible stealing."},
    {.ml_name = "output_worker_admission_probe",
     .ml_meth = _PyCFunction_CAST(py_output_worker_admission_probe),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Exercise high-core text-output worker admission."},
    {.ml_name = "operation_task_arena_cancellation_probe",
     .ml_meth = _PyCFunction_CAST(py_operation_task_arena_cancellation_probe),
     .ml_flags = METH_NOARGS,
     .ml_doc = "Verify stage-local cancellation inside the operation arena."},
    {.ml_name = "options_catalog",
     .ml_meth = _PyCFunction_CAST(py_options_catalog),
     .ml_flags = METH_NOARGS,
     .ml_doc =
         "Return canonical option names, wire kinds, defaults, and groups."},
    {.ml_name = "options_prepare_bytes",
     .ml_meth = _PyCFunction_CAST(py_options_prepare_bytes),
     .ml_flags = METH_VARARGS,
     .ml_doc =
         "Validate serialized options and return an internal runtime capsule."},
    {.ml_name = "options_with_detected_at",
     .ml_meth = _PyCFunction_CAST(py_options_with_detected_at),
     .ml_flags = METH_VARARGS,
     .ml_doc =
         "Clone prepared options with internal operation timestamp metadata."},
    {.ml_name = "schema_registry_merge",
     .ml_meth = _PyCFunction_CAST(py_schema_registry_merge),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Merge inferred logical schema and registry state."},
    {.ml_name = "schema_registry_empty",
     .ml_meth = _PyCFunction_CAST(py_schema_registry_empty),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Return an empty schema-registry JSON document."},
    {.ml_name = "schema_registry_contract_payload",
     .ml_meth = _PyCFunction_CAST(py_schema_registry_contract_payload),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Encode registry canonical_schema as a logical schema contract "
               "payload."},
    {.ml_name = "registry_state_from_json",
     .ml_meth = _PyCFunction_CAST(py_registry_state_from_json),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Compile registry JSON into a reusable native registry-state "
               "capsule."},
    {.ml_name = "logical_schema_payload_validate",
     .ml_meth = _PyCFunction_CAST(py_logical_schema_payload_validate),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Validate a native logical schema payload."},
    {.ml_name = "logical_schema_payload_field_names",
     .ml_meth = _PyCFunction_CAST(py_logical_schema_payload_field_names),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Return top-level field names from a logical schema payload."},
    {.ml_name = "logical_schema_payload_arrow_c_schema",
     .ml_meth = _PyCFunction_CAST(py_logical_schema_payload_arrow_c_schema),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Export a logical schema payload as an Arrow C schema capsule."},
    {.ml_name = "context_schema_probe_from_source",
     .ml_meth = _PyCFunction_CAST(py_context_schema_probe_from_source),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer a logical schema from a source-selected input."},
    {.ml_name = "context_registry_probe_from_source",
     .ml_meth = _PyCFunction_CAST(py_context_registry_probe_from_source),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer and merge registry state from a source-selected input."},
    {.ml_name = "context_schema_probe_from_paths",
     .ml_meth = _PyCFunction_CAST(py_context_schema_probe_from_paths),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer a logical schema from multiple local files."},
    {.ml_name = "context_registry_probe_from_paths",
     .ml_meth = _PyCFunction_CAST(py_context_registry_probe_from_paths),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer and merge registry state from multiple local files."},
    {.ml_name = "context_registry_probe_from_path_sources",
     .ml_meth = _PyCFunction_CAST(py_context_registry_probe_from_path_sources),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer and merge registry state from multiple native path "
               "sources."},
    {.ml_name = "context_registry_probe_from_path_sources_registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_registry_probe_from_path_sources_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer and merge registry state from multiple native path "
               "sources using a compiled native registry-state capsule."},
    {.ml_name = "context_registry_probe_from_path_sources_best_effort",
     .ml_meth = _PyCFunction_CAST(
         py_context_registry_probe_from_path_sources_best_effort),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer and merge registry state from multiple native path "
               "sources, skipping JSON parse failures."},
    {.ml_name =
         "context_registry_probe_from_path_sources_best_effort_registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_registry_probe_from_path_sources_best_effort_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer and merge registry state from multiple native path "
               "sources using a compiled native registry-state capsule, "
               "skipping JSON parse failures."},
    {.ml_name = "context_registry_probe_from_path_source_chunk_provider",
     .ml_meth = _PyCFunction_CAST(
         py_context_registry_probe_from_path_source_chunk_provider),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer and merge registry state from lazily provided "
               "path-source chunks."},
    {.ml_name = "context_registry_probe_from_path_source_chunk_provider_"
                "registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_registry_probe_from_path_source_chunk_provider_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer and merge registry state from lazily provided "
               "path-source chunks using a compiled native registry-state "
               "capsule."},
    {.ml_name = "path_source_plan_create",
     .ml_meth = _PyCFunction_CAST(py_path_source_plan_create),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Parse native path-source descriptors into a reusable native "
               "plan capsule."},
    {.ml_name = "file_metadata_columns",
     .ml_meth = _PyCFunction_CAST(py_file_metadata_columns),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Build generated file metadata columns."},
    {.ml_name = "metadata_stream_wrap",
     .ml_meth = _PyCFunction_CAST(py_metadata_stream_wrap),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Wrap an Arrow C stream with string metadata columns."},
    {.ml_name = "coalescing_stream_wrap",
     .ml_meth = _PyCFunction_CAST(py_coalescing_stream_wrap),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Wrap a supported Arrow C stream and coalesce small batches."},
    {.ml_name = "csv_nested_stream_wrap",
     .ml_meth = _PyCFunction_CAST(py_csv_nested_stream_wrap),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Wrap an Arrow C stream with nested columns rendered as JSON "
               "strings."},
    {.ml_name = "csv_stream_write",
     .ml_meth = _PyCFunction_CAST(py_csv_stream_write),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Write a supported Arrow C stream to a local CSV file."},
    {.ml_name = "csv_stream_write_with_metadata",
     .ml_meth = _PyCFunction_CAST(py_csv_stream_write_with_metadata),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Write a metadata-augmented Arrow C stream to a local CSV "
               "file."},
    {.ml_name = "csv_schema_supported",
     .ml_meth = _PyCFunction_CAST(py_csv_schema_supported),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Return whether a PyArrow schema is supported by the native "
               "CSV writer."},
    {.ml_name = "parquet_stream_write",
     .ml_meth = _PyCFunction_CAST(py_parquet_stream_write),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Write a supported Arrow C stream to a local Parquet file."},
    {.ml_name = "parquet_stream_write_with_metadata",
     .ml_meth = _PyCFunction_CAST(py_parquet_stream_write_with_metadata),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Write a metadata-augmented Arrow C stream to a local Parquet "
               "file."},
    {.ml_name = "parquet_footer_info_json",
     .ml_meth = _PyCFunction_CAST(py_parquet_footer_info_json),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Return bounded native Parquet footer metadata as JSON."},
    {.ml_name = "parquet_stream_preflight_json",
     .ml_meth = _PyCFunction_CAST(py_parquet_stream_preflight_json),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Validate native Parquet stream readiness with bounded memory."},
    {.ml_name = "parquet_stream_read",
     .ml_meth = _PyCFunction_CAST(py_parquet_stream_read),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Open a supported local Parquet file as an Arrow C stream."},
    {.ml_name = "jsonl_stream_write",
     .ml_meth = _PyCFunction_CAST(py_jsonl_stream_write),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Write a supported Arrow C stream to a local JSONL file."},
    {.ml_name = "jsonl_stream_write_with_metadata",
     .ml_meth = _PyCFunction_CAST(py_jsonl_stream_write_with_metadata),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Write a metadata-augmented Arrow C stream to a local JSONL "
               "file."},
    {.ml_name = "jsonl_batch_bytes",
     .ml_meth = _PyCFunction_CAST(py_jsonl_batch_bytes),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Encode one supported PyArrow record batch as JSONL bytes."},
    {.ml_name = "jsonl_batches_bytes",
     .ml_meth = _PyCFunction_CAST(py_jsonl_batches_bytes),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Encode supported PyArrow record batches as JSONL bytes."},
    {.ml_name = "json_compact_bytes",
     .ml_meth = _PyCFunction_CAST(py_json_compact_bytes),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Compact one JSON document to UTF-8 bytes."},
    {.ml_name = "json_array_to_jsonl_bytes",
     .ml_meth = _PyCFunction_CAST(py_json_array_to_jsonl_bytes),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Split one JSON array of objects to UTF-8 JSONL bytes."},
    {.ml_name = "json_array_files_to_jsonl_bytes",
     .ml_meth = _PyCFunction_CAST(py_json_array_files_to_jsonl_bytes),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Read and split local JSON array files to JSON Lines bytes."},
    {.ml_name = "xml_folder_effective_row_tag",
     .ml_meth = _PyCFunction_CAST(py_xml_folder_effective_row_tag),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Return the validated effective row tag for local XML files."},
    {.ml_name = "python_row_json_bytes",
     .ml_meth = _PyCFunction_CAST(py_python_row_json_bytes),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Encode one JSON-like Python row to UTF-8 bytes."},
    {.ml_name = "python_rows_jsonl_bytes",
     .ml_meth = _PyCFunction_CAST(py_python_rows_jsonl_bytes),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Encode JSON-like Python rows to UTF-8 JSONL bytes."},
    {.ml_name = "python_iter_rows_jsonl_bytes",
     .ml_meth = _PyCFunction_CAST(py_python_iter_rows_jsonl_bytes),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Consume and encode a bounded Python row iterator as JSONL."},
    {.ml_name = "jsonl_schema_supported",
     .ml_meth = _PyCFunction_CAST(py_jsonl_schema_supported),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Return whether a PyArrow schema is supported by the native "
               "JSONL writer."},
    {.ml_name = "arrow_direct_schema_supported",
     .ml_meth = _PyCFunction_CAST(py_arrow_direct_schema_supported),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Return whether a PyArrow schema is supported by direct Arrow "
               "ingestion."},
    {.ml_name = "arrow_schema_contract_payload",
     .ml_meth = _PyCFunction_CAST(py_arrow_schema_contract_payload),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Encode a PyArrow schema as a logical schema contract payload."},
    // to_sink (context)
    {.ml_name = "context_to_sink_from_source",
     .ml_meth = _PyCFunction_CAST(py_context_to_sink_from_source),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a sink against a source-selected input."},
    {.ml_name = "context_to_sink_from_path_sources",
     .ml_meth = _PyCFunction_CAST(py_context_to_sink_from_path_sources),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a sink against multiple local path sources."},
    {.ml_name = "context_to_sink_from_path_source_chunk_provider",
     .ml_meth =
         _PyCFunction_CAST(py_context_to_sink_from_path_source_chunk_provider),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a sink against lazily provided path-source chunks."},
    {.ml_name = "context_to_sink_arrow_stream",
     .ml_meth = _PyCFunction_CAST(py_context_to_sink_arrow_stream),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a sink against an Arrow C stream and return "
               "(stream, diagnostics)."},
    {.ml_name = "context_to_registry_sink_from_source",
     .ml_meth = _PyCFunction_CAST(py_context_to_registry_sink_from_source),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a registry-backed sink against a source-selected input."},
    {.ml_name = "context_to_registry_sink_from_path_sources",
     .ml_meth =
         _PyCFunction_CAST(py_context_to_registry_sink_from_path_sources),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a registry-backed sink against multiple local path "
               "sources with source-file metadata."},
    {.ml_name = "context_to_registry_sink_from_path_sources_registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_from_path_sources_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a registry-backed path-source sink with a compiled native "
               "registry-state capsule."},
    {.ml_name = "context_to_registry_sink_from_path_source_chunk_provider_"
                "registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_from_path_source_chunk_provider_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a registry-backed path-source sink from a Python chunk "
               "provider with a compiled native registry-state capsule."},
    {.ml_name = "context_to_registry_sink_from_path_source_chunk_provider_"
                "auto_registry",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_from_path_source_chunk_provider_auto_registry),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer one registry from a path-source chunk provider, then "
               "run a registry-backed sink from a second chunk provider."},
    {.ml_name = "context_to_registry_sink_from_path_source_chunk_provider_"
                "auto_registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_from_path_source_chunk_provider_auto_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer one registry from a path-source chunk provider using a "
               "compiled native registry-state capsule, then run a "
               "registry-backed sink from a second chunk provider."},
    {.ml_name = "context_to_registry_sink_from_path_sources_auto_registry",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_from_path_sources_auto_registry),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer one registry from local path sources, then run a "
               "registry-backed sink with source-file metadata."},
    {.ml_name =
         "context_to_registry_sink_from_path_sources_auto_registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_from_path_sources_auto_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer one registry from local path sources using a compiled "
               "native registry-state capsule, then run a registry-backed "
               "sink with source-file metadata."},
    {.ml_name = "context_to_registry_sink_arrow_stream",
     .ml_meth = _PyCFunction_CAST(py_context_to_registry_sink_arrow_stream),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a registry-backed sink against an Arrow C stream."},
    {.ml_name = "context_to_registry_sink_arrow_sources",
     .ml_meth = _PyCFunction_CAST(py_context_to_registry_sink_arrow_sources),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a registry-backed sink against multiple Arrow C stream "
               "sources with source-file metadata."},
    {.ml_name = "context_to_registry_sink_arrow_sources_registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_arrow_sources_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a registry-backed Arrow-source sink with a compiled "
               "native registry-state capsule."},
    {.ml_name = "context_to_registry_sink_arrow_sources_auto_registry",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_arrow_sources_auto_registry),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer one registry from Arrow C stream sources, then run a "
               "registry-backed sink with source-file metadata."},
    {.ml_name = "context_to_registry_sink_arrow_sources_auto_registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_arrow_sources_auto_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer one registry from Arrow C stream sources using a "
               "compiled native registry-state capsule, then run a "
               "registry-backed sink with source-file metadata."},
    {.ml_name =
         "context_to_registry_sink_arrow_source_chunk_provider_registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_arrow_source_chunk_provider_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Run a registry-backed Arrow-source sink from lazily provided "
               "Arrow-source chunks using a compiled native registry-state "
               "capsule."},
    {.ml_name =
         "context_to_registry_sink_arrow_source_chunk_provider_auto_registry",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_arrow_source_chunk_provider_auto_registry),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer one registry from an Arrow-source chunk provider, then "
               "run a registry-backed sink from a second chunk provider."},
    {.ml_name = "context_to_registry_sink_arrow_source_chunk_provider_"
                "auto_registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_to_registry_sink_arrow_source_chunk_provider_auto_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer one registry from an Arrow-source chunk provider using "
               "a compiled native registry-state capsule, then run a "
               "registry-backed sink from a second chunk provider."},
    {.ml_name = "context_registry_probe_from_arrow_sources",
     .ml_meth = _PyCFunction_CAST(py_context_registry_probe_from_arrow_sources),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer and merge registry state from multiple Arrow C stream "
               "sources without creating an output stream."},
    {.ml_name = "context_registry_probe_from_arrow_sources_registry_state",
     .ml_meth = _PyCFunction_CAST(
         py_context_registry_probe_from_arrow_sources_registry_state),
     .ml_flags = METH_VARARGS,
     .ml_doc = "Infer and merge registry state from multiple Arrow C stream "
               "sources using a compiled native registry-state capsule."},
    {.ml_name = nullptr, .ml_meth = nullptr, .ml_flags = 0, .ml_doc = nullptr},
});

PyModuleDef kModule = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "schema_sanitizer._core_abi3",
    .m_doc = "schema-sanitizer minimal ABI3 bindings (limited API)",
    .m_size = -1,
    .m_methods = kMethods.data(),
    .m_slots = nullptr,
    .m_traverse = nullptr,
    .m_clear = nullptr,
    .m_free = nullptr,
};

PyObject *create_module() noexcept { return PyModule_Create(&kModule); }

} // namespace
} // namespace core_abi3_internal

PyMODINIT_FUNC PyInit__core_abi3(void) {
  return core_abi3_internal::create_module();
}
