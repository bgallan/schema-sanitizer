// Canonical ABI3 method catalogue. Intentionally included more than once.
#ifndef SCHEMA_SANITIZER_ABI3_METHOD
#error "Define SCHEMA_SANITIZER_ABI3_METHOD before including this catalogue"
#endif

// clang-format off: keep the catalogue compact and one-entry-per-block.
SCHEMA_SANITIZER_ABI3_METHOD(context_new, METH_NOARGS, "Create an execution context.")
SCHEMA_SANITIZER_ABI3_METHOD(context_memory_stats_json, METH_VARARGS, "Return memory stats JSON for a context.")
SCHEMA_SANITIZER_ABI3_METHOD(context_performance_stats_json, METH_VARARGS, "Return latest operation performance telemetry as JSON.")
SCHEMA_SANITIZER_ABI3_METHOD(diagnostics_json, METH_VARARGS, "Return live diagnostics JSON for a diagnostics capsule.")
SCHEMA_SANITIZER_ABI3_METHOD(memory_budget, METH_VARARGS, "Derive every internal resource budget from one memory limit.")
SCHEMA_SANITIZER_ABI3_METHOD(process_memory_governor_stats, METH_NOARGS, "Return process-wide operation memory lease diagnostics.")
SCHEMA_SANITIZER_ABI3_METHOD(process_resident_memory_stats, METH_NOARGS, "Return exact process-wide resident-memory ledger diagnostics.")
SCHEMA_SANITIZER_ABI3_METHOD(allocation_registry_stats, METH_NOARGS, "Return bounded native allocation-registry diagnostics.")
SCHEMA_SANITIZER_ABI3_METHOD(process_cpu_governor_probe, METH_VARARGS, "Exercise fair process-wide CPU admission.")
SCHEMA_SANITIZER_ABI3_METHOD(execution_policy, METH_VARARGS, "Derive deterministic execution limits from mode, memory, and CPUs.")
SCHEMA_SANITIZER_ABI3_METHOD(ordered_executor_probe, METH_VARARGS, "Exercise bounded ordinal execution for internal tests.")
SCHEMA_SANITIZER_ABI3_METHOD(ordered_executor_arena_completion_probe, METH_VARARGS, "Measure bounded mutex-free arena completion publication.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_task_arena_reaper_shutdown, METH_VARARGS, "Stop and join the native cleanup reaper under a deadline.")
SCHEMA_SANITIZER_ABI3_METHOD(process_physical_thread_count, METH_NOARGS, "Return the OS-observed current-process physical thread count.")
SCHEMA_SANITIZER_ABI3_METHOD(process_physical_thread_permits_acquire, METH_VARARGS, "Acquire the shared process physical-thread permit domain.")
SCHEMA_SANITIZER_ABI3_METHOD(process_physical_thread_permits_release, METH_VARARGS, "Release shared process physical-thread permits.")
SCHEMA_SANITIZER_ABI3_METHOD(process_external_runtime_thread_permit_lease_acquire, METH_VARARGS, "Acquire an exact RAII permit lease for an external runtime pool.")
SCHEMA_SANITIZER_ABI3_METHOD(process_external_runtime_thread_permit_lease_resize, METH_VARARGS, "Shrink an exact external-runtime permit lease.")
SCHEMA_SANITIZER_ABI3_METHOD(process_external_runtime_thread_permit_lease_metadata, METH_VARARGS, "Return id/generation/amount for an exact external-runtime permit receipt.")
SCHEMA_SANITIZER_ABI3_METHOD(process_external_runtime_thread_permit_lease_amount, METH_VARARGS, "Return the amount owned by an external-runtime permit lease.")
SCHEMA_SANITIZER_ABI3_METHOD(process_external_runtime_resident_threads_add, METH_VARARGS, "Register runtime-observed resident external worker threads.")
SCHEMA_SANITIZER_ABI3_METHOD(process_external_runtime_resident_threads_release, METH_VARARGS, "Release runtime-observed resident external worker attribution.")
SCHEMA_SANITIZER_ABI3_METHOD(process_external_runtime_stack_debt_threads_add, METH_VARARGS, "Charge resident external worker stack debt independently of identity.")
SCHEMA_SANITIZER_ABI3_METHOD(process_external_runtime_stack_debt_threads_release, METH_VARARGS, "Release proven retired external worker stack debt.")
SCHEMA_SANITIZER_ABI3_METHOD(process_external_runtime_residency_update, METH_VARARGS, "Atomically update external resident identity and stack-debt deltas.")
SCHEMA_SANITIZER_ABI3_METHOD(process_thread_stack_reservation_bytes, METH_NOARGS, "Return the native conservative per-thread stack reservation.")
SCHEMA_SANITIZER_ABI3_METHOD(process_file_descriptor_permit_lease_acquire_wait, METH_VARARGS, "Acquire an exact RAII file-descriptor permit lease.")
SCHEMA_SANITIZER_ABI3_METHOD(process_file_descriptor_permit_lease_resize, METH_VARARGS, "Shrink an exact file-descriptor permit lease idempotently.")
SCHEMA_SANITIZER_ABI3_METHOD(process_file_descriptor_permit_lease_metadata, METH_VARARGS, "Return id/generation/amount/opened for an exact FD permit receipt.")
SCHEMA_SANITIZER_ABI3_METHOD(process_file_descriptor_permit_lease_mark_opened, METH_VARARGS, "Mark descriptors opened on their exact FD permit receipt.")
SCHEMA_SANITIZER_ABI3_METHOD(process_file_descriptor_permit_lease_mark_closed, METH_VARARGS, "Mark descriptors closed on their exact FD permit receipt.")
SCHEMA_SANITIZER_ABI3_METHOD(process_file_descriptor_permit_lease_amount, METH_VARARGS, "Return exact file-descriptor permit lease authority.")
SCHEMA_SANITIZER_ABI3_METHOD(process_file_descriptor_mark_opened, METH_VARARGS, "Mark reserved governed descriptors as physically open.")
SCHEMA_SANITIZER_ABI3_METHOD(process_file_descriptor_mark_closed, METH_VARARGS, "Mark governed descriptors as physically closed.")
SCHEMA_SANITIZER_ABI3_METHOD(process_file_descriptor_count, METH_NOARGS, "Return the native OS-observed current-process descriptor count.")
SCHEMA_SANITIZER_ABI3_METHOD(process_file_descriptor_permits_snapshot, METH_NOARGS, "Return native process file-descriptor permit usage/capacity.")
SCHEMA_SANITIZER_ABI3_METHOD(process_physical_thread_mark_running, METH_NOARGS, "Mark one permit-backed Python thread as OS-visible.")
SCHEMA_SANITIZER_ABI3_METHOD(process_physical_thread_mark_stopped, METH_NOARGS, "Mark one permit-backed Python thread as leaving the OS view.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_task_arena_runtime_snapshot, METH_NOARGS, "Return process-wide native arena and reaper diagnostics.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_task_arena_probe, METH_VARARGS, "Exercise two ordered stages on one bounded operation arena.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_task_arena_stealing_probe, METH_NOARGS, "Verify lane-safe work stealing under a skewed native load.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_task_arena_mixed_lane_probe, METH_VARARGS, "Measure mixed-lane stealing on a high-core operation arena.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_task_arena_concurrent_submit_probe, METH_VARARGS, "Verify exact arena admission totals under concurrent producers.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_task_arena_wake_coalescing_probe, METH_VARARGS, "Verify targeted wake epochs and running-worker coalescing.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_task_arena_output_preference_probe, METH_VARARGS, "Verify local high-core output-lane queue preference.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_task_arena_output_steal_probe, METH_VARARGS, "Verify high-core output preference during compatible stealing.")
SCHEMA_SANITIZER_ABI3_METHOD(output_worker_admission_probe, METH_VARARGS, "Exercise high-core text-output worker admission.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_task_arena_cancellation_probe, METH_NOARGS, "Verify stage-local cancellation inside the operation arena.")
SCHEMA_SANITIZER_ABI3_METHOD(options_catalog, METH_NOARGS, "Return canonical option names, wire kinds, defaults, and groups.")
SCHEMA_SANITIZER_ABI3_METHOD(options_prepare_bytes, METH_VARARGS, "Validate serialized options and return an internal runtime capsule.")
SCHEMA_SANITIZER_ABI3_METHOD(options_with_detected_at, METH_VARARGS, "Clone prepared options with internal operation timestamp metadata.")
SCHEMA_SANITIZER_ABI3_METHOD(options_with_operation_context, METH_VARARGS, "Clone prepared options with timestamp and shared memory ledger.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_memory_ledger_create, METH_VARARGS, "Create one atomic cross-language operation memory ledger.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_memory_reservation_create, METH_VARARGS, "Reserve bytes and return an exact native ownership receipt.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_memory_reservation_resize, METH_VARARGS, "Resize an exact native operation-memory ownership receipt.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_memory_reservation_release, METH_VARARGS, "Idempotently release an exact native operation-memory receipt.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_memory_reservation_metadata, METH_VARARGS, "Return id/generation/bytes for an exact operation-memory receipt.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_memory_reservation_bytes, METH_VARARGS, "Return bytes currently owned by an exact memory receipt.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_memory_ledger_reserve, METH_VARARGS, "Reserve Python-owned bytes against an operation ledger.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_memory_ledger_reserve_snapshot, METH_VARARGS, "Reserve bytes and return ledger statistics transactionally.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_memory_ledger_release, METH_VARARGS, "Release Python-owned bytes from an operation ledger.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_memory_ledger_snapshot, METH_VARARGS, "Return limit, current, and peak bytes for an operation ledger.")
SCHEMA_SANITIZER_ABI3_METHOD(operation_memory_ledger_diagnostics, METH_VARARGS, "Return over-release diagnostics for an operation ledger.")
SCHEMA_SANITIZER_ABI3_METHOD(atomic_epoch_create, METH_NOARGS, "Create a fixed-width atomic runtime epoch counter.")
SCHEMA_SANITIZER_ABI3_METHOD(atomic_epoch_increment, METH_VARARGS, "Increment a fixed-width atomic runtime epoch without wrap.")
SCHEMA_SANITIZER_ABI3_METHOD(atomic_epoch_decrement, METH_VARARGS, "Decrement a fixed-width atomic runtime counter without underflow.")
SCHEMA_SANITIZER_ABI3_METHOD(atomic_epoch_value, METH_VARARGS, "Read a fixed-width atomic runtime epoch.")
SCHEMA_SANITIZER_ABI3_METHOD(atomic_epoch_write_le, METH_VARARGS, "Write an atomic runtime epoch into an existing bytearray.")
SCHEMA_SANITIZER_ABI3_METHOD(atomic_epoch_write_activity, METH_VARARGS,
                             "Write groups of four atomic activity counters "
                             "without PyLong materialization.")
SCHEMA_SANITIZER_ABI3_METHOD(atomic_epoch_reset, METH_VARARGS, "Reset a child-process atomic runtime epoch.")
SCHEMA_SANITIZER_ABI3_METHOD(schema_registry_merge, METH_VARARGS, "Merge inferred logical schema and registry state.")
SCHEMA_SANITIZER_ABI3_METHOD(schema_registry_empty, METH_VARARGS, "Return an empty schema-registry JSON document.")
SCHEMA_SANITIZER_ABI3_METHOD(schema_registry_contract_payload, METH_VARARGS, "Encode registry canonical_schema as a logical schema contract payload.")
SCHEMA_SANITIZER_ABI3_METHOD(registry_state_from_json, METH_VARARGS, "Compile registry JSON into a reusable native registry-state capsule.")
SCHEMA_SANITIZER_ABI3_METHOD(logical_schema_payload_validate, METH_VARARGS, "Validate a native logical schema payload.")
SCHEMA_SANITIZER_ABI3_METHOD(logical_schema_payload_field_names, METH_VARARGS, "Return top-level field names from a logical schema payload.")
SCHEMA_SANITIZER_ABI3_METHOD(logical_schema_payload_arrow_c_schema, METH_VARARGS, "Export a logical schema payload as an Arrow C schema capsule.")
SCHEMA_SANITIZER_ABI3_METHOD(context_schema_probe_from_source, METH_VARARGS, "Infer a logical schema from a source-selected input.")
SCHEMA_SANITIZER_ABI3_METHOD(context_registry_probe_from_source, METH_VARARGS, "Infer and merge registry state from a source-selected input.")
SCHEMA_SANITIZER_ABI3_METHOD(context_schema_probe_from_paths, METH_VARARGS, "Infer a logical schema from multiple local files.")
SCHEMA_SANITIZER_ABI3_METHOD(context_registry_probe_from_paths, METH_VARARGS, "Infer and merge registry state from multiple local files.")
SCHEMA_SANITIZER_ABI3_METHOD(context_registry_probe_from_path_sources, METH_VARARGS, "Infer and merge registry state from multiple native path sources.")
SCHEMA_SANITIZER_ABI3_METHOD(context_registry_probe_from_path_sources_registry_state, METH_VARARGS,
                             "Infer and merge registry state from multiple native path sources using a "
                             "compiled native registry-state capsule.")
SCHEMA_SANITIZER_ABI3_METHOD(context_registry_probe_from_path_sources_best_effort, METH_VARARGS,
                             "Infer and merge registry state from multiple native path sources, "
                             "skipping JSON parse failures.")
SCHEMA_SANITIZER_ABI3_METHOD(context_registry_probe_from_path_sources_best_effort_registry_state, METH_VARARGS,
                             "Infer and merge registry state from multiple native path sources using a "
                             "compiled native registry-state capsule, skipping JSON parse failures.")
SCHEMA_SANITIZER_ABI3_METHOD(context_registry_probe_from_path_source_chunk_provider, METH_VARARGS, "Infer and merge registry state from lazily provided path-source chunks.")
SCHEMA_SANITIZER_ABI3_METHOD(context_registry_probe_from_path_source_chunk_provider_registry_state, METH_VARARGS,
                             "Infer and merge registry state from lazily provided path-source chunks "
                             "using a compiled native registry-state capsule.")
SCHEMA_SANITIZER_ABI3_METHOD(path_source_plan_create, METH_VARARGS, "Parse native path-source descriptors into a reusable native plan capsule.")
SCHEMA_SANITIZER_ABI3_METHOD(file_metadata_columns, METH_VARARGS, "Build generated file metadata columns.")
SCHEMA_SANITIZER_ABI3_METHOD(metadata_stream_wrap, METH_VARARGS, "Wrap an Arrow C stream with string metadata columns.")
SCHEMA_SANITIZER_ABI3_METHOD(coalescing_stream_wrap, METH_VARARGS, "Wrap a supported Arrow C stream and coalesce small batches.")
SCHEMA_SANITIZER_ABI3_METHOD(csv_nested_stream_wrap, METH_VARARGS, "Wrap an Arrow C stream with nested columns rendered as JSON strings.")
SCHEMA_SANITIZER_ABI3_METHOD(csv_stream_write, METH_VARARGS, "Write a supported Arrow C stream to a local CSV file.")
SCHEMA_SANITIZER_ABI3_METHOD(csv_stream_write_with_metadata, METH_VARARGS, "Write a metadata-augmented Arrow C stream to a local CSV file.")
SCHEMA_SANITIZER_ABI3_METHOD(csv_schema_supported, METH_VARARGS, "Return whether a PyArrow schema is supported by the native CSV writer.")
SCHEMA_SANITIZER_ABI3_METHOD(parquet_stream_write, METH_VARARGS, "Write a supported Arrow C stream to a local Parquet file.")
SCHEMA_SANITIZER_ABI3_METHOD(parquet_stream_write_with_metadata, METH_VARARGS, "Write a metadata-augmented Arrow C stream to a local Parquet file.")
SCHEMA_SANITIZER_ABI3_METHOD(parquet_footer_info_json, METH_VARARGS, "Return bounded native Parquet footer metadata as JSON.")
SCHEMA_SANITIZER_ABI3_METHOD(parquet_stream_preflight_json, METH_VARARGS, "Validate native Parquet stream readiness with bounded memory.")
SCHEMA_SANITIZER_ABI3_METHOD(parquet_stream_read, METH_VARARGS, "Open a supported local Parquet file as an Arrow C stream.")
SCHEMA_SANITIZER_ABI3_METHOD(jsonl_stream_write, METH_VARARGS, "Write a supported Arrow C stream to a local JSONL file.")
SCHEMA_SANITIZER_ABI3_METHOD(jsonl_stream_write_with_metadata, METH_VARARGS, "Write a metadata-augmented Arrow C stream to a local JSONL file.")
SCHEMA_SANITIZER_ABI3_METHOD(jsonl_batch_bytes, METH_VARARGS, "Encode one supported PyArrow record batch as JSONL bytes.")
SCHEMA_SANITIZER_ABI3_METHOD(jsonl_batches_bytes, METH_VARARGS, "Encode supported PyArrow record batches as JSONL bytes.")
SCHEMA_SANITIZER_ABI3_METHOD(json_compact_bytes, METH_VARARGS, "Compact one JSON document to UTF-8 bytes.")
SCHEMA_SANITIZER_ABI3_METHOD(json_array_to_jsonl_bytes, METH_VARARGS, "Split one JSON array of objects to UTF-8 JSONL bytes.")
SCHEMA_SANITIZER_ABI3_METHOD(json_array_files_to_jsonl_bytes, METH_VARARGS, "Read and split local JSON array files to JSON Lines bytes.")
SCHEMA_SANITIZER_ABI3_METHOD(xml_folder_effective_row_tag, METH_VARARGS, "Return the validated effective row tag for local XML files.")
SCHEMA_SANITIZER_ABI3_METHOD(python_row_json_bytes, METH_VARARGS, "Encode one JSON-like Python row to UTF-8 bytes.")
SCHEMA_SANITIZER_ABI3_METHOD(python_rows_jsonl_bytes, METH_VARARGS, "Encode JSON-like Python rows to UTF-8 JSONL bytes.")
SCHEMA_SANITIZER_ABI3_METHOD(python_iter_rows_jsonl_bytes, METH_VARARGS, "Consume and encode a bounded Python row iterator as JSONL.")
SCHEMA_SANITIZER_ABI3_METHOD(jsonl_schema_supported, METH_VARARGS, "Return whether a PyArrow schema is supported by the native JSONL writer.")
SCHEMA_SANITIZER_ABI3_METHOD(arrow_direct_schema_supported, METH_VARARGS, "Return whether a PyArrow schema is supported by direct Arrow ingestion.")
SCHEMA_SANITIZER_ABI3_METHOD(arrow_schema_contract_payload, METH_VARARGS, "Encode a PyArrow schema as a logical schema contract payload.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_sink_from_source, METH_VARARGS, "Run a sink against a source-selected input.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_sink_from_path_sources, METH_VARARGS, "Run a sink against multiple local path sources.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_sink_from_path_source_chunk_provider, METH_VARARGS, "Run a sink against lazily provided path-source chunks.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_sink_arrow_stream, METH_VARARGS, "Run a sink against an Arrow C stream and return (stream, diagnostics).")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_from_source, METH_VARARGS, "Run a registry-backed sink against a source-selected input.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_from_path_sources, METH_VARARGS,
                             "Run a registry-backed sink against multiple "
                             "local path sources with source-file metadata.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_from_path_sources_registry_state, METH_VARARGS,
                             "Run a registry-backed path-source sink with a compiled native "
                             "registry-state capsule.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_from_path_source_chunk_provider_registry_state, METH_VARARGS,
                             "Run a registry-backed path-source sink from a Python chunk provider with "
                             "a compiled native registry-state capsule.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_from_path_source_chunk_provider_auto_registry, METH_VARARGS,
                             "Infer one registry from a path-source chunk provider, then run a "
                             "registry-backed sink from a second chunk provider.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_from_path_source_chunk_provider_auto_registry_state, METH_VARARGS,
                             "Infer one registry from a path-source chunk provider using a compiled "
                             "native registry-state capsule, then run a registry-backed sink from a "
                             "second chunk provider.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_from_path_sources_auto_registry, METH_VARARGS,
                             "Infer one registry from local path sources, then run a registry-backed "
                             "sink with source-file metadata.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_from_path_sources_auto_registry_state, METH_VARARGS,
                             "Infer one registry from local path sources using a compiled native "
                             "registry-state capsule, then run a registry-backed sink with source-file "
                             "metadata.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_arrow_stream, METH_VARARGS, "Run a registry-backed sink against an Arrow C stream.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_arrow_sources, METH_VARARGS,
                             "Run a registry-backed sink against multiple Arrow C stream sources with "
                             "source-file metadata.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_arrow_sources_registry_state, METH_VARARGS,
                             "Run a registry-backed Arrow-source sink with a compiled native "
                             "registry-state capsule.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_arrow_sources_auto_registry, METH_VARARGS,
                             "Infer one registry from Arrow C stream sources, then run a "
                             "registry-backed sink with source-file metadata.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_arrow_sources_auto_registry_state, METH_VARARGS,
                             "Infer one registry from Arrow C stream sources using a compiled native "
                             "registry-state capsule, then run a registry-backed sink with source-file "
                             "metadata.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_arrow_source_chunk_provider_registry_state, METH_VARARGS,
                             "Run a registry-backed Arrow-source sink from lazily provided Arrow-source "
                             "chunks using a compiled native registry-state capsule.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_arrow_source_chunk_provider_auto_registry, METH_VARARGS,
                             "Infer one registry from an Arrow-source chunk provider, then run a "
                             "registry-backed sink from a second chunk provider.")
SCHEMA_SANITIZER_ABI3_METHOD(context_to_registry_sink_arrow_source_chunk_provider_auto_registry_state, METH_VARARGS,
                             "Infer one registry from an Arrow-source chunk provider using a compiled "
                             "native registry-state capsule, then run a registry-backed sink from a "
                             "second chunk provider.")
SCHEMA_SANITIZER_ABI3_METHOD(context_registry_probe_from_arrow_sources, METH_VARARGS,
                             "Infer and merge registry state from multiple Arrow C stream sources "
                             "without creating an output stream.")
SCHEMA_SANITIZER_ABI3_METHOD(context_registry_probe_from_arrow_sources_registry_state, METH_VARARGS,
                             "Infer and merge registry state from multiple Arrow C stream sources using "
                             "a compiled native registry-state capsule.")
// clang-format on
