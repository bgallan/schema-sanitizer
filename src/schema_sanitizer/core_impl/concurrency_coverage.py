"""Machine-readable concurrency coverage for every supported input and output."""

from __future__ import annotations

from types import MappingProxyType

_INPUT_COVERAGE = {
    "csv": (
        "adaptive_vector_record_framing",
        "frontend_row_decode",
        "inference",
        "materialization",
    ),
    "json": (
        "worker_authoritative_structural_framing",
        "worker_local_row_parse",
        "direct_columnar_materialization",
        "inference",
        "materialization",
    ),
    "json_array": (
        "worker_authoritative_structural_framing",
        "worker_local_row_parse",
        "direct_columnar_materialization",
        "inference",
        "materialization",
    ),
    "jsonl": ("source_prefetch", "row_validation", "inference", "materialization"),
    "ndjson": ("source_prefetch", "row_validation", "inference", "materialization"),
    "xml": ("frontend_row_decode", "inference", "materialization"),
    "parquet": ("column_decode", "materialization"),
    "python": (
        "native_iterator_batching",
        "single_encode_progressive_replay",
        "source_prefetch",
        "inference",
        "materialization",
    ),
}

_OUTPUT_COVERAGE = {
    "csv": (
        "parallel_native_stream",
        "native_parallel_sink",
        "wide_fixed_schema_o1_packet_planning",
        "adaptive_high_core_output_workers",
    ),
    "jsonl": ("parallel_native_stream", "native_parallel_sink"),
    "parquet": (
        "parallel_native_stream",
        "native_parallel_sink",
        "ordered_row_group_overlap",
    ),
    "pyarrow": (
        "parallel_native_stream",
        "arrow_c_stream_table_materialization",
    ),
    "pandas": (
        "parallel_native_stream",
        "record_batch_reader_handoff",
        "direct_stream_adapter_conversion",
        "threaded_adapter_conversion",
    ),
    "polars": (
        "parallel_native_stream",
        "record_batch_reader_handoff",
        "direct_stream_adapter_conversion",
        "chunk_preserving_no_rechunk_conversion",
    ),
    "duckdb": (
        "parallel_native_stream",
        "record_batch_reader_handoff",
        "chunk_preserving_arrow_dataset_handoff",
        "direct_stream_adapter_conversion",
    ),
}

_INPUT_SERIAL_BOUNDARIES = {
    "csv": ("quote_aware_record_framing",),
    "json": ("top_level_value_framing",),
    "json_array": ("top_level_array_framing",),
    "jsonl": ("ordered_line_framing",),
    "ndjson": ("ordered_line_framing",),
    "xml": ("ordered_tag_framing",),
    "parquet": ("footer_and_row_group_coordination",),
    "python": ("gil_bound_python_object_iteration", "ordered_replay_spool"),
}

_OUTPUT_SERIAL_BOUNDARIES = {
    "csv": ("ordered_sink_commit",),
    "jsonl": ("ordered_sink_commit",),
    "parquet": ("ordered_file_commit",),
    "pyarrow": ("external_table_commit",),
    "pandas": (
        "pandas_internal_arrow_table_materialization",
        "pandas_dataframe_commit",
    ),
    "polars": ("polars_dataframe_commit",),
    "duckdb": ("duckdb_relation_binding",),
}

_INPUT_BENEFIT_PROOFS = {
    "csv": "adaptive_vector_framing_plus_parallel_decode_runtime",
    "json": "worker_structural_framing_plus_direct_columnar_runtime",
    "json_array": "worker_structural_framing_plus_direct_columnar_runtime",
    "jsonl": "prefetch_validation_inference_materialization_runtime",
    "ndjson": "prefetch_validation_inference_materialization_runtime",
    "xml": "ordered_tag_framing_plus_parallel_decode_runtime",
    "parquet": "native_column_decode_runtime",
    "python": "single_encode_progressive_replay_plus_parallel_pipeline_runtime",
}

_OUTPUT_BENEFIT_PROOFS = {
    "csv": "native_sink_runtime_plus_wide_fixed_high_core_output_scaling",
    "jsonl": "native_sink_runtime",
    "parquet": "native_row_group_runtime",
    "pyarrow": "direct_arrow_c_stream_plus_parallel_native_stream_runtime",
    "pandas": "record_batch_reader_to_pandas_plus_threaded_arrow_conversion",
    "polars": "direct_reader_to_polars_without_arrow_table_or_rechunk_barrier",
    "duckdb": "chunked_arrow_dataset_to_duckdb_without_arrow_table_barrier",
}

_OUTPUT_HANDOFFS = {
    "csv": "native_arrow_stream_sink",
    "jsonl": "native_arrow_stream_sink",
    "parquet": "native_arrow_stream_sink",
    "pyarrow": "arrow_c_stream_to_pyarrow_table",
    "pandas": "record_batch_reader",
    "polars": "record_batch_reader",
    "duckdb": "record_batch_reader_via_arrow_dataset",
}

INPUT_CONCURRENCY_COVERAGE = MappingProxyType(_INPUT_COVERAGE)
OUTPUT_CONCURRENCY_COVERAGE = MappingProxyType(_OUTPUT_COVERAGE)
INPUT_SERIAL_BOUNDARIES = MappingProxyType(_INPUT_SERIAL_BOUNDARIES)
OUTPUT_SERIAL_BOUNDARIES = MappingProxyType(_OUTPUT_SERIAL_BOUNDARIES)


def concurrency_coverage() -> dict[str, dict[str, tuple[str, ...]]]:
    """Return a detached snapshot of the supported concurrency coverage matrix."""
    return {
        "inputs": dict(INPUT_CONCURRENCY_COVERAGE),
        "outputs": dict(OUTPUT_CONCURRENCY_COVERAGE),
    }


def concurrency_guarantees() -> dict[str, dict[str, dict[str, object]]]:
    """Return parallel stages and unavoidable ordered boundaries per format."""
    return {
        "inputs": {
            name: {
                "parallel_stages": tuple(stages),
                "serial_boundaries": tuple(INPUT_SERIAL_BOUNDARIES[name]),
                "adaptive_small_work_fallback": True,
                "eligible_multi_benefit": True,
                "benefit_proof": _INPUT_BENEFIT_PROOFS[name],
            }
            for name, stages in INPUT_CONCURRENCY_COVERAGE.items()
        },
        "outputs": {
            name: {
                "parallel_stages": tuple(stages),
                "serial_boundaries": tuple(OUTPUT_SERIAL_BOUNDARIES[name]),
                "adaptive_small_work_fallback": True,
                "eligible_multi_benefit": True,
                "benefit_proof": _OUTPUT_BENEFIT_PROOFS[name],
                "terminal_handoff": _OUTPUT_HANDOFFS[name],
                "full_arrow_table_barrier": name in {"pyarrow", "pandas"},
                "explicit_pyarrow_table_output": name == "pyarrow",
                "adapter_internal_full_table_materialization": name == "pandas",
            }
            for name, stages in OUTPUT_CONCURRENCY_COVERAGE.items()
        },
    }


_PAIR_HANDOFF_STAGES = (
    "operation_owned_task_arena",
    "cacheline_isolated_arena_writer_domains",
    "worker_active_streak_accounting",
    "cacheline_isolated_worker_running_publication",
    "sparse_bitset_round_robin_worker_selection",
    "single_modulo_lane_origin_reuse",
    "worker_local_monotonic_peak_active_cache",
    "single_store_worker_active_streak_telemetry",
    "cacheline_isolated_worker_wake_epoch_publication",
    "targeted_worker_wake_epochs",
    "running_worker_wake_coalescing",
    "park_boundary_wake_epoch_sampling",
    "telemetry_aware_clock_elision",
    "empty_to_nonempty_queue_visibility",
    "high_core_sharded_queue_visibility",
    "fixed_physical_queue_visibility_snapshot",
    "compact_queued_task_lane_metadata",
    "one_shot_worker_initialization_publication",
    "park_boundary_first_task_sampling",
    "monotonic_initialized_worker_park_snapshot_elision",
    "worker_local_stolen_task_accounting",
    "single_store_worker_local_stolen_publication",
    "authoritative_started_mask_start_lock_elision",
    "initialized_worker_snapshot_admission_elision",
    "precompiled_stage_submission_plan",
    "modulo_free_ordered_completion_ring",
    "slot_terminal_state_pre_wait_coordination",
    "single_rmw_external_task_lifetime_accounting",
    "worker_sharded_submission_accounting",
    "mutex_owned_queue_counters_single_store_publication",
    "successful_drain_only_queue_visibility_publication",
    "worker_count_sharded_external_completion_accounting",
    "shutdown_waiter_bit_external_completion_notification_elision",
    "single_sentinel_external_task_lease_completion",
    "compile_time_abandonment_single_shard_lease",
    "typed_owner_member_abandonment_lease",
    "single_snapshot_arena_terminal_flags",
    "stop_token_authoritative_high_core_worker_loop",
    "high_core_executor_local_arena_submission_tickets",
    "high_core_single_writer_in_flight_publication",
    "mutex_owned_memory_order_tightening",
    "high_core_single_writer_in_flight_consumption",
    "low_core_worker_sharded_task_telemetry",
    "single_store_worker_local_task_telemetry_publication",
    "all_worker_sharded_task_completion_telemetry",
    "single_store_worker_sharded_submission_telemetry",
    "parallel_arrow_c_stream_handoff",
)


def concurrency_pair_guarantees() -> dict[str, dict[str, dict[str, object]]]:
    """Return an explicit end-to-end contract for every input/output pair."""
    return {
        input_name: {
            output_name: {
                "input_parallel_stages": tuple(input_stages),
                "shared_parallel_stages": _PAIR_HANDOFF_STAGES,
                "output_parallel_stages": tuple(output_stages),
                "serial_boundaries": (
                    *INPUT_SERIAL_BOUNDARIES[input_name],
                    *OUTPUT_SERIAL_BOUNDARIES[output_name],
                ),
                "adaptive_small_work_fallback": True,
                "eligible_multi_benefit": True,
                "source_to_sink_parallel_path": True,
                "terminal_handoff": _OUTPUT_HANDOFFS[output_name],
                "full_arrow_table_barrier": output_name in {"pyarrow", "pandas"},
                "explicit_pyarrow_table_output": output_name == "pyarrow",
                "adapter_internal_full_table_materialization": output_name == "pandas",
                "benefit_proof": (
                    f"{_INPUT_BENEFIT_PROOFS[input_name]}+{_OUTPUT_BENEFIT_PROOFS[output_name]}"
                ),
            }
            for output_name, output_stages in OUTPUT_CONCURRENCY_COVERAGE.items()
        }
        for input_name, input_stages in INPUT_CONCURRENCY_COVERAGE.items()
    }
