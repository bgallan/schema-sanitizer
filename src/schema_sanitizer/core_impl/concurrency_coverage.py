"""Describe machine-readable concurrency coverage for every input and output pair.

Registered concrete contracts are expanded and validated against stages, routes, payload kinds,
and observed evidence so no supported combination lacks a concurrency guarantee.
"""

from __future__ import annotations

from types import MappingProxyType

from . import cancellation as _cancellation_contract_impl  # noqa: F401

# Import the concrete shared mechanisms so coverage can only advertise a
# contract after the enforcing implementation registered itself.
from . import control_plane_budget as _control_plane_contract_impl  # noqa: F401
from . import error_translation as _payload_contract_impl  # noqa: F401
from . import memory_budget as _memory_contract_impl  # noqa: F401
from . import process_resources as _process_resource_contract_impl  # noqa: F401
from .concurrency_contracts import (
    RuntimeConcurrencyContract,
    require_runtime_concurrency_contracts,
    runtime_pair_contract_observations,
    runtime_pair_payload_contract_observations,
    runtime_pair_stage_observations,
    runtime_route_profile_contract_observations,
)
from .concurrency_route_evidence import (
    INPUT_ROUTE_PROFILE_REQUIREMENTS,
    OUTPUT_ROUTE_PROFILE_REQUIREMENTS,
)
from .concurrency_stage_evidence import (
    INPUT_PRIMARY_RUNTIME_STAGE,
    OUTPUT_PRIMARY_RUNTIME_STAGE,
)

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
        "record_batch_reader_direct_duckdb_handoff",
        "direct_stream_adapter_conversion",
    ),
}

_INPUT_SERIAL_BOUNDARIES = {
    "csv": ("quote_aware_record_framing",),
    "json": ("top_level_value_framing",),
    "json_array": ("top_level_array_framing",),
    "jsonl": ("ordered_line_framing",),
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
    "duckdb": "record_batch_reader_direct_to_duckdb_without_arrow_table_barrier",
}

_OUTPUT_HANDOFFS = {
    "csv": "native_arrow_stream_sink",
    "jsonl": "native_arrow_stream_sink",
    "parquet": "native_arrow_stream_sink",
    "pyarrow": "arrow_c_stream_to_pyarrow_table",
    "pandas": "record_batch_reader",
    "polars": "record_batch_reader",
    "duckdb": "record_batch_reader_direct",
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
    "gil_released_native_wait_boundaries",
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
    "transferable_resident_memory_credit",
    "composite_slot_and_byte_admission",
    "process_control_plane_budget",
    "parallel_arrow_c_stream_handoff",
)


_PAIR_RUNTIME_CONTRACTS = (
    "transferable_resident_memory_credit",
    "composite_slot_and_byte_admission",
    "process_control_plane_budget",
)

_PAIR_PAYLOAD_RUNTIME_CONTRACTS = (
    *_PAIR_RUNTIME_CONTRACTS,
    "native_payload_core_call",
)

_SAFETY_CRITICAL_RUNTIME_CONTRACTS = (
    *_PAIR_PAYLOAD_RUNTIME_CONTRACTS,
    "operation_cancellation_checkpoint",
    "process_file_descriptor_admission",
    "external_runtime_pool_claim",
)


def _pair_runtime_contract_evidence() -> tuple[RuntimeConcurrencyContract, ...]:
    """Resolve every shared guarantee to the concrete enforcing callable."""
    return require_runtime_concurrency_contracts(*_PAIR_RUNTIME_CONTRACTS)


def concurrency_pair_guarantees() -> dict[str, dict[str, dict[str, object]]]:
    """Return implementation-backed end-to-end contracts for all 49 pairs."""
    evidence = _pair_runtime_contract_evidence()
    implemented = {
        item.name: bool(item.implementation_module and item.implementation_name)
        for item in evidence
    }
    evidence_view = tuple(
        (item.name, item.implementation_module, item.implementation_name) for item in evidence
    )
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
                "resident_credit_transfer": implemented["transferable_resident_memory_credit"],
                "composite_slot_byte_admission": implemented["composite_slot_and_byte_admission"],
                "control_plane_budgeted": implemented["process_control_plane_budget"],
                "runtime_contract_evidence": evidence_view,
                "terminal_handoff": _OUTPUT_HANDOFFS[output_name],
                "full_arrow_table_barrier": output_name in {"pyarrow", "pandas"},
                "explicit_pyarrow_table_output": output_name == "pyarrow",
                "adapter_internal_full_table_materialization": output_name == "pandas",
                "benefit_proof": (
                    f"{_INPUT_BENEFIT_PROOFS[input_name]}+{_OUTPUT_BENEFIT_PROOFS[output_name]}"
                ),
                "input_primary_runtime_stage": INPUT_PRIMARY_RUNTIME_STAGE[input_name],
                "output_primary_runtime_stage": OUTPUT_PRIMARY_RUNTIME_STAGE[output_name],
            }
            for output_name, output_stages in OUTPUT_CONCURRENCY_COVERAGE.items()
        }
        for input_name, input_stages in INPUT_CONCURRENCY_COVERAGE.items()
    }


def validate_concurrency_pair_contracts() -> tuple[int, tuple[object, ...]]:
    """Execute the matrix contract validator and return pair count + evidence.

    This is intentionally not a metadata-only flag check: resolving the matrix
    fails if the resident-credit, composite-admission, or control-budget
    implementation callable has not registered. Every format pair then inherits
    the same exact registered mechanisms through the common pipeline runtime.
    """
    evidence = _pair_runtime_contract_evidence()
    pairs = concurrency_pair_guarantees()
    pair_count = 0
    for input_name in INPUT_CONCURRENCY_COVERAGE:
        outputs = pairs.get(input_name)
        if outputs is None or set(outputs) != set(OUTPUT_CONCURRENCY_COVERAGE):
            raise RuntimeError(f"incomplete concurrency contract for input {input_name}")
        for output_name, guarantee in outputs.items():
            pair_count += 1
            if not (
                guarantee["resident_credit_transfer"]
                and guarantee["composite_slot_byte_admission"]
                and guarantee["control_plane_budgeted"]
            ):
                raise RuntimeError(
                    f"runtime concurrency contract missing for {input_name}->{output_name}"
                )
    expected = len(INPUT_CONCURRENCY_COVERAGE) * len(OUTPUT_CONCURRENCY_COVERAGE)
    if pair_count != expected:
        raise RuntimeError(f"expected {expected} concurrency pairs, found {pair_count}")
    return pair_count, evidence


def observed_concurrency_pair_guarantees() -> dict[str, dict[str, dict[str, int]]]:
    """Return execution evidence for each concrete input/output pair.

    Unlike :func:`concurrency_pair_guarantees`, this view is populated only when
    production paths actually execute the registered primitives while a real
    source/sink pair is active.
    """
    observed = runtime_pair_contract_observations()
    return {
        input_name: {
            output_name: {
                contract: int(observed.get((input_name, output_name), {}).get(contract, 0))
                for contract in _PAIR_RUNTIME_CONTRACTS
            }
            for output_name in OUTPUT_CONCURRENCY_COVERAGE
        }
        for input_name in INPUT_CONCURRENCY_COVERAGE
    }


def validate_observed_concurrency_pair_contracts() -> int:
    """Require all 49 source/sink paths to have exercised every shared invariant.

    This validator intentionally fails on a fresh process.  CI/integration tests
    should run the format matrix first, then call this function to prove that the
    claimed end-to-end mechanisms were observed rather than merely registered.
    """
    matrix = observed_concurrency_pair_guarantees()
    missing: list[str] = []
    count = 0
    for input_name, outputs in matrix.items():
        for output_name, contracts in outputs.items():
            count += 1
            absent = tuple(name for name, calls in contracts.items() if calls <= 0)
            if absent:
                missing.append(f"{input_name}->{output_name}:" + ",".join(absent))
    if missing:
        raise RuntimeError(
            "runtime format-pair concurrency evidence is incomplete: " + "; ".join(missing)
        )
    return count


def payload_observed_concurrency_pair_guarantees() -> dict[str, dict[str, dict[str, int]]]:
    """Return non-bootstrap execution evidence for every concrete format pair.

    Structural pair admission is deliberately excluded.  Counts in this matrix
    therefore prove that the real payload/runtime path executed each shared
    invariant while the source/sink identity remained active.
    """
    observed = runtime_pair_payload_contract_observations()
    return {
        input_name: {
            output_name: {
                contract: int(observed.get((input_name, output_name), {}).get(contract, 0))
                for contract in _PAIR_PAYLOAD_RUNTIME_CONTRACTS
            }
            for output_name in OUTPUT_CONCURRENCY_COVERAGE
        }
        for input_name in INPUT_CONCURRENCY_COVERAGE
    }


def validate_payload_observed_concurrency_pair_contracts() -> int:
    """Require all 49 real payload paths to exercise every shared invariant.

    The structural one-byte pair bootstrap cannot satisfy this contract.
    Integration CI must execute the actual conversion matrix first.
    """
    matrix = payload_observed_concurrency_pair_guarantees()
    missing: list[str] = []
    count = 0
    for input_name, outputs in matrix.items():
        for output_name, contracts in outputs.items():
            count += 1
            absent = tuple(name for name, calls in contracts.items() if calls <= 0)
            if absent:
                missing.append(f"{input_name}->{output_name}:" + ",".join(absent))
    if missing:
        raise RuntimeError(
            "runtime format-pair payload concurrency evidence is incomplete: " + "; ".join(missing)
        )
    return count


def stage_observed_concurrency_pair_guarantees() -> dict[str, dict[str, dict[str, int]]]:
    """Return format-specific primary-stage evidence for all 49 public pairs."""
    observed = runtime_pair_stage_observations()
    return {
        input_name: {
            output_name: {
                INPUT_PRIMARY_RUNTIME_STAGE[input_name]: int(
                    observed.get((input_name, output_name), {}).get(
                        INPUT_PRIMARY_RUNTIME_STAGE[input_name], 0
                    )
                ),
                OUTPUT_PRIMARY_RUNTIME_STAGE[output_name]: int(
                    observed.get((input_name, output_name), {}).get(
                        OUTPUT_PRIMARY_RUNTIME_STAGE[output_name], 0
                    )
                ),
            }
            for output_name in OUTPUT_CONCURRENCY_COVERAGE
        }
        for input_name in INPUT_CONCURRENCY_COVERAGE
    }


def validate_stage_observed_concurrency_pair_contracts() -> int:
    """Require each pair to execute one advertised input and output stage.

    Shared resident-credit/control-plane observations alone cannot certify a
    release: this gate catches a format-specific route that silently stops
    traversing the concurrency stage advertised by the coverage matrix.
    """
    matrix = stage_observed_concurrency_pair_guarantees()
    missing: list[str] = []
    count = 0
    for input_name, outputs in matrix.items():
        for output_name, stages in outputs.items():
            count += 1
            absent = tuple(name for name, calls in stages.items() if calls <= 0)
            if absent:
                missing.append(f"{input_name}->{output_name}:" + ",".join(absent))
    if missing:
        raise RuntimeError(
            "runtime format-pair stage evidence is incomplete: " + "; ".join(missing)
        )
    return count


def route_profile_runtime_contract_guarantees() -> dict[str, dict[str, int]]:
    """Return orthogonal transport/lifetime evidence without 8x7 explosion."""
    observed = runtime_route_profile_contract_observations()
    requirements = {
        **dict(INPUT_ROUTE_PROFILE_REQUIREMENTS),
        **dict(OUTPUT_ROUTE_PROFILE_REQUIREMENTS),
    }
    return {
        profile: {
            contract: int(observed.get(profile, {}).get(contract, 0)) for contract in contracts
        }
        for profile, contracts in requirements.items()
    }


def validate_route_profile_runtime_contracts() -> int:
    """Require each transport/lifetime profile to exercise its critical path."""
    matrix = route_profile_runtime_contract_guarantees()
    missing: list[str] = []
    for profile, contracts in matrix.items():
        absent = tuple(name for name, calls in contracts.items() if calls <= 0)
        if absent:
            missing.append(profile + ":" + ",".join(absent))
    if missing:
        raise RuntimeError(
            "runtime route-profile concurrency evidence is incomplete: " + "; ".join(missing)
        )
    return len(matrix)


def validate_safety_critical_runtime_contracts() -> int:
    """Require every safety-critical runtime governor and checkpoint."""
    contracts = require_runtime_concurrency_contracts(*_SAFETY_CRITICAL_RUNTIME_CONTRACTS)
    missing = tuple(contract.name for contract in contracts if contract.observed_calls <= 0)
    if missing:
        raise RuntimeError(
            "safety-critical runtime concurrency evidence is incomplete: " + ", ".join(missing)
        )
    return len(contracts)


def validate_native_concurrency_protocol_health() -> None:
    """Fail release certification on observable native ownership corruption."""
    from .runtime_diagnostics import _native_arena_snapshot

    snapshot = _native_arena_snapshot()
    if not snapshot.get("available"):
        raise RuntimeError(
            "native concurrency protocol snapshot is unavailable during release certification"
        )
    if snapshot.get("snapshot_failed"):
        raise RuntimeError(
            "native concurrency protocol snapshot failed during release certification"
        )
    required = (
        "snapshot_schema_fields",
        "completion_memory_protocol_violations",
        "counter_underflows",
        "native_physical_threads",
        "external_runtime_thread_permits",
        "total_physical_thread_permits",
        "native_physical_thread_capacity",
        "thread_permit_snapshot_stable",
        "external_runtime_resident_protocol_violations",
        "external_runtime_resident_threads",
        "external_runtime_stack_debt_threads",
    )
    missing = tuple(name for name in required if name not in snapshot)
    if missing:
        raise RuntimeError(
            "native concurrency protocol snapshot is too old/incomplete for release certification: "
            + ", ".join(missing)
        )
    violations = int(snapshot["completion_memory_protocol_violations"])
    if violations != 0:
        raise RuntimeError(
            f"native completion-memory ownership protocol violations observed: {violations}"
        )
    underflows = int(snapshot["counter_underflows"])
    if underflows != 0:
        raise RuntimeError(f"native concurrency counter underflows observed: {underflows}")
    if int(snapshot["snapshot_schema_fields"]) != 30:
        raise RuntimeError("native concurrency snapshot does not match the current schema")
    if not bool(int(snapshot["thread_permit_snapshot_stable"])):
        raise RuntimeError("native physical-thread permit snapshot was not transactionally stable")
    resident_identity = int(snapshot["external_runtime_resident_threads"])
    resident_stack_debt = int(snapshot["external_runtime_stack_debt_threads"])
    if resident_identity < 0 or resident_stack_debt < 0:
        raise RuntimeError("native external-runtime resident accounting became negative")
    if resident_stack_debt < resident_identity:
        raise RuntimeError(
            "native external-runtime stack debt is below resident identity credit: "
            f"identity={resident_identity}, debt={resident_stack_debt}"
        )

    resident_violations = int(snapshot["external_runtime_resident_protocol_violations"])
    if resident_violations != 0:
        raise RuntimeError(
            f"external-runtime resident-thread protocol violations observed: {resident_violations}"
        )
    managed = int(snapshot["native_physical_threads"])
    external = int(snapshot["external_runtime_thread_permits"])
    total = int(snapshot["total_physical_thread_permits"])
    capacity = int(snapshot["native_physical_thread_capacity"])
    if total != managed + external:
        raise RuntimeError(
            "native physical-thread permit subledgers do not conserve the atomic total: "
            f"total={total}, managed={managed}, external={external}"
        )
    if total > capacity:
        raise RuntimeError(
            "native physical-thread permits exceed physical capacity: "
            f"total={total}, capacity={capacity}"
        )


def validate_format_pair_release_contracts() -> int:
    """Certify structural and real-payload evidence for the public 8x7 matrix.

    The normal structural validator intentionally remains useful for unit tests
    and import-time diagnostics. Format-matrix certification must call this gate
    after the public conversions execute in the same process; bootstrap-only
    observations cannot satisfy it. Orthogonal transport/lifetime profiles are
    deliberately certified by :func:`validate_release_concurrency_pair_contracts`.
    """
    structural_count, _evidence = validate_concurrency_pair_contracts()
    payload_count = validate_payload_observed_concurrency_pair_contracts()
    stage_count = validate_stage_observed_concurrency_pair_contracts()
    validate_safety_critical_runtime_contracts()
    validate_native_concurrency_protocol_health()
    if payload_count != structural_count or stage_count != structural_count:
        raise RuntimeError(
            "runtime format-pair release evidence count does not match structural matrix: "
            f"payload={payload_count}, stages={stage_count}, structural={structural_count}"
        )
    return payload_count


def validate_release_concurrency_pair_contracts() -> int:
    """Require both the real 8x7 matrix and every orthogonal route profile."""
    payload_count = validate_format_pair_release_contracts()
    validate_route_profile_runtime_contracts()
    return payload_count
