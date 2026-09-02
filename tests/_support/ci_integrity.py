"""Enforce fail-closed collection and runtime integrity for platform tests.

Source-only checks authenticate each exact pytest selection before platform work, while
installed-wheel checks prove provenance, dependency ownership, reviewed skips, and clean
native/process ledgers. Runtime mode writes a compact certificate even when pytest fails.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pytest

from meta.ci.quality.platform_test_evidence import (
    EXPECTED_EXACT_SKIP_INVENTORY_SHA256,
    EXPECTED_TEST_INVENTORY,
    _nodeid_inventory_sha256,
)

_STRICT_ENV = "SCHEMA_SANITIZER_STRICT_TEST_RUNTIME"
_PLATFORM_ENV = "SCHEMA_SANITIZER_PLATFORM_ARTIFACT"
_SHARD_ENV = "SCHEMA_SANITIZER_TEST_SHARD"
_EVIDENCE_ENV = "SCHEMA_SANITIZER_INTEGRITY_EVIDENCE"
_STRESS_NODEIDS_ENV = "SCHEMA_SANITIZER_EXPECT_NATIVE_STRESS_NODEIDS"
_VERIFY_COLLECTION_ENV = "SCHEMA_SANITIZER_VERIFY_TEST_INVENTORY"

_REQUIRED_MODULES = (
    "duckdb",
    "pandas",
    "polars",
    "pyarrow",
    "pyarrow.parquet",
    "schema_sanitizer",
    "schema_sanitizer._core_abi3",
    "schema_sanitizer.api_impl.execution_context",
    "schema_sanitizer.core_impl.native_runtime",
)

_PLATFORM_SYSTEM = {
    "linux": "linux",
    "macos-arm64": "darwin",
    "macos-x86_64": "darwin",
    "windows": "win32",
}

_MAX_SKIP_COUNT = {
    ("linux", "concurrency"): 10,
    ("macos-arm64", "concurrency"): 12,
    ("macos-x86_64", "concurrency"): 12,
    ("windows", "concurrency"): 20,
    ("linux", "memory-parquet"): 0,
    ("macos-arm64", "memory-parquet"): 1,
    ("macos-x86_64", "memory-parquet"): 1,
    ("windows", "memory-parquet"): 180,
    ("linux", "io-pipeline"): 0,
    ("macos-arm64", "io-pipeline"): 0,
    ("macos-x86_64", "io-pipeline"): 0,
    ("windows", "io-pipeline"): 31,
    ("linux", "native-stress"): 0,
    ("macos-arm64", "native-stress"): 0,
    ("macos-x86_64", "native-stress"): 0,
    ("windows", "native-stress"): 0,
    ("linux", "release-matrix"): 0,
    ("macos-arm64", "release-matrix"): 0,
    ("macos-x86_64", "release-matrix"): 0,
    ("windows", "release-matrix"): 0,
}

_WINDOWS_POSIX_MEMORY_MODULES = frozenset(
    {
        "test_memory_cancel_linearizes_before_claimed_callback_starts.py",
        "test_memory_cancelled_bridge_retains_submission_until_real_task_terminal.py",
        "test_memory_coordinator_close_waits_for_submission_callback_registration.py",
        "test_memory_coordinator_waiters_are_scoped_to_the_live_close_generation.py",
        "test_memory_external_claim_is_published_atomically.py",
        "test_memory_identity_descriptor_finalizer_closes_real_and_governed_fd.py",
        "test_memory_observation_cannot_release_another_path_claim.py",
        "test_memory_path_identity_charges_fd_and_removes_claim.py",
        "test_memory_rejected_retry_replacement_keeps_previous_owner.py",
    }
)

_OPTIONAL_POSIX_COORDINATION_REASON = (
    "optional cross-process coordination requires POSIX advisory locks"
)

_CONCURRENCY_NODEID_REASONS = {
    **{
        nodeid: frozenset({_OPTIONAL_POSIX_COORDINATION_REASON})
        for nodeid in (
            "tests/concurrency/test_concurrency_cancellation_and_resource_lifecycle.py::"
            "test_cross_process_memory_rejects_combined_overcommit",
            "tests/concurrency/test_concurrency_cancellation_and_resource_lifecycle.py::"
            "test_cross_process_memory_reclaims_dead_owner",
            "tests/concurrency/test_concurrency_cross_process_telemetry_tuning.py::"
            "test_cross_process_reservations_reject_combined_overcommit",
            "tests/concurrency/test_concurrency_cross_process_telemetry_tuning.py::"
            "test_cross_process_registry_reclaims_dead_owner",
            "tests/concurrency/test_concurrency_cross_process_telemetry_tuning.py::"
            "test_telemetry_tuning_uses_bounded_high_percentiles",
            "tests/concurrency/test_concurrency_cross_process_telemetry_tuning.py::"
            "test_telemetry_profile_remains_bounded",
            "tests/concurrency/test_concurrency_cross_process_telemetry_tuning.py::"
            "test_adaptive_concurrency_consumes_tuned_memory_reserve",
        )
    },
    "tests/concurrency/test_concurrency_cancellation_and_resource_lifecycle.py::"
    "test_initialized_runtime_fails_fast_after_fork": frozenset({"fork is unavailable"}),
    "tests/concurrency/test_concurrency_every_format_declares_an_eligible_multi_benefit_proof.py::"
    "test_parquet_output_parallelizes_and_remains_logically_exact": frozenset(
        {
            "every-format-declares-an-eligible-multi-benefit row-group overlap requires "
            "at least four effective workers"
        }
    ),
    "tests/concurrency/test_concurrency_memory_governor.py::"
    "test_process_cpu_governor_observes_live_affinity_changes": frozenset(
        {"process CPU affinity is unavailable", "host exposes only one CPU"}
    ),
    "tests/concurrency/test_concurrency_high_core_mixed_lanes_drain_without_extra_workers.py::"
    "test_output_priority_survives_wake_coalescing": frozenset(
        {"output-steal topology requires at least three runnable CPUs"}
    ),
    **{
        nodeid: frozenset({"output-steal topology requires at least three runnable CPUs"})
        for nodeid in (
            "tests/concurrency/"
            "test_concurrency_output_steal_preference_is_dormant_through_eight_workers.py::"
            "test_output_steal_preference_is_dormant_through_eight_workers",
            "tests/concurrency/"
            "test_concurrency_output_steal_preference_is_dormant_through_eight_workers.py::"
            "test_idle_high_worker_steals_front_output_before_later_broad_work",
        )
    },
    "tests/concurrency/test_concurrency_process_governors_and_pressure.py::"
    "test_native_execution_policy_shrinks_under_process_pressure": frozenset(
        {"host exposes no multi-worker baseline"}
    ),
    "tests/concurrency/test_concurrency_park_transition_rechecks_local_work_without_stranding.py::"
    "test_public_pipeline_reports_bounded_streak_count": frozenset(
        {
            "CPU affinity is required for the four-worker contract",
            "at least four available CPUs are required",
        }
    ),
    "tests/concurrency/test_concurrency_python_is_a_first_class_concurrency_input.py::"
    "test_python_generator_file_outputs_do_not_deadlock_at_high_width": frozenset(
        {
            "exact CPU affinity is unavailable",
            "requires at least 16 visible CPUs",
            "requires at least 32 visible CPUs",
        }
    ),
    "tests/concurrency/test_concurrency_telemetry_benchmark.py::"
    "test_high_core_wide_fixture_configures_32_workers_and_publishes_all_tasks": frozenset(
        {"requires 32 visible CPUs"}
    ),
    "tests/concurrency/test_operation_arena_telemetry_contracts.py::"
    "test_public_four_worker_stats_cover_every_shard": frozenset(
        {
            "CPU affinity is required for the four-worker contract",
            "at least four available CPUs are required",
        }
    ),
    "tests/concurrency/test_threading_policy.py::"
    "test_single_local_conversion_does_not_add_host_threads_or_processes": frozenset(
        {"host thread/process accounting requires Linux /proc"}
    ),
    "tests/concurrency/test_threading_policy.py::"
    "test_detected_cpu_capacity_respects_process_affinity": frozenset(
        {"process CPU affinity is unavailable"}
    ),
}

_CONCURRENCY_REASON_PLATFORMS = {
    _OPTIONAL_POSIX_COORDINATION_REASON: frozenset({"windows"}),
    "fork is unavailable": frozenset({"windows"}),
    "every-format-declares-an-eligible-multi-benefit row-group overlap requires "
    "at least four effective workers": frozenset(
        {"linux", "macos-arm64", "macos-x86_64", "windows"}
    ),
    "host exposes no multi-worker baseline": frozenset(
        {"linux", "macos-arm64", "macos-x86_64", "windows"}
    ),
    "host exposes only one CPU": frozenset({"linux"}),
    "output-steal topology requires at least three runnable CPUs": frozenset(
        {"linux", "macos-arm64", "macos-x86_64", "windows"}
    ),
    "process CPU affinity is unavailable": frozenset({"macos-arm64", "macos-x86_64", "windows"}),
    "CPU affinity is required for the four-worker contract": frozenset(
        {"macos-arm64", "macos-x86_64", "windows"}
    ),
    "at least four available CPUs are required": frozenset({"linux"}),
    "exact CPU affinity is unavailable": frozenset({"macos-arm64", "macos-x86_64", "windows"}),
    "requires at least 16 visible CPUs": frozenset({"linux"}),
    "requires at least 32 visible CPUs": frozenset({"linux"}),
    "requires 32 visible CPUs": frozenset({"linux", "macos-arm64", "macos-x86_64", "windows"}),
    "host thread/process accounting requires Linux /proc": frozenset(
        {"macos-arm64", "macos-x86_64", "windows"}
    ),
}

_WINDOWS_MEMORY_MODULE_REASONS = {
    f"tests/memory/{filename}": "POSIX descriptor-relative filesystem hardening suite"
    for filename in _WINDOWS_POSIX_MEMORY_MODULES
}

_WINDOWS_IO_MODULE_REASONS = {
    "tests/memory/test_memory_process_identity_includes_linux_boot_id.py": (
        "POSIX descriptor-relative filesystem hardening suite"
    )
}

_WINDOWS_MEMORY_NODEID_REASONS = {
    **{
        nodeid: frozenset({_OPTIONAL_POSIX_COORDINATION_REASON})
        for nodeid in (
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_live_owner_prepared_storage_transaction_rolls_back_before_retry",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_live_owner_prepared_memory_transaction_rolls_back_before_retry",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_empty_or_canonical_prefix_never_discards_prepared_journal",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_dead_owner_prepared_transaction_is_completed",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_committed_journal_makes_success_safe_when_cleanup_fails",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_corrupt_journal_fails_closed_without_touching_main_state",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_journal_staging_file_is_reused_without_uuid_orphans",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_journal_staging_hardlink_is_rejected_before_truncate",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_journal_publication_does_not_duplicate_bounded_payloads",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_coordination_main_file_symlink_is_rejected",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_read_only_coordination_query_does_not_publish_a_journal",
            "tests/memory/test_memory_live_owner_prepared_storage_transaction_rolls_back_before_retry.py::"
            "test_committed_marker_publication_failure_rolls_back_incremental_state",
            "tests/memory/test_memory_remote_io_wait_queue_is_bounded_and_recovers_after_cancellation.py::"
            "test_cross_process_memory_overflow_preserves_previous_json",
            "tests/memory/test_memory_remote_io_wait_queue_is_bounded_and_recovers_after_cancellation.py::"
            "test_cross_process_storage_overflow_preserves_previous_json",
            "tests/memory/test_memory_remote_io_wait_queue_is_bounded_and_recovers_after_cancellation.py::"
            "test_telemetry_overflow_preserves_previous_json",
            "tests/memory/test_memory_remote_io_wait_queue_is_bounded_and_recovers_after_cancellation.py::"
            "test_cross_process_memory_release_remains_retryable_after_persist_failure",
            "tests/memory/test_memory_remote_io_wait_queue_is_bounded_and_recovers_after_cancellation.py::"
            "test_cross_process_memory_invalid_state_fails_closed",
            "tests/memory/test_memory_remote_io_wait_queue_is_bounded_and_recovers_after_cancellation.py::"
            "test_cross_process_memory_invalid_lease_entry_fails_closed",
            "tests/memory/test_memory_remote_io_wait_queue_is_bounded_and_recovers_after_cancellation.py::"
            "test_cross_process_storage_invalid_state_fails_closed",
            "tests/memory/test_memory_remote_io_wait_queue_is_bounded_and_recovers_after_cancellation.py::"
            "test_cross_process_storage_invalid_process_entry_fails_closed",
        )
    },
    **{
        nodeid: frozenset({"cross-process coordination requires POSIX flock"})
        for nodeid in (
            "tests/memory/test_memory_process_resource_governor_repairs_from_exact_leases_and_quarantines.py::"
            "test_cross_process_storage_reconciliation_preserves_same_device_sibling_account",
            "tests/memory/test_memory_reserved_finalizer_processed_owner_cannot_stick_claimed_on_recycle_failure.py::"
            "test_cross_process_growth_repairs_journal_when_local_commit_fails",
            "tests/memory/test_memory_reserved_finalizer_processed_owner_cannot_stick_claimed_on_recycle_failure.py::"
            "test_cross_process_release_retains_journal_cleanup_owner_after_fsync_failure",
            "tests/memory/test_memory_sync_retry_does_not_replay_success_after_telemetry_failure.py::"
            "test_memory_lease_releases_with_creation_time_coordination_setting",
            "tests/memory/test_memory_sync_retry_does_not_replay_success_after_telemetry_failure.py::"
            "test_storage_governor_releases_with_creation_time_coordination_setting",
            "tests/memory/test_memory_sync_retry_does_not_replay_success_after_telemetry_failure.py::"
            "test_cross_process_memory_aggregates_live_leases_per_process",
            "tests/memory/test_memory_sync_retry_does_not_replay_success_after_telemetry_failure.py::"
            "test_cross_process_memory_downsize_survives_reduced_capacity_snapshot",
            "tests/memory/test_memory_sync_retry_does_not_replay_success_after_telemetry_failure.py::"
            "test_memory_lease_releases_to_creation_time_coordination_directory",
            "tests/memory/test_memory_sync_retry_does_not_replay_success_after_telemetry_failure.py::"
            "test_storage_release_uses_creation_time_coordination_directory",
        )
    },
    **{
        nodeid: frozenset({"requires POSIX fork"})
        for nodeid in (
            "tests/memory/test_memory_cancel_linearizes_before_claimed_callback_starts.py::"
            "test_after_fork_path_reset_never_acquires_inherited_owner_lock",
            "tests/memory/test_memory_external_runtime_construction_native_exception_rolls_back_standalone_claim.py::"
            "test_parquet_dataset_lifetime_lease_fails_before_inherited_lock_after_fork",
            "tests/memory/test_memory_external_runtime_shrink_partial_failure_keeps_finalizer_exact.py::"
            "test_external_runtime_shrink_fails_before_inherited_lock_after_fork",
            "tests/memory/test_memory_process_lease_amount_is_immutable_and_ledger_authoritative.py::"
            "test_post_fork_child_cannot_reinitialize_retry_runtime",
            "tests/memory/test_memory_remote_io_bypasses_blocked_head_with_multiple_operations.py::"
            "test_fork_child_drops_inherited_resource_contextvars",
            "tests/memory/test_memory_remote_io_closed_loop_delivery_storm_is_iterative.py::"
            "test_native_options_actual_fork_replaces_inherited_locked_cache",
            "tests/memory/test_memory_remote_io_closed_loop_delivery_storm_is_iterative.py::"
            "test_system_pressure_actual_fork_replaces_lock_and_hysteresis",
        )
    },
    "tests/memory/test_memory_cancel_invalidates_already_claimed_retry.py::"
    "test_claim_reader_rejects_hardlinks": frozenset({"POSIX hard-link claim reader required"}),
    "tests/memory/test_memory_external_admission_closes_before_internal_teardown_reserve.py::"
    "test_quarantine_rejects_replaced_root_without_targeting_replacement": frozenset(
        {"POSIX quarantine root descriptor required"}
    ),
    "tests/memory/test_memory_fd_capability_counts_physical_open_and_close.py::"
    "test_fd_capability_accounts_scandir_descriptor_duplication": frozenset(
        {"descriptor-relative scandir is POSIX-only"}
    ),
    "tests/memory/test_memory_limit_enforcement.py::"
    "test_large_file_does_not_cause_file_sized_resident_memory_growth": frozenset(
        {"ru_maxrss byte conversion and allocator baseline are Linux-specific"}
    ),
    "tests/memory/test_memory_spoofed_module_cannot_enter_privileged_notifier.py::"
    "test_quarantine_root_owner_survives_guardian_rejection": frozenset(
        {"POSIX quarantine ownership descriptor required"}
    ),
}

_WINDOWS_IO_NODEID_REASONS = {
    "tests/io/test_input_python_and_local.py::"
    "test_native_python_rows_batch_encoder_checks_pending_signals": frozenset(
        {"requires POSIX interval timers"}
    ),
    "tests/remote/test_remote_process_lifecycle.py::"
    "test_sigint_interrupt_drains_remote_operation_context": frozenset(
        {"POSIX SIGINT delivery required"}
    ),
}


@dataclass(frozen=True, slots=True)
class _SkipRecord:
    """Describe one platform skip using stable pytest evidence."""

    nodeid: str
    reason: str


def _skip_inventory_sha256(skips: list[_SkipRecord]) -> str:
    """Hash exact skip identities and reasons using canonical compact JSON."""
    canonical = json.dumps(
        [asdict(skip) for skip in sorted(skips, key=lambda item: (item.nodeid, item.reason))],
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(f"{canonical}\n".encode()).hexdigest()


def strict_platform_tests_enabled() -> bool:
    """Return whether the installed-wheel CI integrity contract is active."""
    return os.environ.get(_STRICT_ENV) == "1"


def collection_integrity_component() -> str | None:
    """Return the explicitly requested source collection component, if any."""
    component = os.environ.get(_VERIFY_COLLECTION_ENV)
    if component is not None and component not in EXPECTED_TEST_INVENTORY:
        raise pytest.UsageError(f"unreviewed platform collection component: {component!r}")
    return component


def _normalize_skip_reason(longrepr: object) -> str:
    """Extract a stable skip reason from pytest's tuple or textual report."""
    value = longrepr[2] if isinstance(longrepr, tuple) and len(longrepr) == 3 else str(longrepr)
    reason = str(value).strip()
    reason = re.sub(r"^(?:Skipped|SKIPPED):\s*", "", reason)
    return reason


def _native_anomalies() -> dict[str, int]:
    """Capture native counters whose increase proves ownership corruption."""
    from schema_sanitizer.core_impl.process_resources import native_file_descriptor_snapshot
    from schema_sanitizer.core_impl.runtime_diagnostics import _native_arena_snapshot

    arena = _native_arena_snapshot()
    descriptor = native_file_descriptor_snapshot()
    if not arena.get("available") or arena.get("snapshot_failed"):
        raise RuntimeError(f"native arena diagnostics are unavailable: {arena}")
    if descriptor.get("snapshot_failed"):
        raise RuntimeError(f"native descriptor diagnostics are unavailable: {descriptor}")
    required = {
        "counter_underflows",
        "completion_memory_protocol_violations",
        "external_runtime_resident_protocol_violations",
        "external_runtime_resident_threads",
        "external_runtime_stack_debt_threads",
        "external_runtime_thread_permits",
        "native_physical_thread_capacity",
        "native_physical_threads",
        "thread_permit_snapshot_stable",
        "total_physical_thread_permits",
    }
    missing = sorted(required - set(arena))
    if missing:
        raise RuntimeError(f"native arena diagnostics are incomplete: {missing}")
    if int(arena["thread_permit_snapshot_stable"]) != 1:
        raise RuntimeError("native thread-permit snapshot is not transactionally stable")
    managed = int(arena["native_physical_threads"])
    external = int(arena["external_runtime_thread_permits"])
    total = int(arena["total_physical_thread_permits"])
    capacity = int(arena["native_physical_thread_capacity"])
    if total != managed + external or total > capacity:
        raise RuntimeError(
            "native thread-permit ledger does not conserve capacity: "
            f"managed={managed}, external={external}, total={total}, capacity={capacity}"
        )
    resident = int(arena["external_runtime_resident_threads"])
    debt = int(arena["external_runtime_stack_debt_threads"])
    if resident < 0 or debt < resident:
        raise RuntimeError(
            f"native resident-thread ledger is inconsistent: resident={resident}, stack_debt={debt}"
        )
    return {
        "native.counter_underflows": int(arena["counter_underflows"]),
        "native.completion_memory_protocol_violations": int(
            arena["completion_memory_protocol_violations"]
        ),
        "native.external_runtime_resident_protocol_violations": int(
            arena["external_runtime_resident_protocol_violations"]
        ),
        "native_fd.protocol_violations": int(descriptor["protocol_violations"]),
        "native_fd.uncertain_close_debts": int(descriptor["uncertain_close_debts"]),
    }


def _process_anomalies() -> dict[str, int]:
    """Capture process-ledger failures that must remain zero in a CI shard."""
    from schema_sanitizer.core_impl.async_scheduler import async_scheduler_snapshot
    from schema_sanitizer.core_impl.cleanup_dispatcher import cleanup_dispatcher_snapshot
    from schema_sanitizer.core_impl.process_resources import (
        process_file_descriptor_snapshot,
        process_thread_snapshot,
    )
    from schema_sanitizer.core_impl.retry_scheduler import (
        release_guardian_snapshot,
        retry_scheduler_snapshot,
    )
    from schema_sanitizer.core_impl.temporary_janitor import temporary_janitor_snapshot
    from schema_sanitizer.core_impl.temporary_storage_governor import (
        process_temporary_storage_authoritative_snapshot,
        process_temporary_storage_diagnostics,
    )
    from schema_sanitizer.remote_impl.io_permits import process_remote_io_permit_snapshot

    threads = process_thread_snapshot()
    descriptors = process_file_descriptor_snapshot()
    storage = process_temporary_storage_diagnostics()
    storage_authority = process_temporary_storage_authoritative_snapshot()
    async_state = async_scheduler_snapshot()
    cleanup = cleanup_dispatcher_snapshot()
    guardian = release_guardian_snapshot()
    retry = retry_scheduler_snapshot()
    janitor = temporary_janitor_snapshot()
    remote = process_remote_io_permit_snapshot()
    return {
        "threads.over_release_count": int(threads.over_release_count),
        "threads.unknown_lease_releases": int(threads.unknown_lease_releases),
        "fds.over_release_count": int(descriptors.over_release_count),
        "fds.unknown_lease_releases": int(descriptors.unknown_lease_releases),
        "temporary.over_release_count": int(storage.over_release_count),
        "temporary.protocol_violations": int(storage.protocol_violations),
        "temporary.authoritative_protocol_violations": int(storage_authority.protocol_violations),
        "async.protocol_violations": int(async_state.protocol_violations),
        "async.corrupted": int(async_state.corrupted),
        "cleanup.protocol_violations": int(cleanup.protocol_violations),
        "guardian.protocol_violations": int(guardian.protocol_violations),
        "retry.protocol_violations": int(retry.protocol_violations),
        "janitor.protocol_violations": int(janitor.protocol_violations),
        "remote.protocol_violations": int(remote.protocol_violations),
    }


def _increases(before: dict[str, int], after: dict[str, int]) -> list[str]:
    """Render every monotonic anomaly that increased between two snapshots."""
    return [
        f"{name}: {before.get(name, 0)} -> {value}"
        for name, value in sorted(after.items())
        if value > before.get(name, 0)
    ]


def _nonzero(values: dict[str, int]) -> list[str]:
    """Render every anomaly that is nonzero in a fresh pytest process."""
    return [f"{name}: {value}" for name, value in sorted(values.items()) if value != 0]


def _github_provenance() -> dict[str, int | str]:
    """Return validated immutable GitHub run coordinates for this certificate."""
    sha = os.environ.get("GITHUB_SHA", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "")
    if re.fullmatch(r"[0-9a-fA-F]{40}", sha) is None:
        raise RuntimeError("GITHUB_SHA must be a full 40-character commit digest")
    if not run_id.isdecimal() or int(run_id) <= 0:
        raise RuntimeError("GITHUB_RUN_ID must be a positive integer")
    if not run_attempt.isdecimal() or int(run_attempt) <= 0:
        raise RuntimeError("GITHUB_RUN_ATTEMPT must be a positive integer")
    return {"sha": sha.lower(), "run_id": int(run_id), "run_attempt": int(run_attempt)}


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    """Publish one canonical certificate without following a destination symlink."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise RuntimeError(f"integrity certificate target must be a regular file: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    if temporary.exists() or temporary.is_symlink():
        raise RuntimeError(f"integrity certificate temporary path already exists: {temporary}")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if path.is_symlink():
            raise RuntimeError(f"integrity certificate target became a symlink: {path}")
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _assert_wheel_provenance(checkout: Path) -> dict[str, str]:
    """Require every imported project module to originate outside the checkout."""
    provenance: dict[str, str] = {}
    for name, module in sorted(sys.modules.items()):
        if name != "schema_sanitizer" and not name.startswith("schema_sanitizer."):
            continue
        raw_path = getattr(module, "__file__", None)
        if not raw_path:
            continue
        path = Path(raw_path).resolve()
        provenance[name] = str(path)
        if path.is_relative_to(checkout):
            raise RuntimeError(f"{name} came from checkout instead of the certified wheel: {path}")
    if "schema_sanitizer" not in provenance or "schema_sanitizer._core_abi3" not in provenance:
        raise RuntimeError("installed-wheel provenance is incomplete")
    return provenance


def _matches_reviewed_node_reason(
    nodeid: str,
    reason: str,
    reviewed: dict[str, frozenset[str]],
) -> bool:
    """Match one exact test, including only its declared parameter variants."""
    return any(
        reason in reasons
        and (nodeid == base or (nodeid.startswith(f"{base}[") and nodeid.endswith("]")))
        for base, reasons in reviewed.items()
    )


def _skip_is_allowed(
    platform: str,
    shard: str,
    skip: _SkipRecord,
    *,
    selected_inventory_sha256: str | None = None,
) -> bool:
    """Return whether one skip is reviewed within the authenticated collection."""
    reason = skip.reason
    nodeid = skip.nodeid.replace("\\", "/")
    if "could not import" in reason.lower() or "not available" in reason.lower():
        return False
    if shard == "concurrency":
        return platform in _CONCURRENCY_REASON_PLATFORMS.get(reason, frozenset()) and (
            _matches_reviewed_node_reason(nodeid, reason, _CONCURRENCY_NODEID_REASONS)
        )
    if shard == "memory-parquet" and platform.startswith("macos-"):
        return _matches_reviewed_node_reason(
            nodeid,
            reason,
            {
                "tests/memory/test_memory_limit_enforcement.py::"
                "test_large_file_does_not_cause_file_sized_resident_memory_growth": frozenset(
                    {"ru_maxrss byte conversion and allocator baseline are Linux-specific"}
                )
            },
        )
    if shard == "memory-parquet" and platform == "windows":
        module = nodeid.split("::", 1)[0]
        if _WINDOWS_MEMORY_MODULE_REASONS.get(module) == reason:
            return selected_inventory_sha256 == EXPECTED_TEST_INVENTORY["memory-parquet"]["sha256"]
        return _matches_reviewed_node_reason(
            nodeid,
            reason,
            _WINDOWS_MEMORY_NODEID_REASONS,
        )
    if shard == "io-pipeline" and platform == "windows":
        module = nodeid.split("::", 1)[0]
        if _WINDOWS_IO_MODULE_REASONS.get(module) == reason:
            return selected_inventory_sha256 == EXPECTED_TEST_INVENTORY["io-pipeline"]["sha256"]
        return _matches_reviewed_node_reason(
            nodeid,
            reason,
            _WINDOWS_IO_NODEID_REASONS,
        )
    return False


class StrictCollectionIntegrity:
    """Fail a source-only collection whose exact reviewed identity has drifted."""

    def __init__(self, component: str) -> None:
        """Bind the checker to one known platform-test component."""
        if component not in EXPECTED_TEST_INVENTORY:
            raise pytest.UsageError(f"unreviewed platform collection component: {component!r}")
        self.component = component

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        """Compare every selected node ID with the independent reviewed policy."""
        nodeids = tuple(item.nodeid for item in session.items)
        policy = EXPECTED_TEST_INVENTORY[self.component]
        digest = _nodeid_inventory_sha256(nodeids)
        if len(nodeids) == policy["count"] and digest == policy["sha256"]:
            return
        message = (
            f"{self.component} platform collection drifted: "
            f"observed count={len(nodeids)}, sha256={digest}; "
            f"expected count={policy['count']}, sha256={policy['sha256']}"
        )
        print(message, file=sys.stderr)
        pytest.exit(message, returncode=pytest.ExitCode.TESTS_FAILED)


class StrictPlatformIntegrity:
    """Audit one pytest process without relying on test order or runner timing."""

    def __init__(self) -> None:
        """Initialize empty evidence before pytest starts the session."""
        self.platform = os.environ.get(_PLATFORM_ENV, "")
        self.shard = os.environ.get(_SHARD_ENV, "")
        self.evidence_path = Path(os.environ.get(_EVIDENCE_ENV, "artifacts/integrity.json"))
        self.checkout = Path(os.environ.get("GITHUB_WORKSPACE", Path.cwd())).resolve()
        self.github: dict[str, int | str] = {}
        self.skips: list[_SkipRecord] = []
        self.issues: list[str] = []
        self.provenance: dict[str, str] = {}
        self.initial_native: dict[str, int] = {}
        self.previous_native: dict[str, int] = {}
        self.final_native: dict[str, int] = {}
        self.initial_process: dict[str, int] = {}
        self.final_process: dict[str, int] = {}
        self.pytest_exitstatus = int(pytest.ExitCode.INTERNAL_ERROR)
        self.selected_test_count = 0
        self.expected_test_count = 0
        self.selected_test_inventory_sha256 = ""
        self.expected_test_inventory_sha256 = ""

    def pytest_sessionstart(self, session: pytest.Session) -> None:
        """Validate the platform tuple, dependencies, provenance, and clean baseline."""
        del session
        expected_system = _PLATFORM_SYSTEM.get(self.platform)
        if expected_system is None or not sys.platform.startswith(expected_system):
            raise pytest.UsageError(
                f"strict platform identity mismatch: artifact={self.platform!r}, "
                f"sys.platform={sys.platform!r}"
            )
        if (self.platform, self.shard) not in _MAX_SKIP_COUNT:
            raise pytest.UsageError(
                f"strict platform shard is not reviewed: {self.platform}:{self.shard}"
            )
        self.github = _github_provenance()
        for module_name in _REQUIRED_MODULES:
            importlib.import_module(module_name)
        self.provenance = _assert_wheel_provenance(self.checkout)
        self.initial_native = _native_anomalies()
        self.previous_native = dict(self.initial_native)
        self.initial_process = _process_anomalies()
        initial_issues = _nonzero(self.initial_native) + _nonzero(self.initial_process)
        if initial_issues:
            self.issues.append("fresh process began with anomalies: " + "; ".join(initial_issues))

    def pytest_collection_modifyitems(
        self,
        session: pytest.Session,
        config: pytest.Config,
        items: list[pytest.Item],
    ) -> None:
        """Require exactly the declared stress marker membership when requested."""
        del session, config
        expected_raw = os.environ.get(_STRESS_NODEIDS_ENV)
        if expected_raw is None:
            return
        expected = tuple(nodeid for nodeid in expected_raw.split("|") if nodeid)
        actual = tuple(
            sorted(item.nodeid for item in items if item.get_closest_marker("native_stress"))
        )
        if actual != tuple(sorted(expected)):
            raise pytest.UsageError(
                "native_stress collection changed: "
                f"expected={tuple(sorted(expected))!r}, actual={actual!r}"
            )

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        """Require the exact selected-test inventory independently of skip outcomes."""
        selected_nodeids = [item.nodeid for item in session.items]
        self.selected_test_count = len(selected_nodeids)
        collection_policy = EXPECTED_TEST_INVENTORY[self.shard]
        self.expected_test_count = int(collection_policy["count"])
        self.selected_test_inventory_sha256 = _nodeid_inventory_sha256(selected_nodeids)
        self.expected_test_inventory_sha256 = str(collection_policy["sha256"])
        if self.selected_test_count != self.expected_test_count:
            self.issues.append(
                "selected test inventory changed: "
                f"observed={self.selected_test_count}, expected={self.expected_test_count}"
            )
        if self.selected_test_inventory_sha256 != self.expected_test_inventory_sha256:
            self.issues.append(
                "selected test identities changed: "
                f"observed={self.selected_test_inventory_sha256}, "
                f"expected={self.expected_test_inventory_sha256}"
            )

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        """Retain exact node IDs and reasons for every skip report."""
        if not report.skipped:
            return
        skip = _SkipRecord(report.nodeid, _normalize_skip_reason(report.longrepr))
        self.skips.append(skip)
        if report.when == "teardown":
            self.issues.append(f"teardown skip is forbidden: {skip.nodeid} ({skip.reason})")

    @pytest.hookimpl(hookwrapper=True, trylast=True)
    def pytest_runtest_makereport(
        self,
        item: pytest.Item,
        call: pytest.CallInfo[None],
    ) -> Any:
        """Attribute native corruption after the test and all of its teardown."""
        del call
        outcome = yield
        report = outcome.get_result()
        if report.when != "teardown":
            return
        try:
            current = _native_anomalies()
            increases = _increases(self.previous_native, current)
            self.previous_native = current
        except BaseException as exc:  # noqa: BLE001 - diagnostics must fail closed
            increases = [f"native diagnostics failed: {type(exc).__name__}: {exc}"]
        if not increases:
            return
        message = f"native integrity changed after {item.nodeid}: " + "; ".join(increases)
        self.issues.append(message)
        if report.passed:
            report.outcome = "failed"
            report.longrepr = message

    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        """Check final ledgers and skips after session fixtures have torn down."""
        self.pytest_exitstatus = int(exitstatus)
        try:
            self.final_native = _native_anomalies()
            self.final_process = _process_anomalies()
            self.provenance = _assert_wheel_provenance(self.checkout)
            self.issues.extend(_increases(self.initial_native, self.final_native))
            self.issues.extend(_increases(self.initial_process, self.final_process))
        except BaseException as exc:  # noqa: BLE001 - diagnostics must fail closed
            self.issues.append(f"final integrity snapshot failed: {type(exc).__name__}: {exc}")

        rejected = [
            skip
            for skip in self.skips
            if not _skip_is_allowed(
                self.platform,
                self.shard,
                skip,
                selected_inventory_sha256=self.selected_test_inventory_sha256,
            )
        ]
        if rejected:
            self.issues.append(
                "unreviewed skips: "
                + "; ".join(f"{skip.nodeid} ({skip.reason})" for skip in rejected)
            )
        maximum_skips = _MAX_SKIP_COUNT[(self.platform, self.shard)]
        if len(self.skips) > maximum_skips:
            self.issues.append(
                f"skip ceiling exceeded: observed={len(self.skips)}, maximum={maximum_skips}"
            )
        skip_inventory_sha256 = _skip_inventory_sha256(self.skips)
        expected_skip_inventory_sha256 = EXPECTED_EXACT_SKIP_INVENTORY_SHA256.get(
            (self.platform, self.shard)
        )
        if (
            expected_skip_inventory_sha256 is not None
            and skip_inventory_sha256 != expected_skip_inventory_sha256
        ):
            self.issues.append(
                "reviewed skip identities changed: "
                f"observed={skip_inventory_sha256}, expected={expected_skip_inventory_sha256}"
            )

        payload = {
            "format": "schema-sanitizer-platform-integrity-v2",
            "github": self.github,
            "platform": self.platform,
            "shard": self.shard,
            "maximum_skip_count": maximum_skips,
            "skips": [asdict(skip) for skip in self.skips],
            "expected_skip_inventory_sha256": expected_skip_inventory_sha256,
            "skip_inventory_sha256": skip_inventory_sha256,
            "expected_test_count": self.expected_test_count,
            "selected_test_count": self.selected_test_count,
            "expected_test_inventory_sha256": self.expected_test_inventory_sha256,
            "selected_test_inventory_sha256": self.selected_test_inventory_sha256,
            "initial_native_anomalies": self.initial_native,
            "final_native_anomalies": self.final_native,
            "initial_process_anomalies": self.initial_process,
            "final_process_anomalies": self.final_process,
            "issues": sorted(set(self.issues)),
            "pytest_exitstatus": self.pytest_exitstatus,
            "provenance": self.provenance,
            "satisfied": not self.issues and self.pytest_exitstatus == int(pytest.ExitCode.OK),
        }
        try:
            _atomic_write_json(self.evidence_path, payload)
        except BaseException as exc:  # noqa: BLE001 - certificate publication must fail closed
            self.issues.append(
                f"integrity certificate publication failed: {type(exc).__name__}: {exc}"
            )
        if self.issues:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED
            reporter = session.config.pluginmanager.get_plugin("terminalreporter")
            if reporter is not None:
                reporter.write_sep("=", "strict platform integrity failures", red=True)
                for issue in sorted(set(self.issues)):
                    reporter.write_line(issue, red=True)


__all__ = [
    "collection_integrity_component",
    "StrictCollectionIntegrity",
    "StrictPlatformIntegrity",
    "strict_platform_tests_enabled",
]
