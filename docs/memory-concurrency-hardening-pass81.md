# Pass 81 — exact ownership across FD / external-runtime / memory handoff boundaries

Pass 81 extends the pass80 native-receipt model to the remaining split-commit paths that could still lose or duplicate resource authority when an asynchronous exception lands immediately after a native commit.

## 1. Exact file-descriptor permit receipts

The ABI now exposes an RAII file-descriptor permit capsule backed by `ProcessFdPermitLease`:

- `process_file_descriptor_permit_lease_acquire_wait`
- `process_file_descriptor_permit_lease_resize`
- `process_file_descriptor_permit_lease_amount`

`_LedgerEntry.native_fd_lease` is the authoritative owner. `native_fd_amount` remains a mirrored diagnostic/legacy field only.

Release and shrink now operate on the exact receipt before Python forgets authority. If an exception arrives after native commit but before Python publication, retrying the same target is idempotent.

This closes both historical FD failure modes:

1. acquire -> attach -> exception -> manual release + lease release (double release), and
1. Python ledger release/shrink -> exception -> native amount never returned (orphaned permit).

The amount-based ABI remains only for backward compatibility with older binaries.

## 2. Target-based external-runtime ownership

The non-shared external-runtime path now prefers `ProcessExternalRuntimeThreadPermitLease` and wraps it in `_ExactExternalRuntimeNativePermit` instead of treating an integer as authority.

Exact owners support target-based resize. `ExternalRuntimeConcurrencyLease.shrink_to()` uses the target width when available, making retry after post-commit interruption idempotent rather than subtracting the same delta twice.

The shared runtime-pool rollback now detects a first-claim acquisition that committed native capacity but never committed its Python claim. It shrinks the exact envelope to zero and clears mirrored state before retiring the coordinator entry.

`_ExternalRuntimeCleanupState.native_lease` is now the authoritative cleanup object; `native` and `native_amount` are retained as compatibility mirrors for historical test doubles and diagnostics.

## 3. Memory ownership-transfer recovery

`OperationMemoryLease.transfer_stage()` no longer unconditionally disarms the successor finalizer when transfer raises.

On failure it probes the authoritative ledger:

- if the successor capability owns the lease, the successor finalizer remains armed and the predecessor becomes inert;
- if the swap did not commit, the provisional successor is retired;
- if ownership cannot be inspected, both rooted authorities are retained fail-closed; only one capability can authenticate in the exact ledger.

This closes the interruption window after owner swap but before wrapper publication.

## 4. Receipt provenance and fork hardening

Native memory, external-runtime and FD receipts carry process provenance. Memory receipts additionally carry `reservation_id` and generation metadata; external-runtime and FD receipts carry `receipt_id`, generation and owner PID.

Receipt creation/mutation/query operations reject inherited use after `fork()`. Destruction in a child remains harmless because underlying release primitives are process-owner guarded.

## 5. Fault-injection coverage

`tests/memory/test_memory_safety_pass81.py` adds targeted failure tests for:

- FD release interrupted after native commit;
- FD shrink interrupted after native commit and retried;
- shared external-runtime claim interrupted before claim publication;
- non-shared external-runtime target shrink interrupted after native commit;
- memory transfer interrupted immediately after authoritative owner swap;
- exact non-shared external-runtime acquisition;
- receipt provenance/fork guards and cleanup-owner structure.

## Validation

- `tests/memory/test_memory_safety_pass60.py` through `pass81.py`: **262 passed, 1 skipped**.
- Pass81-specific tests: **9 passed**.
- `python -m compileall` over source and pass81 tests: passed.
- Direct C++ syntax/warning validation for all modified ABI translation units passed with:
  - `g++ -std=c++20 -Wall -Wextra -Werror -fsyntax-only`
  - `ordered_executor_probe.cc`
  - `options/prepare.cc`
  - `_core_abi3_module.cc`

The complete `tests/memory` suite still cannot collect in this environment because `schema_sanitizer._core_abi3` has not been built. Project CMake configure also remains unavailable here because the repository requires CMake >= 4.3 while the environment provides CMake 3.31.6.
