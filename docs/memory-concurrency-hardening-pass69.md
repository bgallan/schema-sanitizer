# Pass 69 — concurrency and memory hardening

Pass 69 closes the remaining ownership, liveness, external-runtime identity,
and native-ledger diagnostic gaps identified after pass 68.

## Implemented changes

### Dynamic governor liveness

- Queued requests are revalidated against live refreshed capacity.
- A request that became impossible after a cgroup/RLIMIT/process-capacity shrink
  is removed with a `capacity_shrunk` resource error instead of permanently
  blocking smaller feasible FIFO followers.

### Allocation-safe admission construction

- `acquire_parallel_admission()` now creates a rollback owner and reserves a
  bounded finalizer-escrow generation before the first resource acquisition.
- Memory, execution and control-plane capabilities are rooted immediately after
  each successful acquisition.
- `StageConcurrencyAdmission` is constructed directly in the normal path,
  eliminating the former base-to-stage allocation window.
- Domain ownership is rooted in a pre-existing `pending_domain_lease` slot before
  tuple growth, so a `MemoryError` during publication cannot lose the lease.
- Failed rollback remains retryable through the bounded stage-admission escrow.

### External runtime pool lifecycle and identity

- Runtime-reported resident width `0` is authoritative and retracts stale native
  resident attribution.
- A missing/failing resident probe is fail-closed and also retracts stale identity
  credit rather than discounting unrelated OS threads indefinitely.
- Integrations may provide `schema_sanitizer_thread_pool_identity()` so multiple
  wrappers of the same native/global pool share one coordinator entry.
- Explicit identity tokens are size-bounded; oversized identities fall back to
  per-wrapper isolation instead of retaining unbounded control-plane memory.
- Explicitly identified pools do not retain the wrapper object itself.
- Coordinator entries are globally bounded.
- Physical + logical claim slots share one aggregate global bound in addition to
  the existing per-pool bound.
- Worst-case entry/claim metadata is charged to the static process control-plane
  budget.

### Allocation-before-commit external claims

- Physical and logical claim capability objects and dictionary slots are
  constructed/published before the corresponding native/governor capacity commit.
- Post-commit publication only mutates already-existing slots.
- Empty coordinator entries are retired on precommit failures and global-cap
  rejection.
- ABI wrappers now roll back native thread/FD permits if `PyLong_FromSize_t()`
  cannot publish the granted count after a native commit.

### Native thread-memory accounting

- Thread stack reservation is configurable through
  `SCHEMA_SANITIZER_THREAD_STACK_RESERVATION_BYTES`.
- On supported POSIX hosts, finite `RLIMIT_STACK` is incorporated conservatively;
  the previous 8 MiB value remains the minimum/default.
- Stack reservation accounting uses at least the authoritative active total and
  otherwise managed workers plus `max(external_active, external_resident)`, so
  transient subledger publication cannot undercount active stacks and persistent
  external pools remain represented between operation claims.

### Native permit ledger consistency

- Managed/external/total permit mutation is tracked by both an in-flight writer
  count and a monotonic mutation epoch.
- Runtime snapshots retry until no writer is active, the epoch is unchanged
  across the read and `total == managed + external`.
- Snapshot stability is exported through the ABI and is required by release
  protocol validation.
- Real diagnostic payloads expose their native tuple width; release certification
  rejects binaries older than the pass-69 29-field snapshot schema.

### Resident-thread protocol hardening

- Native resident-thread addition uses saturating CAS rather than unchecked
  `fetch_add`.
- Oversized observations/overflow are counted as protocol violations.
- Resident-thread protocol violations are exported and fail release certification.

### Cross-language FD liveness

- Blocking native FD acquisitions are serialized so only one waiting request
  competes for permits at a time.
- Opportunistic try-acquire traffic cannot repeatedly overtake a blocking waiter.
- The FIFO-serialization lock observes the same timeout deadline.
- Zero-timeout calls preserve the historical one-shot immediate-acquire behavior.

## Validation in this source-only environment

Successful checks:

- `tests/memory/test_memory_safety_pass69.py`: 12 passed.
- hardening regression set pass54–pass69: 215 passed, 2 fork deprecation warnings.
- additional historical pass47/pass49/pass50/pass51 set: 109 passed, 3 skipped.
- Python `compileall` for `src/schema_sanitizer`: passed.
- C++20 syntax-only compilation:
  - `cpp/src/internal/runtime/operation_task_arena.cc`: passed.
  - `cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc`: passed.
  - `cpp/src/api/python_abi3/_core_abi3_module.cc`: passed.
- broad Parquet test collection: 97 passed, 180 skipped; the remaining 3 tests
  fail at import because this source archive has no built `_core_abi3` for the
  environment, not because of a pass-69 assertion/regression.

Environment limitations:

- Full native/runtime pytest collection cannot run without a built
  `schema_sanitizer._core_abi3` for Python 3.13.
- Full CMake configure/build cannot run here because the project requires CMake
  > = 4.3 while the environment provides CMake 3.31.6.
