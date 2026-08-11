# Pass 72 — concurrency / memory hardening

Pass 72 concentrates on the remaining multi-authority and post-commit failure
windows discovered after pass 71. The primary invariant is now:

> After an authoritative side effect commits, every failure path leaves either
> exactly one retryable owner/capability or a fully retired resource. A retry may
> never repeat the primary debit/release merely because secondary bookkeeping
> failed.

## Implemented hardening

### 1. Reserved finalizer escrow: cleanup commit precedes recycle bookkeeping

`ReservedFinalizerEscrow.process_one()` no longer leaves a successfully cleaned
owner stuck in `CLAIMED` when free-ring recycle bookkeeping fails. Successful
cleanup moves the slot to owner-free `RECYCLE_PENDING`, retires active/published
ownership, and makes ring recycling best-effort. Admission scavenges pending
slots later. Quiescence therefore reflects resource ownership rather than an
allocator-sensitive recycle operation.

### 2. Release/finalizer tickets remain authoritative until retirement confirms

Memory leases, temporary-storage leases, cross-process-memory leases, operation
resources/contexts, path claims and related transfer paths no longer clear their
finalizer ticket merely because the primary resource is already released. If
`release_ticket()` does not confirm retirement, the ticket remains attached and
is retried by `release()` or a terminal-safe GC path. This prevents bounded
finalizer-slot leakage after an otherwise successful cleanup.

### 3. Operation-memory constructor rollback is retryable

If `OperationMemoryLease` reserves bytes, registration fails, and the immediate
ledger rollback also fails, the partially constructed lease remains the exact
owner and publishes through its pre-reserved finalizer slot. Construction no
longer destroys the only recovery capability.

Python memory-lease release additionally tracks `physical_released` in the exact
ledger entry. The native debit publishes this state before allocation-capable
observation work; secondary control-plane-ticket retirement happens while the
entry is still rooted. A retry therefore cannot debit resident memory twice.

### 4. Cross-process resident-memory journal/direct-ledger reconciliation

Cross-process memory now uses direction-aware commit ordering:

- growth: persistent journal first, exact direct ledger second;
- shrink/release: exact direct ledger first, persistent journal second.

Failed second commits preserve conservative over-reservation and are reconciled
from the exact direct-ledger total. A release whose journal fsync fails remains a
journal-cleanup owner instead of dropping all authority. Constructor rollback
also keeps the direct capability/finalizer owner if retirement cannot commit.

### 5. Temporary storage: exact process capabilities and retryable release tails

Temporary-storage process reservations remain exact capability based. Pass 72
adds:

- a cross-device move transaction that roots the replacement capability through
  rollback;
- two-phase lease resize that performs filesystem/journal work outside the pool
  condition;
- retry metadata for a process resize committed before local accounting;
- release state (`process_released`, `local_released`) that prevents repeated
  physical or local debits when control-plane retirement fails;
- finalizer-ticket ownership attached to the inert lease immediately, so even
  pre-admission ticket cleanup can be retried;
- post-commit `BaseException` handling: a prepublished process capability is
  activated in the aggregate commit tail; exact rollback is attempted on a
  later exception, and failed rollback leaves the capability globally rooted as
  an orphan for a later admission safe point.

### 6. Remote I/O release is multi-authority retryable

Permit, submitted-coroutine and capacity-registration entries now retain an
explicit `resource_released` state. The primary governor debit can commit while
the entry remains authoritative for its control-plane ticket. If secondary
retirement fails, retry skips the primary release and completes only the
remaining authority. Derived scheduling/delivery remains best-effort after the
primary commit.

### 7. External-runtime configuration state machine

External runtime configuration now records `stable`, `inflight` and `uncertain`
states. Getter/setter callbacks execute outside coordinator locks. A setter that
succeeds but cannot be verified leaves conservative stack debt and an explicit
uncertain state. Pool entries cannot retire while logical acquisition or
configuration is in flight; physical/logical release waits for configuration to
finish. Explicit pool retirement is the proof that can clear persistent debt.

Shutdown now treats configuration-inflight as live work and configuration-
uncertain as an observability failure. Resident-only third-party pools may still
survive once schema-sanitizer owns no active claim.

### 8. External-runtime logical rollback retains its lease

A logical governor lease acquired during construction is no longer silently
lost if later reconciliation fails and `lease.release()` also fails. The
prearmed construction/finalizer owner retains the exact lease for safe-point
cleanup.

### 9. Resident identity / stack-debt native invariant

The native runtime validates final targets for external-runtime residency:
`stack_debt >= identity_credit`. Individual identity/debt mutators reject
transitions that would violate the same invariant, and the combined update keeps
the conservative increase/decrease order inside the native mutation epoch.

### 10. Inflight latches publish allocation-capable counters first

Staged-result cleanup, partition lookahead and external-runtime configuration
prepare their next generation/count before publishing the corresponding
`*_inflight` latch. A `MemoryError` cannot leave a latch active without a valid
owner/generation able to finish it.

### 11. Staged-result ownership eagerly retires finalizer capacity

Successful terminal `consume()`/`abandon()` cancels the prepared finalizer owner
immediately. Completed futures no longer retain scarce finalizer capacity solely
because a `StagedResultOwnership` wrapper remains reachable.

### 12. Advisory telemetry cannot break a committed resource transaction

Resource telemetry is explicitly best-effort even for `BaseException`
subclasses. Cancellation remains authoritative at explicit operation safe
points; telemetry occurring after a resource commit can no longer turn that
commit into a failed public operation.

### 13. Post-commit convergence fault injection

Pass-72 regressions inject failures at the concrete boundaries found during the
audit, including:

- finalizer recycle after successful processor cleanup;
- operation-memory registration + rollback double failure;
- memory post-release observation failure;
- cross-process journal/direct-ledger divergence and journal cleanup failure;
- temporary-storage cross-device rollback, resize reconciliation, post-commit
  `KeyboardInterrupt`, failed exact rollback, and control-ticket retirement;
- external-runtime post-setter verification failure;
- remote-I/O control-ticket failure after the permit debit;
- terminal staged-result finalizer retirement.

These tests assert the convergence property directly rather than relying on an
opcode blacklist: after every injected failure, either an exact authoritative
owner remains or the resource total is already fully retired.

## Validation

Environment validation performed for this source-only pass:

- `tests/memory/test_memory_safety_pass54.py` … `pass72.py`: **264 passed**.
- Additional pass46/pass47/pass49/pass50/pass51/pass72 set: **155 passed, 3 skipped**.
- `tests/memory/test_temporary_storage_permits.py`: **2 passed, 8 skipped**.
- Pass-72-specific regressions: **18 passed**.
- `python -m compileall -q src tests/memory/test_memory_safety_pass72.py`: passed.
- C++20 syntax-only compilation passed for:
  - `cpp/src/internal/runtime/operation_task_arena.cc`
  - `cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc`
  - `cpp/src/api/python_abi3/_core_abi3_module.cc`

Known environment limitations are unchanged:

- this source ZIP does not contain a built `schema_sanitizer._core_abi3` for the
  current Python 3.13 runtime, so native-dependent suites cannot be collected;
- installed CMake is 3.31.6 while the project requires CMake >= 4.3 for the full
  configured native build.
