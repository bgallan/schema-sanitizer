# Pass 76 — Corruption quarantine and terminal-ownership hardening

## Scope

Pass 76 continues the memory/concurrency hardening work from pass 75. The central invariant is now:

> Corruption closes admission, but it must not close exact cleanup authority.

A component that detects a damaged free-ring, counter, or auxiliary index may stop issuing new capabilities, while an already-authenticated owner must still be able to retire/quarantine its resource without making it reusable.

## Changes

### Async scheduler terminal debt

- Added an explicit `BUILDING` terminal-debt state.
- A debt becomes authoritative before per-task publication starts.
- The debt retains the caller's task set while construction is incomplete, so a `MemoryError` during publication cannot orphan active task slots.
- Reaping accepts `BUILDING`, `ACTIVE`, and retry-pending debts.
- Cursor, retry-count, generation, and global debt-count arithmetic is precomputed before authoritative state transitions.
- Partially published task chains are cleared only as part of terminal retirement.

### Stage-admission construction escrow

- Stage construction rollback now owns a separate pre-rooted `RootedFinalizerAuthority` before the first resource is acquired.
- The rooted authority is updated as construction ownership changes.
- Cleanup failures arm/publish the rooted authority instead of relying on a transient stack owner.
- The historical naked-ticket helper remains only as a compatibility/fault-injection surface; production constructors pass the rooted authority.

### BoundedGenerationPool

- `acquire()` validates that the slot obtained from the free-ring is actually `FREE` before changing its generation.
- A free-ring/state mismatch marks the pool corrupted and refuses the admission instead of silently re-keying an active capability.
- Corruption enters quarantine mode: new admissions remain closed, but exact active tokens can still be retired.
- Exact cleanup during quarantine moves a slot to `RETIRED` and never puts it back into the damaged free-ring.
- Counter-underflow paths do not saturate to zero or make damaged slots reusable.

### Generation-pool consumers

- Runtime registry retirement now confirms exact generation retirement before removing the local owner entry.
- Cross-process memory explicit/finalizer retirement confirms exact generation retirement before removing authoritative contribution maps.
- Failed generation retirement therefore leaves enough local ownership to retry or diagnose rather than silently discarding the capability.

### Path-claim admission and owner handoff

- `_PathClaimAdmission` uses a pre-rooted authority and its destructor only arms that authority; it no longer reaches a blocking `release_ticket()` through a helper.
- Admission-to-`PathClaimOwner` transfer reuses the same generation and rooted authority.
- Constructor rollback preserves the historical release fault-injection point but falls back to rooted ACK-only publication on failure.

### TerminalOwnershipLedger

- Owner-count arithmetic is precomputed before publishing an active slot.
- Retirement uses checked decrements; an impossible underflow preserves the terminal proof instead of dropping it with `max(0, ...)`.
- Category retirement validates the full decrement before clearing any matching slots.

### Static control-plane / finalizer footprint

- Removed `_StaticFootprintGuard.__del__`.
- Finalizer-escrow constructors now perform static-footprint rollback explicitly in their constructor exception path.
- Static control-plane rollback no longer saturates `_TOTAL`; inconsistent totals fail closed.
- Rollback arithmetic is prepared before deleting the corresponding static-control entry.

### Provider throttle

- A release with no authoritative in-flight slot is rejected before updating AIMD/circuit-breaker learning state.
- Over-release can no longer change successes, failure streaks, adaptive window, or circuit state.

### Fork preparation

- Remote-I/O and provider-throttle fork-reset objects are allocated as two runtime banks before `fork()`.
- `before_fork` preparation only selects an already-built bank; it no longer constructs locks, thread locals, dictionaries, ordered dictionaries, expiry heaps, or waiter mirrors under the at-fork callback.

### Operation-memory cleanup tails

- Remaining constructor/transfer cleanup tails use rooted retire-or-ACK handling and preserve the primary exception with bounded diagnostic notes.

### Legacy unreserved finalizer path

- `NativeSourcePlan.__del__` no longer falls back to the unreserved `FinalizerEscrow` path.
- Production cleanup therefore requires ownership to have a reserved generation before physical resource ownership is exposed.
- Synthetic `object.__new__` compatibility instances that never acquired production ownership simply have nothing to release.

## New invariants

1. `FREE -> ACTIVE` is forbidden once a generation pool is corrupted.
1. `ACTIVE -> RETIRED` remains possible for an exact authenticated capability while corrupted.
1. A terminal debt is owned from `BUILDING`, not only after all child task slots are published.
1. Construction rollback owners are rooted before the first primary resource acquisition.
1. GC destructors do not synchronously retire escrow tickets through blocking escrow locks.
1. Terminal-ownership counters are never repaired by saturating decrements.
1. At-fork preparation selects preallocated reset state instead of allocating it.

## Validation

- `tests/memory/test_memory_safety_pass76.py`: **13 passed**.
- Accumulated memory-hardening regression `pass54` through `pass76`: **307 passed**.
- Temporary-storage permit tests: **2 passed, 8 skipped**.
- `python -m compileall -q src`: passed.
- `PYTHONPATH=src python meta/ci/check_primary_cleanup.py`: passed.

Additional runtime/pipeline suites that import the native extension cannot be fully executed in this environment because `schema_sanitizer._core_abi3` is not installed. For `tests/concurrency/test_runtime_lifecycle_part01.py`, 2 tests pass and 6 fail at native-core import; the failures are the same environment limitation rather than Python hardening assertions.
