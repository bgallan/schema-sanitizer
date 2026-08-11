# Memory & concurrency hardening — recovered Pass 56

This archive reconstructs the lost Pass 56 on top of the recovered Pass 55. The
original Pass 56 ZIP was completed in the collapsed conversation but its upload
failed; this rebuilt archive therefore preserves the recovered invariants rather
than claiming byte-for-byte identity with that lost artifact.

## Restored invariants

1. **Memory-before-worker admission**

   - Resident-memory credit is acquired before project/native helper capacity.
   - Required-memory admission fails before touching worker capacity when no
     authoritative operation ledger exists.
   - If exact memory admission downshifts the candidate, only the smaller helper
     request is published.
   - Base cleanup is the exact reverse: control → worker → memory.

1. **Exactly-once async ownership and retry delivery**

   - Async task-domain release tests and commits its released state under the same
     authoritative condition lock.
   - `_AsyncSchedulerAdmission.close()` clears each capability only after its
     cleanup succeeds; a throwing cleanup remains retryably owned.
   - A successful user/provider operation is the delivery commit. Cancellation
     arriving after that commit cannot convert success into an apparent retryable
     failure.

1. **Hard O(window) async result ownership**

   - Ordered async scheduling replaces its dynamic pending dictionary with a
     fixed ring of `_AsyncPendingResultSlot` records bounded by worker count.
   - Ring collisions/ownership mismatches fail closed as internal invariant
     violations rather than permitting silent growth.

1. **Deadline-bounded native retained-byte backpressure**

   - `OperationTaskArena::SubmitCharged` no longer waits indefinitely on
     `atomic::wait` for retained-byte capacity.
   - A preallocated condition variable uses `wait_until` with a bounded deadline.
   - The target worker queue mutex is released before waiting and reacquired only
     for lifecycle/ownership revalidation.

1. **Tri-state effective cgroups in Python and C++**

   - `VALUE`, `UNBOUNDED`, and `UNKNOWN` are distinct states.
   - Memory/PID/CPU observations walk the full constraining hierarchy to the
     controller mount root.
   - Unknown Linux observations fail closed for new capacity instead of being
     interpreted as unlimited.
   - Native memory-budget, physical-thread, CPU-capacity and allocation-registry
     paths consume the effective hierarchy rather than only the leaf cgroup.

1. **Terminal metadata/record hardening inherited from recovered Pass 55**

   - Fixed terminal-owner record bank.
   - Explicit metadata byte attribution.
   - Non-throwing rejection diagnostics under memory pressure.

## Superseded historical assertions

Passes 48/49/54 contained tests that intentionally asserted the old
`worker -> bytes` acquisition / `bytes -> worker` release order. Pass 56 changes
that contract to `bytes -> worker` / `worker -> bytes`; those historical test
expectations are updated in this recovered tree to reflect the stronger invariant.
