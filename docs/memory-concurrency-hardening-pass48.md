# Memory / Concurrency Hardening — Pass 48

Pass 48 closes correctness and compositional gaps found after pass 47. The focus is no longer only boundedness of individual registries, but atomic transfer of ownership, fork-safe control primitives, non-interfering finalizer publication, recoverable admission saturation, and end-to-end concurrency contracts backed by real enforcing callables.

## 1. Atomic native active → completion retention

The ordered native executor no longer performs a separate headroom check followed by a completion-byte retain. `OperationTaskArena::TryTransferActiveToCompletion()` is now the only authoritative transition for worker-held active bytes that become completion/result bytes.

A thread-local `ActiveRetainedCharge` scope owns the active credit for the executing worker. Its transfer performs one CAS over `retained_bytes_total`, verifies the target against `queue_byte_capacity`, subtracts active ownership exactly once, and marks the scope transferred. Scope destruction releases active bytes only when no transfer occurred. This removes the TOCTOU between concurrent publishers, avoids a transient active+completion double charge, and prevents a later worker cleanup from double-subtracting transferred bytes.

## 2. Fork-safe newly introduced global locks

The process control-plane budget and cross-process temporary-storage account registry preallocate fresh post-fork state. Child reset swaps in fresh locks/ledgers without acquiring locks inherited from parent threads. Provider-throttle child reset drops inherited parent tickets rather than attempting to release parent ownership from an `after_in_child` callback.

## 3. Finalizer publication never competes with observation

`ReservedFinalizerEscrow` snapshots no longer acquire per-slot locks used by `publish_reserved()`. Publication remains non-blocking from finalizer contexts, while diagnostics use lock-free slot-state reads and monotonic aggregate epochs. Observability therefore cannot itself cause a finalizer handoff failure.

Escrow state now separates:

- recoverable admission rejection because all slots are temporarily occupied;
- irreversible publication failure after ownership exists;
- permanently retired generations.

Only real publication/ownership loss poisons terminal success.

## 4. OOM-safe operation-memory resize

Python operation-memory lease entries are mutable authoritative records rather than replacement tuples. `resize()` performs the physical byte transition and then updates a pre-existing record field without allocating a replacement ledger object after the commit point. The ledger can no longer disagree with physical bytes merely because tuple construction failed after reserve/release.

## 5. BaseException-safe remote grant delivery

Remote grant delivery now reclaims every unvisited grant in the current preallocated batch and any linked replacement batches before propagating `KeyboardInterrupt` or `SystemExit`. A BaseException can no longer leave later waiters marked granted and consuming `_in_use` without any delivery path.

## 6. Availability retry is independent of the scheduler it wakes

Level-triggered process-resource availability no longer relies on the retry scheduler to recover a failed notifier publication. The notifier owns a pre-reserved emergency execution slot and a single bounded emergency governor debt. A dirty availability level can therefore re-drive publication even if the normal retry scheduler is saturated or is itself blocked waiting for the same resource.

The worker-start predicate also treats emergency debt as runnable work when the normal queue is empty.

Notifier dispatch identity is immutable per governor/registration. Production governors bind the canonical runtime dispatcher; tests may inject a private dispatcher explicitly. Mutable module-global replacement can no longer redirect delayed work from one notifier/governor into another instance.

## 7. ABA-resistant finalizer quiescence

Registered finalizer domains expose monotonic publication/progress epochs. Runtime shutdown compares an activity token containing those epochs, not only current cardinalities. A retire+publish sequence that returns the counts to the same values still advances the token and prevents a false quiescence declaration.

## 8. Exact, no-throw control-plane ownership

Control-plane ownership is authenticated by private ledger `(token, owner identity, capability)` and authoritative amount. Release removes the exact ledger entry as its commit point; caller-visible ticket mutation cannot forge the amount returned. Callers retain tickets until release succeeds and no longer clear the only capability before the authoritative release.

Runtime shutdown now requires the control-plane budget to reach zero active tickets/reserved dynamic bytes and rejects over-release/corruption/reconciliation debt.

A conservative static runtime baseline covers pre-reserved escrow/lock infrastructure and is included in governed process headroom.

## 9. Active owners are included in the control plane

Pass 48 extends control-plane accounting beyond queued metadata. Active process-resource leases, remote I/O permits/waiters/capacity registrations/submission reservations, provider request leases, temporary-storage leases and operation-memory lease records keep an exact ticket for their lifetime. Where possible, a waiter's ticket is transferred into the active owner instead of release/reacquire.

This makes the process envelope cover both queued coordination structures and independent live capabilities.

## 10. Provider expiry index is O(live keys) and O(log n)

The pass-47 bounded full scan is replaced by an indexed min-heap with exactly one mutable expiry node per live provider key. Updating a circuit changes that node in place; eviction removes it. Memory remains O(live keys), stale historical entries remain zero, and admission no longer performs an O(keys) expiration scan under the global condition on every request.

## 11. Cross-process pruning is one bounded O(n) pass

Cross-process memory and temporary-storage coordination collect stale keys during one bounded scan and remove them without restarting from index zero after each deletion. The hard process-record ceilings remain in force. This removes the O(n²) liveness-check convoy under `flock` while keeping scratch bounded by the already-hard record ceiling.

## 12. Temporary-storage raw amount APIs are private

Unauthenticated amount-based host-wide temporary-storage reserve/release functions are private implementation primitives and are absent from `__all__` and module public attributes. Production callers use exact `CrossProcessStorageAccount` capabilities. Internal raw hooks remain dynamically resolvable for fault-injection/instrumentation without becoming public authority.

Account open/reserve/release/close also use fork-safe bounded local capability state.

## 13. Resident-credit stage transfer is a real ownership handoff

`OperationMemoryLease.transfer_stage()` now creates a successor capability, atomically changes authoritative owner/capability in the ledger without changing physical bytes, retires the upstream finalizer ticket, and invalidates the upstream handle. Releasing the old owner after handoff is therefore a no-op and cannot invalidate the downstream credit.

## 14. Composite admission owns physical execution capacity + bytes

`CompositeParallelAdmission` now binds an actual project-thread lease to an operation-memory lease. The acquisition order is global: execution capacity first, then resident bytes; failure of byte admission releases the physical execution permit. Partition lookahead reuses its already-owned real executor thread lease rather than claiming a logical slot count independently.

This removes the memory-retained-while-waiting-for-worker inversion in the real executor path.

## 15. 56 pair contracts are implementation-backed

The 8 supported inputs × 7 supported outputs still produce 56 pair guarantees, but the resident-credit, composite-admission and control-budget claims are no longer literal booleans. Enforcing modules register their exact concrete callables in the runtime concurrency-contract registry; coverage validation fails closed if any shared mechanism has no implementation binding and exposes module/qualified-name evidence for every pair.

The mechanism registry also supports runtime observation counters for diagnostic contract probes without making metadata the source of truth.

## 16. Shutdown uses a pre-registered finalizer-domain registry

Finalizer-capable modules register their drain/snapshot/escrow callbacks during normal runtime import. Terminal shutdown iterates that fixed registry and does not dynamically import finalization subsystems while already under memory pressure. Only domains that were actually loaded and capable of creating owners participate.

Shutdown performs repeated domain drains around producer/consumer teardown and uses the epoch activity token as its quiescence barrier.

## 17. Finalizer admission capacity remains fail-closed without false poisoning

Temporary capacity exhaustion rejects new ownership safely and increments admission-rejection diagnostics. Generation exhaustion retires the physical slot permanently rather than wrapping, but does not claim an ownership-loss overflow when no owner was lost. Irreversible publication failures remain terminal.

## Pass 48 regression / adversarial coverage

`tests/memory/test_memory_safety_pass48.py` contains 25 focused tests covering, among other cases:

- single authoritative native active→completion transfer contract;
- inherited-lock replacement for the control budget and storage accounts;
- lock-free escrow observation and recoverable saturation semantics;
- mutable post-commit-safe operation-memory resize records;
- remote `KeyboardInterrupt` batch-tail reclamation;
- scheduler-independent availability emergency debt, including an empty normal queue;
- finalizer epoch changes across equal-cardinality ABA;
- provider fork reset without parent-ticket release;
- indexed one-node-per-key provider expirations;
- private raw temporary-storage amount API;
- exact resident-credit ownership handoff;
- physical-thread-before-bytes composite admission;
- implementation-backed validation of all 56 input/output pairs;
- bounded single-pass cross-process pruning;
- shutdown finalizer-domain epochs and zero-control-plane condition;
- control-plane charging of active owner domains.

## Validation

In the source-only Python tree:

- pass48 focused suite: **25 passed**;
- passes 41–48: **149 passed**;
- passes 17/20/21/30/31/33/39: **148 passed, 1 skipped**;
- passes 15/16/18/27/29/32/34/38/40: **134 passed, 2 skipped**;
- `python -m compileall -q src tests/memory/test_memory_safety_pass48.py`: passed;
- `PYTHONPATH=src python meta/ci/check_primary_cleanup.py`: passed.

The uploaded/source archive does not include a compiled `schema_sanitizer._core_abi3`, so ABI-dependent pytest modules still cannot be collected directly from the source tree.

For native validation, a disposable copy was configured with the environment's CMake 3.31.6 by lowering only that copy's minimum-CMake declaration; the deliverable retains the project's `cmake_minimum_required(VERSION 4.3)`. The modified task-arena translation unit and a translation unit instantiating the modified ordered-executor header compiled successfully under the project's warning-as-error settings. The wider build then continued through unrelated translation units until the validation command's execution limit; no subsequent compile error was observed. The first native validation attempt did catch a malformed forward declaration in the new scope type; that defect was corrected in the deliverable and is now protected by the pass48 source contract test.
