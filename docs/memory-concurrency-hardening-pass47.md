# Memory / Concurrency Hardening — Pass 47

Pass 47 closes second-order failure modes left after pass 46: release commit points that could still allocate, historical scheduler metadata that could grow independently of live owners, lifecycle publication races, and locally bounded subsystems whose control metadata could peak simultaneously.

## Invariants added

### 1. Release commit points are one-way

A release may fail only while all authoritative ownership is still intact. Once physical/logical capacity has been returned, derived scheduler maintenance, diagnostics and notification delivery are no-throw/best-effort operations.

Applied to:

- remote I/O permits and capacity registrations;
- provider-throttle request outcomes;
- process availability publication after resource release.

Remote I/O keeps authoritative operation queues separate from derived weight indexes. A failed derived repair marks scheduling dirty and can be repaired later without resurrecting a released capability or losing a waiter.

### 2. Provider circuit metadata is O(live keys)

The historical circuit-expiration heap was removed. Expired circuits are promoted by scanning the already bounded live endpoint registry. As a result, repeated failures of one endpoint cannot retain one node per historical circuit extension.

Registry diagnostics expose `expiry_entries`, `peak_expiry_entries`, and `stale_expiry_entries`; all remain zero in the heap-free implementation.

### 3. Runtime service publication is transactional

`RuntimeServiceRegistry.reserve()` constructs the control-plane ticket, registry entry and RAII registration before publishing the entry. Constructor failure therefore cannot leave an invisible service reservation.

All lifecycle progress publication is diagnostic-only. `thread.start()` is an irreversible commit point: once the OS thread has started, subsequent progress publication is no-throw and the thread remains represented in the registry until physical exit. Diagnostic `MemoryError` cannot strand `START_AUTHORIZED`, mask a successful start, or break unregister after exit.

### 4. Shutdown uses finalizer quiescence epochs

Runtime shutdown now performs repeated finalizer drains and requires stable snapshots rather than assuming one drain is sufficient. Quiescence barriers are executed after producer closure, after cleanup-producer closure, and again after consumer closure. This handles finalizers created indirectly by the teardown of other services.

The shutdown authoritative snapshot also includes aggregate finalizer-admission capacity and fails closed if its capacity invariant is broken.

### 5. Cross-process temporary-storage ownership is authenticated and bounded

Cross-process storage now has a process-local exact account capability. Host-wide growth/shrink is accepted only from an authenticated account; the process temporary-storage governor composes this with its exact local lease ledger, so byte deltas originate from authoritative local ownership rather than an unauthenticated release path.

Account reserve/release/close are serialized on the account lock, preventing a close-vs-I/O authentication race. The local account registry is hard-bounded and reuses bounded tokens safely because authority is `(token, owner identity, object capability, device)`, preventing ABA on token reuse.

Host-wide process records are also hard-bounded and stale records are pruned in place rather than by copying the entire process map while holding the interprocess lock.

### 6. Availability is level-triggered under publication failure

Process resource governors retain preconstructed availability-delivery objects. A release does not need to allocate a callback object after its commit point.

If publication fails, a dirty availability level is retained. Level-triggered governors schedule a preconstructed autonomous retry, and normal snapshots are also retry safe-points. Thus a single OOM cannot permanently lose the only wakeup that made capacity available.

Notifier instances bind their emergency-thread governor on first worker admission, and each delivery seals its dispatch identity at authoritative registration/first publication. This prevents one notifier's delayed work from being redirected onto another notifier's permit pool or callback identity by later global replacement.

### 7. Global process control-plane budget

A new process-wide `control_plane_budget` bounds the aggregate retained metadata of dynamic runtime control structures. Conservative tickets are currently charged for:

- process-resource waiters;
- remote-I/O waiters;
- provider-throttle endpoint states;
- runtime-service registrations;
- retry-scheduler items;
- cleanup-dispatcher calls;
- temporary-janitor pending artifacts.

The process memory-pressure snapshot separates payload/native reservation bytes from control-plane bytes and exposes their governed sum. Adaptive parallel admission subtracts both classes from process headroom. Control-plane tickets are authenticated by `(token, owner identity, object capability)` and the authoritative byte amount lives only in the private ledger, so mutating a ticket cannot forge a larger release.

When the real ABI3 process-resident ledger is loaded, payload and control-plane admission serialize through one shared governed-memory lock and enforce the combined native envelope atomically. Source-only tooling and Python lifecycle tests that intentionally have no real ABI3 module retain the independent hard control-plane bound rather than gaining an accidental native dependency.

### 8. Transferable resident credits and composite admission

`OperationMemoryLease.transfer_stage()` transfers ownership of already-reserved bytes between pipeline stages without a release/reacquire gap.

`CompositeParallelAdmission` binds a parallel slot count to one resident-byte lease. `acquire_parallel_admission()` derives a slot target from process pressure and commits the corresponding bytes as one operation-owned reservation, reducing the slot count on governed memory rejection rather than leaving partial byte ownership.

Partition lookahead uses this composite admission when an operation memory ledger is active. All 56 declared input/output concurrency pairs advertise resident-credit transfer, composite slot+byte admission, and process control-plane budgeting.

### 9. Remote grant publication is allocation-safe after mutation

Grant batches reserve their storage before any waiter is removed from authoritative queues. Delivery uses prebuilt per-waiter callbacks and a non-recursive pump. Capacity changes advance a lazy scheduling epoch rather than rebuilding all weight buckets eagerly under the main lock.

### 10. Finalizer escrow generations never wrap

Reserved finalizer tickets use a fixed 63-bit ticket envelope. When a slot generation would exceed the representable generation, that physical slot is retired permanently instead of wrapping. This prevents a stale historical ticket from becoming authoritative again and keeps ticket size bounded.

`FinalizerEscrowCapacitySnapshot` exposes capacity, active, available, retired and overflow state. `finalizer_admission_snapshot()` aggregates the process-global teardown domains used by owner admission.

### 11. Cross-process coordination pruning is bounded

Both cross-process memory and temporary-storage coordination enforce hard process-record ceilings and prune dead owners in place. Cleanup no longer constructs an O(processes) replacement dictionary while holding `flock`.

## Fault-injection / regression coverage

`tests/memory/test_memory_safety_pass47.py` covers:

- provider release after derived-index `MemoryError`;
- provider circuit metadata bounded independently of failure count;
- remote permit release after scheduler `MemoryError`;
- capacity-registration prepare failure preserving the exact capability;
- exact/serialized/bounded cross-process storage accounts and safe token reuse;
- process-global control-plane exhaustion, exact release authority, source-only fallback, and duplicate-release diagnostics;
- resident-credit stage transfer without ownership change;
- composite slot/byte admission ownership;
- all 56 input/output concurrency contracts;
- finalizer generation retirement instead of wraparound;
- runtime registry constructor failure before publication;
- irreversible thread-start publication behavior;
- level-triggered availability retry debt;
- shutdown quiescence barriers;
- in-place, hard-bounded cross-process process registries.

## Validation in the source-only environment

- `tests/memory/test_memory_safety_pass47.py`: **19 passed**.
- passes 41/42/43/44/45/46/47: **124 passed** (includes pass47).
- passes 17/20/21/30/31/33/39: **148 passed, 1 skipped**.
- passes 15/16/18/27/29/32/34/38/40: **134 passed, 2 skipped**.
- pass24 has the same source-only limitation as pass46: **11 tests pass and 2 imports fail because the uploaded archive contains no compiled `schema_sanitizer._core_abi3` extension**. The same two pass24 tests fail identically on the pass46 baseline. Native-dependent concurrency/contract suites cannot be collected for the same reason.
- `python -m compileall -q src/schema_sanitizer`: passed.
- `python meta/ci/check_primary_cleanup.py`: passed.

The native C++ task arena retains the pass-46 transaction/rollback guarantees. Pass 47 composes those native retained-byte guarantees with Python fan-out through the new resident-credit/composite-admission layer rather than introducing a second independent native queue-credit mechanism.
