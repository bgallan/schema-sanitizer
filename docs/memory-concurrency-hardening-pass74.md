# Pass 74 — finalizer ownership and quiescence hardening

Pass 74 continues the process-wide memory/concurrency hardening work from pass 73. The central change is that cleanup authority is now preserved independently from opportunistic finalizer publication and from secondary control-plane retirement.

## Implemented changes

### 1. Pre-rooted prepared-finalizer authority

`ReservedFinalizerEscrow` now supports an exact RESERVED owner being rooted before external ownership is exposed. A rooted owner can then be armed without waiting for the slot lock. If publication races with temporary slot-lock contention, the handoff is already durable and a later safe point promotes the armed RESERVED generation to PUBLISHED.

`PreparedFinalizerCleanup` uses this path from reservation time. Safe-point draining scans bounded active generations so an armed-but-not-yet-PUBLISHED owner is discoverable. Cancellation also fails safe: if exact ticket retirement cannot commit, the rooted capsule is converted to ACK-only cleanup and armed rather than being left as an unreachable RESERVED generation.

This removes the former dependency on a second successful `__del__` publication attempt.

### 2. Separate pre-rooted owner for aggregate cross-process memory

The aggregate cross-process memory coordinator now pre-roots a compact `_ProcessCrossMemoryFinalizerOwner`, not `_ProcessCrossMemoryReservation` itself. This distinction is essential: rooting the reservation would prevent its own destructor from ever running.

The separate owner carries the exact generation, owner identity and capability needed for safe-point authentication. The reservation remains collectible and its non-blocking destructor only arms the already-rooted owner. Explicit primary release marks that owner ACK-only before secondary finalizer-ticket retirement. Stale capability fault injection continues to fail closed and leaves the owner published for retry.

### 3. Control-plane tickets fail closed and are ledger-rooted

The process control-plane budget now stores the exact `ControlPlaneTicket` object in its authoritative owner ledger. A live ticket with a stale capability no longer returns success: it returns `False` and leaves the reservation intact.

Failed exact retirement can set `retire_requested` on the ledger-rooted ticket. Runtime safe points drain these deferred retirements, so callers that legitimately lose their local secondary reference after `release_control_plane()` fails do not destroy the last physical owner. A genuinely stale/non-authoritative ticket cannot request retirement of somebody else's generation.

### 4. ACK-after-primary-release across payload/HTTP/output wrappers

The remaining payload ownership wrappers now distinguish the two lifecycle operations:

- ownership transfer / unused finalizer -> CANCEL;
- primary release already committed -> ACK-only retirement.

The change covers remote byte/text payloads, synchronous HTTP payloads, upload buffers and result/sink-result ownership. Their destructors only clear local finalizer references after `defer_prepared_finalizer_cleanup()` has accepted a durable handoff.

Remote-I/O permits and ProviderThrottle destructors were hardened in the same way.

### 5. Authoritative quiescence counters no longer saturate underflow

Lifecycle counters whose zero value participates in close/shutdown decisions now use checked decrements and sticky protocol violations rather than `max(0, counter - n)` masking. This includes:

- temporary-storage pending/active leases;
- partition-lookahead inflight submissions;
- async scheduler task slots, active operations and terminal debt;
- temporary-storage governor device users;
- Remote-I/O pending submissions, waiting/synchronous waiters and in-use permits.

A protocol violation can therefore drain conservatively but cannot be reported as a clean quiescent state.

### 6. Remote-I/O shutdown admission freeze and observability

Remote-I/O now exposes an admission-closed latch and rejects new submissions, capacity registrations and waiter/permit admission once shutdown freezes the subsystem. Runtime shutdown includes Remote-I/O waiting/synchronous-waiter counts and protocol violations in its drain decision and observability failures.

### 7. Secondary-tail ownership retained in additional control paths

Control-ticket retirement in runtime registry, temporary storage and memory-budget paths was tightened so authoritative state is not discarded before secondary retirement commits. The process-wide deferred control-plane mechanism provides a systemic fallback for remaining direct release call sites.

## Pass 74 regression coverage

`tests/memory/test_memory_safety_pass74.py` adds 10 focused tests covering:

- prepared-finalizer publication while its exact slot lock is contended;
- cancellation fallback to rooted ACK-only cleanup;
- stale live control-plane capability fail-closed behavior;
- ledger-rooted deferred control retirement;
- temporary-storage governor identity pinning on user-counter underflow;
- Remote-I/O admission freeze;
- separate pre-rooted aggregate cross-process finalizer ownership and real GC handoff;
- removal of saturating decrements from authoritative quiescence counters;
- shutdown accounting of Remote-I/O waiters/protocol failures;
- ACK-after-primary-release and conditional finalizer handoff in I/O wrappers.

Final validation in this environment:

- `PYTHONPATH=src pytest -q tests/memory/test_memory_safety_pass54.py ... test_memory_safety_pass74.py`: **283 passed**, 2 fork deprecation warnings.
- `PYTHONPATH=src python -m compileall -q src tests`: passed.
- `PYTHONPATH=src python meta/ci/check_primary_cleanup.py`: passed.

The older broad concurrency suites that import the compiled native runtime cannot be collected in this container because `schema_sanitizer._core_abi3` is not installed. This is an environment limitation also present on the pass-73 baseline, not a pass-74 regression. Several much older memory tests likewise contain already-obsolete source-contract expectations; recent pass54-pass74 regression coverage is green.
