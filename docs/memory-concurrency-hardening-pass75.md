# Pass 75 — Finalizer ownership, rollback and authoritative accounting hardening

Pass 75 continues the concurrency/memory hardening work from pass 74. The
central change is that cleanup authority is now physically rooted independently
from the Python wrapper whose `__del__` arms it. The GC path therefore never
needs to retain the object being collected, take a blocking escrow lock, or
reconstruct primary ownership after a secondary bookkeeping failure.

## 1. Separate pre-rooted finalizer authority

Added `core_impl/rooted_finalizer.py` with `RootedFinalizerAuthority`.

The authority is allocated before ticket ownership is exposed and carries only
the exact cleanup state needed by a normal safe point. It does **not** retain
the wrapper whose destructor must remain reachable.

The lifecycle is now explicitly:

```
INERT / RESERVED
    -> PRIMARY_ARMED
    -> ACK_ONLY       (primary cleanup has committed or was cancelled)
    -> RETIRED
```

`ACK_ONLY` is irreversible and is published before any fallible secondary
retirement operation.

## 2. Prepared finalizer cleanup no longer roots its capsule

`PreparedFinalizerCleanup` now proxies a separate `RootedFinalizerAuthority`.
The prepared escrow roots the authority rather than the capsule itself.

Consequences:

- a capsule abandoned immediately after reservation can still reach `__del__`;
- tuple-returning compatibility helpers cannot strand a RESERVED ticket if a
  later allocation fails;
- slot-lock contention during GC publication retains the exact cleanup owner;
- safe points scan active generations, so armed-but-still-RESERVED owners are
  promoted without requiring the wrapper to remain alive.

`cancel_prepared_finalizer_cleanup()` and
`acknowledge_prepared_finalizer_cleanup()` now transition to ACK-only **before**
calling `release_ticket()`. An injected exception or stale secondary
retirement can therefore never replay primary cleanup.

## 3. Specialized finalizer escrows migrated away from `self`

The same separate-owner model is applied to the specialized resource domains
identified by the pass-74 audit:

- direct cross-process memory leases;
- operation memory leases;
- operation memory ledgers;
- temporary-storage leases;
- operation execution resources;
- operation execution contexts;
- path-claim owners;
- partition lookahead controllers.

The corresponding destructors no longer call blocking `release_ticket()` and
no longer publish `self` into `ReservedFinalizerEscrow`.

Safe-point owners retain only exact cleanup state, for example:

- memory lease: ledger + lease id + owner id + capability;
- memory ledger: native capsule + host-wide cross-process reservation;
- temporary storage: pool + exact lease capability / orphan process and
  control-plane tails;
- path claim: marker/claim path + descriptor owner + admission;
- partition lookahead: future + future context + executor + runtime
  registration.

## 4. Cross-process aggregate rollback hardening

Construction rollback in `_ProcessCrossMemoryCoordinator.acquire()` now marks
the partially-built reservation terminal before it can be finalized.

This prevents an ordinary ceiling rejection from producing a false irreversible
finalizer overflow after all contribution accounting has already been rolled
back.

The finalizer-ticket fallback also commits `_primary_released = True` before
attempting ticket retirement. If the escrow cannot retire synchronously, the
published owner is therefore ACK-only and cannot replay contribution release.

## 5. `BoundedGenerationPool` is transactional under allocation failure

All Python integer arithmetic that can allocate is precomputed before slot/ring
publication:

- next generation;
- token;
- next active count;
- next retired count.

The authoritative tail contains only assignments into pre-existing storage.

Release no longer clamps an active-count underflow. An ACTIVE slot paired with
`active <= 0` marks the namespace corrupted and pins it fail-closed. Snapshot
observability exposes this state via `BoundedGenerationSnapshot.corrupted`.

## 6. Control-plane deferred retirement reconciles its own ledger

`_ProcessControlPlaneBudget.release()` now rebuilds dirty counters from the
owner ledger before deciding that a release cannot commit.

`drain_requested_retirements()` also reconciles dirty counters before scanning
requested retirements. Deferred cleanup therefore no longer depends on an
unrelated diagnostic `snapshot()` call to repair authoritative accounting.

## 7. Temporary-storage exact finalization and byte accounting

`TemporaryStoragePermitPool` gained wrapper-independent exact lookup/release by
`lease_id + owner_id + capability`.

Temporary-storage finalizers can now complete process, local and control-plane
retirement without retaining `TemporaryStorageLease`.

Admission byte counters no longer saturate underflow:

- pending reservation bytes;
- pending resize growth;
- active reservation bytes.

An impossible decrement preserves the conservative charge and increments the
sticky protocol-violation counter instead of manufacturing new capacity.

## 8. Retry-scheduler admission byte counters

The retry scheduler now uses checked byte decrements for:

- `_pending_bytes`;
- `_ready_bytes`;
- `_emergency_bytes`;
- `_successor_bytes`.

Underflow preserves the existing charge and records a protocol violation,
preventing a bookkeeping bug from opening admission capacity prematurely.

## 9. Operation resources, contexts, path claims and lookahead

Operation resource/context escrows are pre-rooted before later construction can
publish primary resources. Partial construction cleanup clears owner fields
only after each cleanup commit; failures remain reachable from the escrow.

A remote coordinator is placed into the pre-rooted resource authority
immediately after construction, before close-race validation, so a failed reject
cleanup cannot become stack-only.

Path claims use a separate authority containing pathname-marker and descriptor
state. The finalizer safe point releases the claim and admission without
retaining `PathClaimOwner` or retiring the same finalizer generation twice.

Partition lookahead mirrors its primary future/context/executor/runtime owners
into a separate authority. A failed `Future` result and a failed context cleanup
retain both owners for another safe point; the primary-exception cleanup gate
remains green.

## Validation

Final validation in the pass-75 build tree:

- `python -m compileall -q src/schema_sanitizer tests/memory/test_memory_safety_pass75.py` — PASS
- `python meta/ci/check_primary_cleanup.py` — PASS
- `pytest -q tests/memory/test_memory_safety_pass{54..75}.py` — **294 passed**
- `pytest -q tests/memory/test_memory_safety_pass75.py` — **11 passed**
- `pytest -q tests/memory/test_temporary_storage_permits.py` — **2 passed, 8 skipped**

A broader historical/pipeline collection still cannot execute in this container
because the native extension `schema_sanitizer._core_abi3` is unavailable. The
failure occurs during test collection/import and is reproducible independently
of pass-75 changes.

The pass-75 tests specifically exercise:

1. abandoned prepared capsule remains collectible while its authority stays
   rooted;
1. CANCEL survives a `release_ticket()` exception without primary replay;
1. rejected aggregate cross-process acquisition does not create false overflow;
1. aggregate finalizer fallback is ACK-only before publication;
1. generation-pool underflow pins the namespace corrupted;
1. control-plane deferred retirement self-reconciles dirty counters;
1. path-claim escrow roots a separate authority, not the claim owner;
1. direct cross-process escrow roots a separate authority, not the lease;
1. specialized destructors contain no blocking escrow retirement or
   `publish_reserved(..., self)` handoff;
1. authoritative admission-byte counters do not use saturating decrements;
1. every specialized migrated domain uses a pre-rooted authority.
