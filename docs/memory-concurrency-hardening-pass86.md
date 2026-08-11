# Pass 86 — Replay-safe rooted finalizers

Pass 86 hardens the finalizer protocol introduced in pass 85. The owner-first
handoff remains, but finalizer generations and exact resource capabilities now
survive interruption after an irreversible cleanup effect without replaying that
effect or poisoning teardown capacity.

## Invariants added

### One rooted owner, one escrow generation

`ReservedFinalizerEscrow.reserve_rooted(owner)` now rejects a second active
generation for the same owner. A retry of the *same unarmed RESERVED* owner is
idempotent and returns its existing ticket, closing a lost-return handoff without
creating duplicate cleanup authority.

The durable arm is generation-specific. `RootedFinalizerAuthority` tracks
`_escrow_armed_ticket`; the historical `_escrow_armed` boolean is only a
compatibility mirror. A stale arm for an old ticket cannot make a newer
generation processable.

Rollback of a partially committed rooted reservation clears the matching owner
ticket and arm state. A stale wrapper that later tries to publish a generation
already retired is treated as an acknowledgement only when the owner is no
longer rooted anywhere in the escrow.

### Replay-safe exact cleanup

`FinalizerReplayCapability` records the postcondition of an exact release. It
is now used by:

- Python operation-memory leases;
- temporary-storage leases;
- direct cross-process memory leases.

For these domains, a finalizer interrupted after the exact owner has been
retired but before `authority.arg*` cleanup can call the release API again with
the same capability. The retry is an acknowledgement, not an unknown release
or a second physical/logical release. A different capability remains rejected.

Direct cross-process memory also rebuilds its free-slot mirror from the exact
ledger after retirement, so an interruption after the release commit cannot
lose reusable generation capacity.

### Path-claim admission is exact-owner based

The scalar `_PATH_CLAIM_ADMISSIONS` is no longer release authority. Live
admission ownership is held by a bounded owner-first generation pool. The
scalar is reconciled from exact owners and the path-finalizer pulse consults the
exact owner pool.

This makes `_run_path_claim_admission_finalizer()` replay-idempotent: an
interruption after exact owner retirement cannot decrement admission twice.
A pristine generation pool is preallocated for the forked-child reset path so
post-fork callbacks do not allocate the full slot namespace.

### `RECYCLE_PENDING` is repair, not corruption

An owner-free slot in `RECYCLE_PENDING` has completed resource cleanup but has
not yet returned to the admission ring. Pass 86 exposes this state explicitly
in `FinalizerEscrowCapacitySnapshot` and `FinalizerAdmissionDomain`.

The capacity invariant is now:

```
active + available + recycle_pending + retired == capacity
```

Failure/interruption while recycling an already-retired owner no longer sets
`overflowed=True`. A later admission/safe point may scavenge the pending slot.
Irreversible publication/authentication failures still poison the escrow.

## Fault-injection coverage

`tests/memory/test_memory_safety_pass86.py` covers:

1. duplicate rooted-owner reservation and lost-return retry;
1. exact generation arm and duplicate rejection after publication;
1. rooted-reservation rollback clearing stale ticket/arm state;
1. stale publication after rollback not causing false terminal overflow;
1. `RECYCLE_PENDING` preserving capacity invariants without poisoning;
1. replay-idempotent path-claim admission cleanup;
1. replay-idempotent operation-memory exact release;
1. replay-idempotent temporary-storage exact release;
1. replay-idempotent direct cross-process exact release and slot recovery;
1. finalizer-admission propagation of `recycle_pending`;
1. source contracts for exact arm/replay capability/fork-safe owner slots.

(The runtime file contains nine pytest functions; several functions validate
multiple fault boundaries/invariants.)

## Validation in this environment

- pass 60–86: `318 passed, 1 skipped`;
- pass 50–86 with the five known ABI-dependent tests deselected:
  `487 passed, 2 skipped, 5 deselected`;
- the same five tests fail if selected because `schema_sanitizer._core_abi3` is
  not built in this environment; no additional executable failures were found;
- `python -m compileall` succeeds;
- no C++ source is modified by pass 86.

The full native/CMake validation remains subject to the existing environment
limitations documented by prior passes (`_core_abi3` absent; project CMake
version newer than the installed runner).
