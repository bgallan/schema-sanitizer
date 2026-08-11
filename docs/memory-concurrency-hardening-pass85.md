# Pass 85 — Owner-first generations and interruption-safe finalizer state

Pass 85 hardens the two Python primitives that remained underneath most exact
resource owners: `BoundedGenerationPool` and `ReservedFinalizerEscrow`.

## 1. Owner-first bounded generations

Production code no longer depends on `acquire() -> int` as the only cleanup
authority. `BoundedGenerationPool` now provides:

- `acquire_for(owner)` — roots an already-created exact owner before returning
  its ABA-resistant integer token.
- `release_for(owner)` — retires by object identity and is retry-idempotent.
- `token_for(owner)`, `owner_for(token)`, and `owns_owner(...)` for authenticated
  routing/diagnostics.
- `_owners` is the authoritative fixed-capacity slot table. Ring/state/count
  fields are reconstructible mirrors.

The legacy integer-only API is retained only for compatibility tests and legacy
injection paths.

Production migrations include:

- external-runtime physical/logical claim slots;
- runtime service registry tokens;
- cross-process memory contribution generations;
- retry scheduler generations.

This removes the `CALL acquire() -> return int -> caller STORE_*` handoff gap:
cleanup can always identify a generation by the pre-existing owner even if the
returned integer never reaches caller state.

## 2. Owner-first rooted finalizer reservations

`ReservedFinalizerEscrow.reserve_rooted(owner)` replaces the production pattern
`reserve_ticket() -> root_reserved(ticket, owner)`.

The slot owns the finalizer authority before ticket handoff. If construction is
interrupted, rollback can retire by exact owner identity instead of depending on
the returned integer.

Production finalizer domains migrated in this pass include operation memory,
cross-process memory, temporary storage, path claims, operation contexts,
partition lookahead and prepared finalizer cleanup.

## 3. Recoverable finalizer processing state machine

Both the reserved escrow and the legacy compatibility escrow now include a
`PROCESSED` state.

The important transitions are:

```
PUBLISHED -> CLAIMED -> PROCESSED -> RECYCLE_PENDING/FREE
```

- an exception while `CLAIMED` restores the exact owner to a processable state;
- once `PROCESSED` is visible, later safe points perform bookkeeping only and do
  not invoke the processor again;
- owner-free recycle bookkeeping is separated from the external cleanup effect.

Python cannot prove exactly-once for an arbitrary external side effect across
the single bytecode boundary between callback return and publishing
`PROCESSED`. Production cleanup callbacks therefore remain target-based and
idempotent. Pass 85 guarantees that a `CLAIMED` owner is not made permanently
unreachable and that a visible `PROCESSED` owner is not replayed.

## 4. Exact activity at safe points

Finalizer `active_count()` / `published_count()` safe-point observations are now
computed from fixed slot authority instead of trusting activity mirrors that can
become stale after an asynchronous interruption. Atomic counters remain useful
for telemetry/progress epochs, but they no longer decide whether a live owner
must be drained.

## 5. External-runtime claim rollback fixes

Owner-first claim construction exposed and fixed a reentrancy bug during the
migration: rollback of an unpublished claim used to call `claim.release()` while
already holding the non-reentrant coordinator lock.

Pass 85 adds construction-only `_abort_unpublished()` retirement. No coordinator
reentry occurs before the claim has been published.

Target-zero cleanup also retires the owner-aware generation slot even if an
interrupted prior cleanup has already removed the claim dictionary mirror. A
lost/stale dict entry can therefore no longer leave exact claim-slot capacity
permanently occupied.

## 6. Retry/cross-process/registry retirement

Consumers now retire generation slots by exact owner identity. If generation
retirement committed but a subsequent mapping update was interrupted, a retry
can finish bookkeeping without replaying the resource retirement.

The retry scheduler uses generation `0` as a non-owning cancellation marker
rather than allocating a throwaway bounded generation solely to invalidate a
claimed retry.

## 7. Fault-injection coverage

`tests/memory/test_memory_safety_pass85.py` covers:

- interruption after owner publication inside bounded-generation acquire;
- lost integer handoff recovered by owner identity;
- post-commit generation release retry;
- interrupted owner-first finalizer reservation;
- `CLAIMED` rollback to a processable owner;
- `PROCESSED` bookkeeping failure without processor replay;
- legacy escrow claim recovery;
- owner-first migration of all production generation consumers;
- owner-first rooted-finalizer source contracts;
- physical claim-slot retirement after claim-dict mirror loss;
- logical claim-slot retirement after claim-dict mirror loss.

Historical source-contract tests that explicitly required the superseded naked
integer/ticket APIs were updated to assert the new owner-first invariants.

## Validation

Executed in this environment:

- pass50–pass59: `169 passed, 1 skipped, 5 deselected`;
- the five deselected tests require the unavailable `_core_abi3` extension;
- pass60–pass85: `309 passed, 1 skipped`;
- aggregate runnable hardening validation: **478 passed, 2 skipped**;
- pass85-specific tests: **11/11 passed**;
- `python -m compileall -q src` passed.

No C++ production source was modified in pass 85, so the C++ ABI syntax surface
is unchanged from pass 84.

The same environment limitation remains for native integration tests: the local
checkout does not contain a built `schema_sanitizer._core_abi3` module. Full
native/CMake validation therefore still needs a build-capable environment.
