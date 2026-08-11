# Pass 84 — concurrency and memory hardening

Pass 84 continues the exact-owner model introduced in passes 80–83. The focus is no longer aggregate underflow detection: it is keeping retry authority alive across control-plane handoff, finalizer/configuration interleavings, and the C++→Python ABI boundary.

## 1. Retryable external-runtime tombstones

- A finalizer that encounters `config_inflight` publishes its exact claim at target zero and **does not acknowledge its escrow generation**.
- Safe-point execution now detects the actually armed rooted authority (`_escrow_armed`), so the retry signal works with the object the escrow really executes, not just the Python wrapper used by focused tests.
- Manual target-zero release is non-blocking. If configuration is active, the live wrapper transfers its pre-rooted cleanup authority to the escrow and returns; the third-party callback is never waited on by the releasing thread.
- Tombstone draining is bounded and owner-preserving. Failure leaves the exact claim and its retry authority recoverable.

Invariant: a target-zero marker is not cleanup completion. The claim slot/finalizer generation remains authoritative until the underlying envelope reaches zero.

## 2. Exact bounded claim cardinality

- Added one process-wide `BoundedGenerationPool` for external-runtime physical/logical claim slots.
- Exact slot ownership is the coordinator-wide admission authority.
- Claim dictionaries remain routing indexes.
- `_EXTERNAL_RUNTIME_TOTAL_{PHYSICAL,LOGICAL}_CLAIMS` are diagnostic mirrors only and are reconciled for snapshots; stale values cannot block cleanup or create capacity.
- `active_count()` provides O(1) exact cardinality without scanning mappings or allocating temporary containers.
- Coordinator reset retires any exact slots represented by its entries before clearing routing state.

Invariant: cardinality comes from exact bounded slots, never from a separately published aggregate integer.

## 3. Conservative, generation-stable residency probes

- `None` from the identity probe means unavailable/failure, not an authoritative zero.
- A failed probe preserves the previous resident CPU identity and cannot reduce stack debt.
- An explicit `0` remains an authoritative observation and may retract prior identity.
- Stable-probe retries are bounded (`_MAX_EXTERNAL_RUNTIME_STABLE_PROBE_RETRIES`). Continuous configuration churn fails closed instead of spinning indefinitely.
- `config_generation` is bounded and fails closed at exhaustion; it no longer grows as an unbounded Python integer.

Invariant: uncertain observation never manufactures process thread headroom.

## 4. Exact control-plane capabilities

- `ControlPlaneTicket` is now only a caller wrapper.
- The budget roots a separate `_ControlPlaneCapability`, so losing the return value after owner-map commit can collect the wrapper and request retirement on the still-rooted exact capability.
- Owner-map insertion and removal handle “commit then raise” / asynchronous-exception boundaries by inspecting exact membership before deciding rollback or retry.
- Mirror drift in `_reserved` / `_active` is repaired from exact owners and is no longer, by itself, treated as ledger corruption.
- Token recycling remains fixed-capacity and does not use growable append paths.

Invariant: a caller wrapper is never the only object capable of retiring committed control-plane capacity.

## 5. Autonomous deferred memory-close retry

- Once the last exact `OperationMemoryLease` retires, failure of the ledger-level close tail no longer makes that child release replayable.
- The already pre-rooted ledger finalizer authority is armed for an autonomous safe-point retry.
- The retry verifies that no exact child owners/native bytes remain, then retries only cross-process close ownership and advisory cleanup.
- A failed retry remains published; a successful retry clears replay aliases before retiring the escrow generation.

Invariant: child release commits once; journal/cross-process tail failure is a separate retryable ledger obligation.

## 6. FD uncertain-close slot authority

- Preallocated uncertain-close slots are the capacity authority.
- A stale-high `_UNCERTAIN_FD_CLOSE_COUNT` cannot reject an actually free exact slot.
- Slot metadata is prepared before publishing `lease` membership.
- Duplicate/retry paths repair incomplete metadata, reconcile the count from exact slot occupancy and republish terminal-owner observability idempotently.

Invariant: terminal debt exists iff an exact debt slot owns the lease; telemetry cannot create or suppress that ownership.

## 7. Allocation-free post-commit ABI returns

The exact ABI mutators now construct every `tuple`/`PyLong` result **before** mutating native authority:

- operation-memory reservation resize/release;
- external-runtime permit receipt resize;
- FD permit receipt resize;
- FD receipt mark-opened/mark-closed.

If Python object allocation raises `MemoryError`, no native commit has happened. Once the native mutation commits, the function only returns the already-built result object.

Invariant: there is no `native exact commit → fallible Python result construction` split-commit window.

## 8. Historical contract updates

Focused source-contract tests from earlier passes were updated only where a stronger later primitive superseded the original implementation pattern:

- pass50: transfer rollback may use rooted-authority retirement; control-plane token recycling is implemented inside the exact capability release helper;
- pass54: native ordered-executor workers use `ProcessPhysicalThreadPermitLease` RAII rather than manual acquire/release pairs;
- pass55: uncertain-FD terminal publication is factored into an idempotent helper;
- pass69/pass70: transient failed residency probes preserve previous CPU identity; only explicit zero retracts it;
- pass71: external claim cardinality is exact-slot based rather than aggregate-counter based;
- pass78: control-plane mirror divergence is repairable and does not automatically quarantine exact ownership.

## Validation

- `tests/memory/test_memory_safety_pass84.py`: **13 passed**.
- passes 60–84: **298 passed, 1 skipped**.
- passes 50–84 after updating superseded source contracts: **467 passed, 2 skipped**; the remaining 5 tests require a built `schema_sanitizer._core_abi3` and therefore cannot execute in this environment.
- `python -m compileall -q src`: passed.
- C++ syntax/warnings validation passed with `g++ -std=c++20 -Wall -Wextra -Werror -fsyntax-only` for:
  - `cpp/src/api/python_abi3/options/prepare.cc`
  - `cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc`
  - `cpp/src/internal/runtime/operation_task_arena.cc`
- Full CMake configuration is not available in this environment: repository requires CMake >= 4.3; installed CMake is 3.31.6.

## Pass 84 acceptance properties

After any tested interruption/fault boundary:

1. every retained resource has an exact recoverable owner or explicit conservative debt;
1. a derived counter cannot prevent exact cleanup;
1. uncertain runtime observation cannot lower CPU identity;
1. finalizer target-zero publication keeps a retry generation until real cleanup;
1. loss of a caller wrapper cannot orphan committed control-plane capacity;
1. post-commit Python allocation failure is structurally impossible for the exact ABI mutators hardened here;
1. deferred host-level memory close retries without replaying a retired child owner.
