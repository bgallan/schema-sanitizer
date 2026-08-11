# Memory and concurrency hardening — pass 51

Pass 51 closes the next set of correctness, liveness, fork, and strict-boundedness
gaps found after pass 50. The central theme is that moving an owner between
representations must not depend on a growable Python container after the source
representation has been retired.

## Transactional fork generations

- The central fork manager captures one exact, bounded handler generation during
  `before` and the child executes only that captured generation.
- A handler registered by another `before` callback belongs to the next fork and
  can no longer receive `after_in_child` without preparation.
- Preparation success is tracked independently per captured handler. A handler
  whose `before` failed is inert in the child instead of running a reset against
  missing prepared state.
- Handlers carry an explicit mode (`prepared_swap`, `child_safe`, or
  `quarantine_only`) so an unclassified allocating child reset cannot silently
  execute.
- Runtime components hardened in this pass use spare child banks prepared during
  normal execution; fork callbacks select/swap state rather than materializing
  large locks, dictionaries, deques, or token rings in the child.

## Allocation-free terminal epoch reads

- Native `AtomicEpoch` now exposes direct little-endian writes into an existing
  writable byte buffer.
- The finalizer activity ABI writes the complete activity tuple into a
  preallocated Python buffer without creating `PyLong` objects for the
  correctness path.
- Shutdown quiescence therefore does not depend on allocating Python integers to
  observe the native atomic counters.
- The source-only fallback remains fixed-width and lock-backed.

## Finalizer/free-ring commit hardening

- Reserved finalizer free-ring operations use prepare/commit helpers: ring head,
  tail/count, generation, and ticket encoding are prepared before owner
  visibility changes.
- Reservation increments conservative activity before making the owner visible;
  release/process prepare slot recycling before retiring the owner.
- Legacy finalizer publication prepares cursor movement before publishing the
  owner, removing the remaining post-publication integer-allocation window.
- Finalizer static footprint is derived from the actual Python object layout and
  preallocated child banks, with a conservative allocator margin rather than a
  single opaque multiplier.

## Control-plane token transactions

- Reusable free tokens are now peeked, not consumed, until native shadow and
  Python owner publication both succeed.
- Release prepares ring reinsertion before authority is retired; a failed
  bookkeeping operation cannot lose reusable namespace capacity after commit.
- Native-shadow initialization is prewarmed outside the global governed-memory
  admission lock.
- The default bounded control-plane envelope is 256 MiB. This accommodates the
  now-derived worst-case static footprint of all global escrows while remaining
  subordinate to the shared resident-memory envelope.

## Physically bounded retry scheduler

- Retry deadlines use a preallocated indexed min-heap with exactly one physical
  node per logical retry; stale historical heap nodes and OOM-dependent heap
  compaction are gone.
- Retry generations use a bounded slot+generation pool.
- Replacement is destination-first: prepare/publish the successor before
  retiring the previous owner.
- Deadline-to-ready transitions retain one authoritative owner while changing
  state/index. A failed ready publication cannot destroy the retry.
- Ready scheduling is driven by authoritative per-key/subsystem state instead of
  `popleft -> append` queue rotation that could lose the runnable index.

## Shared owner-state discipline

The same destination-first model was applied to other terminal/liveness
subsystems:

- `ReleaseGuardian` keeps the owner authoritative across ready, active, delayed,
  parked, and dead-letter states. Dead-letter owners remain identity-indexed so
  they cannot be adopted twice with a different cleanup method.
- `CleanupDispatcher` keeps an authoritative owner map across active, delayed,
  parked, and dead-letter transitions. A failed destination index update cannot
  make the cleanup object unreachable.
- `AvailabilityNotifier` keeps delivery ownership in its authoritative map while
  retrying/rearming. Queue rotation is no longer an ownership transition.

## Pure operation-memory snapshots

- `OperationMemoryLedger.snapshot()` is now observational: it no longer drains
  abandoned finalizers as a side effect.
- `OperationMemoryLedger.safe_point()` is the explicit operation that drains
  abandoned memory-finalizer work.
- The historical concurrency test that relied on snapshot side effects now calls
  the explicit safe point, preserving the stronger diagnostic contract.

## Bounded identity namespaces

- Added reusable fixed-width helpers for capability-authenticated lease IDs.
- Process-resource, operation-memory, temporary-storage, provider-throttle, and
  selected terminal/runtime identities no longer rely on lifetime-growing Python
  integers.
- Small fixed-capacity registries use preallocated slot+generation pools.
- Native `OperationTaskArena` generation exhaustion is fail-closed rather than a
  silent `uint64_t` wrap that could reintroduce ABA.
- `TerminalOwnershipRegistry` latches generation exhaustion and refuses further
  generation-dependent mutation instead of continuing with a frozen generation.

## Cross-process transactions and bounded housekeeping

- Cross-process memory/storage journals distinguish owner commit from stale
  housekeeping. An exception in the body cannot persist a partial owner delta
  merely because context-manager cleanup executes.
- Per-transaction process-liveness work is bounded; excess stale records are
  conservatively retained for a future transaction rather than extending a
  host-wide `flock` indefinitely.
- Cross-process coordinator contribution IDs use bounded generations.

## Native executor lock hold reduction

- Low-core ordered-executor retained-byte estimation is computed before entering
  the executor mutex, matching the high-core arena discipline.
- Submit/consume/cancel paths no longer hold the central executor mutex while
  recursively estimating a potentially large result graph.

## Bounded shutdown diagnostics

- Terminal failure recording uses a preallocated bounded failure-code store on
  the correctness path instead of dynamically growing lists/f-strings while the
  process may already be under memory pressure.
- Fork state for shutdown conditions and related runtime registries is prepared
  in normal execution and swapped in the child.

## Production pair evidence

- Public file-to-file and file-to-analytical entry points continue to use the
  shared `RuntimeConcurrencyPairAdmission` and keep the concrete source/sink
  identity active through their writer/materialization path.
- The one-byte structural handoff sentinel is now explicitly tagged as bootstrap
  evidence and is retired immediately after the generational input-to-output
  handoff. It no longer contaminates `current_charged_memory_bytes` or
  `peak_charged_memory_bytes` diagnostics.
- A second payload-observation matrix excludes the structural bootstrap. The
  existing pass-50 validator remains for compatibility; the pass-51 validator
  can only be satisfied by admissions/credits observed while real downstream
  work is active.
- This keeps coverage claims honest on installations that do not have every
  optional analytical adapter: structural 56-pair coverage and actual payload
  execution evidence are distinct signals rather than one synthetic counter.

## Validation

Validation was performed against the final pass-51 source tree.

- Native ABI3 build in the isolated validation copy: **completed and linked to
  100%**.
- Full `tests/memory` with the compiled pass-51 ABI3 module: **859 passed,
  21 skipped, 0 failed**.
- `test_memory_safety_pass51.py`: **29 passed**.
- Memory/concurrency governor + hardening pass1-pass5 + threading policy/output/
  materialization/native executor with ABI3: **96 passed, 3 skipped**.
- Source-only memory passes 34-51: **330 passed, 3 skipped**.
- Python `compileall`: passed.
- `meta/ci/check_primary_cleanup.py`: passed.

The validation build lowers the CMake requirement only in its isolated copy to
match the available builder. The deliverable retains
`cmake_minimum_required(VERSION 4.3)` and contains no compiled `_core_abi3` or
other validation build artifact.
