# Memory and concurrency hardening — pass 49

Pass 49 closes the remaining gaps found after pass 48, with emphasis on atomic
ownership transitions, fork-safe finalization, shutdown quiescence, compositional
memory accounting, and proof that the concurrency contracts are exercised by the
real enforcement primitives.

## Authoritative memory accounting

- Added the ABI3 operation `operation_memory_ledger_reserve_snapshot`.
  Reservation and the post-reservation accounting result are now one native
  transaction. If construction of the Python result fails, the native reservation
  is rolled back before the exception escapes.
- `OperationMemoryLedger.close()` is rollback-safe: failure while preparing the
  closing snapshot cannot leave the ledger permanently stuck in `_closing`.
- `OperationMemoryLease.resize()` uses mutable pre-established ledger entries so
  a Python allocation cannot split the physical resize from the logical lease
  size.
- Public resident-memory headroom is now the amount genuinely admissible to a
  Python payload owner, while an internal raw snapshot preserves the exact
  physical process-pool counters.
- The process control-plane budget is shadow-charged into the same native resident
  pool when ABI3 is available. Native-only conversions can therefore no longer
  consume memory logically reserved for Python control structures.
- Source-only builds retain a bounded Python fallback and do not require ABI3.

## Control-plane budget

- Lifetime token allocation reuses released slots with a fresh capability, so
  token growth is bounded by simultaneous/high-water ownership rather than total
  process lifetime.
- Release is capability-authenticated and serialized with governed payload
  admission.
- Static control memory is registered by stable component identity rather than by
  a fixed global heuristic constant.
- Global finalizer escrows register conservative footprints including slot state,
  locks, free rings, generations, and preallocated child banks. Ephemeral/local
  escrows do not leak into the static baseline.
- Shutdown freezes the static registry before teardown.

## Finalizer escrows

- Finalizer activity and publication/progress epochs use fixed-width preallocated
  counters. State transitions never silently lose an ABA epoch because Python
  bigint allocation failed.
- Reservation uses a preallocated free ring; normal reserve/release is O(1)
  rather than scanning all slots under a global reserve lock.
- Activity snapshots use maintained counters instead of O(capacity) slot scans.
- Recoverable admission saturation is distinct from irreversible publication or
  ownership loss. A full escrow does not poison terminal shutdown merely because
  new work was rejected.
- Generation exhaustion retires the affected slot instead of wrapping and
  reintroducing ABA.
- Snapshot/read paths do not take the per-slot locks used by non-blocking
  finalizer publication.

## Fork safety and inherited-owner quarantine

- Finalizer escrows use preallocated child banks so ordinary `fork()` preparation
  does not create tens of thousands of fresh lock objects.
- Inherited owner graphs are retained in a bounded, generational post-fork
  quarantine. They are dropped only at a normal safe point, outside at-fork
  callbacks, and replacement child banks are regenerated there.
- Multiple forks before a safe point cannot overwrite earlier quarantine roots.
- CPython does not abort `fork()` when a `register_at_fork(before=...)` callback
  raises. Pass 49 therefore never relies on such an exception: preparation-bank
  exhaustion records an authoritative sentinel; the child becomes inert/fail-
  closed without acquiring inherited locks or running inherited destructors.
- Child reset is idempotent per process so module-level and object-level at-fork
  hooks cannot consume two prepared banks for the same fork.
- Finalizer, control-plane, shutdown-observer and concurrency-contract registry
  locks are replaced/reset safely after fork.

## Shutdown quiescence

- Quiescence requires both stable fixed-width activity epochs and zero remaining
  publicable/reserved owners. Two numerically identical snapshots with live owners
  can no longer be mistaken for a drained runtime.
- The finalizer registry is frozen before teardown. New finalization domains
  cannot appear between the quiescence barrier and consumer shutdown.
- Duplicate domain names fail closed unless they are a verified semantic module
  reload of the same contract, in which case the stale callbacks are replaced.
- Finalizer activity buffers are allocated before terminal teardown and reused
  during the quiescence barrier.
- Mandatory shutdown modules are preloaded during normal runtime operation.
  Terminal phases resolve attributes from those module objects rather than doing
  new imports under memory pressure.
- The native task-arena reaper is only touched at terminal shutdown if the ABI3
  module was already loaded.
- A general shutdown-observer registry provides the same preload/freeze model to
  non-finalizer runtime subsystems.

## Native task arena

- Active retained bytes -> completion retained bytes is a single authoritative
  transfer. The worker scope records that its active charge was consumed so it
  cannot subtract the same credit twice.
- Legacy public helpers that separately checked and retained completion bytes are
  removed from the public arena API, preventing future callers from recreating
  the original TOCTOU.
- Queue item count and queue byte capacity are independent. A small worker count
  no longer collapses the byte ceiling to an unrealistically tiny value.
- Task-arena cleanup reaper reservations now track exact bytes actually queued.
  Empty arenas no longer reserve their full hypothetical byte capacity while
  every truly queued byte still has teardown capacity reserved.

## Composite execution admission

- Composite admission owns actual helper-thread permits and resident-memory
  credits.
- When adaptive memory admission halves the candidate slot count, surplus physical
  thread permits are immediately returned rather than being retained until close.
- `close()` attempts both memory and execution cleanup even when the first release
  fails, preserving the primary error and bounded secondary diagnostics.
- Memory-bearing call sites can require the memory half of the composite permit;
  they fail closed instead of silently degrading to execution-only admission.
- `OperationMemoryLease.transfer_stage()` performs a real generational ownership
  handoff: the successor receives a fresh capability and the upstream handle is
  invalidated without releasing/reacquiring the physical bytes.

## Concurrency contract coverage

- The active input/output pair is tracked by a PID-sealed context.
- Each real enforcing primitive records evidence against that concrete pair:
  control-plane reservation, transferable resident-memory credit, and composite
  slot+byte admission.
- Validation can now reject any pair lacking observed evidence rather than merely
  checking that generic helper functions exist.
- The pass-49 contract suite exercises all 8 input types × 7 output types = 56
  pair identities through the real enforcement primitives and verifies that all
  three guarantees were observed for every pair.

## Cross-process storage and memory

- Stale-owner pruning uses bounded preallocated scratch storage and remains O(n)
  under the interprocess file lock; it does not allocate a dynamic list of all
  stale keys during housekeeping.
- Temporary-storage coordination remains capability-authenticated. Raw
  quantity-based helpers are private seams only, retained where necessary for
  disabled-coordination/fault-injection behavior.
- Per-filesystem waits remain isolated: global account authentication does not
  hold a global lock while waiting on one filesystem/device.

## Additional lifecycle fixes found during implementation

Native validation exposed several correctness issues that source-only tests could
not see; these were fixed as part of pass 49:

- remote/process memory diagnostics retain their stable public `limit_name` while
  the combined governed envelope remains enforced internally;
- abandoned operation-memory owners are drained at a normal safe point rather
  than doing unsafe work from `__del__`;
- async-bridge startup failure and synchronous completion release exactly one
  physical thread lease and registry/finalizer owner;
- terminal `RegistryStream` state keeps a stable empty close-item collection
  rather than changing shape to `None`;
- normal execution-policy derivation additionally clamps worker count using the
  composed payload headroom and reports `memory_limited`, while explicit
  deterministic test overrides retain their defined semantics.

## Validation

The pass-49 working tree was validated in both source-only and ABI3 modes.

- The native ABI3 extension was built to completion and linked successfully.
  The validation copy used the environment's CMake 3.31.6 by lowering only the
  validation copy's `cmake_minimum_required`; the deliverable retains the
  project's `cmake_minimum_required(VERSION 4.3)`.
- The critical modified translation units compiled successfully, including the
  task arena, ABI method table, module table, and transactional reservation
  implementation.
- `test_memory_safety_pass49.py`: 35/35 with ABI3; source-only mode skips only
  the tests that explicitly require a real native module.
- Passes 47-49 plus operation-memory-ledger and memory-limit enforcement:
  91/91 with ABI3 in the final validation round.
- Historical source-only regression blocks were also exercised after the new
  control-plane shadow implementation.
- Some historical Remote I/O tests assert nominal capacities exactly; when the
  host pressure controller is actively scaling capacity they can fail by design
  (for example nominal 8 -> effective 6). The adaptive pressure protection is
  intentionally not disabled to make such environment-sensitive assertions pass.
- Python bytecode compilation and the primary-cleanup CI checker are part of the
  final packaging validation.
