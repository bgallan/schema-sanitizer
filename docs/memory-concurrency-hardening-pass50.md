# Memory and concurrency hardening — pass 50

Pass 50 closes the next set of correctness and boundedness gaps found after pass
49\. The focus is on making bookkeeping itself transactional, making finalizer
activity genuinely atomic, centralizing fork handling, removing lifetime growth
from remaining registries/tokens, and making the 56 input/output concurrency
contracts flow through production entry points rather than test-only evidence.

## Atomic finalizer activity and bounded escrows

- Added a fixed-width `AtomicEpoch` primitive. With ABI3 loaded it is backed by
  `std::atomic<uint64_t>`; source-only builds use a sealed lock-backed fallback.
  Publication/progress/activity counters no longer use multibyte Python buffers
  whose intermediate carry state could be observed concurrently.
- Finalizer reservation increments the conservative active count before making an
  owner visible. If publication/reservation cannot complete, the pre-publication
  activity charge is rolled back.
- The legacy finalizer cleanup domain follows the same ordering and uses prepared
  child state as well; the shutdown barrier therefore does not depend on a weaker
  legacy counter.
- Reserved finalizer escrows retain the O(1) preallocated free-ring model and
  fixed-width generations introduced by earlier passes.
- `OperationMemoryLease.transfer_stage()` now puts every operation after finalizer
  ticket reservation inside a rollback-protected construction region. Failure to
  allocate the successor handle, lock, capability or metadata cannot orphan a
  reserved finalizer slot.

## Central fork manager

- Per-instance `os.register_at_fork` registrations were removed from finalizer
  escrows. Ephemeral escrows can now be garbage collected instead of being kept
  alive forever by bound-method callbacks registered globally with CPython.
- Added a bounded central fork manager. Runtime components register descriptors
  with this single dispatcher rather than installing an unbounded number of
  process-global callbacks.
- Child callbacks are fail-closed by default. A callback that has no prepared
  child state runs only when explicitly registered as
  `child_safe_without_prepare=True`.
- Fork callback contracts carry an explicit generation and a conservative
  callable fingerprint. Defaults, keyword defaults, closures and opaque captured
  owners are no longer treated as equivalent merely because their bytecode is the
  same.
- Reloads of known runtime singletons remain supported through explicit semantic
  owner roles/generations instead of globally weakening callback identity.
- In a poisoned post-fork child, inherited owner roots remain quarantined rather
  than being generically decref'd at a safe point; arbitrary inherited `__del__`
  code therefore cannot run against potentially inconsistent locks/FD/thread
  state before `exec()`/exit.
- Runtime subsystems that can reset safely use prepared/minimal child state;
  allocating reconstruction in `after_in_child` is avoided for hardened paths.

## Transactional process-resource accounting

- Generic process-resource release now prepares every next counter/value before
  removing the lease capability. The authoritative commit consists only of the
  prepared assignments and ownership removal.
- Lease shrink uses the same prepare-then-commit discipline. A Python integer
  allocation failure cannot leave the lease amount and aggregate counters at
  different generations.
- Progress/diagnostic transitions after authoritative mutations are no-throw and
  cannot make callers retry an already-committed resource transition.

## Transactional retry scheduler

- New/replacement retries are prepared before becoming authoritative.
- Heap insertion, subsystem charge and control-plane tickets are installed with
  exact rollback. A `heapq.heappush()` failure cannot leave an item in `_current`
  with no executable deadline.
- Replacement is now `prepare new -> publish new -> retire old`; failure to
  prepare the replacement preserves the previous retry.
- Hostile/fault-injected heap insertion that mutates and then raises is repaired
  by identity before authority is returned to the caller.
- Emergency/successor promotion precomputes counters and uses the same rollback
  rules, preventing duplicate representation or a retry that exists only in
  bookkeeping.
- Post-commit retirement of obsolete retry resources is conservative/no-throw.

## Composite thread + memory admission

- Failure to acquire helper threads no longer returns early with an execution-only
  one-slot permit when `require_memory=True`. The code falls back to one serial
  slot and still acquires the required resident-memory credit.
- Adaptive reduction returns surplus physical thread permits immediately.
- Composite cleanup attempts both resident-memory and execution-resource release
  even when one cleanup fails.
- A caller can inject the exact operation-memory ledger that must back the
  admission, avoiding accidental fallback to an unrelated/no ledger.

## Cross-process memory ownership

- Direct cross-process leases use a bounded slot+generation namespace instead of
  lifetime-growing `itertools.count()` identities.
- Constructor state is terminal-safe before a finalizer ticket is reserved. If
  direct-lease registration fails, the ticket is explicitly rolled back and a
  partially initialized object cannot enter a publish/drain loop.
- Direct-lease ledger entries are mutable preallocated records. After the
  host-wide journal commit, updating the local reserved byte count does not
  require allocating a replacement tuple; resize therefore cannot split host and
  local generations on `MemoryError`.
- Additional lifetime sequences used by hardened infrastructure are capped or
  migrated to bounded generation/token schemes before they can grow Python
  integers without limit.

## Control-plane and static-memory accounting

- Control-plane free token reuse uses a fixed ring. Release cannot silently lose
  reusable token identities because a Python list append failed under memory
  pressure.
- Static control-plane entries and totals are prepared transactionally. The total
  is computed before publishing a new entry.
- Static runtime footprint is admitted into the governed/native shadow before the
  corresponding component materializes its large preallocated structures. If the
  shadow cannot accommodate it, registration is rolled back before physical
  allocation.
- The native resident shadow is synchronized on authoritative control/static
  transitions rather than as a side effect of diagnostic snapshots.
- Resident/control snapshots are taken under the composed admission lock and are
  observational. Snapshot calls do not create ledgers, reserve memory or mutate
  the governed envelope.
- Governed headroom uses one consistent definition and no longer subtracts the
  control-plane shadow twice.

## Registries and shutdown publication

- Finalizer domains, shutdown observers, concurrency contracts, observed pair
  evidence and static-control kinds have explicit hard caps.
- Finalizer registry insertion rolls back if its secondary index cannot be
  updated.
- Freeze constructs the complete immutable domain/escrow view first and publishes
  it only after all allocations succeed. Shutdown can no longer observe a
  half-frozen registry while new domains remain admissible.
- Callback/domain equivalence is conservative and accounts for defaults, closure
  cells and captured owner identity. Reload acceptance is explicit rather than
  inferred from bytecode alone.
- Diagnostic epochs/transitions are bounded and no-throw on authoritative paths,
  so observability cannot turn a successful ownership commit into an apparent
  failure.

## Production 56-pair concurrency admission

- Added a common `RuntimeConcurrencyPairAdmission` used by the public file-to-file
  and file-to-analytical entry points.
- The production helper activates the concrete input/output identity, acquires the
  actual composite slot+byte primitive, obtains a resident credit and performs a
  generational input-to-output handoff.
- The short boundary credit is released before result/output diagnostics are
  materialized, so it does not pollute user-visible charged-memory measurements.
- Cleanup is composed with prepared-input and operation-context teardown: every
  owner is attempted even if an earlier cleanup fails, while the primary
  exception remains primary.
- The pass-50 native contract test iterates all 8 input types x 7 output types =
  56 identities through the same production helper used by those public entry
  points. This is stronger than pass 49's manual invocation of three generic
  primitives under an identity context.

## Execution-policy integration

- The automatic execution policy reacts to genuinely dynamic resident/control
  ownership without treating an otherwise idle, smaller process envelope as
  pressure before the operation installs its canonical capacity.
- Native and Python sides therefore share the same hard envelope without allowing
  static baseline accounting to spuriously reduce contractually idle worker
  policy.

## Native ABI additions

- ABI3 exposes the fixed-width atomic epoch operations used by finalizer and
  shutdown accounting.
- The new C++ operations compile under the project's warning/error policy and are
  linked into `_core_abi3.abi3.so` in the validation build.
- The deliverable retains `cmake_minimum_required(VERSION 4.3)`. Only the isolated
  validation copy lowered that line to the CMake 3.31.6 available in the test
  environment; the change is not present in the source ZIP.

## Validation

Final pass-50 validation was performed against the same source tree that is
packaged for delivery:

- Historical source-only memory regression selection: **492 passed, 6 skipped**.
- `test_memory_safety_pass50.py` with the compiled ABI3 module: **29 passed**.
- Full `tests/memory` with a real pass-50 `_core_abi3`: **830 passed, 21 skipped,
  0 failed**.
- Core concurrency memory-governor/hardening pass1-pass5 with ABI3: **72 passed,
  1 skipped**.
- Native ABI3 build: completed and linked successfully to 100% in the isolated
  validation tree.
- Python `compileall`: passed.
- `meta/ci/check_primary_cleanup.py`: passed.

The full `tests/concurrency` directory was not used as a green success metric:
an earlier broad run is long-running and three lifecycle tests concerning
immediate `Result`/`ArrowCStream` drop cleanup were reproduced unchanged on the
original pass-49 tree with the same ABI. They are therefore pre-existing
lifecycle semantics rather than pass-50 regressions. The memory/concurrency
hardening subsets listed above are green.
