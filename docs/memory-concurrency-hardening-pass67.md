# Pass 67 — construction, external-pool authority, fork and route hardening

Pass 67 closes the remaining composition gaps found after pass 66. The focus is
transactional construction of external-runtime ownership, process-global pool
coordination, fail-closed pool verification, post-fork quarantine for Parquet
lifetimes, native physical-thread accounting, route-profile certification, and
observable completion-memory protocol integrity.

## External runtime construction is prearmed from the first commit

`acquire_external_runtime_threads()` now creates an
`_ExternalRuntimeConstructionEscrow` before acquiring any logical borrow, shared
logical claim, or native physical claim. The prepared finalizer state is updated
after every authoritative acquisition. Exceptions between borrow -> logical ->
physical -> alignment -> wrapper publication therefore retain an exact cleanup
suffix instead of leaking a hidden claim.

This covers both operation-borrowed and standalone acquisition paths, including
an injected failure after the native claim but before logical down-alignment.

## One process-global coordinator per external runtime

The split logical/physical maps from pass 66 are replaced by
`_ExternalRuntimePoolCoordinatorEntry`. One entry owns:

- shared native physical permits;
- shared logical governor lease;
- per-operation logical and physical claims;
- verified configured pool width; and
- generation state used to distinguish overlap from a fresh pool generation.

Overlapping operations never re-expand a pool. Once every claim retires, a new
generation may re-expand only after capacity has first been re-admitted.
Configurable standalone runtimes can degrade to a safe width >= 2 rather than
falling directly from N workers to serial. Fixed, observable pools such as
Polars remain exact-admission.

## Configurable pools are verified fail-closed

`constrain_external_runtime_worker_pool()` now requires both observation and
configuration APIs. Getter/setter failures become resource errors instead of
being interpreted as proof of successful restriction. Every setter is followed
by a read-back; `observed > admitted` is rejected. A lower observed width is
safe and causes the caller's lease to shrink accordingly.

A fresh generation may grow a previously reduced global pool, but an overlapping
generation remains monotonic at the already verified width.

## Post-fork quarantine extends to Parquet dataset lifetimes

`_DatasetLifetimeOwner` and `_DatasetLifetimeLease` now carry process identity and
check it before touching inherited mutexes. Explicit access/release in a child
process fails through the runtime fork guard instead of waiting on a lock whose
owner thread disappeared at `fork()`. Destructors avoid cross-process release.

## Native physical-thread accounting distinguishes external pools

The native runtime has a dedicated external-runtime permit domain:

- `acquire_process_external_runtime_thread_permits()`;
- `release_process_external_runtime_thread_permits()`;
- Python ABI methods with the corresponding names; and
- `external_runtime_thread_permits` in the runtime snapshot.

Managed thread-start admission now subtracts the admitted external-runtime pool
from OS-observed unmanaged threads before projecting total physical pressure,
preventing the live external pool from being charged once as an observed thread
set and again as managed start permits.

## Completion-memory ownership violations are observable

`CompletionMemoryLease` remains move-only. Its native retained-byte return now
uses a checked atomic decrement. An impossible underflow increments
`completion_memory_protocol_violations` and safely clamps the counter in the
noexcept cleanup path instead of silently hiding protocol corruption behind a
saturating subtraction. The diagnostic snapshot schema is therefore version 7
and the Python ABI tuple contains 25 fields.

The release gate rejects a non-zero observable completion-memory protocol
violation count.

## Orthogonal transport/lifetime route certification

Pass 67 augments the 8 x 7 format-pair matrix with route profiles rather than
multiplying the entire matrix:

Input profiles:

- `local_path`
- `remote_chunks`
- `directory_source_plan`
- `materialized_memory`
- `python_iterator`
- `staged_remote`

Output profiles:

- `local_file`
- `remote_staged_commit`
- `stream`
- `analytical_adapter`

Runtime contract observations made during a real pair are also attributed to the
active route profiles. The release gate requires every route profile to have
executed its critical memory/slot/control/native/cancellation contracts at least
once. The pair scope does not become closed until its route ContextVar has also
been restored, preserving retryable cleanup semantics.

## Safety-critical runtime contract gate

The release certification now separately requires observed safety-critical
contracts for:

- retained-memory credit;
- composite slot+byte admission;
- process control-plane budget;
- native payload entry;
- cancellation checkpoints;
- file-descriptor admission; and
- external-runtime pool claims.

This keeps micro-optimisation metadata out of the hard release contract while
making the resource-safety invariants non-optional.

## Validation performed in the packaging environment

- pass62-pass67 memory hardening: **75 passed** (two expected Python 3.13
  `fork()` deprecation warnings from real deadlock regressions);
- pass67-specific regressions: **11 passed**;
- pass45 plus selected pass42/43/48/54 compatibility checks: **25 passed**;
- selected pass49/50/53 pair-admission checks: **6 passed, 2 skipped** because
  the compiled native core is unavailable in this source-only environment;
- `python -m compileall -q src`: passed;
- C++20 syntax compilation: `operation_task_arena.cc`, `ordered_executor.hh`,
  `ordered_executor_probe.cc`, and `_core_abi3_module.cc`: passed.

A complete CMake build cannot be run here because the repository requires CMake
4.3 while the packaging environment provides CMake 3.31.6.
