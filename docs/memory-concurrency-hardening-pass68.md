# Pass 68 — atomic physical admission, rollback escrow, and route/lifetime hardening

Pass 68 implements the follow-up findings from the pass 67 audit. The focus is
on eliminating the last cross-domain physical-thread TOCTOU, making composed
stage construction retryable under release failure, separating external-runtime
claims from thread identity, strengthening release certification, and closing
Parquet lifetime leaks/fork gaps.

## One atomic authority for all physical-thread reservations

Managed/native workers and external-runtime claims no longer commit capacity by
CASing two independent counters. Both paths now reserve through
`g_process_total_thread_permits`, whose CAS is the sole physical admission commit
point. The managed and external counters are subledgers populated only after the
combined reservation commits.

Release is conservation-safe as well: a domain first removes the amount it
actually owns and only that committed amount is returned to the total ledger.
An over-release can therefore no longer steal capacity belonging to the other
domain.

The runtime snapshot now exposes:

- `total_physical_thread_permits`;
- `external_runtime_resident_threads`;
- the existing managed and external active subledgers.

The ABI runtime snapshot grows from 25 to 27 fields.

## Active external claims are no longer treated as thread identity

Pass 67 inferred that active external permits represented a subset of the
OS-observed unmanaged threads. That inference is unsafe because a permit count
contains no thread identity.

Pass 68 separates three concepts:

- active external-runtime operation claims;
- configured runtime width;
- explicitly reported resident external workers.

Only the last category may offset OS-observed unmanaged threads. The native ABI
therefore adds resident attribution add/release methods independent of active
permits. Python runtimes do **not** infer residency from `cpu_count()` or
`thread_pool_size()` because those commonly describe configured capacity, not
proof that matching OS threads exist. A runtime integration must explicitly
expose `schema_sanitizer_resident_thread_count()` before resident attribution is
accepted.

This is intentionally fail-safe: an uninstrumented runtime can be charged
conservatively, but unrelated process threads can no longer be discounted merely
because an operation happens to own N external permits.

## Stage-domain construction has a pre-reserved rollback escrow

`acquire_stage_concurrency_admission()` now reserves a bounded
`ReservedFinalizerEscrow` generation before acquiring any stage ownership.
Every successful secondary-domain acquisition is attached immediately to the
still-private `StageConcurrencyAdmission`; there is no temporary list that is
the sole owner of an acquired domain.

If a later acquisition fails, `close()` unwinds the exact ownership graph in
reverse order. If a domain release itself fails, the complete partially-closed
stage capability is published into the already-reserved escrow and can be
retried by `drain_abandoned_memory_finalizers()`.

The construction escrow is capped at 1024 simultaneous admissions. Exhaustion
fails before any new stage resource is acquired, and its static footprint is
registered with the process control-plane budget.

## Release certification is fail-closed on native diagnostics

`validate_native_concurrency_protocol_health()` no longer treats unavailable or
failed native snapshots as success. Release certification now requires a pass68
snapshot and verifies:

- completion-memory protocol violations == 0;
- native counter underflows == 0;
- `total == managed + external`;
- `total <= native physical capacity`.

An old 25-field native binary therefore cannot certify a pass68 release by
silently omitting the new invariant.

## Route profiles now prove route-specific safety mechanisms

The transport/lifetime profile matrix is no longer six/four copies of the same
generic five contracts.

Examples of the stronger requirements:

- local path/file routes require process FD admission evidence;
- remote and directory routes require composed stage admission plus FD evidence;
- Python iterator paths require stage-admission evidence;
- remote staged output requires stage + FD evidence;
- analytical adapters require an observed external-runtime pool claim;
- already materialized/stream routes retain only the mechanisms they actually
  need.

This prevents a file-descriptor or external-runtime observation from an
unrelated route from certifying another route accidentally.

## Parquet stream owners are fork-safe and tombstones are compacted

`_ParquetStreamKeepaliveOwner` now stores its creator PID in `__slots__` and at
construction. Its destructor therefore has a real process-identity guard rather
than comparing the current PID with a fallback that always returned the same
PID.

The per-factory `_keepalive` list also performs amortized in-place compaction once
it reaches 64 entries. Dead weakrefs from abandoned streams no longer accumulate
without bound on long-lived factories; live stream ownership is preserved.

## Pass 68 regression coverage

`tests/memory/test_memory_safety_pass68.py` adds regressions for:

- the single native atomic total authority;
- separation of active claims and resident thread identity;
- persistence/retirement of explicitly reported resident runtime width;
- retryable stage-domain rollback after an injected release failure;
- fail-closed native release certification and total/subledger conservation;
- route-specific contract requirements;
- the Parquet stream-owner PID guard;
- dead keepalive weakref compaction.

## Validation performed in this environment

- pass68-specific regressions: **8 passed**;
- pass54-pass68 hardening regressions: **203 passed** with two expected Python
  3.13 `fork()` deprecation warnings;
- additional Parquet fallback/direct-I/O selection: **14 passed, 9 skipped**;
- release-gate integration test: **1 skipped** because the compiled native/adaptor
  environment is unavailable;
- `python -m compileall -q src`: passed;
- C++20 syntax compilation passed for:
  - `operation_task_arena.cc`;
  - `ordered_executor_probe.cc`;
  - `_core_abi3_module.cc`.

The complete `tests/memory` collection cannot run from this source-only archive:
four collection paths require `schema_sanitizer._core_abi3`, which is not present
for Python 3.13 in the packaging environment. The first historical-suite failure
was isolated and confirmed to be that same missing native module.

A complete CMake/native build is also unavailable here because the repository
requires CMake 4.3+ while this environment provides CMake 3.31.6. The modified C++
translation units nevertheless pass direct C++20 syntax compilation.
