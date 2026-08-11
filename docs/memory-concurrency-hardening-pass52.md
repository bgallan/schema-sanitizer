# Memory and concurrency hardening — pass 52

Pass 52 closes the next set of process-wide memory, lifecycle, fork, and
concurrency-composition gaps found after pass 51. The central theme is that a
subsystem being locally bounded is not sufficient: lifecycle state, native
metadata, asynchronous workers, OS resources, and pipeline-stage admissions must
remain bounded and mutually consistent at process scope.

## Authoritative availability-notifier lifecycle

- `AvailabilityNotifier` now makes shutdown/reopen/snapshot decisions from its
  authoritative `_queued` owner map rather than the legacy compatibility deque.
- `STOPPED` can no longer be published while an authoritative delivery owner is
  still queued.
- Fork reset quarantines the authoritative queued owners before replacing child
  state.
- Pass-52 regression tests enforce the quiescence invariant directly.

## Reachable fork-handler contracts

- Registering `after_in_child` with `quarantine_only` is now rejected: such a
  callback was structurally unreachable because no prepared handler generation
  could execute it.
- Child-safe reset callbacks explicitly opt in with
  `child_safe_without_prepare=True`; prepared callbacks continue to use the
  transactional `before`/`after_in_child` mode.
- A repository-wide AST test prevents future unprepared child callbacks from
  silently falling back to an inert quarantine registration.

## Physically bounded native allocation metadata

- The live-allocation registry no longer grows an `unordered_map` overflow.
  Allocation ownership metadata is stored in a sharded, preallocated flat table
  with no registration-time heap allocation.
- Registry capacity is bounded per pool and under a process-wide **64 MiB**
  metadata ceiling.
- The process resident-memory envelope reserves that fixed metadata bank up
  front. User-visible `reserved_bytes` remains payload/control accounting, while
  advertised resident capacity excludes the bank so registry churn cannot create
  false pressure or make capacity fluctuate.
- Destroying a pool reconciles any still-live registry entries from global
  telemetry, avoiding ghost ownership counters.
- Native ABI telemetry exposes current/peak registry metadata, record capacity,
  live entries, and rejected registrations.

## Hard process thread and descriptor ceilings

- Requested thread limits are clamped by a sanitizer absolute ceiling, cgroup
  `pids.max`, applicable `RLIMIT_NPROC`, and a conservative memory-derived thread
  capacity.
- The memory-derived limit reserves a conservative stack allowance per worker
  and keeps a minimum process memory reserve.
- Requested file-descriptor limits are clamped by a sanitizer absolute ceiling
  and `RLIMIT_NOFILE`, with runtime headroom retained.
- Environment configuration can reduce these capacities but can no longer turn a
  hard guardrail into an arbitrarily large limit.

## Process-global asynchronous admission

- `ordered_indexed_results` and `unordered_indexed_results` now reserve from a
  fixed process-global async-task namespace before creating queues or Tasks.
- Each admission also reserves control-plane bytes for operation and per-worker
  scheduler metadata.
- Concurrent async operations therefore compose against one bounded process
  capacity rather than each independently allocating its local maximum window.
- If no global task slot/control credit is available, work falls back to inline
  sequential execution instead of allocating an ungoverned scheduler.
- Fork reset replaces the async-admission counters with clean child-safe state.

## Event-driven ordered-executor shutdown

- Native `OrderedExecutor::WaitUntil` no longer wakes every 50 microseconds to
  poll completion.
- A shutdown waiter arms the existing completion condition and sleeps until a
  completion transition or deadline.
- Normal `Finish()` remains cheap: it performs notification work only when a
  waiter is actually armed.

## Cleanup-dispatcher lock ordering

- Cleanup control-plane credits are reserved before the dispatcher condition lock
  is acquired.
- Failed publication rolls the ticket back after releasing the dispatcher lock.
- Worker completion detaches a ticket while holding local state and releases the
  global control-plane credit afterward.
- This removes the unnecessary dispatcher-lock -> memory-governor-lock nesting
  from correctness paths.

## Allocation-free emergency owner banks

- Memory-budget and cross-process emergency owner roots are fixed-size
  preallocated arrays rather than bounded lists that still needed `append()`
  allocations under memory pressure.
- Publication into the emergency bank is indexed and allocation-free on the
  critical retention path.
- Fork reset and draining preserve the fixed physical footprint.

## Integral stage concurrency admission

- `CompositeParallelAdmission` now owns three resources as one transaction:
  execution slots, resident-memory bytes, and control-plane metadata bytes.
- It is also exposed as `StageConcurrencyAdmission` /
  `acquire_stage_concurrency_admission` to make the process-wide stage contract
  explicit.
- Acquisition can shrink under pressure, and rollback/close release all three
  resources without losing the primary exception.
- Runtime concurrency diagnostics advertise the stage-concurrency-admission
  contract.

## Bounded remote grouping scans

- S3, GCS, and Azure bulk discovery no longer materialize an O(number-of-groups)
  tuple of every grouping key merely to feed indexed async scheduling.
- `drain_ordered_iterable_results` consumes arbitrary iterables in batches capped
  by the async window/process task ceiling, retaining only O(concurrency)
  auxiliary references.
- Each batch inherits the same process-global async admission and control-plane
  accounting as indexed scheduling.
- The primary grouping map remains bounded by the already governed discovery
  metadata; the extra scan representation is now physically window-bounded.

## Validation

Validation was performed against the final pass-52 source tree.

- Native ABI3 Release build with GCC 14 and `-Werror`: **completed and linked to
  100%**.
- `tests/memory + tests/remote` with the compiled ABI3 module: **939 passed,
  27 skipped, 0 failed**.
- Concurrency memory-hardening pass1-pass5: **63 passed, 0 failed**.
- Targeted pass-52/concurrency-v52 suites: **35 passed, 0 failed**.
- Pass-52 + async scheduler source/runtime tests: **21 passed, 0 failed**.
- Native allocation-registry telemetry smoke returned a bounded registry with
  zero live/rejected records after initialization.
- Python `compileall`: passed.
- `meta/ci/check_primary_cleanup.py`: passed.

The available builder provides CMake 3.31 while the project requires CMake 4.3.
As in the previous pass, native validation used an isolated copy whose minimum
CMake declaration alone was lowered for the builder. The deliverable retains
`cmake_minimum_required(VERSION 4.3)` and contains no compiled ABI module or
validation build artifact.
