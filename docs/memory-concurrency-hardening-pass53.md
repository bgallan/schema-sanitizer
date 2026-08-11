# Memory / concurrency hardening — pass 53

Pass 53 turns several pass-52 bounds from scope-local accounting into explicit
lifetime ownership, makes process concurrency react to live OS pressure, and
closes the remaining gaps between Python scheduling, native worker creation,
discovery metadata, cancellation, and shutdown.

## Index

- [Deferred ledger close is exactly once](#deferred-ledger-close-is-exactly-once)
- [Discovery metadata follows object lifetime](#discovery-metadata-follows-object-lifetime)
- [Reserve before materialization](#reserve-before-materialization)
- [Fork callbacks are prepared or quarantined](#fork-callbacks-are-prepared-or-quarantined)
- [Async scheduler lifecycle and terminal debt](#async-scheduler-lifecycle-and-terminal-debt)
- [Async result bytes and fairness](#async-result-bytes-and-fairness)
- [Dynamic thread and descriptor headroom](#dynamic-thread-and-descriptor-headroom)
- [Native physical threads and CPU are separate domains](#native-physical-threads-and-cpu-are-separate-domains)
- [Integral stage admission](#integral-stage-admission)
- [Real 56-pair payload certification](#real-56-pair-payload-certification)
- [Native allocation registry hardening](#native-allocation-registry-hardening)
- [Pipeline result retention modes](#pipeline-result-retention-modes)
- [Bounded discovery close and streaming summaries](#bounded-discovery-close-and-streaming-summaries)
- [Integration hardening found during validation](#integration-hardening-found-during-validation)
- [Validation](#validation)

## [Deferred ledger close is exactly once](#index)

`OperationMemoryLedger.close()` may begin while leases are still live. Pass 53
makes terminal completion a single transaction shared by `close()` and the last
lease release:

- the native/cross-process owner is released only after reserved bytes reach
  zero;
- the finalizer-escrow ticket is detached and returned exactly once;
- a later `close()` cannot leak or double-return the ticket;
- regression coverage repeats deferred-close cycles beyond the escrow-bank size
  so gradual exhaustion cannot hide behind short tests.

This closes the pass-52 leak where a correctly deferred close could permanently
consume one of the 8192 finalizer slots.

## [Discovery metadata follows object lifetime](#index)

Directory/source discovery no longer treats the lexical discovery scope as the
lifetime of its metadata charge.

- `DirectoryMetadataBudget` owns an `OperationMemoryLease`.
- Successful discovery transfers that lease into a retained metadata owner.
- `DiscoveredDirectoryInput` and `PartitionRunPlan` carry the owner while the
  discovered graph remains reachable.
- Closing the construction scope therefore does not make live metadata appear
  free to the process governor.
- Both asynchronous and strict blocking discovery use the same retained owner.

The result is lifetime accounting rather than scope accounting: physical Python
objects and their governed charge disappear together.

## [Reserve before materialization](#index)

Metadata builders now pre-admit capacity before growing retained Python
containers.

- URI graphs are charged before the retained tuple is published.
- Reference windows reserve before materialization and reconcile exact retained
  counts.
- Association/dictionary metadata is charged before result dictionaries and
  plan-reference lists grow.
- Rollback releases provisional credit if materialization fails.
- `single_file` discovery now uses the same metadata envelope as directory
  discovery instead of building several ungoverned O(n) maps.

A small `source_discovery_memory` helper owns the accounting/cache mechanics;
the discovery phase owner remains bounded and cohesive.

## [Fork callbacks are prepared or quarantined](#index)

Production registrations no longer use `child_safe_without_prepare=True` for
callbacks that can allocate Python objects after `fork()`.

- callbacks that need child state use parent-side prepared swaps;
- callbacks that are not required in a fork child are marker-only
  `quarantine_only` registrations;
- quarantine registrations cannot carry an unreachable child callback;
- AST regression coverage prevents allocation-prone child-safe registrations
  from being reintroduced.

The post-fork child path is therefore limited to prebuilt state swaps,
primitive publication, or explicit quarantine.

## [Async scheduler lifecycle and terminal debt](#index)

The process-global async scheduler is now a runtime service with explicit
admission state.

- `RUNNING` admission can be closed before process shutdown drains services.
- new operations cannot create queues/tasks after async shutdown begins;
- shutdown waits for active operations through a bounded condition wait;
- test reset reopens the scheduler without leaking closed state into later
  operations.

Worker cancellation is also bounded. Tasks are cancelled and waited for up to
a deadline. A task that suppresses `CancelledError` transfers its ownership
into a fixed terminal-debt bank instead of causing shutdown to wait forever or
releasing its admission while it is still alive.

## [Async result bytes and fairness](#index)

Async boundedness now covers bytes as well as item counts.

- queued/pending results own operation-memory leases until consumed;
- the default estimator includes conservative object/control overhead;
- callers with opaque or externally sized results can provide
  `retained_bytes(result)` for exact charging before queue publication;
- leftover results are drained/released on error and cancellation.

Process task slots are shared fairly across concurrent operations. Normal-sized
pools reserve useful headroom for other operations while tiny 1–2 slot pools
retain full forward progress. Unused fair share remains work-conserving.

## [Dynamic thread and descriptor headroom](#index)

Python process-resource governors refresh the effective OS ceiling rather than
freezing it at import time.

Thread admission combines the configured sanitizer ceiling with live:

- cgroup `pids.max - pids.current`;
- applicable `RLIMIT_NPROC` headroom;
- conservative resident-memory/stack headroom.

Descriptor admission combines its configured ceiling with live
`RLIMIT_NOFILE`, current `/proc/self/fd` usage, and a separate teardown reserve.
Both domains may correctly report zero normal-admission headroom; no artificial
minimum of two threads or sixteen descriptors can overrule the OS.

## [Native physical threads and CPU are separate domains](#index)

Native worker creation now participates in a process-global physical-thread
envelope.

- arena/reaper `std::thread` creation acquires native physical-thread capacity;
- Linux builds also observe `/proc/self/task`, so Python/provider threads reduce
  the native creation headroom;
- multiple arenas cannot independently create their own unbounded worker set;
- telemetry exposes live/running/rejected native worker counts.

Physical parked threads and runnable CPU are deliberately separate resources.
`ProcessCpuGovernor` independently refreshes `available_cpu_capacity()` for each
admission, so wide arenas can exist while only the currently allowed number of
workers execute. Runtime cgroup/affinity reductions therefore stop new runnable
admissions without destroying already created workers.

## [Integral stage admission](#index)

`StageConcurrencyAdmission` is now a distinct capability rather than an alias of
`CompositeParallelAdmission`.

A stage transaction owns, as requested:

- physical execution slots;
- resident payload bytes;
- process control-plane bytes;
- additional named domains supplied by the stage, such as async or remote-I/O
  permits.

Additional domains are acquired only after the base slot/byte admission and are
released in reverse order. Any failed domain acquisition rolls back the entire
transaction while preserving the primary exception. Memory ownership can be
transferred to the next stage without a release/reacquire gap.

## [Real 56-pair payload certification](#index)

The historical 8-input × 7-output matrix still has its structural contract, but
release certification no longer accepts registration alone.

Every real public pair must now observe, on the payload path:

- a distinct stage admission;
- positive resident-memory admission;
- an actual memory-lease stage transfer;
- process control-plane ownership.

The tiny positive sentinel used to prove physical memory admission is resized to
zero immediately after transfer, so certification does not distort public
`current_charged_memory_bytes` diagnostics. A separate release gate requires
all **56/56** payload observations. CI environments with the full optional
adapter set execute the actual public conversions.

## [Native allocation registry hardening](#index)

The pass-52 flat registry is strengthened further.

- primary-shard saturation probes a deterministic secondary shard before
  rejecting a registration;
- telemetry records secondary probes, collision rejections, and maximum shard
  occupancy;
- the global metadata bank is proportional to the process memory ceiling rather
  than always reserving the full 64 MiB maximum;
- that proportional bank is frozen after first process initialization so
  `memory.current` growth cannot paradoxically shrink the bank and increase the
  advertised payload ceiling;
- current/peak metadata and capacity remain visible through the native ABI.

This preserves bounded/no-growth allocation-time behavior while reducing both
false OOM from shard skew and oversized control-plane reservations in small
containers.

## [Pipeline result retention modes](#index)

Partition execution can now select how much historical state it retains:

- `full` preserves the existing complete `PartitionRunResult` history;
- `metadata_only` keeps compact audit metadata while dropping large discovered
  input/native-state ownership;
- `streaming` keeps no per-partition history and relies on the existing callback
  for immediate consumption.

The final registry/native state still remains available independently of the
history mode.

## [Bounded discovery close and streaming summaries](#index)

`DirectoryMetadataBudget.close()` uses a bounded deadline instead of an
unbounded condition wait. A failed close leaves ownership retryable rather than
claiming success.

Source summaries no longer construct an O(files) list of sizes. They stream
local/remote file objects and cache the `(count, bytes)` summary by discovered
object identity, so many plans sharing one directory do not repeatedly rescan
its file tuple.

## [Integration hardening found during validation](#index)

Several cross-domain issues became visible only after composing all pass-53
changes and were fixed as part of the same pass.

- Cross-process memory coordination now tightens a live capacity monotonically
  when the process ceiling falls; increases are deferred until no owner is live,
  preventing capacity changes from orphaning reservations.
- `OperationTaskArena` now applies allocation-free epoch backpressure when
  retained queued/active bytes temporarily fill the arena. Producers wait for a
  release notification instead of converting transient pressure into OOM;
  completion-only ownership still fails fast to avoid deadlock.
- The allocation-registry metadata bank is stable for the process lifetime, so a
  reservation made against one process snapshot cannot be admitted against a
  later, larger ceiling created by metadata-bank shrinkage.
- Partition lookahead binds its speculative stage admission explicitly to the
  armed parent operation ledger. Local converter boundaries do not rely on a
  `ContextVar` being active, so N+1 overlap remains governed rather than being
  silently disabled.
- `Result` and `ArrowCStream` finalizers publish prepared cleanup only; rich
  close/resource-owner/keepalive work occurs at the governed finalizer safe
  point. GC threads do not perform the rich teardown path directly.

## [Validation](#index)

Pass-53 validation uses the real ABI3 extension built from the modified native
sources. The deliverable itself does not contain that compiled validation
artifact.

The final validation report is recorded in the release response and includes:

- native Release build with GCC 14 and `-Werror`;
- memory/remote hardening suites;
- the complete concurrency suite in bounded chunks;
- pipeline tests, including lookahead and discovery lifetime behavior;
- pass-53 targeted regression tests;
- source cleanup/CMake checks and Python bytecode compilation;
- comparison against pass 52 for maintenance-quality gates so no new baseline
  failure is introduced.

The source deliverable retains `cmake_minimum_required(VERSION 4.3)`.
