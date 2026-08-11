# Concurrency and memory hardening

Schema-Sanitizer applies concurrency limits and resource accounting at both the
operation and process boundaries. The goal is to prevent two individually safe
calls from oversubscribing the same process, filesystem, remote session, or
shutdown path when they overlap.

## Index

- [Resident-memory hierarchy](#resident-memory-hierarchy)
- [Temporary-storage hierarchy](#temporary-storage-hierarchy)
- [Telemetry-tuned safety margins](#telemetry-tuned-safety-margins)
- [Remote concurrency and ownership](#remote-concurrency-and-ownership)
- [Concurrency invariants](#concurrency-invariants)
- [Validation](#validation)
- [Streaming disk admission and cleanup quarantine](#streaming-disk-admission-and-cleanup-quarantine)
- [Process threads, descriptors, and cross-process memory](#process-threads-descriptors-and-cross-process-memory)
- [Cancellation, deadlines, and fork safety](#cancellation-deadlines-and-fork-safety)
- [Pressure feedback and provider throttling](#pressure-feedback-and-provider-throttling)
- [Per-operation observability](#per-operation-observability)
- [Bounded remote backlog and fail-closed shared state](#bounded-remote-backlog-and-fail-closed-shared-state)
- [Retryable teardown and fixed coordination lifetimes](#retryable-teardown-and-fixed-coordination-lifetimes)
- [Linearizable retries and crash recovery](#linearizable-retries-and-crash-recovery)
- [Capability ledgers and terminal runtime quiescence](#capability-ledgers-and-terminal-runtime-quiescence)
- [Retirement visibility and exactly-once teardown](#retirement-visibility-and-exactly-once-teardown)

## [Resident-memory hierarchy](#index)

Each public conversion owns an `OperationMemoryLedger`. Python staging objects
and native allocators reserve from that same atomic operation ledger. A second,
process-wide native pool receives every scalable native allocation and every
Python-owned external reservation, so concurrent operations cannot each consume
the full host allowance independently.

The process ceiling is refreshed from the safe host or cgroup allowance at
operation boundaries without clearing live accounting. Lower capacity samples
stop new allocations until existing reservations drain; recovered capacity can
be used by later work. The operation limit remains fixed for the lifetime of the
call.

Retained leases are thread-safe and exactly-once. Resizing and final release are
serialized, failed constructors remain inert, and abandoned Python leases return
their reservation during finalization. Multipart upload parts, HTTP response
bytes, and decoded response text reserve transient copies before allocation and
retain only the final object's exact charge.

Internal diagnostics are available through
`process_resident_memory_snapshot()`, `process_memory_pressure_snapshot()`, and
operation-ledger snapshots. They report capacity, current reservations,
historical peaks, current Linux RSS when available, and an approximate
`untracked_rss_bytes` value for interpreter, SDK, TLS, thread-stack, allocator,
and fragmentation overhead. Resource errors caused by the process ceiling carry
those RSS observations without exposing payload contents. Operation diagnostics
also retain live-at-close and over-release counters instead of silently hiding
cleanup underflow.

## [Temporary-storage hierarchy](#index)

Temporary files are governed separately because on-disk bytes are not resident
memory. Every operation retains its own spool limit, while a process-wide
filesystem governor serializes reservations across operations using the target
filesystem device. A fixed emergency margin remains free for the runtime and
operating system.

The governor accounts for staged artifacts before creation, supports exact
resize after materialization, migrates reservations when a lease changes
filesystem, and releases abandoned leases during finalization. It also resamples
free space so unrelated processes consuming the same filesystem reduce new
admission.

Operation and process filesystem diagnostics retain live-at-close bytes,
active leases, and over-release counters. Deployments with multiple independent
workers can additionally set `SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS=1`.
On POSIX hosts this enables a `flock`-protected reservation registry keyed by
filesystem device. Admission and release become atomic across processes, dead
owners are reclaimed using PID start tokens, and PID reuse cannot inherit stale
reservations. `SCHEMA_SANITIZER_COORDINATION_DIR` selects the shared host-local
registry directory. Unsupported platforms retain the existing process-local
protection and filesystem failure handling.

## [Telemetry-tuned safety margins](#index)

Safety-margin tuning is deliberately opt-in. Setting
`SCHEMA_SANITIZER_TELEMETRY_TUNING=1` records a bounded host-local profile in the
coordination directory. Operation close records opaque RSS overhead, while
successful temporary reservations record the free-space floor needed by staged
artifacts. Only the newest 256 samples are retained.

Future calls use a nearest-rank p95 with hard safety bounds. Resident-memory
reserve cannot exceed 25% of the governed process capacity or 2 GiB, and the
temporary free-space floor cannot exceed 4 GiB. Static defaults remain lower
bounds, malformed or unavailable profiles are ignored, and telemetry never
weakens a configured limit. With tuning disabled, persisted samples have no
effect and the historical deterministic policy is unchanged. Production and
fuzzing harnesses may also call the internal `record_resource_telemetry` helper
to feed validated observations into the same bounded profile.

## [Remote concurrency and ownership](#index)

Multi-mode remote work uses one operation-owned event-loop thread and one shared
provider-session pool. A weighted process-wide governor admits work across
unrelated event loops. File and directory transfers reserve weight from their
estimated I/O chunks, while metadata calls remain lightweight. FIFO ordering
allows at most four bounded small-request bypasses ahead of a blocked large
request, preventing both head-of-line latency and starvation.

Each coordinator registers its derived capacity only for its own lifetime. The
process ceiling therefore rises to the largest active operation policy and
shrinks again when that operation closes; an old high-concurrency call cannot
permanently relax future admission. Session creation remains single-flight per
provider key. Key-local single-flight gates are retained only while creators or
waiters actively use them, so a long operation cannot accumulate locks from a
large number of failed or one-shot endpoints. A client or context manager that
finishes creation after pool shutdown is closed immediately instead of becoming
orphaned.

Every synchronous wait on remote work has a transport deadline. Closing the
coordinator uses one monotonic deadline for task cancellation, provider cleanup,
and host-thread join. Cancellation-resistant coroutines force the daemon loop to
stop and produce an explicit shutdown error rather than leaving a live thread.

Prefetched staging results use an explicit ownership slot. Exactly one side may
consume or abandon a completed result; late completion after cancellation closes
its own staging resources. Storage permits are returned when submission fails or
when a staged task never transfers ownership. Prefetch windows and native worker
policies narrow from live process headroom before allocations fail. Partition
lookahead uses a one-slot daemon host with bounded shutdown rather than waiting
indefinitely for a cancellation-resistant executor worker.

`process_remote_io_permit_snapshot()` exposes active capacity registrations,
current and peak weighted usage, queue depth, bounded bypasses, cancellations,
and cleanup underflow diagnostics.

## [Concurrency invariants](#index)

- All operation-local work shares one worker arena and one memory ledger.
- Concurrent calls share process CPU admission, exact resident-byte admission,
  weighted remote-I/O admission, and per-filesystem temporary-space admission.
- Ordered results, diagnostics, and failures still commit by source ordinal.
- Prefetch windows and temporary artifacts remain bounded under cancellation.
- A resource published after its consumer closes is closed by its producer.
- Cleanup may fail explicitly at its deadline, but it must not wait forever.
- Process and operation counters return to their baseline after successful,
  failed, cancelled, and abandoned paths.

## [Validation](#index)

The hardening is covered by deterministic Python race tests, historical
concurrency-scaling suites, remote fault-injection tests, and the native repeated
concurrency probe. The native probe mixes external resident reservations with
allocator-backed native buffers under ThreadSanitizer and ASan/UBSan.

New resource owners must cover normal completion, construction failure,
cancellation, explicit close, and abandoned finalization.

## [Streaming disk admission and cleanup quarantine](#index)

Remote downloads no longer reconcile temporary storage only after the response
has completed. HTTP, GCS, S3, and Azure writers grow their shared
`TemporaryStorageLease` before every local write. Unknown-length and
understated-length responses therefore fail admission before they cross the
filesystem emergency reserve. Retry truncation returns attempt-only growth and
successful completion reconciles the lease to the exact retained file size.

Deleting a staged path is also part of resource ownership. If `unlink` or
`rmtree` fails, the lease is not released. The artifact is moved, when possible,
to a host-local quarantine and a bounded daemon janitor retries deletion. The
lease is returned only after the path is confirmed absent. Stale quarantine
entries from terminated processes are cleaned best-effort by the janitor worker.
Discovery runs outside the operation handoff path, is serialized, retries
transient root-scan failures, and iterates the quarantine tree directly instead
of first allocating a list containing every stale artifact.
Temporary admission accounts for both bytes and artifact/inode counts, so a
workload of many tiny files cannot exhaust the filesystem while remaining under
the byte limit.

## [Process threads, descriptors, and cross-process memory](#index)

Project-owned worker slots, remote event-loop hosts, bridge threads, janitor
threads, open files, sockets, and persistent provider sessions are admitted by
process-wide count governors. The defaults preserve operating-system headroom
and can be tightened with `SCHEMA_SANITIZER_MAX_PROJECT_THREADS` and
`SCHEMA_SANITIZER_MAX_OPEN_FILES`. Count-governor waits use directly removable
FIFO waiter objects rather than retaining cancelled ticket tombstones. Their
queues are bounded, fail fast on overload, and expose both queue capacity and
rejection counts in process snapshots.

Deployments running several independent workers may additionally enable:

```text
SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS=1
SCHEMA_SANITIZER_COORDINATION_DIR=/host-local/shared/path
```

The optional resident registry uses a POSIX `flock`, PID start tokens, and one
aggregate entry per live PID instance. Individual in-process leases still track
their own contributions, so resize and release remain exact while the shared
JSON grows with the number of worker processes rather than the number of
operations. Legacy per-operation entries remain readable during rolling
deployments. The registry performs conservative incremental admission, recovers
dead owners, and complements rather than replaces the exact in-process
native/Python ledger. The corresponding temporary-storage option remains
`SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS=1`.

## [Cancellation, deadlines, and fork safety](#index)

A public cancellation scope applies one event and monotonic deadline to nested
work:

```python
import schema_sanitizer as ss

with ss.operation_cancellation(timeout_seconds=120) as cancellation:
    result = ss.to_jsonl("input.csv", "output.jsonl", input_format="csv")
```

The token is propagated into worker threads and the remote event loop. Resource
waits, retry backoff, staging, remote admission, and operation startup check it
cooperatively. Nested scopes inherit parent cancellation and the earliest
parent/child deadline. Leaving a scope cancels its token so detached work cannot
outlive the operation silently. A cancellation error is never classified as
retryable. The synchronous-to-asynchronous bridge also applies a finite fallback
wait when no explicit operation deadline is present.

An initialized runtime is deliberately not reusable after `fork()`. A child
fails fast instead of inheriting possibly locked mutexes, thread pools, provider
clients, or event loops. Multiprocessing integrations must use `spawn`,
`forkserver`, or execute a fresh process image.

## [Pressure feedback and provider throttling](#index)

Adaptive concurrency consumes Linux cgroup and PSI signals when available. The
runtime samples `memory.current`, finite `memory.high`/`memory.max`, cumulative
`memory.events`, and `/proc/pressure/memory`. Capacity falls quickly under
pressure and recovers one bounded step at a time after a cooldown, avoiding
oscillation and late OOM reactions.

Effective cgroup limits include every visible constraining ancestor. On cgroup
v2, an absent controller file at the mount root is treated as the root's defined
resource-control exemption; an absent file below that root, an unreadable file,
or malformed/truncated contents remain an unknown observation and fail closed.

Remote retries are also coordinated across operations. Each provider or HTTP
endpoint has an AIMD request window, bounded circuit breaker, current/peak
in-flight accounting, and throttling counters. HTTP 429/503 and provider
`SlowDown` responses reduce the window, while `Retry-After` delays block new
admission for that endpoint. Successful traffic restores capacity gradually.
The process-wide endpoint registry is itself bounded: idle least-recently-used
state is evicted under host churn, while in-flight and open-circuit state is
protected until it becomes safely discardable. An all-protected registry
(fully in-flight, cooling down, or both) temporarily rejects new keys instead
of growing past its limit. Oversized endpoint identities are reduced with
incrementally fed hashing,
so the registry neither retains the original string nor allocates one equally
large encoded copy. Diagnostic reads for unknown keys do not create entries.

On glibc Linux hosts, `SCHEMA_SANITIZER_MALLOC_TRIM=auto` may return retained
allocator pages after a large operation only when system pressure and RSS
signals justify it. It has a cooldown and can be explicitly enabled or disabled.

## [Per-operation observability](#index)

`schema_sanitizer.process_operation_diagnostics()` returns immutable copies of
live and recently completed operation records. An optional operation ID filters
the bounded ring. Records include operation-local memory and temporary-storage
peaks plus process snapshots for project threads, file descriptors, filesystem
bytes/inodes, quarantine backlog, system pressure, and weighted remote I/O when
present. Provider-throttle diagnostics include tracked and active endpoint keys,
open circuits, registry capacity, evictions, and saturation rejections.
Snapshotters are weakly held, so diagnostics do not prolong resource lifetimes.

## [Bounded remote backlog and fail-closed shared state](#index)

Weighted remote admission now has two independent ceilings. The waiter ceiling
bounds loop-affine futures already requesting permits, while the submission
ceiling applies before a coroutine is handed to an operation event loop. This
prevents a fast synchronous producer from accumulating an unlimited future set
while the loop is busy or starved. Both limits expose current/peak usage and
rejection counters. Remote operation IDs and diagnostic labels are reduced to
bounded digests before they can be retained by either layer.

Provider request leases use a lock-protected terminal claim, so racing success,
failure, neutral release, and finalization paths publish exactly one AIMD
outcome. The native process-memory governor now uses a bounded deque of waiter
identities; timed-out waiters erase themselves directly instead of leaving
process-lifetime ticket tombstones.

Cross-process memory, temporary-storage, and telemetry documents no longer
truncate serialized JSON to their maximum file size. An oversized update fails
before `truncate()`, preserving the previous valid document and its live
reservations. Malformed JSON, unknown state versions, invalid roots, and
semantically invalid lease or process entries now fail closed instead of being
interpreted as an empty reservation map. Local lease counters are committed
only after that shared update succeeds, so an I/O failure leaves ownership
intact and retryable instead of silently desynchronizing process-local and
host-wide accounting.

## [Retryable teardown and fixed coordination lifetimes](#index)

Operation teardown is now a serialized, retryable state transition. The final
resource-domain reference enters `closing` before cleanup, blocks new forks and
remote admission, and commits `closed` only after the coordinator, directory
metadata, temporary-storage pool, memory ledger, and thread permit have
completed their ownership transitions. A transient journal failure therefore
does not consume the only retry handle. Partial resource-domain construction is
rolled back in reverse ownership order, with secondary cleanup failures attached
to the original exception.

Staged paths follow the same rule. They retain their path and storage lease until
delete, lease release, or janitor handoff has succeeded. A janitor that has begun
shutdown rejects the transfer explicitly; the caller remains retryable. Legacy
internal callbacks that returned `None` after accepting ownership remain
compatible.

Interprocess lock acquisition uses non-blocking `flock` polling with a finite
deadline. Cross-process memory and temporary-storage reservations snapshot both
the opt-in setting and coordination directory when ownership is created, so a
mid-operation environment change cannot redirect or skip cleanup. Memory state
is aggregated by PID start-token identity, reducing coordination-file
cardinality from O(live operations) to O(live processes). Pressure-reducing
resizes are always allowed even when a newer capacity observation is lower than
remaining usage; only growth is subject to admission.

Temporary-storage admission now rejects operation-local saturation and pool
shutdown before touching shared state. The lease object is constructed inertly
before process-wide publication and activated only after both process and local
accounting have committed. Constructor failures, cancellation, and shared
admission errors therefore cannot leave an unowned reservation or trigger a
phantom finalizer release.

## [Linearizable retries and crash recovery](#index)

Keyed retries now have an explicit execution boundary. A worker claims an item,
then commits `CLAIMED -> RUNNING` under the scheduler condition lock immediately
before calling user code. Cancellation uses the same lock and generation, so a
successful cancellation before that transition cannot be followed by a late
callback invocation. While a key is running, one replacement may be retained as
a coalesced successor; a second callback for that key never runs concurrently.
Generation metadata is removed when the last state for a key disappears.

Cleanup publishing uses per-subsystem queues and Deficit Round Robin. The global
item/byte ceilings remain authoritative, while subsystem accounting prevents one
producer from consuming every queue slot. Failed worker permits in the retry
scheduler, cleanup dispatcher, and temporary janitor are exact owners: they
block further worker acquisition and are retained in bounded fail-closed slots
until direct release or guardian transfer succeeds.

External claims keep strict hard-link rejection for canonical records, but the
sweeper understands the exact two-name/same-inode state produced when a process
crashes after atomic `link()` publication. It validates bytes and inode before
removing only the private alias. Publication is complete only after the link is
synced, the private alias is unlinked, and that unlink is synced to the parent
directory. Coordination roots are descriptor-pinned and checked against their
path identity before mutation.

Fork handlers do not attempt cleanup. They replace process-local locks and
registries and retain inherited owners without invoking methods or finalizers,
because those objects may contain locks held by parent threads that vanished at
`fork()`. The child must still `exec()` or use a fresh `spawn`/`forkserver`
process before invoking schema-sanitizer operations.

The native one-worker arena checks an oversized retained charge before subtracting
from capacity. Detached-state reaper limits are now admission limits; if the
bounded reaper cannot accept a state, queued closure destruction occurs
synchronously outside arena locks instead of growing another unbounded backlog.

For explicit process lifecycle management, `shutdown_concurrency_runtime()`
uses one absolute monotonic deadline and closes producers before consumers:
retry scheduler, temporary janitor, cleanup dispatcher, and finally the release
guardian. Resources that cannot be released by the deadline remain reachable in
bounded fail-closed ownership structures.

## [Capability ledgers and terminal runtime quiescence](#index)

Process thread and FD accounting now uses capability-bearing ledger entries.
Returning capacity requires the exact lease identity, lease ID, process
generation, and private capability accepted at admission. Amount-only releases
are non-mutating, and finalizer releases authenticate without retaining the
lease object or depending on weak-reference lifetime ordering.
Capacity wakeups are bounded, one-shot notifications executed by a separately
governed notifier worker.

Retry identities carry exact type tags and bounded composite metadata, avoiding
Python cross-type equality collisions and adversarial custom hash/equality hooks.
The cleanup dispatcher keeps runnable, delayed, dead-letter, and parked work in
separate structures, so a terminal owner never blocks later executable cleanup.
The release guardian governs its own workers, deduplicates owners throughout
their lifecycle, and uses non-throwing bounded failure summaries.

Threaded helpers use two-phase publication: registry authority and resource
capacity are reserved before `Thread.start()`, then activated without allocation.
A live host therefore remains discoverable even if post-start activation fails.
The runtime shutdown is single-flight and terminal, closes global admission, and
progresses through explicit producer, cleanup-producer, consumer, guardian,
notifier, emergency-budget, and native-reaper phases under one monotonic
deadline. Parked or dead-letter ownership is reported explicitly and cannot be
mistaken for successful draining.

The temporary janitor fixes quarantine roots by descriptor identity and retains
FD close plus lease return as one transaction. The native arena reserves cleanup
capacity during admission and uses a lazy bounded joinable reaper with a timed
ABI shutdown hook. The integral runtime snapshot uses a process-wide diagnostic
epoch. Its schema is additive so monitoring code can ignore newer fields.

## [Retirement visibility and exactly-once teardown](#index)

Worker termination is now a transaction rather than an early registry removal.
Scheduler, dispatcher, guardian, notifier, and janitor workers remain in a
`RETIRING` state until their exact permit has been returned or durably retained.
Release-guardian owners also have explicit lifecycle states, so shutdown never
publishes an owner already executing its release capability.

Availability notification is level-triggered and acknowledged. Registration
after capacity has already returned schedules an immediate wakeup, while a
rearm during callback execution produces one coalesced successor. A notifier
that reaches its hard close deadline parks remaining events and cannot execute
subsystem code afterward.

A descriptor whose close result is uncertain is removed from use but not from
accounting. Its ledger-backed FD lease becomes bounded process-lifetime debt;
the potentially recycled descriptor number is never retried. Integral runtime
diagnostics expose that debt and all retiring workers.

Native arena shutdown now quiesces producers before stopping reaper consumers.
Parking is promoted autonomously and terminal ownership is explicit, bounded,
observable, and incompatible with successful process shutdown.
