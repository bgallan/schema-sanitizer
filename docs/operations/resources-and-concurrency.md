# Resources and concurrency

Schema-Sanitizer governs resident memory, temporary storage, threads, file
descriptors, remote work, and cleanup as one operation-scoped resource model.
Every conversion has a fixed operation budget, while overlapping conversions
also compete for process-wide capacity. Two individually valid calls therefore
cannot each assume that they own the whole host, container, filesystem, or
remote connection pool.

This document describes the supported operational behavior. The implementation
rules that make cleanup and cross-thread ownership safe are documented in
[Concurrency lifecycle invariants](../internals/concurrency-lifecycle.md).

## Index

- [Operator controls](#operator-controls)
- [Resident-memory hierarchy](#resident-memory-hierarchy)
- [Charged resident-memory domains](#charged-resident-memory-domains)
- [Caller-owned and untracked memory](#caller-owned-and-untracked-memory)
- [Temporary storage](#temporary-storage)
- [Threads and file descriptors](#threads-and-file-descriptors)
- [Remote work](#remote-work)
- [Pressure feedback and safety margins](#pressure-feedback-and-safety-margins)
- [Cancellation, deadlines, and fork safety](#cancellation-deadlines-and-fork-safety)
- [Observability and shutdown](#observability-and-shutdown)
- [Operational invariants](#operational-invariants)
- [Validation](#validation)

## [Operator controls](#index)

The public resource options are intentionally small:

| Control | Behavior |
|---|---|
| `memory_limit_bytes` | Positive resident-memory budget for one operation. `None` derives a safe value from the current host or container allowance. The resolved value is fixed when the operation starts. |
| `multi_threading` | Enables governed parallel execution. It does not bypass any process-wide memory, thread, descriptor, or remote-work limit. |

They can be passed directly to a converter or grouped in
`ResourceOptions`; see the [option reference](../reference/options.md).

Deployments may impose stricter process controls with environment variables:

| Variable | Purpose |
|---|---|
| `SCHEMA_SANITIZER_MAX_PROJECT_THREADS` | Upper bound for project-owned physical threads. Live process-thread, cgroup PID, OS, and memory headroom may reduce it further; CPU availability separately limits runnable credits. |
| `SCHEMA_SANITIZER_MAX_OPEN_FILES` | Upper bound for governed file-descriptor reservations. OS limits and observed external descriptors may reduce it further. |
| `SCHEMA_SANITIZER_THREAD_STACK_RESERVATION_BYTES` | Conservative per-thread stack charge used when deriving safe physical-thread capacity. |
| `SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS=1` | Enables host-local resident-memory coordination between worker processes on supported POSIX systems. |
| `SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS=1` | Enables host-local temporary-storage coordination between worker processes on supported POSIX systems. |
| `SCHEMA_SANITIZER_COORDINATION_DIR` | Selects the host-local directory shared by cross-process ledgers and optional telemetry. |
| `SCHEMA_SANITIZER_TELEMETRY_TUNING=1` | Enables bounded, host-local safety-margin tuning. Persisted samples never weaken an explicit limit. |
| `SCHEMA_SANITIZER_MALLOC_TRIM` | Selects allocator page-return behavior on supported glibc Linux hosts; the default is `auto`. |

Cross-process options require all cooperating processes to use the same
host-local coordination directory. They complement the exact in-process
governors; they do not replace them.

## [Resident-memory hierarchy](#index)

`memory_limit_bytes` is the single public resident-memory input for an
operation. The resolved limit backs one native atomic operation ledger shared
by Python staging code and the native reader, inference, materialization, and
writer paths.

Each scalable allocation is also charged to an exact process-wide resident
pool. Concurrent operations cannot each spend the complete host allowance.
When a refreshed host or cgroup observation lowers process capacity, existing
owners remain valid but new growth waits or fails until usage drains. A later
increase can admit new work; it never rewrites the fixed per-operation limit.

Reservations are made before the corresponding scalable allocation or blocking
read. Retained leases are thread-safe, can be resized, and remain attached to
the object or stage that owns the bytes. Stage handoff transfers the existing
credit instead of releasing and reacquiring it through an unaccounted gap.

The ledger is not an RSS meter. Interpreter state, stacks, allocators, SDKs,
TLS, page cache, and fragmentation contribute to process RSS without being
exactly attributable to one operation. Automatic sizing leaves a safety reserve
for those domains.

## [Charged resident-memory domains](#index)

| Domain | Accounting behavior |
|---|---|
| Native parsing, inference, materialization, writing, reorder windows, Arrow construction, and workers | The real upstream allocation size, including allocator metadata, alignment, and guards, is reserved before allocation and released on every failure or free path. |
| Materialized text, bytes, and memory-view input | A conservative lifetime lease is acquired before native execution and retained until the owning result, stream, or operation closes. |
| Source and directory metadata | URIs, file records, associations, plans, and retained wrappers are charged before publication. Escapable file objects keep the metadata owner alive. |
| Remote control responses | Source bytes, transient immutable or Unicode copies, and retained response wrappers are admitted before materialization. |
| Download and upload windows | HTTP, S3, GCS, and Azure reads, writes, chunks, and multipart manifests retain bounded leases while their bytes are live. |
| Async and ordered results | Expected bytes are admitted before work when known, reconciled before queue publication, and retained until consumption or cleanup. |
| Partition and schema-registry workflows | Child contexts share the root workflow ledger, including discovery metadata, lookahead, temporary staging, and remote work. |

Format-specific sublimits may reject work earlier, but they never enlarge the
operation-wide limit. Temporary-file contents use the separate disk hierarchy;
their in-memory transfer windows remain resident-memory charges.

## [Caller-owned and untracked memory](#index)

The following memory is deliberately outside exact operation accounting:

- interpreter and import machinery, garbage-collector metadata, thread stacks,
  allocator bookkeeping, and opaque third-party SDK internals;
- caller objects and provider clients created before the operation, although an
  input text or byte object retained by an operation receives a conservative
  equivalent lease;
- filesystem page cache and bytes stored in temporary files;
- bounded option, path, exception, and privacy-safe diagnostic objects whose
  size is independent of hostile input cardinality;
- eager analytical results after ownership transfers to PyArrow, pandas,
  Polars, or another Arrow consumer. A lazy DuckDB relation is the exception
  described below.

Yielded batches and analytical results become caller-owned after transfer.
Keeping many of them alive can therefore grow RSS beyond the completed
operation's current ledger even though the conversion itself respected its
budget. A lazy DuckDB relation is backed by a private connection proxy: the
relation value is caller-owned, while its upstream conversion chain remains
governed until the final related proxy closes. See the
[Result lifetime contract](../reference/python-api.md#result-lifetime-and-duckdb).

## [Temporary storage](#index)

On-disk bytes are governed separately from resident memory. Each operation has
a spool ceiling, and all operations share per-filesystem process governors with
an emergency free-space reserve. Admission accounts for bytes and artifact
counts, preventing workloads of tiny files from exhausting inodes while staying
under the byte limit.

Staged artifacts reserve before creation, grow before each write, reconcile to
their retained size, and migrate authority when a lease moves to another
filesystem. A failed retry truncation returns attempt-only growth. Unrelated
processes consuming the same filesystem lower new admission through refreshed
free-space observations.

Deletion is part of ownership. If `unlink` or `rmtree` fails, the storage lease
is not returned. The artifact moves, when possible, to a host-local quarantine;
a bounded janitor retries deletion and returns capacity only after absence is
confirmed. Janitor admission, discovery, and shutdown are bounded.

With `SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS=1`, supported POSIX
workers additionally coordinate through a `flock`-protected, bounded registry.
PID start tokens prevent PID reuse from inheriting stale reservations, and dead
owners can be reclaimed without trusting unauthenticated amount-only releases.

## [Threads and file descriptors](#index)

Project-owned Python hosts, native workers, cleanup workers, provider pools, and
integrated external runtimes share a physical-thread envelope. Runnable CPU
capacity is a separate dynamic resource: a pool may retain parked workers while
cgroup or affinity changes reduce how many may run concurrently.

Physical-thread capacity considers configured policy, observed process threads,
cgroup PID headroom, OS limits, and conservative stack memory. Runnable
capacity separately considers CPU availability, affinity, and cgroup CPU
limits. External runtime pools are charged without assuming that a configured
width is proof of a matching resident thread identity.

CPU admission keeps process affinity live while amortizing Linux cgroup
hierarchy discovery over a bounded 250 ms sampling interval. Refreshed capacity
is published before the governor admission mutex is acquired, so procfs and
cgroup file reads never sit inside the per-task FIFO critical section. Waiting
admissions periodically refresh the sample: quota increases wake progress and
quota reductions stop new leases after already admitted work drains. The cache
itself contains no process-shared lock and invalidates inherited observations
by process identity. This is defensive containment only; the initialized
runtime remains unsupported after `fork()` as described below.

File-descriptor admission covers local inputs and outputs, remote sockets,
provider sessions, temporary files, coordination journals, directory scans,
and native readers and writers. Reservations and physically open descriptors
are tracked separately. Physical close must complete before the corresponding
open state and reservation can be returned. A descriptor whose close result is
uncertain is removed from use and retained as bounded terminal debt; its number
is never retried after it may have been recycled by the OS.

Thread and descriptor waits are bounded and cancellation-aware. A live capacity
shrink rejects a queued request that has become impossible, allowing smaller
feasible requests behind it to progress.

## [Remote work](#index)

Synchronous and asynchronous transfers share one weighted process-wide remote
governor. Weight derives from bounded I/O work, while file-descriptor footprints
track network and local-file needs independently. Admission occurs before work
is submitted to an event loop, so a fast producer cannot create an unbounded
future backlog.

The queue is work-conserving and starvation-bounded: a small request may bypass
a blocked large request only a bounded number of times. Provider sessions are
created single-flight per bounded endpoint identity. Entries remain owned until
physical client or context-manager cleanup succeeds; a resource that finishes
construction after pool shutdown closes itself instead of becoming orphaned.

Provider throttling coordinates retries across operations with bounded AIMD
windows, circuit state, `Retry-After` handling, and one mutable expiry entry per
live endpoint. In-flight or cooling-down entries cannot be evicted. If every
entry is protected, a new endpoint is rejected instead of growing the registry.

Every synchronous bridge has a finite transport or operation deadline. Closing
a remote coordinator uses one monotonic deadline for cancellation, provider
cleanup, and host-thread join. Cancellation-resistant work remains explicit
terminal ownership rather than being silently detached.

## [Pressure feedback and safety margins](#index)

On Linux, automatic sizing and adaptive concurrency use the process's resolved
cgroup hierarchy rather than assuming the cgroup mount root. Every visible
constraining ancestor participates in memory, CPU, and PID limits. Unknown,
truncated, migrating, malformed, or incomplete observations fail closed for new
capacity; an explicit unbounded controller state remains distinct from an
unreadable one.

Memory pressure consumes cgroup current/high/max values, memory events, and PSI
when available. Capacity falls quickly under pressure and recovers in bounded
steps after a cooldown. These signals alter new admission, not existing exact
ownership.

Telemetry tuning is opt-in. It stores only a bounded recent sample set in the
coordination directory and derives conservative safety floors within hard
bounds. Malformed profiles are ignored, static defaults remain lower bounds,
and telemetry cannot relax a configured operation or process limit.

On supported glibc Linux systems, allocator trimming may return retained pages
after a large operation when pressure and RSS observations justify it. Trimming
is rate-limited and never substitutes for exact reservation accounting.

## [Cancellation, deadlines, and fork safety](#index)

A public cancellation scope propagates one event and monotonic deadline through
native work, worker threads, remote queues, retry backoff, and staging:

```python
import schema_sanitizer as ss

with ss.operation_cancellation(timeout_seconds=120):
    ss.to_jsonl("input.csv", "output.jsonl", input_format="csv")
```

Nested scopes inherit parent cancellation and the earliest parent or child
deadline. Leaving a scope cancels its token so detached work cannot silently
outlive the operation. Cancellation is never classified as a retryable provider
failure.

An initialized runtime is deliberately not reusable after `fork()`. A child
fails fast rather than touching mutexes, ledgers, event loops, providers, or
receipts inherited from vanished parent threads. Multiprocessing integrations
must use `spawn`, `forkserver`, or a fresh executable image.

## [Observability and shutdown](#index)

`schema_sanitizer.process_operation_diagnostics()` returns immutable copies of
live and recently completed operation records. An optional operation ID filters
the bounded ring. Records include operation memory and temporary-storage peaks
plus process snapshots for threads, descriptors, filesystem bytes and
artifacts, quarantine backlog, pressure, and weighted remote work when present.
Provider diagnostics expose bounded endpoint usage, circuits, evictions, and
saturation rejection without retaining raw payload identities.

Diagnostic snapshotters are weakly held and best-effort: observing a resource
must not prolong its lifetime or turn a committed release into a failed public
operation. Internal modules expose more detailed resident, pressure, remote,
and integral-runtime snapshots for tests and embedders, but modules ending in
`_impl` are not public compatibility API.

Runtime shutdown is single-flight and deadline-bounded. It closes admission and
producers before cleanup consumers, repeatedly drains finalizer domains, and
requires stable activity epochs. Cleanup that cannot complete by the deadline
stays reachable as bounded, observable ownership; shutdown cannot report clean
quiescence while a protocol violation, uncertain descriptor, retiring worker,
or terminal owner remains.

## [Operational invariants](#index)

- Every charged allocation, staged artifact, physical worker, descriptor, and
  remote request has an operation or process owner before use.
- Concurrent operations share process capacity; an operation-local limit never
  enlarges it.
- Reservation happens before scalable allocation, blocking I/O, publication,
  or physical resource use.
- Ordered results, diagnostics, and failures still commit by source ordinal.
- Resource transfer changes authority without an uncharged release/reacquire
  interval.
- Cleanup may fail explicitly at a deadline, but it may not wait forever or
  return capacity before physical ownership has ended.
- Successful, failed, cancelled, abandoned, and construction-failure paths
  converge to baseline counters or explicit conservative terminal debt.
- Unknown host state reduces new admission; it never manufactures headroom.

## [Validation](#index)

Resource behavior is covered by deterministic race and fault-injection tests,
format-route contract checks, remote failure tests, cross-process tests, and
native repeated-concurrency probes under ASan/UBSan and ThreadSanitizer. New
resource owners must cover construction failure, normal completion,
cancellation, explicit close, abandoned finalization, and process-boundary
rejection.

The exact implementation acceptance rules live in the
[validation contract](../internals/concurrency-lifecycle.md#validation-contract).
