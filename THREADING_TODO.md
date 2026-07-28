# Deterministic threading architecture TODO

Status: complete; retained as the deterministic concurrency design record.

The first implementation slice is now present:

- all seven public converters accept `threading_mode`, defaulting to `single`;
- one native-derived immutable execution policy reports effective workers,
  queues, reorder capacity, worker arenas, remote windows, PyArrow threading,
  and any fallback-to-one-worker reason;
- single mode forces every Schema-Sanitizer-owned concurrency limit to one,
  disables PyArrow threads, stages remote chunks inline, and uses blocking
  same-thread DNS/HTTP/GCS/S3/Azure operations without an event loop, transfer
  manager, or helper-thread escape hatch;
- multi mode applies bounded policy-driven concurrency to remote discovery,
  transfer/staging, source-plan prefetch, and supported PyArrow fallbacks; and
- initial cross-mode tests compare logical JSONL output and registry state and
  prove the single prefetch/sync paths do not construct a project executor;
- a native ordinal executor now provides a strict inline path for `single` and a
  bounded `std::jthread` pool, bounded queues, ordered reorder slots,
  cancellation, and earliest-ordinal failure semantics for `multi`; and
- one operation-wide native task arena now owns the complete N-worker CPU
  budget for inference, materialization, CSV/JSONL fragment preparation, and
  native Parquet column preparation. Stages reuse stable physical workers,
  narrow upstream/output lanes are complementary, and workers start lazily on
  first use; no stage can multiply the operation worker ceiling; and
- lane-safe work stealing now prevents a slow packet from pinning later work to
  one physical queue. Idle workers may take only tasks whose lane includes that
  physical worker, so worker-private parser/builder/compression slots remain
  unique, the N-worker ceiling is unchanged, and ordered executors still own
  canonical commit and earliest-failure order. Wakeups remain targeted unless a
  task is queued behind an active worker; and
- arena submission no longer serializes on one global mutex. Per-worker lazy
  startup locks, atomic lane cursors, a non-empty queue bitmap, deepest-compatible
  victim selection, and power-of-two placement reduce contention under concurrent
  producers and skewed packet cost. Backlog-driven admission reuses a compatible
  started idle helper before creating another native thread: sequential tasks stay
  on one helper, while blocked bursts still expand safely to the N-worker ceiling;
  and
- arena task-admission totals are now published per physical worker instead of
  through one operation-global RMW. Producers targeting the same queue are
  already serialized by that queue's mutex, so they update its exact submitted
  shard with load/store; diagnostics sum at most 32 shards. The strict inline
  path retains one uncontended counter, and queue placement, stealing, order,
  cancellation, and worker limits are unchanged; and
- external-task lifetime shards now encode the shutdown waiter in the high bit
  of the same atomic completion counter. Normal tasks publish one completion RMW
  without a futile notification; once destruction installs the waiter bit, the
  returned pre-increment value triggers the exact wake. The single-atomic total
  order makes the drain lost-wakeup-proof for all arena-backed ordered stages; and
- arena-backed ordered stages now own a stage-local cancellation domain.
  Cancelling one stage promptly stops its active packets without shutting down
  the shared arena. Arena tasks use C++23 `std::move_only_function` plus a
  move-only completion lease, so normal completion and queue abandonment both
  publish the executor's final access without an extra shared allocation;
  result publication wakes the coordinator only when the next canonical ordinal
  becomes available, and the bounded dispatch window removes the need for a
  second result-space condition variable; and
- hardened allocation ownership registries are split into sixteen address
  shards. Allocation/free safety checks, guard validation, and the aggregate
  retained-bucket ceiling are unchanged, while workers no longer serialize on
  one process-wide or operation-wide registry mutex; and
- one shared stage-policy narrowing helper preserves the aggregate worker-arena
  budget while reducing queue/reorder windows for stages that cannot amortize
  every host worker. CSV encoding uses at most four output workers, but upstream
  inference/materialization may still consume the complete operation arena; and
- native materialization prepares memory-accounted contiguous row packets in
  private worker arenas while one coordinator performs diagnostics merge and
  Arrow append in source order. Nested JSON rows are reparsed in worker-private
  documents to avoid sharing mutable on-demand parser arenas; and
- packet rows and target bytes are derived from the operation budget, oversized
  text or nested rows are isolated, and a plan-complexity policy narrows cheap
  scalar stages to fewer workers while allowing nested/wide plans to consume
  more of the safe host policy.
- stage admission now also accounts for observed work shape. Wide flat plans with
  at least 24 columns reuse four upstream workers for both materialization and
  JSONL encoding instead of opening a second lane. Moderately nested plans use
  four builders; nested JSONL begins on those same workers and promotes
  geometrically only after accumulated packet work proves the stream is
  sustained. A newly admitted arena worker must claim the first task that
  justified its creation before it may steal compatible work, preventing an
  orphaned physical queue and compensating thread startup; and
- JSON object member literals are compiled once per struct as contiguous
  `"name":` / `,"name":` prefixes. Root and nested encoders reuse them for every
  row, and the packet estimator uses their exact escaped sizes instead of a
  six-times-name upper bound. This removes repeated escaping and avoids
  financing workers with bytes that will never be emitted; and
- CSV and JSON parser arenas derive their initial block from the operation I/O
  chunk. A 1 MiB public budget therefore starts with 16 KiB parser blocks rather
  than attempting a 1 MiB allocation plus allocator overhead. Normal 512 MiB
  operations retain the 1 MiB fast path, while low-memory multi requests safely
  fall back to one worker and complete without false out-of-memory failures; and
- ordered async scheduling now stores failures until their source ordinal is
  reached, provider bulk discovery uses fixed worker tasks instead of unbounded
  `asyncio.gather`, and source-plan discovery reports the earliest canonical
  failure; and
- multi-mode remote chunk staging now owns one event-loop host thread per staged
  iterator, one reusable provider client, one global transfer semaphore, and a
  drained cancellation domain. Probe chunks are retained under the shared
  policy and transferred to the streaming provider as a bounded prefix. The
  streaming manifest resumes at the exact retained file count, so retained files
  are neither downloaded twice nor emitted twice; and
- public file conversions, analytical conversions, direct execution-context URI
  inputs, and multi-source registry warm-up now create one lazy remote operation
  context. In `multi`, initial listing, single-file staging, lazy directory
  packets, and final remote output upload share one event-loop host for the
  complete public operation. Local-only calls do not create that host. Remote
  directory packets are bounded by both file count and known bytes derived from
  `memory_limit_bytes`; oversized files are isolated without changing source
  order; and
- native CSV and JSONL output now use the same ordinal executor as
  materialization. Workers encode immutable row ranges into private,
  byte-accounted fragments; one coordinator owns headers, physical byte order,
  statistics, final flush, and error publication. The reorder window retains at
  most one fragment per effective output worker, oversized rows are isolated,
  and `single` remains a strict inline path that creates no native thread; and
- text output admission is now based on packet work observed from the first
  non-empty Arrow batch. Tiny outputs stay inline and create no arena helper; a
  later larger batch can promote the executor after draining its previous ordinal
  domain, reusing operation-owned workers while preserving byte order, bounded
  memory, and earliest-failure semantics; and
- local CSV, JSONL, native Parquet, and PyArrow Parquet path outputs publish
  through unique sibling files and `os.replace`. Existing destinations remain
  untouched until writer close succeeds; failures remove only partial staging
  files. Invalid threading modes are rejected before staging is created; and
- adaptive native inference now reparses nested JSON rows in worker-private
  documents and emits compact preorder evidence packets. One coordinator
  validates and reduces those packets through the existing shape-then-statistics
  semantics in row order. Flat/small or low-memory batches stay on the serial
  scanner; parallel inference is capped at eight workers, packet evidence is
  charged to tracked arenas, and oversized rows are scanned inline only after
  earlier ordinals drain; and
- native Parquet output now prepares independent leaf-column page, encoding, and
  compression artifacts through the ordinal executor when schema width, row
  count, and memory reserve can amortize the pool. One coordinator assigns
  physical offsets, commits column chunks and page indexes in schema order, and
  writes the footer/trailer once; narrow, small, or low-memory groups stay
  serial; and
- `OperationExecutionContext` now owns a temporary-storage permit pool derived
  from the same memory budget. Remote source packets reserve known/estimated
  bytes before prefetch, resize to exact staged size, and hold the lease until
  consumption or cancellation. Final remote-output spools hold an exact lease
  through upload, and failure/cancellation regressions prove permits drain.

Operation-wide provider-session pooling and provider-specific large-object
publication are now implemented. Generic HTTP transport now has real-socket
fault injection on every supported wheel platform, bounded transient retries,
replay-safe PUT restart from byte zero, immediate cancellation, and complete
staging cleanup for fatal interruption paths. GCC 14 ThreadSanitizer covers both
the focused ordinal executor and a repeated 64-test ABI3 differential suite on
Linux, including fixed-clock public-operation goldens and the fully instrumented
bundled zlib used by Parquet GZIP. The strict synchronous remote backend is
now implemented and tested through real loopback DNS/HTTP plus blocking
GCS/S3/Azure SDK doubles. Bounded one-partition source lookahead is now
implemented for static `multi` pipelines. Native parser fuzzing now reuses the
same four entry points under libFuzzer or a deterministic standalone engine,
runs under Linux TSan/ASan-UBSan, and has supported native sanitizer/concurrency
lanes on Windows and both macOS architectures. The benchmark matrix now covers
width, nesting, source count, compression, memory, CPU quotas, all wheel
platforms, and explicit S3/GCS/Azure emulators.
Packet coalescing
amortizes queue/reorder synchronization for materialization and text output,
while very cheap or very small workloads can still favor the inline executor.
`single` therefore remains the default.

The single-threaded engine is the correctness reference. The multi-threaded
engine may change latency and resource use, but it must not change observable
data behavior. Given the same input bytes, source ordering, options, initial
schema registry, and explicit run metadata, both modes must produce:

- the same Arrow schema, field order, nullability, rows, row order, and values;
- the same canonical schema registry, generation, variants, and ordered schema
  drift records;
- the same `on_error` decisions and the same earliest source-order failure for
  `stop`;
- the same non-timing diagnostic counters and logically equivalent CSV, JSONL,
  and Parquet output.

File bytes are not the compatibility boundary because container metadata and
compression libraries may encode equivalent data differently. Generated UTC
metadata must be captured once per operation, before work is scheduled, so it
cannot depend on worker completion order. Equivalence tests will inject a fixed
clock; wall time, CPU time, thread counts, and scheduling telemetry are expected
to differ.

## One algorithm, two executors

The public control should be `threading_mode="single" | "multi"`. The first
certified release should default to `single`; changing the default to `multi`
requires all equivalence gates below to pass on every supported platform. There
should be no public worker-count knob: the effective worker count and every
queue/reorder limit are derived from `memory_limit_bytes`, available CPUs, and
hard internal ceilings. A small budget may reduce a requested multi-threaded
run to one effective worker without changing its result.

Both modes must use the same framing, task, reducer, materializer, and sink
implementations. Only the executor changes:

```text
canonical source plan
    -> ordered incremental framer
    -> ordinal work packets (source, first row, batch)
    -> inline executor OR bounded worker pool
    -> bounded ordinal reorder buffer
    -> ordered inference reducer
    -> frozen registry + compiled materialization plan
    -> inline executor OR bounded worker pool
    -> bounded ordinal reorder buffer
    -> single ordered sink/registry commit
```

The inline executor runs each packet immediately and is the single-threaded
oracle. The pool executor runs packets concurrently, but the coordinator exposes
completed packets only in ordinal order. Batch boundaries are determined before
dispatch and therefore do not depend on worker timing.

## Ordered stages and allowed parallelism

1. **Source planning and framing stay ordered.** Directory children retain
   deterministic filename order. Incremental CSV, JSON, and XML scanners own
   cross-chunk state and emit only complete-record packets with stable source
   and row ordinals.
1. **Inference can compute local evidence in parallel.** Workers return
   immutable, packet-local shape/type evidence. The existing registry/inference
   reducer consumes that evidence in ordinal order, so type promotion, collision
   suffixes, field versions, and drift order match the inline executor.
1. **The materialization plan is frozen before parallel materialization.** Each
   worker receives the same immutable compiled plan, owns a private memory arena
   and diagnostics delta, and returns one Arrow batch tagged with its ordinal.
1. **Commit is single and ordered.** One coordinator merges diagnostics,
   publishes Arrow batches, updates the registry result, and writes CSV, JSONL,
   or Parquet. Workers never mutate a shared builder, registry, diagnostics
   object, or output stream.
1. **Partition pipelines remain sequential.** In additive mode, partition
   `N + 1` depends on the registry returned by partition `N`. Strict mode will
   initially use the same partition loop. Parallelism belongs inside a
   partition; cross-partition parallel writes require a separate design.

Remote discovery/download remains asynchronous in multi mode, but results are
delivered to the source plan in canonical order. Bulk provider groups are fed
through fixed scheduler workers, and staged chunks share one event loop, one
provider client, and one operation-local transfer semaphore instead of nesting
a chunk thread pool around independent async schedulers. Single mode uses a
window of one, no project-owned remote coordinator or `ThreadPoolExecutor`,
native worker count one, and `use_threads=False` for PyArrow fallbacks. Its
remote path uses blocking stdlib HTTP, direct Botocore, synchronous GCS JSON API
and ADC, and synchronous Azure Blob calls with SDK concurrency fixed to one.

The remote coordinator lifetime is now the complete public operation for public
file/analytical conversion, direct execution-context URI staging, and registry
warm-up. Initial listing, probe, stream, and output upload reuse that event-loop
host in `multi`. Compatible aiohttp sessions, entered S3 clients, and Azure
service clients are pooled on that host and close exactly once after submitted
work drains; incompatible HTTP headers and Azure accounts remain isolated.
Remote packets are bounded by both file count and known bytes. Remote source
packets and final output spools use operation-owned temporary-disk permits.
Strict remote `single` execution now performs resolver and provider calls on the
caller thread and rejects accidental coroutine submission at the operation
context boundary.

## Whole-pipeline concurrency model

The operation must be treated as a bounded staged graph rather than a set of
independent pools:

```text
canonical source inventory
    -> ordered source packet prefetch/read/decompress
    -> ordered framing and optional parallel inference evidence
    -> frozen materialization plan
    -> parallel materialization packets
    -> ordered output packet preparation
    -> single ordered local commit
    -> optional remote multipart/atomic publish
```

One operation context owns the immutable policy, cancellation domain, source and
output ordinals, remote event loop, native CPU executor, memory permits, and
temporary-storage permits. A stage may borrow capacity from the shared policy;
it must not independently multiply the host worker count. The source frontend,
registry reducer, diagnostic merge, output byte order, footer/trailer commit, and
final object publication remain single-owner and ordered.

Current integration uses one operation-wide native CPU arena for inference,
materialization, ordered CSV/JSONL preparation, and native Parquet leaf-column
preparation. Arrow C stream sidecars carry the arena across metadata, coalescing,
nested CSV, registry, path, and compatible multi-source wrappers. Remote I/O
continues on its single operation event-loop host, and temporary-memory/disk
permits remain shared. This separates the CPU worker ceiling from asynchronous
I/O concurrency without multiplying either budget.

Current integration also covers remote inventory, permit-bounded download prefetch,
native inference/materialization, ordered CSV/JSONL fragment preparation,
concurrent native Parquet leaf-column preparation with ordered physical commit,
and final permit-bounded publication under one operation lifetime. Large S3
spools use ordered multipart completion, GCS spools use resumable committed-offset
reconciliation, and Azure block uploads receive a policy-bounded SDK concurrency
window. The writers pull from a bounded native
stream, so source download and materialization can overlap with downstream
consumption. CSV/JSONL workers now encode immutable row packets into private
bounded byte fragments; the coordinator flushes them in ordinal order while
retaining sole ownership of headers, row order, escaping policy, error
publication, statistics, and final flush. The remaining safe output slices are:

1. Remote output overlap: provider-specific multipart/resumable publication now
   consumes a completed, permit-held local spool with ordered completion and
   abort-on-failure semantics. A later optimization may stream already committed
   local writer parts while following parts are produced, but only if the writer
   exposes immutable boundaries and the complete-spool route remains the
   correctness fallback.
1. Partition pipelines: registry dependencies keep partition commits ordered.
   Static `multi` pipelines now admit exactly one immutable prepared source for
   partition `N + 1` while `N` converts or publishes. The child contexts share
   the operation permit pool and remote coordinator but keep distinct timestamps.
   Inference, registry mutation, callbacks, and output publication remain strictly
   ordinal; later preparation failures are retained until their source ordinal.
   Callable per-partition options and `single` remain fully sequential.

Local file read-ahead should use bounded source packets and the same operation
permits. It should not add a second general-purpose worker pool. Compressed
single streams that are inherently sequential stay on the ordered frontend;
independent files or splittable blocks may be prefetched in `multi`.

## Measured single-versus-multi crossover

The checked-in benchmark harness is `benchmarks/bench_threading_modes.py`. It
verifies byte-identical direct CSV/JSONL output and logical row equivalence for
full public conversions after removing operation-generated UTC timestamps.
Measurements on 2026-07-17 used Linux, Python 3.13.5, 56 logical CPUs, a 256 MiB
operation budget, one warm-up, and median-of-three timings for the 120,000-row
case:

| Case | `single` | `multi` | Speedup |
|---|---:|---:|---:|
| CSV output, scalar rows | 0.195 s | 0.072 s | 2.72x |
| JSONL output, scalar rows | 0.188 s | 0.031 s | 6.07x |
| CSV output, nested rows | 0.102 s | 0.044 s | 2.32x |
| JSONL output, nested rows | 0.035 s | 0.010 s | 3.54x |
| Full JSONL-to-CSV pipeline | 1.579 s | 1.060 s | 1.49x |
| Full JSONL-to-JSONL pipeline | 1.573 s | 1.103 s | 1.43x |

A focused native-Parquet matrix uses logical schema/row equality because
physical row-group boundaries and compression artifacts are not the compatibility
boundary. On the same 56-CPU/256-MiB host, a 30,000-row Snappy median measured:

| Case | `single` | `multi` | Speedup |
|---|---:|---:|---:|
| Parquet output, 4 scalar leaves | 0.072 s | 0.070 s | 1.03x |
| Parquet output, 16 scalar leaves | 0.223 s | 0.115 s | 1.93x |
| Nested JSONL-to-Parquet pipeline | 0.304 s | 0.230 s | 1.32x |

A separate 120,000-row, 16-column isolated matrix measured 1.64x for
uncompressed, 1.53x for Snappy, and 2.57x for GZIP. The adaptive writer keeps
small nested groups and narrow scalar groups serial, so `multi` does not launch
column workers merely because it was requested.

The dedicated partition-lookahead harness, `benchmarks/bench_partition_lookahead.py`,
uses a loopback HTTP source with controlled latency and rejects logical output
differences before reporting timings. A 2026-07-19 median-of-three run with six
partitions, 5,000 rows per partition, 40 ms source latency, a 128 MiB operation
budget, and one warm-up measured 0.705 s for `single`, 0.724 s for deliberately
sequential `multi`, and 0.500 s for static `multi` with one-partition lookahead.
That is 1.41x versus `single` and 1.45x versus sequential `multi` on this controlled
workload. It isolates overlap value rather than defining a production threshold.

The fixed cost is material at small sizes. At 100 scalar rows, `multi` was
approximately 3.4x-4.3x slower for isolated scalar text output and 1.4x slower
for the full pipeline. Around 1,000 rows results were mixed and sensitive to
noise, identifying the crossover region rather than a stable threshold. At
10,000 rows, full-pipeline speedup was approximately 1.53x-1.66x and isolated
scalar output was 1.97x-3.89x faster. These figures characterize this host and workload;
they are not a universal threshold or service-level guarantee. Keep `single` as
the correctness/default mode and select `multi` for sufficiently substantial
CPU, escaping, nesting, source, compression, or network work.

## Memory, backpressure, and failure

The operation budget is divided before workers start: fixed reader/writer and
reorder reserves are subtracted first, then the remaining parallel pool is
divided into per-worker arenas. Input and output queues are bounded. A fast
worker must block when the reorder window is full, so a slow early packet cannot
cause unbounded retention of later batches. No worker may treat the full
operation memory limit as its private allowance.

Each worker returns either a value or a structured failure carrying its ordinal.
The coordinator commits successful earlier packets and reports the lowest
failing ordinal, regardless of which worker failed first. It then cancels later
work, drains futures, releases Arrow callbacks and arenas, and removes temporary
outputs. `skip_row` and `emit_null_row` decisions remain packet-local but their
diagnostic deltas are merged in ordinal order.

## Implementation checklist

- [x] Add cross-mode golden helpers that compare schema, ordered rows, canonical
  registry JSON, drift JSON, diagnostics, exceptions, and logical file contents.
  `tests/threading_golden.py` now provides one reusable logical comparison layer
  for analytical results and CSV/JSONL/Parquet outputs. Drift clocks are compared
  exactly because every operation now supplies one explicit native timestamp.

- [x] Build a fixed-clock equivalence matrix for CSV, JSON, JSON array, JSONL,
  NDJSON, XML, native/fallback Parquet, directory inputs, nested shapes, schema
  versions, all error policies, strict/additive registries, and warm-up flows.
  The 21-case matrix covers every local text frontend, native and forced-fallback
  Parquet input/output, CSV/JSONL output, deterministic directory discovery,
  recoverable `skip_row`/`emit_null_row`, earliest `stop` failure, strict registry
  canonical-baseline validation and cleanup, additive generation/drift, nested
  version creation,
  reusable native registry state across operation clocks, and multi-partition
  native warm-up state. One operation clock is captured before scheduling and is
  reused for ingestion metadata and every native drift event.

- [x] Add `threading_mode` to the native option catalog, public signatures,
  Python normalization, serialized option contract, README, examples, and API
  option matrices.

- [x] Introduce one immutable execution policy derived from `threading_mode`,
  `memory_limit_bytes`, CPU availability, and hard ceilings; report requested
  mode, effective workers, queue bounds, and fallback-to-one-worker reason.

- [x] Make remote discovery, remote staging, source-plan prefetch, and PyArrow
  fallback threading consume that shared policy. Prove single mode creates no
  project-owned worker pool.

- [x] Remove multiplicative remote concurrency: use fixed workers for provider
  discovery groups and one event-loop thread, reusable provider client, global
  transfer semaphore, ordinal failures, cancellation drain, and temp cleanup for
  each multi-mode staged iterator.

- [x] Reuse a policy-bounded prefix of probe staging in the paired streaming
  provider, advance the remaining manifest by the exact retained file count, and
  cover multi-chunk no-duplicate/no-gap behavior with a regression test.

- [x] Introduce `OperationExecutionContext` as the whole public-operation owner
  for file/analytical calls, direct execution-context URI input, and registry
  warm-up. It captures the immutable policy once; initial directory listing,
  lazy staging, and remote output upload use one lazy event-loop host, and a
  regression proves source preparation and output publication receive the same
  context instance. Remote packets are bounded by file count and known bytes.

- [x] Add operation-owned temporary-disk permits for remote source packets and
  final remote-output spools. Reserve known or estimated bytes before prefetch,
  resize to exact staged size, hold leases through consumption/upload, reject
  aggregate or filesystem exhaustion with structured errors, and prove release
  after success, failure, cancellation, and repeated close.

- [x] Add operation-lifetime provider-session pooling for `multi`. Reuse one
  compatible aiohttp connector/session, entered S3 client manager, and Azure
  service client per operation/event loop; separate incompatible HTTP headers
  and Azure accounts; drain submitted work before closing providers exactly
  once. `single` bypasses this pool completely.

- [x] Add a synchronous remote backend for strict one-host-thread `single`
  execution, including DNS and provider SDK calls. HTTP uses blocking stdlib
  connections and same-thread resolution; GCS uses the JSON API with synchronous
  ADC; S3 uses direct Botocore with one connection and no transfer manager;
  Azure uses the synchronous SDK with `max_concurrency=1`. Directory packets are
  serial and reuse one blocking provider handle. Real loopback tests reject any
  client-side `Thread.start`, record DNS on the caller thread, and provider
  doubles assert same-thread execution. An exact-byte S3 regression prevents
  duplicate chunk writes.

- [x] Add native ordinal packet, inline executor, bounded worker-pool, reorder
  buffer, cancellation, and per-worker memory-arena primitives below the ABI3
  layer. The inline path creates no native thread; the pool uses bounded
  `std::jthread` workers and ordered delivery. Native conversion already runs
  behind the existing GIL-releasing ABI boundary.

- [x] Replace independent native stage pools with one operation-wide task
  arena. The arena is created from the immutable execution policy, propagated
  through Arrow C streams and multi-source wrappers, and reused by inference,
  materialization, CSV/JSONL, and native Parquet. Stable complementary lanes let
  narrow upstream and output stages overlap without exceeding N workers; lazy
  startup creates only workers that receive work. Directed probes verify strict
  inline `single`, exact N-worker reuse, no cross-lane worker-state collision,
  lifecycle/cancellation, and TSan-clean callback destruction.

- [x] Split inference into packet-local evidence plus one ordered reducer.
  Workers classify scalar evidence and build validated compact preorder trees in
  private parser/PMR state; the coordinator alone interns names and performs the
  canonical shape and statistics passes. Adaptive selection keeps flat, small,
  and sub-96-MiB worker-pool workloads serial, uses up to the operation's
  effective workers when profitable, isolates oversized rows, and preserves byte-identical logical schema
  payloads and diagnostics across order-sensitive, flattening, low-memory,
  repeated nested, and deterministic mixed-tree differential regressions. The
  mixed-tree matrix varies scalar/list/struct promotions across packet boundaries
  and multiple flattening depths. Broader fixed-clock registry/drift fuzz
  coverage remains part of the golden-matrix item above.

- [x] Split materialization into private row preparation plus one ordered commit;
  keep Arrow builders, diagnostics, registries, and file writers single-owner.

- [x] Coalesce contiguous rows into memory-accounted packets derived from the
  shared operation policy. Isolate oversized raw or nested rows, preserve
  row-local errors inside a packet, and adapt stage workers to plan complexity
  so synchronization does not dominate trivial schemas.

- [x] Parallelize CSV/JSONL output as private ordinal byte fragments plus one
  ordered writer. Bound packet rows and estimated expansion bytes, retain at
  most one fragment per effective worker, isolate oversized rows, securely wipe
  released buffers, preserve earliest-ordinal failures, validate modes before
  opening output files, and prove byte-identical cross-mode output.

- [x] Route native Parquet leaf-column page preparation, encoding, and
  compression through the ordinal policy. Workers emit private column artifacts;
  one coordinator preserves schema/row order, assigns physical offsets, commits
  page indexes, and writes the footer/trailer. Row-group targets and worker count
  reserve values, artifacts, and compression candidates; small, narrow, or
  low-memory groups stay serial. Cross-mode tests cover scalar/nested data,
  Snappy/GZIP/uncompressed output, multiple batches, nulls, logical equality,
  and strict no-new-thread behavior in `single`.

- [x] Admit native Parquet column workers gradually from row-group volume:
  the minimum-row threshold now funds each additional worker instead of acting
  as a binary gate that could activate all requested workers at once. This
  keeps narrow row groups monotonic at 4-to-8 workers without changing column
  order, physical commit order, or the operation memory budget.

- [x] Add provider-specific multipart/resumable output publication behind the
  same operation context. Large S3 spools upload memory-bounded parts
  concurrently and complete them in ordinal order; large GCS spools use
  resumable ranges, query the durable offset after lost responses, and continue
  from that exact byte; Azure receives a bounded SDK block-upload window. The
  complete local spool remains the correctness fallback and its temporary-storage
  lease is retained through completion or abort. Directed regressions cover
  out-of-order S3 parts, earliest failure, worker drain, sequential large-object
  `single`, GCS partial-response recovery, GCS abort, and Azure concurrency.

- [x] Add bounded one-partition source lookahead without parallel registry
  mutation or output commit. Static `multi` pipelines with more than one
  effective worker use a dedicated one-slot preparation executor and no new
  public option. Partition `N` forks the context for `N + 1`, sharing the single
  temporary-storage permit pool and remote coordinator while retaining a
  distinct fixed timestamp. Local fully prepared inputs may arm lookahead before
  CPU conversion; lazy remote-native inputs wait until current consumption;
  remote outputs arm only after the exact spool permit is secured. The next
  partition is never inferred, registered, callback-published, or committed
  early. Errors remain attached to their ordinal, `N + 2` is never submitted,
  callable option factories and `single` stay sequential, temporary-capacity
  contention defers preparation to the proper ordinal, and retained remote probe
  prefixes transfer into materialization without duplicate downloads.

- [x] Publish local path outputs through unique sibling staging files and atomic
  replacement. Preserve existing destinations on failure, clean staging files,
  retain destination permissions when replacing, and cover success/failure with
  regressions.

- [x] Add cancellation, earliest-error, and memory-pressure regressions under
  forced out-of-order completion across source staging, inference,
  materialization, text/Parquet output, temporary-storage permits, S3 multipart,
  GCS resumable publication, and operation-owned provider shutdown. Active work
  is drained before abort/close and the lowest canonical failure wins.

- [x] Add process-level SIGINT/KeyboardInterrupt stress and abrupt interpreter
  shutdown coverage. Normal interruption now unwinds through operation close,
  cancels and drains submitted remote work, and joins the host. The remote host
  thread is daemonized as a final fallback so an abandoned context cannot hang
  CPython shutdown; explicit close remains the deterministic resource boundary.

- [x] Add provider-emulator network fault injection to the platform CI matrix.
  A real loopback aiohttp server runs against the built ABI3 wheel on Linux,
  Windows, macOS x86-64, and macOS arm64. Directed cases cover truncated GET
  bodies, publication disconnects, complete PUT replay from byte zero,
  retryable HEAD/status recovery, bounded retry exhaustion, delayed
  cancellation without retry, fatal `BaseException` staging cleanup, SIGINT
  drain, and abrupt interpreter shutdown. The suite exposed and fixed
  aiohttp's implicit idempotent retry reusing an exhausted file payload.

- [x] Compile the final-tree native executor, inference, materialization,
  CSV/JSONL output, concurrent Parquet writer, and ABI modules with Clang 17
  ASan/UBSan on Linux and run 31 directed native threading tests without a
  sanitizer finding. The inherited Parquet footer reader remained
  uninstrumented because its `std::regex` translation unit did not complete
  under the instrumented compiler.

- [x] Add `SCHEMA_SANITIZER_SANITIZER=tsan` build support and a focused native
  CTest probe for the ordinal executor. GCC 14 completed 100 repeated rounds of
  ordered success, deliberately out-of-order dual failure, and cooperative
  cancellation without a TSan finding. The bundled Swift Clang 17 runtime in
  this container could not link because its Linux TSan archive references
  unavailable libdispatch/Blocks symbols.

- [x] Run the full ABI3 extension under GCC 14 ThreadSanitizer on Linux. The
  reproducible gate links the matching runtime into a sanitizer-first embedded
  CPython launcher, compiles its TSan policy into that executable, verifies the
  loaded build, repeats 64 cross-mode
  executor/inference/materialization/text/Parquet, fixed-clock public-path, and
  bounded partition-lookahead tests in fresh per-domain interpreters and CI shell steps, and instruments the
  bundled zlib so GZIP output remains inside the TSan boundary. Each domain
  records its result only after `pytest_sessionfinish`, receives a bounded grace
  period for normal shutdown, and terminates only a lingering non-instrumented
  interpreter teardown; a timeout before session completion remains a hard
  failure. This avoids the PyArrow finalization interaction observed when
  chaining domains while preserving one attributable stage per finding. It
  requires no process-environment configuration. Local native-module discovery
  reads each CMake sanitizer setting and excludes incompatible TSan/ASan builds
  unless the matching runtime symbol is already linked into the process, so a
  newer instrumented build cannot poison ordinary Release validation.
  Third-party CPython/PyArrow accesses are excluded only when wholly owned by
  their non-instrumented binary modules.

- [x] Extend native sanitizer coverage to parser fuzzing and supported
  platform concurrency gates. The JSON, CSV, XML, and Parquet entry points can
  use Clang libFuzzer or a deterministic standalone mutation engine without
  changing parser ownership. Linux runs exact promoted regressions plus bounded
  campaigns under both ASan/UBSan and GCC 14 TSan; the TSan ordinal-executor
  probe is repeated in the same fully instrumented build. Windows AMD64 runs
  the standalone targets and executor probe under MSVC ASan, while macOS
  x86-64 and arm64 use AppleClang ASan/UBSan. Every campaign has explicit seed,
  run count, and maximum input size. The new gate exposed and fixed a real CSV
  EOF transition bug where an unterminated quoted record spanning the final
  chunk could index an empty `string_view`.

- [x] Add a reproducible single-versus-multi benchmark for isolated inference,
  isolated CSV/JSONL output, and complete scalar/nested JSONL pipelines,
  including exact schema/diagnostic, byte, or logical equivalence as appropriate,
  warm-ups, repeated medians, host metadata, and JSON reports. On the reference
  56-CPU Linux host with 256 MiB, the final 60,000-row median showed 1.96x
  isolated nested-inference speedup, 2.00x-3.69x text-fragment speedup,
  1.40x-1.46x complete nested-pipeline speedup, and 1.41x-1.42x scalar-pipeline
  speedup while scalar inference remained at 1.01x through adaptive serial
  selection. The harness now supports `--only parquet` and logical Parquet
  verification. The final 30,000-row Snappy median measured 2.32x for isolated
  16-column output, 1.34x for the nested JSONL-to-Parquet pipeline, and 1.33x
  for the scalar pipeline while a narrow four-column sink remained serial and
  measured 0.91x. A focused 120,000-row 16-column
  matrix measured 1.64x uncompressed, 1.53x Snappy, and 2.57x GZIP.

- [x] Extend the benchmark matrix across width, deeper nesting, source count,
  compression, remote providers, CPU quotas, memory limits, Windows, and macOS.
  `bench_threading_matrix.py` runs each dimension in a fresh process and rejects
  any single/multi logical mismatch before retaining timings. The `ci` profile
  executes on Linux, Windows, macOS x86-64, and macOS arm64; the `standard` and
  `full` profiles add width, nesting, source-count, memory, compression, and
  Linux/Windows affinity quotas. `bench_remote_providers.py` measures complete
  remote-input/remote-output pipelines against MinIO, fake-gcs-server, and
  Azurite with logical Parquet equivalence outside the timed region. Timings are
  artifacts, not pass/fail thresholds, and `single` remains the default.

## v27 high-thread amortization pass

- [x] Increase the bounded materialization packet ceiling from 256 to 512 rows
  while preserving the byte target, reorder capacity, worker arenas, and single
  operation memory budget. This reduces per-packet coordination without
  allowing large rows to bypass byte isolation.
- [x] Carry the output packet byte estimate into CSV/JSONL workers and reserve
  the fragment once before encoding. Allocation failure remains a native OOM
  result and ordered output ownership is unchanged.
- [x] Re-run fresh-process 1/2/4/8/16 end-to-end scaling with logical output
  verification. Relative to v26, the retained scalar JSONL pipeline improves
  11.0% at four workers, 15.8% at eight, and 12.6% at sixteen on the reference
  host. The full measurements and reverted experiments are recorded in
  `CONCURRENCY_SCALING_V27.md`.

## v28 flat JSONL throughput pass

- [x] Add line-delimited JSONL boundary discovery so the coordinator does not
  structurally parse every object before workers perform the authoritative
  parse.
- [x] Aggregate flat JSONL inference per packet and reduce aggregates in ordinal
  order while preserving first-seen field order, null promotion, diagnostics,
  and fallback for nested containers.
- [x] Materialize supported flat scalar JSONL packets into worker-local Arrow
  builders and hand completed arrays to the stream without row-by-row rebuild.
- [x] Reuse worker scratch state and reserve fixed-width, bitmap, offset, and
  estimated UTF-8 capacity once per packet.
- [x] Raise the bounded materialization ceiling to 2,048 rows and allow up to
  eight workers for cheap scalar plans, still constrained by packet bytes,
  reorder capacity, and the one operation memory budget.
- [x] Shard the hardened allocation registry, add inline common-case ownership
  slots, and use an unsynchronized registry only for provably packet-private
  pools.
- [x] Preserve logical batch diagnostics across direct Arrow packet handoff and
  keep productive C++ translation units within the 500-line maintenance bound.
- [x] Re-run the complete test suite and fresh-process scalar JSONL benchmark.
  The retained v28 path is 22.2% faster than v27 at four workers and 26.7%
  faster at eight and sixteen; 4-to-8 improves 5.8%, while 8-to-16 remains flat.
  Detailed measurements and reverted experiments are in
  `CONCURRENCY_SCALING_V28.md`.
- [x] Prototype packet-owned slab or recyclable builder storage behind the
  existing memory pool. Slabs were rejected, while bounded exact-size worker-
  private block recycling was retained after it reduced allocator and memory
  traffic without weakening Arrow lifetime, hardened ownership checks, the
  single operation budget, deterministic output, or four/eight-worker behavior.

## v29 ordered multi-source and high-thread pass

- [x] Group compatible JSONL/NDJSON paths into one continuous native stream and
  asynchronously prepare their initial blocks in the operation arena. Publish
  only canonical source order and retain each child's mapped owner, source name,
  frontend state, and deferred error.
- [x] Give the first source a bounded 12,288-row head start on 16+ CPU hosts so
  low workers begin materialization while high workers prepare later sources.
  Cover empty and uneven files, global row order, `source_file`, diagnostics,
  and fixed-clock single/multi equivalence.
- [x] Carry an internal local-input byte estimate into materialization without a
  new public option or diagnostic field. Use nine scalar workers only for short
  operations on 16+ CPU hosts and return to eight for sustained work.
- [x] Retain exact-size/alignment Arrow blocks in a capped worker-private cache.
  Keep them live and budget-accounted while cached, defer secure wiping until
  eviction/destruction/upstream release, and preserve hardened ownership and
  double-release validation.
- [x] Reuse root-object snapshot storage and retain indexes into the live row
  instead of copying every `FieldRef` for every wide row.
- [x] Defer raw flat-JSONL inference to workers and emit compact ordered evidence
  with generic budgeted fallback for wide or nested shapes. Keep inference at
  eight workers after higher widths proved memory-bandwidth regressions.
- [x] Verify deterministic ordered data across 1/2/4/8/16 CPU widths. Raw hashes
  differ only in generated operation timestamps; normalized rows and hashes are
  identical.
- [x] Re-run steady-state and interleaved benchmarks. The retained 100,000-row
  path improves 8-to-16 by 4.5% in the official nine-repeat run and 6.4% in a
  30-pair interleaved run; v29 is about 8% faster than v28 at both widths. The
  500,000-row gain is 1.0%, confirming a remaining sustained memory-bandwidth
  boundary. Detailed measurements are in `CONCURRENCY_SCALING_V29.md`.
- [x] Investigate direct typed extraction into final Arrow layouts or NUMA-aware
  packet placement only if it improves sustained 8-to-16 scaling without a new
  public tuning surface, extra pools, relaxed wiping/ownership, or regressions
  at 1/2/4/8 CPUs. v30 retains prevalidated direct scalar extraction with
  borrowed UTF-8 until immediate Arrow append; the reference host exposes no
  actionable NUMA topology.

## v30 direct scalar extraction pass

- [x] Add a flat scalar row path that validates the complete row before builder
  mutation, retains primitives inline, and borrows already-valid UTF-8 only
  until its immediate worker-private Arrow append. Preserve owning fallback for
  formatted/coerced text and the generic path for variants and nested values.
- [x] Extend scalar/root builders with a narrow direct append contract and keep
  its reusable scratch in `BatchAppender`, with no borrowed lifetime crossing a
  packet, parser reset, or Arrow-array handoff.
- [x] Raise the bounded packet ceiling to 5,120 rows while retaining the 1 MiB
  byte target, reorder capacity, exact packet owners, and the single operation
  memory budget. Use nine short scalar workers on 16+ CPU hosts and eight for
  sustained work after ten/long-nine variants failed A/B stability.
- [x] Add regressions for Unicode/escaped/empty/null UTF-8 and ordered strict
  `skip_row`/`emit_null_row` behavior. Re-run the complete suite: 1,791 pass and
  4 skip.
- [x] Re-run interleaved performance validation. Against v29, v30 improves the
  complete 100,000-row pipeline by 16.5% at eight CPUs and 17.2% at sixteen.
  Within v30, sixteen CPUs improve the 32-pair median by 5.3% and win 25 pairs;
  sustained 500,000-row materialization improves 1.2%. Details are in
  `CONCURRENCY_SCALING_V30.md`.
- [x] Prototype packet-level pre-sized final Arrow layouts only if complete-row
  validation, deterministic ordinal errors, exact buffer ownership, and the one
  public memory budget can all be retained. The raw final-layout prototype was
  correct but 1.9–2.9% slower and was removed. The reference host still exposes
  no actionable NUMA topology.

## v31 adaptive wide-plan pass

- [x] Remove the unconditional four-worker ceiling for short flat plans with at
  least 24 root columns. Classify the frozen plan as fixed-width dominant or
  UTF-8 heavy without a public tuning surface.
- [x] Use up to sixteen workers and 128 KiB packets for short fixed-width
  dominant plans; use at most eight workers for short UTF-8-heavy plans and
  retain four workers for sustained wide operations.
- [x] Reject 64 KiB packets, unrestricted UTF-8-heavy concurrency, raw final
  Arrow layouts, parser/layout caches, specialized scanners, worker affinity,
  and scalar microbatch variants after A/B regressions or neutral results.
- [x] Add 24-column fixed-width and mixed UTF-8 single/multi regressions and run
  the complete suite: 1,793 pass and 4 skip.
- [x] Re-run the 1/2/4/8/16 wide coercion curve. Sixteen CPUs improve 8-to-16 by
  3.7%; against v30 at sixteen CPUs, v31 improves 11.3% on the numeric/coercive
  fixture and 5.7% on the mixed UTF-8 fixture. Details are in
  `CONCURRENCY_SCALING_V31.md`.
- [x] Implement column-partitioned packet materialization for retained, very
  wide flat JSONL plans without duplicate row parsing. Complete-row validation,
  deterministic row/column first-error order, exact Arrow child ownership, a
  two-packet/eight-group reorder bound, and the single public memory budget are
  preserved. Narrow plans and unsupported observations retain the v31 path.

## v32 column-partitioned packet pass

- [x] Parse eligible JSONL objects once into frozen plan order while preserving
  missing null slots, original strict extras, duplicate-field behavior, scanner
  error precedence, and raw fallback for non-empty nested observations.
- [x] Partition one logical packet into at most eight contiguous root-column
  groups and materialize each group in a worker-local projected appender.
- [x] Reduce complete-row and worker failures by canonical
  `(source_row_index, column_ordinal)` before publishing any Arrow batch.
- [x] Reparent finalized Arrow children into one root without copying buffers;
  null source child slots before releasing partial roots.
- [x] Bound retained reorder state to one packet below sixteen workers and at
  most two packets/sixteen partial roots at sixteen workers.
- [x] Keep the normal plan-ordered path matrix-free; charge compatibility
  indices, row scratch, packet owners, builders, and the partition owner/control
  block to the operation pool derived from `memory_limit_bytes`.
- [x] Restrict rollout to fixed-width-dominant scalar JSONL plans with at least
  128 root columns and `on_error=stop`; retain v31 for narrower, UTF-8-heavy,
  nested, variant, `skip_row`, and `emit_null_row` cases.
- [x] Add ten v32 regressions and run the focused concurrency/layout matrix.
  Current validation: 375 pass and 9 optional skips. The global suite reaches
  1,019 pass and 522 skips; its remaining 75 tests require unavailable PyArrow.
- [x] Measure the direct Arrow C Stream path on the available five-CPU host.
  Medians for 20,000 x 128 are 1.928 s / 1.457 s / 1.176 s at 1 / 2 / 4 CPUs;
  at four CPUs the partitioned path is 2.6% faster than an exact same-options
  build with the threshold temporarily disabled.
- [ ] Prototype bounded worker-local JSON packet preflight only if it improves
  complete pipeline timings while retaining scanner-stage precedence, one parse
  per row, the same memory budget, and a finite parsed-packet window.

## v33 evidence-driven telemetry pass

- [x] Add constant-space, operation-local performance telemetry with no
  environment variables, global sampler, extra executor, or independent memory
  budget. Expose the most recent report through
  `ExecutionContext.performance_stats()`.
- [x] Measure overlapping pipeline phases, task queue/run time by work kind,
  packet and column-group progress, queue/active peaks, steals, started workers,
  and exact operation-pool current/peak/limit bytes.
- [x] Keep capacity pressure, cache/memory-hierarchy suspicion, and proven DRAM
  bandwidth saturation as distinct states. Native telemetry never claims DRAM
  saturation without hardware evidence.
- [x] Add a fresh-process affinity benchmark with optional generic `perf stat`
  counters and an explicit PCM/uProf/uncore bandwidth sidecar. Require at least
  85% of a same-host sustainable baseline before reporting DRAM saturation.
- [x] Add directed native/public-API and classifier regressions. On the local
  five-CPU host, complete 5,000 x 128 JSONL-to-JSONL medians are 0.472 / 0.358 /
  0.293 seconds at 1 / 2 / 4 CPUs (1.61x at four), while the exact operation
  pool remains at 3.53% of its 256 MiB limit.
- [ ] Run the telemetry matrix at 1/2/4/8/16 CPUs on the reference high-core
  host with fixed NUMA placement, generic CPU/cache counters, measured DRAM
  bandwidth, and a same-node sustainable baseline. Use the result to choose the
  next implementation frontier instead of assuming a memory bottleneck.
- [ ] Prototype bounded worker-local JSON packet preflight only if the telemetry
  identifies frontend/input dominance and the complete-pipeline A/B improves
  without changing scanner precedence, one-parse semantics, memory ownership,
  or the finite parsed-packet window.

## v34 fixed-host evidence protocol

- [x] Replace first-N affinity with exact nested CPU sets derived from physical
  core topology, optionally restricted to one NUMA node. Accept a reviewed
  complete plan report as the exact affinity input for later runs.
- [x] Add mandatory CPU and memory-node placement through `numactl`, child-side
  affinity/policy snapshots, and explicit failure when required binding cannot
  be enforced.
- [x] Run timing samples in isolated processes with alternating ascending and
  descending worker order so 8-to-16 evidence is not tied to one thermal or
  allocator-history direction.
- [x] Add a direct Arrow C Stream baseline that releases schema, arrays, and
  stream without PyArrow or serialization, paired with the complete production
  JSONL writer against `/dev/null` or a real file.
- [x] Preserve generic `perf` evidence separately from platform DRAM bandwidth,
  support workload-specific measured/sustainable sidecars, and aggregate
  repeated uncore event rows rather than overwriting channels.
- [x] Emit an evidence-driven next-frontier decision covering sustained useful
  scaling, output, frontend/preflight, proven DRAM saturation, cache/latency,
  and reorder/imbalance without changing production policy speculatively.
- [x] Validate the protocol locally at 1/2/4 CPUs. For 5,000 x 128, Arrow C
  Stream reaches 1.66x and JSONL-to-devnull 1.63x at four CPUs; the host has only
  five CPUs and cannot provide valid 8/16 evidence.
- [ ] Execute short and sustained 1/2/4/8/16 matrices on the reference host with
  the reviewed nested CPU plan, fixed NUMA node, generic counters, platform DRAM
  measurements, and a same-node sustainable baseline.
- [ ] Implement exactly one production prototype selected by the resulting
  `next_frontier` evidence, then repeat the paired matrix and determinism/memory
  regression suite.

## v35 paired high-core evidence suite

- [x] Retain every isolated ABBA timing sample with round and traversal metadata,
  calculate median absolute deviation, and require stable paired 8/16 evidence
  before recommending any production change.
- [x] Add a single short+sustained high-core suite that locks one reviewed
  CPU/NUMA plan and selects a frontier only when both profiles are complete and
  coherent.
- [x] Add resumable per-profile collection guarded by the exact command and a
  source/host/plan fingerprint, so interrupted high-core runs cannot mix
  revisions, hosts, affinities, dimensions, or counter configurations.
- [x] Revalidate imported affinity plans on the current host and distinguish
  timing readiness from complete counter+DRAM diagnostic readiness.
- [ ] Execute the v35 short and sustained profiles on the reference 16-CPU host
  with fixed NUMA placement, at least seven paired samples, generic counters,
  and workload-specific DRAM measurements against a same-node sustainable
  baseline.
- [ ] Implement exactly one production prototype selected by the stable
  cross-profile `suite_frontier`, then repeat determinism, ownership, bounded
  memory, and 1/2/4/8/16 performance validation.

## v36 high-width worker-state reduction

- [x] Replace eager per-worker direct/full/projected materializer construction
  with first-use initialization bound to the stable physical worker slot.
- [x] Preserve private Arrow builders, exact child reparenting, canonical first
  errors, the two-packet/eight-group reorder bound, and the single operation
  memory budget.
- [x] Reuse one PMR projected `FieldRef` scratch vector per worker instead of
  allocating it for every column-group task.
- [x] Emit plan-ordered JSON fields directly into `FlatRowBatch`, retaining only
  the `planned_seen` bitmap and preserving duplicate, missing, extra,
  empty-container, parse-error, and nested-fallback semantics.
- [x] Add v36 source and functional regressions. Current focused validation:
  12 v32/v36 directed tests (excluding the comparator-heavy long v32 case) and
  56 telemetry/layout tests pass; the native 18,000-row single and multi smoke
  completes under the 64 MiB budget.
- [ ] Execute an exact v35/v36 short+sustained A/B on the reference sixteen-CPU
  host using the v35 reviewed affinity/NUMA plan, at least seven paired samples,
  generic counters, and same-node DRAM bandwidth evidence.
- [ ] Instrument worker/group builder first-use counts in benchmark-only
  telemetry if the 16-CPU A/B cannot distinguish construction savings from
  frontend, output, or memory-hierarchy limits.

## v37 dedicated JSONL activation and cost-balanced fan-out

- [x] Stop canonicalizing public `jsonl`/`ndjson` inputs to generic `json`;
  preserve `jsonl` through auto-detection, text preparation, and path-source
  planning so the existing native fan-out gate is reachable.
- [x] Add a production telemetry regression requiring submitted and merged
  column groups plus Arrow merge calls; distinguish logical ingestion batches
  from physical `output_batches`.
- [x] Use the same plan-ordered representation for eligible stop-on-error JSONL
  in single and multi modes, preserving scanner/validation/conversion stage
  precedence and exact exception text.
- [x] Freeze direct logical-batch row limits and preserve an existing logical
  ingestion batch count when native text writers report physical packet counts.
- [x] Replace equal-column boundaries with deterministic contiguous ranges
  balanced by static logical conversion cost, retaining at least two columns
  per group and the eight-group ceiling.
- [x] Submit expensive groups first through a stable permutation; keep Arrow
  merge in frozen column order and reject duplicate/out-of-range results through
  a compact assembly bitmap.
- [x] Preserve complete-row validation and canonical first failure by
  `(source_row_index, column_ordinal)` even when a later heavy group starts and
  fails before column 0.
- [x] Move projected-plan construction into the partition module so productive
  C++ sources remain below 500 lines.
- [x] Validate the final ABI3 module: 5 v37 regressions, 101 focused concurrency/
  telemetry/layout/documentation/input tests, and 332 maintenance tests pass;
  24 environment-dependent optional cases skip and the known long v32
  comparator stress remains deselected.
- [ ] Execute the exact v35 paired v36/v37 short+sustained A/B on the reference
  sixteen-CPU host. Record logical and physical batch counts and add per-group
  queue/runtime telemetry only if the paired result is ambiguous.
- [ ] Prototype a deterministic two-packet cost-ordered wavefront only if real
  16-CPU evidence shows reduced group tail but remaining idle capacity. Preserve
  the two-packet/eight-group bound, packet-order publication, canonical errors,
  and exact Arrow child ownership.

## v38 stable logical column materializer slots

- [x] Replace the lazy physical-worker by column-group appender matrix with
  stable logical `packet_slot * group_count + group_index` states.
- [x] Reuse the existing one/two-packet reorder window as the ownership bound,
  reducing the sixteen-worker/eight-group sustained appender ceiling from 128
  to 16.
- [x] Acquire and release packet slots through a compact coordinator bitmap;
  release only after every expected group is accepted and merged, including
  fatal cleanup paths.
- [x] Bind each logical state permanently to one projected group and reject
  mismatched state/group tasks.
- [x] Keep ordinary fallback materialization private to physical workers and
  account every projected state under the same operation memory pool.
- [x] Skip projected `FieldRef` scratch entirely for plan-ordered JSONL rows.
- [x] Add `column_slots_initialized` and `column_slot_reuses` telemetry plus a
  production regression requiring bounded initialization and exact accounting
  against submitted groups.
- [x] Make deferred JSONL raw rows depend on more than one effective worker,
  not merely a requested multi mode; restore exact one-mebibyte fallback output.
- [x] Split column preparation and assembly into focused translation units while
  retaining the 500-line productive-source limit and exact layout contract.
- [x] Validate the relinked module: 52 focused concurrency/threading tests and
  363 maintenance/documentation/layout tests pass; a 2,048 x 128 run submits 80
  groups, initializes 4 local logical slots, and reuses them 76 times.
- [ ] Execute the exact v35 paired v37/v38 short+sustained A/B on the reference
  sixteen-CPU host with fixed affinity and NUMA placement. Capture slot reuse,
  operation-memory peak, cache misses, group queue delay, and group runtime.
- [ ] Prototype a bounded two-packet cost-ordered wavefront only if real 16-CPU
  evidence still shows idle workers after the builder working-set reduction.

## v39 adaptive JSONL row parallelism

- [x] Use operation telemetry to identify serial JSONL plan-ordering as the
  dominant sustained wide-stream cost instead of continuing to tune Arrow merge.
- [x] Add explicit internal frontend modes for plan-ordered column fan-out and
  validated-raw row packets without changing the public Python API.
- [x] Preserve scanner-stage precedence with source-ordered syntax and field-count
  validation before raw rows can reach materialization workers.
- [x] Keep column fan-out for known microloads and prefer row packets for unknown
  or sustained wide JSONL streams.
- [x] Retarget row packets to at most two units per effective worker, including
  deterministic per-frontend-batch splitting when global size hints are absent.
- [x] Add strategy telemetry for logical column packets and JSONL row packets.
- [x] Preserve exact single/multi output, registry, diagnostics, generated
  metadata, parser byte offsets, first errors, ownership, and one-budget bounds.
- [x] Validate local v38/v39 medians on the four-CPU quota: 3.02x at 256 x 128
  and 3.28x at 8,192 x 128 for strict JSONL materialization.
- [ ] Execute the paired v38/v39 1/2/4/8/16 ABBA matrix on the reference host
  with fixed NUMA placement and hardware counters.
- [ ] Prototype immutable scanner-token handoff only if the 16-CPU profile shows
  source-ordered syntax validation as the remaining dominant serial fraction.

## v40 bounded immutable JSON token handoff

- [x] Retain an immutable top-level JSON member index while the v39 frontend
  performs source-ordered validation, without sharing parser objects across
  threads.
- [x] Compact each member entry to eight bytes (`key_offset`, `value_offset`)
  and reconstruct validated key/value ends from canonical separators.
- [x] Charge token storage to the operation PMR pool using one eighth of the
  sole memory budget, clamped to 64 KiB-32 MiB, with complete-row rollback and
  ordinary raw-row fallback on exhaustion.
- [x] Integrate escape detection into the scanner pass and decode only escaped
  key tokens in workers.
- [x] Preserve exact scanner/parser/conversion precedence: canonical frontend
  failures cancel outstanding tasks, while rows outside the fast scanner's
  accepted subset fall back without becoming stricter than single mode.
- [x] Add indexed-row, indexed-field, and fallback-row telemetry and require
  complete accounting in production regressions.
- [x] Restore logical batch diagnostic parity for demonstrably fixed-width row
  packets by modeling monolithic power-of-two builder capacity without copying
  physical Arrow packets.
- [x] Validate the final ABI3 module with 27 focused v32-v40 regressions, 20
  threading/materialization/output/telemetry tests, and 361 maintenance/layout
  checks; the two known long v32 comparator stresses remain deselected.
- [x] Isolate the handoff effect locally with A-B-B-A: 171.37 ms pooled median
  enabled versus 198.63 ms disabled for 8,192 x 128 under 64 MiB, a 13.7%
  elapsed-time reduction on the four-CPU quota.
- [ ] Execute the paired v39/v40 1/2/4/8/16 ABBA matrix on the reference
  sixteen-CPU host with fixed NUMA placement. Capture token coverage, frontend
  fraction, operation-memory peak, IPC, cache misses, queue delay, and 8-to-16
  speedup.
- [ ] Prototype parallel scanner/token-index partitions or SIMD delimiter/string
  scanning only if the sixteen-CPU profile proves source-ordered validation is
  still the dominant serial fraction. Preserve exact byte offsets, row-atomic
  rollback, source-order scanner precedence, and bounded owner lifetime.

## v41 ordered parallel JSONL validation

- [x] Move sustained JSONL syntax validation and immutable token capture out of
  the serial frontend and into bounded row-packet tasks.
- [x] Reuse the operation-wide `OperationTaskArena` for both validation and
  materialization so worker count remains the effective CPU count, not twice it.
- [x] Add a complete frontend-batch validation barrier before materialization
  publication, preserving source-order scanner precedence and exact byte
  offsets.
- [x] Split the existing one-eighth token allowance into deterministic,
  disjoint, row-proportional packet quotas whose sum cannot exceed the sole
  operation memory budget.
- [x] Preserve row-atomic token fallback, worker-private parsers, immutable raw
  owners, exact duplicate/escape semantics, and cancellation cleanup.
- [x] Avoid frontend token-descriptor reservation in deferred mode and retain
  serial validated-raw behavior when effective memory policy permits only one
  worker.
- [x] Add validation phase/task telemetry and submitted/completed packet
  counters; diagnose combined validation plus materialization worker activity
  without losing stage-specific values.
- [x] Validate 27 focused v32-v40 contracts, 3 dedicated v41 regressions, 23
  telemetry checks, and 363 maintenance/documentation/layout tests.
- [x] Isolate the local effect with A-B-B-A under four-CPU affinity: pooled
  median 133.11 ms for parallel validation versus 189.72 ms for the otherwise
  identical serial-validation path, a 29.8% elapsed-time reduction.
- [ ] Execute the paired v40/v41 1/2/4/8/16 ABBA matrix on the reference
  sixteen-CPU host with fixed NUMA placement. Capture stage overlap, validation
  and materialization parallelism, queue wait, stealing, IPC, cache misses,
  memory peak, token coverage, and paired 8-to-16 speedup.
- [ ] Add a bounded stage-aware arena policy only if the sixteen-CPU trace shows
  validation/materialization interference; do not create a second worker pool.
- [ ] Prototype SIMD line/string classification only if ordered line framing is
  still a material serial fraction after validation parallelization.

## v42 one-pass deferred JSONL framing

- [x] Bypass `FlatRowBatch` parallel metadata vectors and the export traversal
  when the resolved frontend mode is deferred raw JSONL; append final `RowRef`
  values directly while retaining chunk and source-name owners once.
- [x] Keep plan-ordered, serial validated, JSON-array, and materialized-field
  paths on the existing `FlatRowBatch` implementation.
- [x] Move JSONL newline search ahead of shared-owner copies and segment-vector
  construction so ordinary one-chunk records use no chunk-crossing state.
- [x] Preserve checked multi-chunk assembly, the 128 MiB/65,536-segment limits,
  CRLF trimming, exact byte offsets, source metadata, and a final line without
  a newline.
- [x] Reject correct validation/materialization overlap after a local 3.3%
  regression caused by competition inside the shared arena.
- [x] Reject first-row adaptive token reservation after an isolated 2.85%
  regression versus bounded geometric growth and exact-block pool reuse.
- [x] Validate 33 focused v32-v42 regressions, 46 threading/telemetry tests, and
  332 maintenance/layout checks; eight optional cases skip and the two known
  long v32 Python comparators remain deselected.
- [x] Measure paired local evidence across thirty samples per variant: v42
  reduces end-to-end time by 2.81% on 8,192 x 128 and 1.86% on 100,000 x 4,
  while reducing `frontend_read` by 5.61% and 6.72% respectively.
- [ ] Execute the paired v41/v42 1/2/4/8/16 ABBA matrix on the reference
  sixteen-CPU host with fixed NUMA placement and hardware counters.
- [ ] Prototype a bounded chunk-level newline index only if ordered framing
  remains material at sixteen CPUs; carry at most one partial record across
  chunks and preserve exact offsets, owners, CRLF, and record limits.
- [ ] Add stage-specific JSONL output telemetry if serialization becomes the
  measured critical path; do not widen queues or reorder memory speculatively.

## v43 adaptive fixed-width JSONL output above eight CPUs

- [x] Profile JSONL output after v42 and identify the permanent four-worker,
  upstream-lane ceiling as a structural 8-to-16 boundary.
- [x] Reject batched scanner framing, borrowed `RowRef` packet ranges, fixed
  schema-level row estimation, global output-task priority, and granular
  production output timers after isolated regressions or insufficient benefit.
- [x] Classify only wide flat schemas with fixed-cost scalar output as eligible;
  keep strings, variable binary, dictionaries, and nested output on v42.
- [x] Preserve the v42 worker/lane/admission path exactly when the operation
  arena exposes eight workers or fewer.
- [x] Above eight workers, derive the fixed-wide ceiling as half the operation
  arena clamped to four-eight workers, use the high output lane, and accumulate
  work across batches for geometric admission.
- [x] Prove at compile time that eight workers remain disabled and sixteen
  workers select eight output workers only for eligible schemas.
- [x] Exercise a synthetic sixteen-worker arena with separate 8+8 lanes and
  verify sixteen total native threads, zero lane overlap, and no oversubscription.
- [x] Validate 36 v32-v43 regressions, 40 threading/output/telemetry tests, and
  381 maintenance/layout tests; retain the two known long v32 deselections.
- [x] Verify local dormancy against v42: 199.81 ms versus 199.57 ms on
  8,192 x 128, a 0.12% difference inside four-CPU host noise.
- [ ] Execute paired v42/v43 1/2/4/8/16 ABBA on a real sixteen-CPU host with at
  least 256 MiB, fixed affinity/NUMA placement, peak RSS, IPC, LLC misses,
  output active workers, queue wait, and paired 8-to-16 speedup.
- [ ] Keep eight output workers only if the real host improves without a
  measurable 1-8 CPU regression; otherwise reduce the ceiling to six or revert.

## v44 stable fixed-wide output admission

- [x] Extract bounded output admission into a focused internal policy helper
  without changing generic CSV, nested, or variable-width behavior.
- [x] Reuse the complete v43 high-core fixed-wide output policy from the first
  packet instead of creating a four-worker executor and draining/recreating it
  at eight workers after a later batch.
- [x] Skip the 64-row admission sampling pass only when the stable high-core
  policy is active; retain exact per-row packet byte estimation.
- [x] Preserve the eight-slot queue/reorder bound, high output lane, lazy worker
  startup, one operation arena, and the sole `memory_limit_bytes` control.
- [x] Add an ABI3 admission probe proving `4 -> 8` and two generations for the
  v43 geometric sequence versus `8 -> 8` and one generation for v44.
- [x] Reject broadening stable admission to one-through-eight CPU microloads
  after a two-block-per-variant 64-row A-B-B-A showed no consistent benefit.
- [x] Verify four-CPU dormancy against the original v43 path: 302.87 ms versus
  304.21 ms on 8,192 x 128, a 0.44% difference inside host noise.
- [x] Validate 38 focused v32-v44 regressions, 33 executor/policy/telemetry
  tests, 9 output/materialization/memory tests, and 363 maintenance/layout
  checks; retain two known long deselections and two optional skips.
- [ ] Execute paired v43/v44 1/2/4/8/16 evidence on the reference host with at
  least 256 MiB and record executor generations, output active workers,
  ordinal wait, peak RSS, IPC, LLC misses, and NUMA placement.
- [ ] Investigate lane-compatible queue indexing only if the real sixteen-CPU
  trace shows scheduler scan cost after the admission barrier is removed.

## v45 bounded high-core local output preference

- [x] Reproduce the sixteen-worker local head-of-line case where dedicated
  high-lane output tasks sit behind broad upstream tasks on the same queues.
- [x] Prefer only the earliest dedicated high-lane task on high-half workers;
  permit one consecutive bypass before forcing FIFO progress, retain FIFO among
  output tasks, no preemption, unchanged stealing, and ordinal publication.
- [x] Preserve the worker's reserved first task and all lazy-start ownership
  semantics.
- [x] Compile separate low/high-core worker loops so one-through-eight workers
  keep the exact legacy `front()`/`pop_front()` hot path.
- [x] Reject a lane-compatibility queue index after an approximately 7% mixed
  scheduler-probe regression.
- [x] Reject an idle-worker bitset after its atomic maintenance cost exceeded
  the bounded slot scan it replaced.
- [x] Add deterministic 8/16-worker output-preference and mixed-lane liveness
  probes, including zero oversubscription and complete queue drain.
- [x] Restore the TSAN ordered-executor target by linking arena telemetry and
  JSON token-writer dependencies; complete the race test successfully.
- [ ] Execute paired v44/v45 1/2/4/8/16 evidence on the reference host and
  record output queue wait, ordinal wait, queue depth by lane, RSS, IPC, LLC
  misses, NUMA placement, and end-to-end throughput.
- [ ] Retain the preference only if the sixteen-CPU application workload gains
  consistently without a measurable regression through eight CPUs.

## v46 output-aware compatible stealing

- [x] Reproduce the high-core inversion where an idle high worker steals later
  broad work from the back of a victim queue while earlier dedicated output
  remains at the front.
- [x] Prefer only a compatible dedicated high-lane task at the front of the
  already-selected deepest victim queue; retain the existing reverse scan for
  every other case.
- [x] Keep the preference compile-time dormant through eight workers and add no
  queue scan, compatibility index, extra queue, worker, or public control.
- [x] Preserve v45 local one-bypass fairness, ordered publication, cancellation,
  exact output, memory bounds, and the single-thread no-worker contract.
- [x] Add deterministic 8/16-worker ABI3 probes proving 0/3 low-core promotion,
  7/7 high-core promotion, complete drain, and the exact worker ceiling.
- [x] Replace the timing-sensitive four-worker stealing probe with a fully
  blocked placement that deterministically forces compatible remote stealing.
- [x] Measure fresh-process scheduler evidence: 31,071 us v45 versus 27 us v46
  median output latency in the amplified inversion; mixed synthetic medians do
  not regress on the current five-CPU host.
- [ ] Execute paired v45/v46 fixed-wide JSONL output on the reference sixteen-CPU
  host with affinity/NUMA control, output queue wait, task-kind steals, ordinal
  wait, RSS, IPC, LLC misses, and paired 8-to-16 throughput.
- [ ] Investigate source-aware packet placement only if real traces still show
  idle compatible high workers; do not add a broader scan or lane index without
  end-to-end evidence.

## v47 distributed ordered completion publication

- [x] Identify the executor-wide result mutex and shared completion counter as a
  high-worker convergence point after arena tasks finish.
- [x] Give every active arena ordinal one bounded atomic completion slot derived
  from the existing dispatch-window uniqueness contract.
- [x] Publish normal worker outcomes without the executor-wide result mutex or a
  shared completed-result counter; wait only on the exact next ordinal slot.
- [x] Retain one coordinator take mutex for historical single-consumer semantics
  without exposing it to worker completion.
- [x] Allocate either legacy completion slots or arena completion slots for one
  executor, never two full reorder windows.
- [x] Preserve cancellation, fatal allocation handling, shutdown leases,
  circular slot reuse, exact ordinal failures, Arrow ownership, and the sole
  `memory_limit_bytes` resource control.
- [x] Revalidate accepting state and expected ordinal at the actual inline,
  arena, and local-pool reservation point, closing a submit/finish race.
- [x] Add a high-volume ABI3 completion probe and deterministic 1/4/16-worker
  regressions with exact checksum, complete drain, and worker ceilings.
- [x] Reject private worker wake epochs and redundant-mask suppression after
  unstable or regressive paired measurements.
- [x] Measure paired synthetic evidence on the five-CPU host: v47 reduces
  elapsed time by 20.9% at 8 workers and 12.1% at 16 workers for the
  16-iteration case, and by 15.4%/11.6% for the 64-iteration case.
- [x] Run a paired real JSONL validation/materialization guardrail on the
  current five-CPU host: nine alternating samples preserve the exact output
  hash and measure 623.99 ms for v46 versus 630.44 ms for v47, a 1.03%
  difference inside host noise rather than a claimed end-to-end gain.
- [x] Validate 52 v29-v47 scaling regressions, 42 threading/telemetry/output
  regressions, 468 maintenance/memory/layout checks, and the 2.48 s TSAN
  executor target.
- [ ] Repeat paired v46/v47 end-to-end 1/2/4/8/16 evidence on the reference
  sixteen-CPU host with fixed affinity/NUMA placement, completion publication
  time, ordinal wait, queue depth, RSS, IPC, LLC misses, output equivalence,
  and fixed-wide JSONL/CSV output workloads.
- [ ] Inspect coordinator submit/take cadence and packet granularity only if the
  real host still plateaus after completion contention is removed; do not widen
  reorder memory speculatively.

## v48 high-core authoritative submit reservation

- [x] Identify the duplicated executor mutex acquisition in every arena packet
  submission after v47 removed completion-side contention.
- [x] Route only arena executors above eight workers through one authoritative
  reservation covering accepting state, ordinal, dispatch capacity, in-flight,
  and scheduled-task accounting.
- [x] Preserve the exact v47 path through eight workers and leave inline and
  standalone-pool execution unchanged.
- [x] Roll back ordinal and in-flight ownership on arena submission failure;
  retain external-task lease ownership for scheduled-task cleanup.
- [x] Preserve dispatch/reorder memory bounds, exact ordinal failures,
  cancellation, first-error precedence, Arrow ownership, and the sole
  `memory_limit_bytes` control.
- [x] Reject JSON validation/materialization fusion, worker continuations,
  higher packet counts, broader mutex retention, and an all-worker reservation
  after neutral or regressive paired measurements.
- [x] Add deterministic 8/16-worker probes proving exact checksums, complete
  drain, circular slot reuse, and worker ceilings across the high-core gate.
- [x] Measure the high-core standalone scheduler probe: sixteen-worker median
  elapsed reductions of 4.47%, 0.44%, and 2.01% for 4, 16, and 64 synthetic
  iterations respectively on the oversubscribed five-CPU host.
- [x] Preserve exact output in the four-worker 40,000-by-64 JSONL guardrail;
  record the 1.91% dormant-path spread as host noise rather than a speedup.
- [x] Validate 57 scaling regressions, 62 threading/telemetry/output checks,
  485 maintenance/memory/layout checks, and the 2.71 s TSAN executor target.
- [ ] Execute paired v47/v48 1/2/4/8/16 end-to-end evidence on the reference
  physical sixteen-CPU host with fixed affinity/NUMA placement, submit wait,
  queue depth, ordinal wait, RSS, IPC, LLC misses, and exact output.
- [ ] Investigate coordinator production cadence and per-stage packet cost only
  if the reference host still plateaus; do not widen reorder memory or packet
  count without an end-to-end gain.

## v49 high-core worker-local telemetry batching

- [x] Identify per-task task-start/task-finish telemetry atomics as shared
  cache-line publication points across high-core arena workers.
- [x] Accumulate exact task counts, queue wait, run time, and maxima in one
  fixed stack buffer per physical worker and flush every 32 tasks, before wait,
  and on worker exit.
- [x] Keep the immediate v48 telemetry path unchanged through eight workers and
  activate batching only for the existing 9-32-worker high-core loop.
- [x] Forward active-task telemetry only when the authoritative arena peak
  actually increases.
- [x] Preserve exact final telemetry, deterministic output, cancellation,
  worker ceilings, reorder bounds, single mode, and the sole
  `memory_limit_bytes` control.
- [x] Measure 16-worker standalone medians on the oversubscribed five-CPU host:
  14.37% lower elapsed time for the four-iteration case and 15.18% for the
  64-iteration case at 150,000 tasks.
- [x] Add high-core exactness, low/high deterministic, source-contract, and
  ThreadSanitizer coverage.
- [ ] Repeat paired v48/v49 1/2/4/8/16 end-to-end workloads on a physical
  sixteen-CPU host with fixed affinity/NUMA placement, exact output, task
  publication cost, queue depth, coordinator wait, RSS, IPC, and LLC misses.

## v50 high-core startup and wake fast paths

- [x] Identify the repeated per-packet `start_mutex` acquisition after a worker
  has already been safely published through `started_mask`.
- [x] Above eight workers, use the existing acquire/release started bit as a
  lock-free fast path while retaining the locked path for first creation,
  concurrent startup, failure rollback, and shutdown.
- [x] Preserve the complete v49 submission and notification path through eight
  workers and keep single mode strictly inline.
- [x] Suppress high-core work-epoch writes and condition-variable notifications
  only when the selected target is already running and no compatible idle
  helper exists.
- [x] Retain target wakeups, real helper wakeups, lazy worker admission, output
  priority, compatible stealing, cancellation, and exact drain behavior.
- [x] Reject bounded victim selection, stolen-counter batching, and an explicit
  idle mask after neutral, unstable, or regressive measurements.
- [x] Return immediately when idle-worker candidate masks are empty, avoiding
  up to two full 16-slot negative scans per saturated submission.
- [x] Measure eleven alternating runs of the saturated 16-worker submission
  probe: 300,000 queued tasks fall from 143.366 ms to 70.368 ms median wall
  time and from 630.437 ms to 351.539 ms median process CPU on the
  oversubscribed five-CPU host.
- [x] Validate 64 historical scaling tests, 62 threading/telemetry/output
  tests, 486 maintenance/memory/layout tests, and the 2.65 s ThreadSanitizer
  executor target.
- [ ] Repeat paired v49/v50 1/2/4/8/16 end-to-end workloads on a physical
  sixteen-CPU host with fixed affinity/NUMA placement, submission throughput,
  coordinator wait, queue depth, steals, context switches, IPC, LLC misses,
  RSS, and exact output hashes.

## v51 fully budgeted high-core dispatch window

- [x] Identify that the sixteen-worker cross-batch path used only 18 of 32
  reorder slots already reserved and charged to the operation memory policy.
- [x] Use the complete `OrderedExecutor::dispatch_window()` only behind the
  existing at-least-sixteen-CPU and more-than-eight-worker gate.
- [x] Preserve the one-through-eight-worker path, column-partition exclusion,
  strict single mode, first-error ordinal, output priority, and cancellation.
- [x] Reject persistent-running flags, reduced active accounting, queue-local
  output counters, and alternative steal victim searches after neutral or
  regressive measurements.
- [x] Measure the 16-worker skew probe: the median falls from 1,827.826 ms with
  18 slots to 1,487.001 ms with 32 slots, an 18.65% reduction on the
  oversubscribed five-CPU host.
- [x] Run an A-B-B-A real-pipeline analogue with 4 workers: the full window wins
  both adjacent comparisons, cuts coordinator wait, raises combined worker
  parallelism, and keeps tracked peak memory effectively unchanged.
- [x] Verify byte-for-byte identity across old-window multi, full-window multi,
  and strict single output: SHA-256
  `563fc370ab13a9119fb58fc6b804ac43df1bd912b11a58bb4245799b4fb11a19`.
- [x] Remove the accidental unbuilt dispatch instrumentation copy and guard the
  single canonical production owner with a regression test.
- [x] Validate 68 historical scaling tests, 62
  threading/telemetry/materialization/output tests, 510
  maintenance/memory/documentation/layout/distribution tests, and the 2.70 s
  ThreadSanitizer ordered-executor target.
- [ ] Repeat paired v50/v51 1/2/4/8/16 end-to-end workloads on a physical
  sixteen-CPU host with fixed affinity/NUMA placement, in-flight occupancy,
  coordinator wait, per-worker idle time, queue depth, IPC, LLC misses, RSS,
  memory-ledger peak, and exact output hashes.

## v52 shared zero-copy RowRef packet ranges

- [x] Identify packet-local `std::vector<RowRef>` allocation and shallow copy as
  serial O(rows) coordinator work before inference, validation, and
  materialization.
- [x] Move each frontend batch once into one `OwnedRowBatch` containing the
  canonical row vector and source-byte owner.
- [x] Represent packet row ranges as disjoint `std::span<RowRef>` views while
  retaining exactly one shared owner per packet.
- [x] Preserve deferred JSON token ownership by retaining the shared batch from
  validated storage until the final dependent packet completes.
- [x] Remove the redundant persistent `RowBatch` member from the parallel source.
- [x] Reject queue-placement, mask-publication, persistent-running, and
  queue-local output-counter experiments after neutral or regressive evidence.
- [x] Measure nine-run interleaved end-to-end A/B evidence: 2.85% lower median
  wall time for 30,000 x 128 JSONL and 7.25% lower for 500,000 x 8 JSONL, with
  exact output hashes and unchanged tracked memory.
- [x] Verify the available-host scaling curve: 1.00x, 1.66x, and 2.40x at one,
  two, and four effective workers.
- [ ] Repeat paired v51/v52 1/2/4/8/16 end-to-end workloads on a physical
  sixteen-CPU host with fixed affinity/NUMA placement, coordinator packet-build
  time, queue occupancy, ordinal wait, IPC, LLC misses, RSS, memory-ledger peak,
  and exact output hashes.
- [x] Remove the fixed-width wide JSONL packet-accounting pass without
  parallelizing it: v53 prepares one exact conservative schema bound per batch
  and preserves identical packet boundaries and error order.

## v53 constant-cost wide JSONL packet planning

- [x] Identify recursive fixed-width row estimation as serial O(rows x columns)
  output-coordinator work.
- [x] Precompute the exact conservative fixed-width row bound once per schema.
- [x] Prepare fast-path eligibility once per validated Arrow batch and require
  exact zero null counts for the root and every child.
- [x] Preserve the original row-aware path for nulls, unknown null counts,
  variable-width values, nested values, dictionaries, and CSV.
- [x] Avoid unsafe cross-batch pointer identity caching; preparation is tied to
  the current validated batch object.
- [x] Verify all supported fixed-cost scalar kinds and ten packet caps against
  the v52 estimator.
- [x] Measure the focused 1,000,000-row x 128-column planning reduction from
  429.136 ms to 0.331 ms with identical estimates and checksum.
- [x] Measure nine fresh-process strict-contract A/B pairs with identical output:
  609.284 ms v52 versus 603.057 ms v53 at the wall-time median.
- [ ] Repeat v52/v53 on a physical 16-CPU host with fixed affinity/NUMA and
  coordinator/output-worker hardware counters before changing output worker
  ceilings or packet geometry.

## v54 bounded low-core output progress

- [x] Identify deterministic output head-of-line blocking behind one broad
  materialization task in four-worker shallow upper-lane queues.
- [x] Track real queued output work under the existing per-slot mutex so queues
  without output avoid scans and shared atomic publication.
- [x] Enable one bounded local bypass only for four-to-five-worker arenas, real
  output telemetry tasks, upper-half lanes, and queue depths from two to four.
- [x] Let an idle compatible helper recover only a front output task from the
  already-selected victim queue; retain constant-time remote preference.
- [x] Force the next local dequeue back to FIFO and verify two-wave fairness.
- [x] Preserve the v45 compile-time high-core path above eight workers and the
  legacy FIFO path at six through eight workers.
- [x] Measure the focused four-worker inversion reduction from 2,156 us to
  75 us median, with two of two first-wave outputs promoted and only two of
  four promoted in the fairness probe.
- [x] Verify exact 250,000-row output across seven alternating processes; treat
  the mixed 0.43% wall-time median as host noise while recording a 1.59% paired
  CPU reduction with six of seven v54 wins.
- [x] Reject output buffer pools, raw-owner retention, early worker-side Arrow
  release, narrow-output inline admission, cross-batch prefetch, and
  packet-geometry changes after unstable or regressive evidence.
- [ ] Repeat v53/v54 on a physical 16-CPU host with fixed affinity/NUMA, output
  and materialization queue waits, promotions, steals, commit wait, IPC, LLC
  misses, RSS, memory-ledger peak, and exact hashes.

## v55 wide flat JSONL inference aggregation

- [x] Profile the four-worker wide pipeline and identify inference as the
  dominant phase after v52-v54 coordinator/output improvements.
- [x] Confirm that every packet above sixteen root fields falls back from the
  compact scalar aggregate to generic per-row preorder evidence.
- [x] Keep the first sixteen fields inline and add a packet-local tracked PMR
  overflow bounded at 512 fields and one inference worker arena.
- [x] Add an ordered-position fast path while preserving linear fallback for
  missing, reordered, and duplicate keys.
- [x] Preserve generic fallback for nested shapes, long keys, and more than 512
  fields.
- [x] Measure seven alternating fresh-process wide A/B pairs: 28.20% lower wall
  median, 16.45% lower CPU median, and 40.70% lower inference median, with the
  same 56,914,214-byte tracked peak and exact output hash.
- [x] Verify the available-host curve remains monotonic: 1.00x, 2.13x, 2.49x,
  and 3.69x at one through four effective workers.
- [x] Keep the eight-column guardrail within shared-host noise and retain the
  exact inline representation for the narrow path.
- [ ] Repeat v54/v55 at 1/2/4/8/16 workers on a physical sixteen-CPU host with
  fixed affinity/NUMA placement, compact/generic packet counts, reducer time,
  IPC, LLC misses, memory bandwidth, RSS, ledger peak, and exact hashes.

## v56 compact scalar-category dispatch

- [x] Profile the v55 wide inference worker loop after packet-local overflow
  removed generic evidence construction.
- [x] Expose the existing compact `ValueView::Tag` through a read-only noexcept
  accessor; do not expose backing storage.
- [x] Replace repeated scalar category predicates and unconditional container
  checks with one exhaustive tag switch in flat packet inference.
- [x] Keep string parsing options, empty-container behavior, nested fallback,
  diagnostics, field order, and first-error precedence exact.
- [x] Use the same tag mapping in the reference scalar observer so serial and
  generic inference retain one canonical classification.
- [x] Reject coordinator reduction caching, sparse cancellation polling, and
  materialization container-branch changes after weak or regressive evidence.
- [x] Measure the focused classifier: 470.277 ms to 306.603 ms median for 64
  million values, or 1.54x, with an identical checksum.
- [x] Measure nine alternating 60,000-row by 128-column inference probes: 10.07%
  lower paired inference median and 8.56% lower paired CPU median, with the same
  schema hash and 269 tasks.
- [x] Keep the complete-pipeline claim conservative: seven alternating pairs
  show 0.98% lower wall and 0.94% lower CPU medians with byte-identical output.
- [x] Verify the available-host curve remains monotonic at one through four
  effective workers: 1.00x, 2.27x, 2.76x, and 4.13x.
- [ ] Repeat v55/v56 at 1/2/4/8/16 workers on a physical sixteen-CPU host with
  fixed affinity/NUMA, IPC, branch misses, LLC misses, memory bandwidth, worker
  run/queue times, ledger peak, and exact hashes.

## v57 single-pass flat JSONL inference parsing

- [x] Identify general object iteration as repeated worker-private overhead in
  flat JSONL inference after v55-v56 compact aggregation and tag dispatch.
- [x] Add one internal flat-object visitor that skips discarded key hashing and
  scans primitive tokens only once.
- [x] Preserve exact string decoding, strict float validation, empty-container
  behavior, nested fallback, error offsets, and source-ordinal reduction.
- [x] Classify ordinary integer tokens lexically against exact signed 64-bit
  bounds; invoke floating conversion only for actual floats and integer
  literals outside `int64`.
- [x] Keep the generic object visitor unchanged for materialization, nested
  inference, and callers that use key hashes or numeric values.
- [x] Extract flat aggregate ownership to
  `internal/inference/parallel_flat_evidence.*` and keep every production file
  below 500 lines.
- [x] Measure the focused 60,000 x 128 parser probe: 369.337 ms to 237.257 ms
  median, or 1.56x, with an identical checksum.
- [x] Measure nine alternating inference probes: 58.69% lower wall, 64.53%
  lower CPU, and exact schema hash in all runs.
- [x] Measure seven alternating complete wide conversions: 34.33% lower paired
  wall median and 38.12% lower CPU median, with byte-identical output.
- [x] Verify the 250,000 x 8 guardrail improves 7.56% in wall and 10.49% in CPU
  medians without changing output.
- [x] Verify the available-host curve remains monotonic through four effective
  workers.
- [ ] Repeat v56/v57 at 1/2/4/8/16 workers on a physical sixteen-CPU host with
  fixed affinity/NUMA, parser IPC, branch misses, memory bandwidth, worker
  run/queue time, ledger peak, and exact output hashes.

## v58 validation-certified positional JSONL materialization

- [x] Profile v57 after single-pass inference and identify wide Arrow
  materialization as the dominant worker phase.
- [x] Certify exact compiled-plan root order during the existing worker-side
  JSON validation scan without a second key pass.
- [x] Carry the proof in bounded packet row flags and expose an exact completed
  telemetry counter.
- [x] Materialize eligible direct-scalar rows positionally without key decoding,
  key hashing, `FieldRef` construction, or row snapshots.
- [x] Preserve the canonical path for escaped, reordered, missing, duplicated,
  nested, and variant-bearing rows.
- [x] Measure seven alternating Arrow A/B pairs: 213.107 ms v57 versus
  184.814 ms v58 at the median, with seven of seven v58 wins and no tracked
  memory increase.
- [x] Measure seven complete JSONL A/B pairs: 253.702 ms v57 versus 234.766 ms
  v58 at the median, five of seven wins, and byte-identical output.
- [x] Verify the available-host curve remains monotonic at 1,279.604 ms,
  305.729 ms, and 182.119 ms for one, two, and four workers.
- [ ] Repeat v57/v58 at 1/2/4/8/16 workers on a physical sixteen-CPU host with
  fixed affinity/NUMA placement, direct-scalar builder time, IPC, LLC misses,
  memory bandwidth, queue wait, ledger peak, and exact hashes.

## v59 direct lexical JSONL scalar materialization

- [x] Profile v58 after positional key lookup removal and identify generic
  per-cell `ParseValue` construction as the dominant remaining wide Arrow
  materialization cost.
- [x] Convert exact null, bool, int64, float64, unescaped UTF-8, integer
  timestamp, date32, and time32 tokens directly into existing scalar slots.
- [x] Keep the generic parser and conversion owner authoritative for escaped
  strings, coercions, nested values, variants, invalid tokens, diagnostics, and
  `on_error` behavior.
- [x] Preserve validated-row ownership so borrowed unescaped UTF-8 remains
  valid through append without a new cache, arena, queue, or memory lease.
- [x] Measure seven alternating 30,000 x 128 Arrow-stream pairs: 12.76% lower
  wall, 21.50% lower CPU, 60.45% lower materialization run time, and 80.76%
  lower materialization queue wait, with seven of seven wins.
- [x] Verify the complete JSONL writer guardrail remains byte-identical and
  statistically neutral after output becomes the dominant phase.
- [x] Verify exact single/multi output for direct scalars, quoted coercions,
  escaped strings, temporal parsing, and mixed fallback rows.
- [x] Verify the available-host curve remains monotonic at one, two, and four
  effective workers.
- [ ] Repeat v58/v59 at 1/2/4/8/16 workers on a physical sixteen-CPU host with
  fixed affinity/NUMA, direct-hit counters, materialization queue/run time, IPC,
  branch misses, LLC misses, memory bandwidth, ledger/RSS peaks, and exact
  output hashes.

## v60 pair-digit JSONL integer formatting

- [x] Re-profile the complete v59 JSONL pipeline after direct lexical Arrow
  materialization and identify integer text formatting as repeated output-worker
  CPU work.
- [x] Replace `std::to_chars` with one allocation-free two-digit formatter
  shared by every signed and unsigned Arrow integer width.
- [x] Preserve exact `INT64_MIN`, `INT64_MAX`, and `UINT64_MAX` output using an
  overflow-safe signed magnitude conversion.
- [x] Measure the focused 64-million-value formatter reduction from 623.532 ms
  to 476.741 ms median, or 1.31x, with identical encoded length and checksum.
- [x] Measure nine alternating complete wide conversions: 2.60% lower paired
  wall median and 1.70% lower paired CPU median, with exact output hashes.
- [x] Verify the available-host curve remains monotonic at one, two, and four
  CPU affinity levels.
- [x] Reject and remove the homogeneous-integer packet kernel after a 2.14%
  paired wall regression and 1.73% paired CPU regression.
- [ ] Repeat v59/v60 at 1/2/4/8/16 workers on a physical sixteen-CPU host with
  fixed affinity/NUMA, output run/queue time, scalar mix, IPC, branch misses,
  LLC misses, memory bandwidth, ledger/RSS peaks, and exact hashes.

## v61 run-based JSON string escaping

- [x] Re-profile the v60 complete JSONL pipeline and identify per-byte string
  appends as repeated output-worker CPU work for ordinary UTF-8 values.
- [x] Copy maximal safe spans with one append and escape only control bytes,
  quotes, and backslashes.
- [x] Preserve byte-exact UTF-8, all seven short escapes, lowercase `\u00xx`,
  single/multi parity, packet ownership, secure wiping, and bounded memory.
- [x] Measure the focused mixed-string encoder: 897.898 ms to 455.037 ms
  median, or 1.97x, with identical encoded length and checksum.
- [x] Measure seven alternating complete string-heavy conversions: 10.23%
  lower paired wall median and 11.68% lower paired CPU median, with exact output.
- [x] Verify the available-host curve remains monotonic at one, two, and four
  CPU affinity levels: 1.00x, 1.98x, and 3.09x.
- [ ] Repeat v60/v61 at 1/2/4/8/16 workers on a physical sixteen-CPU host with
  fixed affinity/NUMA, escape density, output run/queue time, IPC, branch
  misses, LLC misses, memory bandwidth, ledger/RSS peaks, and exact hashes.

## v62 bounded output-packet window reclamation

- [x] Re-profile v61 and identify 434 sub-millisecond output tasks plus
  255.95 ms aggregate queue wait on the 15,000 × 64 string workload.
- [x] Reclaim only packet bytes released when a narrowed output stage removes
  reorder slots, preserving the original aggregate reorder-window bound.
- [x] Bound every reclaimed target by one eighth of the preserved per-worker
  arena and leave single-thread policy unchanged.
- [x] Enable reclamation only for wide flat variable-width JSONL schemas;
  preserve the established exact fixed-width path.
- [x] Halve output tasks from 434 to 217 and reduce aggregate queue wait by
  34.9% without increasing tracked peak memory.
- [x] Measure eleven alternating complete conversions: 1.96% lower paired wall
  median and 3.29% lower paired CPU median, with exact output hashes.
- [x] Verify monotonic available-host scaling at one, two, and four CPUs:
  1.00x, 1.92x, and 2.81x.
- [x] Reject and remove word-at-a-time escaping, output-lane separation,
  contiguous member-prefix storage, and string-cell bypass experiments after
  complete-pipeline regressions or inconclusive CPU results.
- [ ] Repeat v61/v62 at 1/2/4/8/16 workers on a physical sixteen-CPU host with
  fixed affinity/NUMA, packet-size distribution, output run/queue time,
  materialization overlap, IPC, branch misses, LLC misses, memory bandwidth,
  ledger/RSS peaks, and exact hashes.

## v63 cross-batch wide JSONL output admission

- [x] Identify per-Arrow-batch output admission as a repeated barrier for wide
  variable-width JSONL when each batch exposes only one or two packets.
- [x] Admit the complete bounded output-stage ceiling from the first batch only
  on the v62 wide flat variable-width path.
- [x] Reuse the existing operation arena; create no additional threads and keep
  strict single-thread execution inline.
- [x] Preserve the v62 packet target, queue/reorder capacity, Arrow ownership,
  ordered commit, cancellation, secure cleanup, and one public memory limit.
- [x] Measure a 3.80% aggregate median wall reduction in the final ABBA sample
  and a 4.83% paired median reduction in the geometric precursor study.
- [x] Verify the available-host curve remains monotonic at one, two, and four
  CPUs, with the measured 2→4 reduction increasing from 15.33% to 16.91%.
- [x] Reject and remove exact escape-aware estimation after it reduced packet
  count but accidentally serialized per-batch output and regressed wall time.
- [ ] Repeat v62/v63 at 1/2/4/8/16 CPUs on a physical sixteen-CPU host with fixed
  affinity/NUMA, cross-batch in-flight distribution, output/materialization
  overlap, IPC, branch/LLC misses, memory bandwidth, ledger/RSS peaks, and exact
  hashes before widening variable-width output beyond four workers.

## v64 native Parquet input and all-format concurrency audit

- [x] Audit every supported input/output route against operation policy, the
  shared native CPU arena, bounded async I/O, deterministic ordering, and the
  single public memory limit.
- [x] Confirm that all formats are policy-integrated but that native Parquet
  page/column decode, ordered JSON-array/XML framing, PyArrow fallback work,
  and final third-party adapter construction were not all native-arena work.
- [x] Attach native Parquet input to the already-existing operation arena and
  decode independent row-group columns on the upstream lane.
- [x] Give every selected worker a private lazy file handle and private page
  scratch; commit Arrow children strictly by original column ordinal.
- [x] Keep single mode, small windows, one-column projections, and unavailable
  arenas on the exact previous serial loop.
- [x] Bound workers by arena width, columns, useful estimated output, a ceiling
  of 32, and a saturating aggregate scratch estimate limited to one quarter of
  the derived native-reader buffer.
- [x] Measure isolated wide Parquet decode: 367.555 ms (1), 226.962 ms (2),
  172.315 ms (4), and 150.530 ms (5 paired median), preserving 60,000 rows and
  one ordered batch.
- [x] Add source contracts and byte-identical single/multi Parquet rewrite
  coverage without PyArrow.
- [ ] Profile large single-file JSON-array and XML framing before attempting
  parallel range discovery; reject any design that duplicates parsing, changes
  first-error order, or retains a complete document.
- [ ] Repeat Parquet input at 1/2/4/8/16 physical CPUs across uncompressed, GZIP,
  projected, nested and list-heavy fixtures with fixed NUMA, RSS/ledger, I/O,
  branch/LLC and exact logical-output measurements.

## v65 persistent native Parquet column-worker resources

- [x] Reject JSON-array cross-file prefetch and direct raw-row experiments after
  complete-pipeline regressions or results indistinguishable from noise.
- [x] Identify repeated construction of column worker state and repeated file
  opens on every native Parquet row window as the dominant multi-window input
  overhead left after v64.
- [x] Retain one private binary input handle and page scratch area per selected
  operation-arena worker for the lifetime of the native Parquet stream.
- [x] Reuse those resources only across windows of the same stream; release all
  parallel decode scratch at each row-group boundary while preserving handles.
- [x] Keep single mode on the exact serial loop and create no pool, worker, queue,
  public option, environment variable, or additional memory budget.
- [x] Measure five alternating v64/v65 pairs: paired median reductions of 32.38%
  at two workers and 18.27% at four, with all multi-worker pairs winning.
- [x] Measure seven additional pairs at five workers: 21.28% paired median
  reduction, with all seven pairs winning.
- [x] Verify a monotonic nine-sample v65 curve of 1,167.700 ms (1), 787.163 ms
  (2), 621.320 ms (4), and 611.684 ms (5), or 1.00x/1.48x/1.88x/1.91x.
- [ ] Repeat the multi-window Parquet matrix at 8/16 physical CPUs with fixed
  affinity/NUMA, open/read syscall counts, storage-cache controls, RSS/ledger,
  projected/nested fixtures, compression variants, and exact logical hashes.

## v66 bounded fixed-cost Parquet column groups

- [x] Re-profile v65 multi-window fixed-wide Parquet input and identify one
  ordered task per column and window as the dominant remaining coordinator cost.
- [x] Partition only sustained flat fixed-cost columns into contiguous,
  cost-balanced ranges, with at most one range task per selected worker/window.
- [x] Preserve original column and earliest-failure order by ordering ranges and
  decoding every range strictly from its first column to its last.
- [x] Keep strings, variable binary, delta-length byte arrays, variable
  dictionaries, repeated/list layouts, and narrow schemas on the exact v65
  one-column-task route.
- [x] Reuse v65 private worker handles/scratch and the operation-wide upstream
  arena; add no worker, pool, queue, reorder slot, option, environment variable,
  or memory budget.
- [x] Reduce the 400,000 x 32 fixed-wide fixture from 416 input tasks to 26/52/65
  at 2/4/5 workers.
- [x] Measure seven alternating pairs: 10.38%, 17.34%, and 12.88% lower paired
  median wall at 2/4/5 workers, with effectively unchanged RSS.
- [x] Verify a monotonic v66 curve of 650.316 ms, 412.342 ms, 308.799 ms, and
  258.860 ms for 1/2/4/5 workers, or 1.00x/1.58x/2.11x/2.51x.
- [x] Reject grouping variable-width columns after skewed string fixtures
  regressed; retain dynamic per-column scheduling for those inputs.
- [ ] Repeat fixed-cost grouping at 8/16 physical CPUs with fixed affinity/NUMA,
  grouped-task imbalance, read syscall counts, compression/projection variants,
  IPC, LLC misses, RSS/ledger peaks, and exact logical hashes.

## v67 direct CSV scalar output and interleaved packet sizing

- [x] Identify the per-cell JSON render/decode round trip and JSON-object-based
  CSV estimate as redundant work in the concurrent native CSV output stage.
- [x] Render null, boolean, integer, floating-point, string, and large-string
  CSV cells directly from validated Arrow buffers.
- [x] Retain the generic JSON-token fallback for temporal, decimal, binary,
  dictionary, nested, and all other supported kinds.
- [x] Replace JSON-object row estimation with strict CSV cell bounds, delimiters,
  newline accounting, and a 2x interleave margin.
- [x] Reduce the focused wide-scalar output task count from 116 to 86 without
  changing queue/reorder slots or tracked peak memory.
- [x] Measure paired wall reductions of 3.90%, 1.60%, and 0.88% at 2/4/5 CPUs,
  while reducing aggregate output-worker CPU by roughly 30-35%.
- [x] Verify a monotonic v67 curve of 917.226, 370.268, 217.173, and 211.435 ms
  for 1/2/4/5 CPU affinity, or 1.00x/2.48x/4.22x/4.34x.
- [x] Reject exact 57-task sizing after longer output tasks regressed four-CPU
  end-to-end latency; retain the measured 86-task interleaving point.
- [ ] Repeat CSV output at 8/16 physical CPUs across numeric, string-heavy,
  temporal, nested, and skewed schemas with fixed affinity/NUMA, allocator,
  branch/LLC, memory-ledger, RSS, and exact-byte measurements.

## v68 direct CSV logical scalar rendering

- [x] Remove the remaining JSON-temporary/decode round trip for native CSV binary,
  timestamp, date, time, duration, and decimal scalar cells.
- [x] Reuse the authoritative JSONL base64, temporal, duration, and decimal
  formatters with an internal quote-control argument instead of duplicating text
  semantics in the CSV writer.
- [x] Preserve the generic JSON fallback for intervals, dictionaries, and nested
  values whose CSV representation still requires canonical JSON text.
- [x] Keep worker ceilings, queues, reorder slots, public options, and the single
  `memory_limit_bytes` contract unchanged.
- [x] Verify byte-identical single/multi CSV output for parsed temporal values and
  manually constructed Arrow binary/decimal/duration arrays.
- [x] Measure a monotonic v68 wall-time curve of 833.077, 341.542, 241.164, and
  236.188 ms at 1/2/4/5 CPU affinity.
- [x] Reject raising the CSV output ceiling to five workers and run-based quote
  escaping because they regressed one or more available CPU configurations.

## v69 compact generic inference key evidence

- [x] Re-profile nested JSONL inference after v68 and identify repeated
  packet-local field-name copies as the dominant avoidable allocation in the
  generic parallel evidence path.
- [x] Store each distinct key once in a tracked contiguous byte buffer and let
  preorder evidence nodes retain bounded 32-bit key indices.
- [x] Use a compact open-addressing table with full-byte collision checks,
  avoiding one allocator-owned hash node per distinct key.
- [x] Cache the ordered reducer's global `StrId` once per distinct packet key so
  shape and statistics passes do not repeat interner lookups.
- [x] Reduce `InferenceEvidenceNode` from 56 to 24 bytes and keep key count,
  lengths, offsets, and aggregate storage under explicit 32-bit bounds.
- [x] Charge all key storage to the existing packet-local PMR and preserve the
  single public `memory_limit_bytes` contract.
- [x] Measure preparation-only paired median improvements of 11.55% at two CPUs
  and 10.10% at four, with tracked-peak reductions of 58.44% and 59.94%.
- [x] Verify a monotonic complete v69 curve of 5,483.968, 3,195.261, and
  1,770.876 ms for 1/2/4 CPU execution, or 1.00x/1.72x/3.10x.
- [x] Preserve exact schema payload, field order, diagnostics, first error, and
  low-memory behavior for repeated-key and high-cardinality nested fixtures.
- [x] Split compact-key ownership into a 149-line implementation unit and keep
  the generic builder below 500 lines.
- [x] Reject nested-CSV scratch reuse, fallback packet retuning, direct recursive
  CSV serialization, and redundant-wakeup suppression after regressions, RSS
  growth, or unstable complete-pipeline results.
- [ ] Validate generic nested inference at 8/16 physical CPUs on a host whose
  affinity and cgroup quota both expose those processors, recording allocator,
  LLC, ledger/RSS, promotion order, and exact schema/output hashes.

## v70 compact generic inference indices and adaptive trusted reduction

- [x] Compact generic subtree indices from host `size_t` to checked 32-bit
  packet-local indices, reducing evidence nodes from 24 to 16 bytes.
- [x] Compact evidence row begin/end/source-byte fields from 24 to 12 bytes with
  explicit pre-narrowing limits.
- [x] Preserve complete authoritative shape validation for every concurrency
  configuration.
- [x] Compile two statistics traversals and select the trusted branch-free
  specialization only for packets built with at least four effective workers.
- [x] Retain the original validated statistics traversal below four workers,
  avoiding the unstable low-concurrency result of the universal trusted path.
- [x] Measure long-block inference reductions of 1.76% at two CPUs and 3.80% at
  four, with tracked-peak reductions of 28.86% and 23.91%.
- [x] Verify a monotonic complete v70 curve of 1,028.569, 486.993, and 297.489 ms
  for 1/2/4 CPU execution, or 1.00x/2.11x/3.46x.
- [x] Preserve exact schema payload, field order, diagnostics, CSV bytes, and
  SHA-256 across single/multi execution.
- [x] Reject path-ID caching, packed 12-byte nodes, and universal low-worker
  trusted reduction after regressions or unstable measurements.
- [ ] Repeat generic nested inference at 8/16 physical CPUs with fixed affinity,
  matching cgroup quota, NUMA placement, allocator/LLC counters, tracked/RSS
  peaks, and exact schema/output hashes.

## v71 complete input/output concurrency coverage

- [x] Inventory every supported input and output and define the guarantee as
  participation in the common bounded operation arena, with a concurrent stage
  on sufficiently large eligible work.
- [x] Add a machine-readable coverage matrix for CSV, JSON, JSON array,
  JSONL/NDJSON, XML, Parquet, PyArrow, pandas, Polars, DuckDB, and all three
  native file sinks.
- [x] Inject the operation task arena into CSV and XML frontends and preserve
  grouped JSONL arena propagation, instead of limiting arena visibility to
  downstream inference/materialization.
- [x] Parallelize CSV record decoding after quote-aware serial framing, using
  bounded contiguous ranges, task-local tracked arenas, ordered commit, and
  earliest-ordinal failure.
- [x] Parallelize sufficiently large `xml_row_tag` elements after ordered tag
  framing, with memory-accounted staging, at most two contiguous ranges per
  worker, and strict ordinal commit.
- [x] Preserve serial fallback for small/narrow/one-row formats where task
  startup would make `multi` slower.
- [x] Verify CSV scaling of 1,860.479, 1,727.688, and 1,680.334 ms at 1/2/4 CPU
  affinity and large-row XML scaling of 841.951, 672.784, and 529.832 ms.
- [x] Add runtime telemetry contracts proving eligible CSV/XML multi inputs
  publish input work while single publishes zero tasks and returns identical
  schema, order, and diagnostics.
- [x] Guard every output: native CSV/JSONL/Parquet sinks own parallel output
  stages; PyArrow/pandas/Polars/DuckDB receive the same parallel native Arrow
  stream before their external adapter step.
- [ ] Repeat the complete format matrix at 8/16 physical CPUs with matching
  affinity and cgroup quota, fixed NUMA placement, RSS/ledger peaks, task counts,
  and exact logical/output hashes.

## v72 JSON worker-authoritative arrays and output guarantee strengthening

- [x] Separate JSON/JSON-array worker-authoritative raw materialization from the
  existing JSONL deferred-validation mode so JSONL error-policy conditions do
  not change.
- [x] Preserve ordered top-level framing while removing the duplicate
  coordinator parse for planned JSON and JSON-array rows.
- [x] Carry the `json_array` object-only contract as a compact row flag and
  preserve the exact earliest-ordinal public error in workers.
- [x] Extend direct packet-local Arrow construction from JSONL to flat scalar
  JSON arrays, with a 64-row minimum for JSON/JSON-array packets.
- [x] Keep nested/list/struct arrays on the generic ordered worker path and keep
  one-row JSON documents on one useful task.
- [x] Measure paired v71-to-v72 wall improvements of 8.81% for `json_array` and
  9.46% for top-level-array `json`, with 8/8 victories for both routes.
- [x] Reduce tracked peaks by 55.66% and 55.36% by eliminating prepared-row
  retention on eligible flat arrays.
- [x] Verify monotonic 1/2/4 curves of 1,032.033/699.426/588.782 ms for
  `json_array` and 1,038.937/726.548/571.042 ms for `json` arrays.
- [x] Propagate `threading_mode` through analytical and execution-context
  adapters and request pandas `use_threads=False/True` explicitly.
- [x] Extend the machine-readable format contract with unavoidable ordered or
  external boundaries, requiring every input/output to retain a non-empty
  concurrent stage.
- [x] Reject one-row JSON column partitioning after worker/appender setup cost
  exceeded useful work.
- [ ] Repeat the complete seven-input/seven-output matrix at 8/16 physical CPUs
  with matching cgroup quota, fixed NUMA placement, adapter dependencies
  installed, exact logical hashes, task telemetry, and RSS/ledger peaks.

## v73 ordered native Parquet row-group overlap

- [x] Audit the complete seven-input/seven-output matrix and identify native
  Parquet output as the remaining practical four-CPU route that could publish
  zero sink tasks for wide, short Arrow batches.
- [x] Reuse the common output lane to prepare independent row groups with two
  workers on a four-worker arena and at most four workers on larger hosts.
- [x] Preserve single-thread inline execution and avoid writer-owned threads,
  pools, queues, public resource knobs, or environment variables.
- [x] Keep prepared row-group retention within one quarter of the single public
  `memory_limit_bytes` budget and commit strictly by ordinal.
- [x] Detect and fix relative page-index offsets in the asynchronous path by
  encoding indexes only after ordered absolute commit.
- [x] Verify native footer/index readiness and exact logical single/multi data.
- [x] Measure paired four-CPU wall reductions of 8.99% uncompressed, 17.50%
  Snappy, and 34.92% GZIP, with output tasks increasing from zero to 54.
- [x] Verify a monotonic uncompressed curve of 1,216.606, 580.666, and
  328.415 ms for 1/2/4 effective CPUs, or 1.00x/2.10x/3.70x.
- [x] Extend the machine-readable guarantee so every supported input/output has
  an explicit eligible-work benefit proof and declared serial/external boundary.
- [x] Reject universal cross-batch lookahead and short-row-group column tasks
  after regressions in other sinks or excessive task overhead.
- [ ] Repeat all seven inputs and seven outputs at 8/16 physical CPUs with
  matching cgroup quota, fixed NUMA placement, adapter dependencies installed,
  codec variants, task/steal telemetry, RSS/ledger peaks, and exact logical
  hashes.

## v74 adaptive vector CSV record framing

- [x] Re-profile every supported input/output after v73 and identify ordered CSV
  record framing as the weakest remaining native input boundary.
- [x] Add adaptive vector byte search for wide CSV records while preserving one
  authoritative quote-aware ordered scanner.
- [x] Cache the next line break per chunk and avoid rescanning the same suffix
  after each quoted segment.
- [x] Keep short rows scalar and fall back after four nearby quote transitions,
  preventing the quote-dense near-quadratic prototype.
- [x] Preserve escaped quote pairs across chunk boundaries, CRLF, multiline
  fields, maximum record limits, source ownership, and earliest error ordinal.
- [x] Record the framing acceleration in the machine-readable seven-input and
  seven-output concurrency guarantee without hiding ordered/external borders.
- [x] Measure paired four-CPU reductions of 71.47% for wide sparse CSV and
  17.84% for dense short quoted CSV, both with 5/5 wins.
- [x] Verify monotonic 1/2/4 CPU curves for both sparse-wide and dense-short CSV.
- [x] Verify that the faster CSV input propagates to native CSV, JSONL, and
  Parquet sinks, reducing wall time by 65.08%, 62.78%, and 59.12% respectively.
- [x] Add chunk-boundary and telemetry regressions proving exact single/multi
  values, zero single input tasks, and real eligible multi activity.
- [ ] Repeat the complete seven-input/seven-output matrix at 8/16 physical CPUs
  with matching cgroup quota, fixed NUMA placement, adapter dependencies,
  hardware counters, exact logical hashes, and RSS/ledger peaks.

## v75 worker-authoritative structural JSON framing

- [x] Remove the duplicate full coordinator syntax walk for eligible `json` and
  `json_array` rows whose authoritative parse already belongs to workers.
- [x] Add a bounded 64-level structural framer that respects strings, escapes,
  object/array nesting, and exact top-level value boundaries.
- [x] Fall back to the canonical span scanner for deep, incomplete, or suspicious
  values so public diagnostics and first-error order remain unchanged.
- [x] Fix primitive JSON values split exactly at an input-chunk boundary instead
  of accepting the partial prefix as a complete scalar.
- [x] Preserve the separate JSONL deferred-validation policy and avoid changing
  its `on_error` behavior.
- [x] Measure paired four-CPU wall/CPU reductions of 8.52%/9.03% for
  `json_array`, with 4/4 wins, and 8.51%/7.83% for top-level-array `json`.
- [x] Verify a monotonic `json_array` curve of 1,316.655, 843.785, and
  731.046 ms at 1/2/4 effective CPUs.
- [x] Verify that the faster input propagates to native CSV, JSONL, and Parquet
  sinks and retain the complete seven-input/seven-output coverage contract.
- [ ] Repeat the complete format matrix at 8/16 physical CPUs with matching
  cgroup quota, adapter dependencies, hardware counters, exact logical hashes,
  and RSS/ledger peaks.

## v76 first-class pure-Python input concurrency

- [x] Promote pure-Python row streams to an eighth machine-readable input
  contract with explicit GIL and ordered replay boundaries.
- [x] Accept lists, tuples, and one-shot iterables/generators of dictionaries in
  all seven public analytical and native file-output converters.
- [x] Preserve `input_mode="single_file"` semantics for one ordered Python row
  stream and reject incompatible explicit file formats.
- [x] Add an ABI3 iterator encoder that consumes at most 4,096 rows per call,
  avoiding one Python/native transition and one temporary Python list per row.
- [x] Keep Python object iteration GIL-bound and avoid a Python producer thread;
  single remains completely inline while multi feeds the existing C++ arena.
- [x] Route Python rows through the JSONL packet path so inference,
  materialization, CSV, JSONL, Parquet, PyArrow, pandas, Polars, and DuckDB use
  the same integral pipeline as file inputs.
- [x] Keep one-shot iterables replayable through the bounded operation-derived
  spool without materializing the source iterable.
- [x] Move sequence source-byte accounting into the first native encoding pass,
  removing the duplicate Python `json.dump` traversal while preserving typed
  `memory_limit_bytes` errors.
- [x] Measure the previous one-row iterator route against v76: wall reductions
  of 37.18% at two CPUs and 43.24% at four CPUs on 50,000 rows x 24 columns.
- [x] Verify a monotonic generator curve of 1,280.833, 676.611, and 534.856 ms
  at 1/2/4 effective CPUs, or 1.00x/1.89x/2.39x.
- [x] Verify sequence conversion at 299.087 ms on four CPUs after removing the
  802.122 ms duplicate Python preflight.
- [x] Add runtime contracts for public CSV/JSONL/Parquet output, generator
  replay, ordinal row errors, single/multi parity, native task activity, and
  optional analytical adapters.
- [ ] Repeat Python-input scaling on 8/16 physical CPUs with matching cgroup
  quota, adapter dependencies installed, generator I/O stalls, hardware
  counters, exact logical hashes, and replay-spool filesystem variants.

## v77 progressive single-encode Python replay and pairwise guarantee

- [x] Record one bounded encoded replay stream for both reusable Python
  sequences and one-shot iterables.
- [x] Keep sequence and generator source cursors monotonic so every row is
  encoded or iterated exactly once across schema probing and final execution.
- [x] Remove the full-generator drain from `seek(0)` and replay the recorded
  prefix immediately before progressively extending the same spool.
- [x] Preserve the single public memory limit, replay-spool ceiling, temporary
  disk admission, exact order, earliest ordinal error, and strict single mode.
- [x] Publish an explicit machine-readable 8-input x 7-output matrix containing
  input stages, shared arena/Arrow handoff, output stages, boundaries, and a
  composed benefit proof for all 56 pairs.
- [x] Verify repeated replay equality, exactly-once sequence encoding,
  exactly-once generator iteration, and CSV/JSONL/Parquet sink coverage.
- [x] Measure sequence wall reductions of 20.60%/19.09%/18.68% for
  CSV/JSONL/Parquet and generator reductions of 7.67%/10.64%/6.24%.
- [ ] Repeat the complete 56-pair matrix on physical 8/16-CPU hosts with
  adapter dependencies, fixed NUMA placement, replay filesystem variants,
  hardware counters, exact logical hashes, and RSS/ledger peaks.

## v78 analytical stream handoffs

- [x] Remove the sanitizer-owned generic `pyarrow.Table` handoff before pandas,
  Polars, and DuckDB adapter conversion.
- [x] Route public analytical conversion and internal `ExecutionContext` adapter
  sinks through one `RecordBatchReader` conversion boundary.
- [x] Preserve the direct Arrow C Stream to `pyarrow.Table` path because Table is
  the exact public PyArrow output type.
- [x] Forward strict single/multi policy to pandas `use_threads=False/True` and
  report Arrow's library-internal table materialization explicitly.
- [x] Pass the reader directly to Polars without a sanitizer-owned full Arrow
  table.
- [x] Bind DuckDB through an ordered batch-backed Arrow dataset, preserving
  schema and lazy relation ownership without a one-shot reader dataset.
- [x] Add per-result terminal conversion-route telemetry and exact reader/resource
  closure contracts.
- [x] Extend every one of the 56 input/output guarantees with terminal handoff,
  explicit-table, and adapter-internal-table metadata.
- [x] Verify concurrency v29-v78 plus API, sink, lifecycle, options, and
  maintenance regressions in the available environment.
- [ ] Repeat all analytical routes with PyArrow, pandas, Polars, and DuckDB
  installed together on physical 8/16-CPU hosts, recording wall time, first-batch
  latency, peak RSS, Arrow pool bytes, exact logical hashes, and retained batches.

## v80 low-core worker-sharded task telemetry

- [x] Move 2-4-worker task completion telemetry from contended operation-global
  atomics into 32 fixed cache-line-aligned physical-worker shards.
- [x] Batch up to eight completions locally and flush residual values on idle or
  worker destruction while preserving exact submitted/started/finished totals,
  queue/run sums, and maxima.
- [x] Aggregate global inline values and worker shards only when producing the
  operation telemetry document.
- [x] Preserve the v79 immediate path for 5-8 workers and the established
  high-core 32-task batching above eight workers.
- [x] Extend all 56 input/output contracts with the common worker-sharded arena
  stage and add runtime-verifiable shard-batch telemetry.
- [x] Measure a paired four-CPU scheduler reduction of 2.29% on 250,000 exact
  task completions without changing output, task count, memory, or API controls.
- [ ] Repeat at physical 2/4/8/16/32 CPU with fixed NUMA placement and hardware
  cache-coherence counters, then run the complete 56-pair matrix.

## v81 worker-active streak accounting

- [x] Keep each arena worker active across adjacent local or stolen packets until
  it has exhausted compatible work and is about to park.
- [x] Replace per-packet global active increments/decrements and running-state
  stores with one start and one stop transition per worker busy streak.
- [x] Recheck the local queue while holding its mutex before clearing `running`,
  so high-core wake coalescing cannot strand work appended after a failed steal.
- [x] Preserve exact peak-active accounting, `active_tasks()` drain semantics,
  cancellation, output preference, lane compatibility, and lazy bounded workers.
- [x] Expose `worker_active_streaks` in operation telemetry and add the common
  `worker_active_streak_accounting` stage to all 56 source-to-sink contracts.
- [x] Measure paired preloaded-queue reductions of 21.76% with two workers and
  33.84% with four workers over 150,000 packets, winning 15/15 comparisons in
  both cases with exact task/peak/drain counts.
- [ ] Repeat on physical 8/16/32 CPU hosts and run the complete 56-pair physical
  adapter matrix with exact hashes, RSS, queue depth, active streaks, task count,
  and hardware coherence counters.

## v82 targeted worker wake epochs

- [x] Replace the operation-global `work_epoch` with one cache-line-aligned wake
  generation per physical worker slot.
- [x] Publish a generation and notification only for an inactive target or a
  real idle compatible helper.
- [x] Reuse the v81 under-mutex queue recheck so a running target cannot park
  past an unnotified appended packet.
- [x] Keep helper wakeups generation-based so notification timing alone cannot
  lose work.
- [x] Add the targeted-wake and running-worker coalescing stages to all 56
  source-to-sink concurrency contracts.
- [x] Measure paired streaming reductions of 11.82% at two workers and 7.72% at
  four workers over 200,000 packets.
- [x] Complete 1,000 mixed-lane park/wake cycles at 2/4/8/16 workers with exact
  drain and activity counts.
- [ ] Repeat on physical 8/16/32-CPU hosts with hardware coherence counters and
  the complete adapter-backed 56-pair matrix.

## v83 telemetry-aware clocks and park-boundary epoch sampling

- [x] Remove the private wake-epoch acquire load performed after every completed
  packet and refresh the generation only under the queue mutex before parking
  and after waking.
- [x] Preserve v81/v82 lost-wake safety, targeted notifications, shutdown, lane
  compatibility, cancellation, and exact drain accounting.
- [x] Avoid enqueue, run-start, and inline clock reads when an operation arena
  has no `PerformanceTelemetry` consumer.
- [x] Cache the immutable telemetry pointer once per worker while retaining exact
  queue-wait, run-time, maximum, and batch metrics when telemetry is enabled.
- [x] Add `park_boundary_wake_epoch_sampling` and
  `telemetry_aware_clock_elision` to all 56 source-to-sink contracts.
- [x] Measure telemetry-free reductions of 20.72% with two workers and 21.87%
  with four workers over 200,000 packets, with exact task, queue, activity, and
  wake-generation counts.
- [ ] Repeat telemetry-enabled and telemetry-free matrices on physical
  8/16/32-CPU hosts with every analytical adapter installed.

## v84 transition-only queue visibility and worker initialization

- [x] Publish `nonempty_mask` only when a worker queue changes from zero to one
  queued task; steady-state appends retain existing visibility.
- [x] Publish `initialized_mask` only after a reserved worker acquires its first
  real local task.
- [x] Cache `first_task_pending` in the owning worker and refresh it only at
  startup and under-mutex park/wake boundaries, including startup-failure
  recovery.
- [x] Preserve exact lanes, stealing, output preference, targeted wakes, queue
  depth, task counts, cancellation, order, first error, and memory bounds.
- [x] Add `empty_to_nonempty_queue_visibility`,
  `one_shot_worker_initialization_publication`, and
  `park_boundary_first_task_sampling` to all 56 source-to-sink contracts.
- [x] Measure isolated 200,000-task median reductions of 6.58% with two workers
  and 2.92% with four workers against v83 on the available host.
- [ ] Repeat with hardware cache-coherence counters and the complete physical
  8/16/32-CPU adapter matrix.

## v91 worker-count-sharded external completion accounting

- [x] Replace the remaining executor-global external-completion RMW with up to
  32 cache-line-aligned lifetime shards.
- [x] Assign consecutive admitted packets through a mutex-owned circular shard
  cursor without integer division or changes to arena task placement.
- [x] Carry the selected shard through `ExternalTaskLease` so failed admission,
  queued-task destruction, cancellation, and normal completion reach the same
  exact lifetime counter.
- [x] Snapshot and wait for exact scheduled totals independently per shard,
  removing the now-redundant aggregate `scheduled_tasks_` counter.
- [x] Add `worker_count_sharded_external_completion_accounting` to every one of
  the 56 input/output pair contracts, including pure-Python rows.
- [x] Validate exact drain and cancellation at 2/4/5/8/16 workers and repeat
  fresh-process stress runs without stranded tasks.
- [x] Measure isolated median reductions of 6.57% at four logical workers,
  13.56% at eight, and 6.36% at sixteen; the latter two oversubscribe the
  available five-CPU host and are scheduler evidence rather than physical
  scaling proof.
- [ ] Repeat the full 56-pair matrix on fixed 8/16/32-CPU hosts with hardware
  cache-coherence counters, exact hashes, first-error ordinals, and RSS/ledger
  peaks.

## v92 high-core executor-local arena submission tickets

- [x] Seed each >8-worker arena-backed `OrderedExecutor` once from the shared
  lane cursor.
- [x] Advance a plain local ticket inside the existing high-core coordinator
  mutex and pass it explicitly to the arena for every subsequent packet.
- [x] Restore the complete 1–8-worker packet submission path exactly to v91.
- [x] Preserve the shared atomic cursor for direct concurrent arena producers.
- [x] Cover failed-admission rollback without rewinding a potentially observed
  worker-selection ticket.
- [x] Add `high_core_executor_local_arena_submission_tickets` to all 56 I/O
  contracts, including pure-Python rows and every analytical output adapter.
- [x] Measure a paired median reduction of 3.26% at sixteen workers over
  300,000 identical short packets, winning 7/13 alternating comparisons;
  record the noisy -0.97% absolute-median difference on the five-CPU host.
- [x] Reject queue-depth, in-flight, universal-lock, and low-core local-ticket
  candidates whose A/B results shifted cost between worker counts.
- [ ] Repeat the complete adapter matrix on fixed 8/16/32-CPU hosts with lane
  balance, cursor coherence, exact hashes, first errors, and RSS peaks.

## v93 mutex-owned queue counters with single-store publication

- [x] Replace per-queue `queued.fetch_add/fetch_sub` operations with exact
  mutex-owned `queued_local` increments/decrements and one atomic snapshot
  store per mutation.
- [x] Replace the submitted-shard atomic load-plus-store sequence with a plain
  mutex-owned `submitted_local` increment and one atomic snapshot store.
- [x] Preserve lock-free exact `queued_tasks()` and `submitted_tasks()` reads,
  empty-to-nonempty visibility, stealing, targeted wakes, and shutdown drain.
- [x] Reset both private and published queue depth after worker shutdown.
- [x] Add `mutex_owned_queue_counters_single_store_publication` to all 56
  input/output contracts, including pure-Python rows and every analytical sink.
- [x] Validate ordered completion and concurrent direct producers at
  2/4/5/8/16 workers with exact submitted totals and zero final queue depth.
- [x] Measure paired prequeue median reductions of 2.15%/2.53%/4.13%/3.30%/
  5.99% at 2/4/5/8/16 workers respectively on the available five-CPU host.
- [x] Reject full mask selection, high-core modulo overloads, and ticket-derived
  completion affinity after their A/B results regressed at least one worker lane.
- [ ] Repeat the complete 56-pair matrix on fixed 8/16/32-CPU hosts with
  cache-coherence counters, exact hashes, first-error ordinals, and RSS/ledger
  peaks.

## v94 successful-drain-only queue visibility

- [x] Remove `nonempty_mask` RMWs from failed local probes.
- [x] Remove repeated empty-mask RMWs after a stale remote candidate is
  validated empty under its queue mutex.
- [x] Keep zero-to-one and successful last-packet one-to-zero publications
  exact under the queue mutex.
- [x] Preserve the existing acquire/release ordering for real zero-to-one and
  one-to-zero visibility transitions.
- [x] Add the shared stage to all 56 input/output pair contracts, including
  pure-Python rows and every analytical output.
- [ ] Repeat with fixed affinity and cache-coherence counters on physical
  8/16/32-CPU hosts, then run the complete adapter-backed 56-pair matrix.

## v95 single-store worker-local steal publication

- [x] Replace the arena worker shard's atomic load-plus-store with a plain
  writer-local increment and one relaxed atomic snapshot store.
- [x] Apply the same ownership rule to optional performance telemetry.
- [x] Preserve lock-free exact bounded diagnostics and all stealing semantics.
- [x] Add the stage to all 56 input/output contracts, including pure-Python.
- [x] Validate forced stealing and mixed lanes at 4/8/16 workers.
- [ ] Repeat end-to-end with fixed affinity and hardware coherence counters on
  physical 8/16/32-CPU hosts.

## v96 authoritative started-mask admission fast path

- [x] Extend the established started-mask fast path to sustained 4–8-worker
  arenas without changing the 2–3-worker admission path.
- [x] Publish the bit only after the `std::jthread` owner is installed under
  `start_mutex`, preserving acquire/release startup safety.
- [x] Remove the redundant operation-global `started` counter and derive exact
  diagnostics with bounded `std::popcount(started_mask)`.
- [x] Add `authoritative_started_mask_start_lock_elision` to all 56 input/output
  contracts, including pure-Python rows and every analytical sink.
- [x] Validate lazy startup, ordered completion, mixed lanes, stealing, and
  exact drain at 2/4/5/8/16 workers.
- [ ] Repeat the complete matrix on fixed 8/16/32-CPU hosts with mutex and
  coherence counters, exact hashes, first-error ordinals, and RSS peaks.

## v98 stop-token-authoritative parallel worker loops

- [x] Compile a distinct worker-loop shutdown policy instead of branching on
  worker count for every packet.
- [x] Retain the v97 `stopping` acquire check for 2-3 workers.
- [x] Use the worker-owned jthread stop token as the packet-loop authority for
  4-32 workers while preserving `stopping` in admission, startup, and park.
- [x] Pass the same token into every task so active work observes shutdown and
  stage cancellation through the existing cooperative contract.
- [x] Add `stop_token_authoritative_high_core_worker_loop` to all 56 I/O pair
  contracts, including pure-Python rows and all analytical outputs.
- [x] Validate order, cancellation, exact counters, and zero drain at
  2/4/5/8/16 workers with the real ABI3 module.
- [x] Measure isolated paired reductions of 3.62%, 8.19%, 11.01%, and 11.44%
  at 4/5/8/16 writers respectively; treat oversubscribed native 16-worker wall
  time as inconclusive on the available five-CPU host.
- [ ] Repeat the complete adapter matrix on fixed 8/16/32-CPU machines with
  hardware coherence counters, cancellation latency, exact hashes, and RSS.

## v99 initialized-worker snapshot admission

- [x] Establish and document `initialized_mask ⊆ started_mask` and
  `initialized_mask ⊆ admitted_mask` throughout normal arena admission.
- [x] Reuse one initialized-worker snapshot for idle selection, saturated-lane
  reservation elision, and the per-worker startup fast path.
- [x] Keep stale snapshots conservative and preserve startup failure rollback,
  queue mutex ordering, stopping checks, and targeted wake generations.
- [x] Add `initialized_worker_snapshot_admission_elision` to all 56 input/output
  pair contracts, including pure-Python rows and analytical outputs.
- [x] Measure 200,000 saturated admissions at 2/4/5/8/16 workers with exact
  submitted totals, started workers, and zero final queue depth.
- [ ] Repeat the complete adapter matrix on fixed 8/16/32-CPU hosts with
  hardware coherence counters, exact hashes, first-error ordinals, and RSS.

## v100 single-sentinel external task leases

- [x] Make `owner_` the sole mutable ownership sentinel in `ExternalTaskLease`.
- [x] Clear one pointer instead of owner plus callback on every move and normal completion.
- [x] Preserve defensive null-callback handling and exactly-once abandonment.
- [x] Add `single_sentinel_external_task_lease_completion` to all 56 I/O contracts.
- [x] Validate native order, cancellation, external drain, and 2/4/5/8/16 workers.
- [x] Measure a 4.89% paired median reduction over 21 isolated fresh-process comparisons and a 4,096-byte ABI3 size reduction.
- [ ] Repeat full adapter throughput and closure-allocation profiling on fixed 8/16/32-CPU hosts.

## v101 compile-time abandonment and single-copy lease shards

- [x] Move the fixed external-task abandonment callback from per-task storage to
  a non-type template policy.
- [x] Keep `owner_` as the sole mutable ownership sentinel and preserve
  exactly-once abandonment.
- [x] Reuse the lease-owned shard during execution and remove the duplicate
  completion-shard lambda capture in both arena submission paths.
- [x] Reduce the 64-bit lease from 24 to 16 bytes and each ordered arena closure
  by 16 bytes relative to v100.
- [x] Add `compile_time_abandonment_single_shard_lease` to all 56 input/output
  contracts, including pure-Python rows and every analytical output.
- [x] Measure a 2.38% paired-median isolated reduction with 16/21 wins and a
  944-byte ABI3 Release size reduction under identical build flags.
- [ ] Repeat adapter-backed allocator/cache profiling on fixed 8/16/32-CPU
  hosts with exact hashes, first-error ordinals, cancellation latency, and RSS.

## v102 typed-owner member abandonment lease

- [x] Replace the `void*` lease owner with the concrete executor owner type.
- [x] Replace the abandonment thunk and cast with a compile-time member pointer.
- [x] Preserve the 16-byte lease and owner-only exactly-once sentinel.
- [x] Add `typed_owner_member_abandonment_lease` to all 56 I/O contracts.
- [ ] Repeat allocator and instruction-cache profiling on fixed 8/16/32-CPU hosts.

## v103 single-snapshot arena terminal flags

- [x] Replace separate cancellation and fatal atomics with independent bits in one monotonic terminal word.
- [x] Reduce normal arena outcome publication from two acquire loads to one.
- [x] Preserve cancellation precedence, fatal terminalization, order, drain, Arrow ownership and bounded memory.
- [x] Add `single_snapshot_arena_terminal_flags` to all 56 input/output contracts.
- [ ] Repeat complete adapter throughput on fixed 8/16/32-CPU hosts with hardware cache/coherence counters.

## v104 high-core single-writer in-flight publication

- [x] Prove that every `in_flight_` writer executes while holding the executor mutex.
- [x] Preserve the historical atomic RMW path for inline, 2-8-worker, local-pool and ordered-consumption paths.
- [x] Replace only the >8-worker arena submission increment with a mutex-serialized relaxed load plus release store.
- [x] Keep `in_flight()` lock-free with its acquire snapshot and preserve exact dispatch-window decisions.
- [x] Add `high_core_single_writer_in_flight_publication` to all 56 input/output contracts, including pure-Python input.
- [x] Validate order, rollback, cancellation, exact drain and bounded completion slots at 2/4/5/8/16 workers.
- [ ] Repeat 8-to-16 and 16-to-32 throughput on fixed physical hosts with cache-coherence counters.

## v105 mutex-owned memory-order tightening

- [x] Prove that internal dispatch-window and end-of-stream decisions using
  `in_flight_` hold `mutex_` and therefore do not need acquire loads.
- [x] Keep the public `in_flight()` acquire snapshot and all release
  publications unchanged for external coordination.
- [x] Reduce the arena slot claim from acquire-release to acquire-only; the
  later ready-state store remains the sole result publication release.
- [x] Relax only the second ready-state validation after the earlier acquire
  and the authoritative executor mutex.
- [x] Remove the unread `completed_count_` field and all three dead writes.
- [x] Add `mutex_owned_memory_order_tightening` to all 56 I/O contracts,
  including pure-Python rows and every output adapter.
- [ ] Repeat the adapter matrix on ARM64 and fixed 8/16/32-CPU hosts to measure
  the weak-memory barrier reduction with hardware counters.

## v107 single-store worker-local task telemetry

- [x] Make the existing 2–4-worker telemetry shard ownership explicit with
  private cumulative task, queue-wait, run-time, maximum, and batch totals.
- [x] Replace four `fetch_add` operations, two potential maximum CAS loops, and
  one batch-count RMW per worker flush with plain accumulation plus relaxed
  atomic snapshot stores.
- [x] Preserve concurrent lock-free telemetry reads and historical modulo bit
  patterns through unsigned local accumulation and `std::bit_cast`.
- [x] Keep the conservative v80 gates unchanged for one, 5–8, and >8 workers.
- [x] Add `single_store_worker_local_task_telemetry_publication` to all 56
  input/output contracts, including pure-Python input and analytical outputs.
- [x] Measure a 77.75% paired-median reduction in the isolated six-kind flush
  benchmark with 15/15 candidate wins.
- [ ] Repeat telemetry-enabled end-to-end matrices on fixed 2/4/8/16/32-CPU
  hosts with coherence counters, exact hashes, first errors, RSS, and ledger
  peaks before broadening the worker-count gate.

## v108 worker-sharded submission telemetry publication

- [x] Add a dedicated cache-line-aligned submission telemetry shard for every
  physical arena queue.
- [x] Reuse the queue mutex that already serializes producers targeting one
  shard; add no new synchronization object.
- [x] Replace the global submitted-task `fetch_add` and peak-depth CAS path for
  multi-worker admission with plain local accumulation and atomic stores.
- [x] Keep producer-written submission shards separate from worker-written task
  shards to avoid producer/worker false sharing.
- [x] Aggregate inline and worker-sharded submitted totals and queue-depth peaks
  in final telemetry and diagnosis.
- [x] Add `single_store_worker_sharded_submission_telemetry` to all 56
  input/output contracts, including pure-Python rows and analytical outputs.
- [x] Measure 33.25% isolated reduction with one producer and 94.13% with four
  producers, both with 15/15 candidate wins.
- [ ] Repeat end-to-end telemetry matrices on fixed 2/4/8/16/32-CPU hosts with
  coherence counters, exact hashes, first-error ordinals, RSS, and ledger peaks.

## v109 all-worker sharded completion telemetry

- [x] Route every valid 2-32-worker arena completion batch to the matching
  cache-line-aligned single-writer telemetry shard.
- [x] Preserve eight-task flushes at 2-8 workers and 32-task flushes above
  eight workers.
- [x] Remove direct operation-global per-task publication from the 5-8-worker
  range and global batch publication from the 9-32-worker range.
- [x] Preserve exact live/final task totals, timing totals, maxima, ordering,
  cancellation, shutdown, and bounded telemetry lag.
- [x] Add `all_worker_sharded_task_completion_telemetry` to all 56 input/output
  contracts, including pure-Python rows and all analytical outputs.
- [x] Validate the real arena at 5/8/16 workers and retain 15/15 benchmark wins
  in every widened range.
- [ ] Repeat end-to-end matrices on fixed 8/16/32-CPU hosts with cache-line
  transfer counters, exact hashes, first-error ordinals, RSS, and ledger peaks.

## v110 worker-local monotonic peak-active cache

- [x] Add one plain high-water mark to each worker activity-streak owner.
- [x] Offer an active count to the operation-global peak only when it exceeds
  every count previously observed by that same physical worker.
- [x] Preserve the exact global active increment/decrement and exact peak CAS
  semantics, including concurrent starts, parks, cancellation, and shutdown.
- [x] Keep worker-running publication and the under-mutex pre-park queue recheck
  unchanged so targeted wake coalescing cannot strand work.
- [x] Add `worker_local_monotonic_peak_active_cache` to all 56 input/output
  contracts, including pure-Python rows and every analytical output.
- [x] Validate repeated real-arena park/wake waves at 4/8/16 workers with exact
  active, peak, started, submitted, and zero-drain values under TSan.
- [x] Record isolated paired median peak-bookkeeping reductions of 17.01%,
  29.40%, and 26.25% at 4/8/16 workers respectively; do not present them as
  end-to-end throughput gains.
- [ ] Repeat the complete adapter matrix on fixed 8/16/32-CPU hosts with exact
  hashes, first-error ordinals, cancellation latency, RSS/ledger peaks, and
  hardware cache-coherence counters.

## v111 single-store worker-active-streak telemetry

- [x] Move the worker-active-streak total from one operation-global RMW to the
  existing cache-line-aligned physical-worker telemetry shard.
- [x] Keep one plain cumulative total per worker and publish an exact relaxed
  atomic snapshot with no shared fetch-add.
- [x] Aggregate the historical global counter plus all bounded worker snapshots
  under the unchanged `worker_active_streaks` telemetry key.
- [x] Preserve exact live activity, peak activity, park/wake behavior, ordering,
  cancellation, drain, and public telemetry schema.
- [x] Add `single_store_worker_active_streak_telemetry` to all 56 input/output
  contracts, including pure-Python rows and every analytical output.
- [x] Record 15/15 isolated wins at 2/4/8/16 workers without claiming
  end-to-end pipeline throughput.
- [ ] Repeat telemetry-enabled adapter matrices on fixed 8/16/32-CPU hosts with
  exact hashes, first-error ordinals, RSS/ledger peaks, and coherence counters.

## v112 monotonic initialized-worker park snapshots

- [x] Treat a cached false `first_task_pending` value as permanent for the
  lifetime of one physical worker and stop reloading the slot atomic on every
  later park and wake.
- [x] Remove the wake-generation sample from the local-work recheck branch; the
  next actual park boundary samples under the same queue mutex.
- [x] Capture the generation that satisfies the condition-variable predicate
  and remove the redundant post-wait acquire load.
- [x] Preserve startup failure publication, targeted wake correctness, stop-token
  shutdown, activity streaks, stealing, order, cancellation, and zero drain.
- [x] Add `monotonic_initialized_worker_park_snapshot_elision` to all 56
  input/output contracts, including pure-Python rows and every analytical
  output.
- [x] Record consistently positive isolated park-boundary snapshot evidence at
  2/4/8/16 workers without claiming condition-variable or end-to-end pipeline
  throughput.
- [ ] Repeat wake-heavy adapter matrices on fixed 8/16/32-CPU hosts with exact
  hashes, first-error ordinals, cancellation latency, RSS/ledger peaks, and
  hardware coherence counters.

## v113 high-core sharded queue visibility

- [x] Preserve one queue-visibility publication shard for 1-8 workers.
- [x] Split 9-32-worker arenas into cache-line-aligned groups of eight workers.
- [x] Publish empty-to-nonempty and final-drain bits only in the owning shard.
- [x] Precompute in each submission plan the bounded visibility-shard pointers
  intersecting its lane, and combine only relevant initialized shards for
  operation-wide stealing.
- [x] Preserve targeted wakes, one-shot startup, deterministic order, earliest
  ordinal failure, cancellation, backpressure, exact counters, and zero drain.
- [x] Add `high_core_sharded_queue_visibility` to all 56 input/output contracts,
  including pure-Python rows and every analytical output.
- [x] Record 15/15 isolated wins at 12/16/32 workers without claiming
  end-to-end parser or sink throughput.
- [ ] Repeat mixed-lane matrices on fixed 16/32-CPU hosts with affinity, NUMA
  placement, exact hashes, first-error ordinals, RSS, and coherence counters.

## v114 cache-line-isolated worker running publication

- [x] Move each physical worker's `running` atomic to an explicit 64-byte
  boundary, away from queue/submission/steal snapshots.
- [x] Preserve all existing acquire/release operations and targeted-wake logic.
- [x] Keep the memory increase bounded to at most one cache line per worker and
  retain the 32-worker arena cap.
- [x] Add `cacheline_isolated_worker_running_publication` to all 56 input/output
  contracts, including pure-Python rows and analytical outputs.
- [x] Validate real arena execution at 2/4/8/16 workers with exact completion,
  activity, peak activity, submitted totals, and zero drain.
- [x] Record isolated paired evidence for 2/4/8 independently contended worker
  pairs without presenting it as end-to-end throughput.
- [ ] Repeat the full adapter matrix on fixed physical hosts with cache-line
  transfer counters, affinity, exact hashes, error ordinals, RSS, and latency.

## v115 sparse-bitset round-robin worker selection

- [x] Preserve the exact ticket-origin round-robin ordering for startup
  reservation and initialized idle-worker selection.
- [x] Split lane-local candidates at the ticket origin and visit only set bits
  with `std::countr_zero`, avoiding a modulo and empty-slot test per lane
  position.
- [x] Retain a direct preferred-worker path when every lane slot is eligible.
- [x] Preserve startup reservation CAS behavior, targeted helper wakes, lane
  isolation, deterministic output, earliest failure, cancellation, bounded
  queues, and zero drain.
- [x] Add `sparse_bitset_round_robin_worker_selection` to all 56 input/output
  contracts, including pure-Python rows and every analytical output.
- [x] Validate exhaustive equivalence through eight-wide lanes, randomized
  equivalence at 16/24/32, and real arena execution at 2/4/8/16/32 workers.
- [x] Record 15/15 isolated wins at 8/16/32 lane widths without presenting the
  result as parser, materializer, sink, lock, or wake-latency throughput.
- [ ] Repeat full adapter matrices on fixed 8/16/32-CPU hosts with affinity,
  exact hashes, first-error ordinals, RSS/ledger peaks, and hardware counters.

## v116 cache-line-isolated arena writer domains

- [x] Place upstream, output, and all-lane submission cursors on independent
  64-byte boundaries without changing their atomic types or ticket semantics.
- [x] Separate stopping/startup state from producer cursor traffic and place
  exact active/peak-active accounting on its own bounded writer domain.
- [x] Preserve relaxed cursor RMWs, exact active fetch-add/fetch-sub operations,
  peak CAS behavior, targeted wakes, stealing, order, cancellation, and drain.
- [x] Add `cacheline_isolated_arena_writer_domains` to all 56 input/output
  contracts, including pure-Python rows and every analytical output.
- [x] Validate concurrent upstream/output/all producers against the real arena
  at 2/4/8/16/32 workers and repeat the scenario under ThreadSanitizer.
- [x] Record 15/15 isolated wins for two, three, and five independent writers
  without presenting the result as end-to-end pipeline throughput.
- [ ] Repeat mixed-stage adapter matrices on fixed 8/16/32-CPU hosts with
  affinity, exact hashes, first-error ordinals, RSS, and coherence counters.

## v117 cache-line-isolated worker wake publication

- [x] Keep each per-worker `wake_epoch` aligned at a 64-byte boundary.
- [x] Align the following queue control block so it cannot occupy the unused
  tail of the wake-generation cache line.
- [x] Preserve release wake increments, acquire park snapshots, notifications,
  running-target coalescing, helper wakes, and shutdown broadcast semantics.
- [x] Keep padding fixed per worker under the existing 32-worker ceiling.
- [x] Add `cacheline_isolated_worker_wake_epoch_publication` to all 56
  input/output contracts, including pure-Python rows and analytical outputs.
- [x] Record 15/15 isolated wins with a queue writer and with a concurrent wake
  observer, without presenting the result as end-to-end throughput.
- [ ] Repeat wake-heavy adapter matrices on fixed 8/16/32-CPU hosts with
  affinity, exact hashes, error ordinals, context switches, RSS, and hardware
  coherence counters.

## v118 single-modulo lane-origin reuse

- [x] Normalize each explicit or reserved submission ticket once per lane.
- [x] Reuse the normalized origin for initialized-idle selection, startup
  reservation, saturated placement, the precompiled alternative, and helper
  selection.
- [x] Replace steady-state `(ticket + delta) % width` operations with bounded
  add/subtract advancement while preserving exact size_t wraparound through a
  rare overflow fallback.
- [x] Keep the historical ticket-taking selection overload for compatibility
  and add an explicit normalized-origin overload used by the arena hot path.
- [x] Add `single_modulo_lane_origin_reuse` to all 56 input/output contracts,
  including pure-Python rows and every analytical output.
- [x] Validate non-power-of-two and power-of-two widths, near-overflow tickets,
  concurrent upstream/output/all producers, ThreadSanitizer, and exact drain.
- [ ] Repeat saturated adapter matrices on fixed 8/16/32-CPU hosts with
  instruction counts, exact hashes, first-error ordinals, RSS, and latency.

## v119 fixed physical queue-visibility snapshots

- [x] Replace high-core initialized-mask shard discovery with fixed 2-4
  physical visibility loads derived from immutable arena width.
- [x] Preserve the single acquire snapshot for 1-8-worker arenas.
- [x] Apply the exact initialized/allowed mask after combining physical shards,
  including partially initialized startup states.
- [x] Keep release transition publication, queue mutexes, victim compatibility,
  deepest-queue selection, telemetry, and drain unchanged.
- [x] Add `fixed_physical_queue_visibility_snapshot` to all 56 input/output
  contracts, including pure-Python rows and every analytical output.
- [x] Validate forced stealing at 9/16/17/24/25/32 workers under native and
  ThreadSanitizer builds.
- [ ] Repeat complete adapter workloads on fixed-affinity 8/16/24/32-CPU hosts
  with steal, cache-coherence, RSS, exact-hash, and first-error measurements.

## v120 compact queued-task lane metadata

- [x] Retain `size_t` lane arithmetic in submission plans and narrow only after
  the lane has been clamped to the 32-worker arena ceiling.
- [x] Store queued packet lane begin/end bounds as `std::uint8_t` and widen them
  explicitly for compatibility, dedicated-output, and relative-worker checks.
- [x] Preserve callable ownership, queued timestamps, telemetry kind, FIFO and
  output preference, compatible stealing, order, cancellation, and drain.
- [x] Add `compact_queued_task_lane_metadata` to all 56 input/output contracts,
  including pure-Python rows and every analytical output.
- [x] Validate all lane kinds and relative worker indices under native and
  ThreadSanitizer builds at low- and high-core widths.
- [x] Record paired fixed-affinity queue-packet evidence without presenting it
  as end-to-end parser, materializer, or sink throughput.
- [ ] Repeat full adapter workloads on fixed 8/16/32-CPU hosts with allocator,
  LLC, queue-depth, exact-hash, first-error, RSS, and latency measurements.
