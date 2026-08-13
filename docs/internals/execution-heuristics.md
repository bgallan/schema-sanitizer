# Execution heuristics

This document records implementation-facing rules for adaptive task arenas,
packet sizing, output routes, and remote staging. These heuristics preserve
the public schema, ordering, error, and resource contracts while remaining
free to evolve with the implementation.

For stable schema behavior, see
[Schema and registry](../reference/schema-and-registry.md). For resource
ownership and limits, see
[Resources and concurrency](../operations/resources-and-concurrency.md).

## Index

- [Operation-wide native task arena](#operation-wide-native-task-arena)
- [Parallel-inference packet heuristics](#parallel-inference-packet-heuristics)
- [Text-output packet heuristics](#text-output-packet-heuristics)
- [Parquet route and storage heuristics](#parquet-route-and-storage-heuristics)
- [Remote staging heuristics](#remote-staging-heuristics)
  - [Partition source lookahead](#partition-source-lookahead)
- [Flat JSONL inference parsing](#flat-jsonl-inference-parsing)
- [Bounded output-lane progress](#bounded-output-lane-progress)
- [Validation-certified positional JSONL materialization](#validation-certified-positional-jsonl-materialization)
- [Direct lexical positional JSONL materialization](#direct-lexical-positional-jsonl-materialization)
- [Scalable arena packet metadata](#scalable-arena-packet-metadata)

## [Operation-wide native task arena](#index)

One `multi` operation owns one native task arena for CPU work. Inference,
materialization, CSV/JSONL packet encoding, and native Parquet column preparation
borrow lanes from that arena instead of constructing stage-local pools. The
physical ceiling is the effective worker count derived from CPU capacity and the
single memory budget. A stage may use fewer workers when its packet count, schema
complexity, or memory reserve cannot amortize all N.

Worker identities are stable for worker-private parser, builder, and compression
state. Upstream lanes occupy the low physical indices and output lanes the high
indices; two narrow adjacent stages can therefore overlap on complementary
workers while the operation-wide active-task peak remains at or below N. A stage
that needs the full arena uses all workers. Workers are started lazily on their
first submitted task, avoiding N-thread startup for small or serially profitable
operations. Worker busy streaks retain exact live active accounting. Each
physical worker also keeps a private monotonic high-water mark for active-count
values it has already offered to the operation-wide peak, so later streaks at
or below that value avoid a redundant shared maximum-counter load without
weakening exact peak diagnostics. Each submission keeps a preferred physical queue for cache locality,
but an idle worker may steal from the back of another queue only when the task's
lane contains the thief. This removes head-of-line stalls behind unusually slow
rows or columns without aliasing worker-private state. The scheduler wakes only
the preferred worker for an empty idle queue; compatible peers are notified when
work is placed behind an active or already queued task. Above eight workers,
a high-half worker may select the earliest local task dedicated to the high
output lane ahead of broad upstream backlog on that same queue. This preference
is local, permits at most one consecutive bypass before forcing FIFO progress,
preserves FIFO among dedicated output tasks, never bypasses the first task
reserved during lazy worker startup, and does not alter stealing or ordinal
publication. Four-to-five-worker arenas enable the narrower shallow-queue
variant described under [Bounded output-lane progress](#bounded-output-lane-progress);
six-through-eight-worker arenas retain strict local FIFO. Queue/reorder bounds
and the operation memory budget are unchanged.

The arena does not own commit order. Each stage retains its bounded ordinal
executor and reorder window, and coordinators remain the only owners of schema
reduction, diagnostics merge, Arrow publication, file byte order, and final
commit. Executor destruction waits until its last arena callback has completely
returned; the completion counter is intentionally the callback's final
synchronized action so condition variables cannot be destroyed while a worker
still notifies them.

## [Parallel-inference packet heuristics](#index)

Inference keeps the existing two-pass semantics for every row: first discover
container shapes, then apply scalar and nested statistics using the shapes known
at that exact source ordinal. `multi` does not let workers mutate the inference
context. Workers instead reparse JSON in private documents and emit one compact
preorder stream of immutable evidence nodes. Each node stores its subtree end,
so the ordered reducer can traverse children without allocating one vector per
value. Field names are interned only by that reducer, in canonical row and field
order.

Parallel inference is adaptive. The first frontend batch is profiled from a
bounded prefix of already-parsed values. The pool is selected only when the
batch contains enough nested values, enough rows/estimated bytes, at least two
effective workers, and at least 96 MiB in the policy's aggregate worker pool.
Materialized flat/scalar input, small batches, and lower-memory runs use the
reference scanner without creating an inference executor. Raw-only JSONL is the
exception: parsing is intentionally deferred to worker-private documents. Its
flat scalar packets retain sixteen fields inline and may grow through a tracked
packet-local PMR overflow up to 512 fields. Stable field order uses direct
positional updates; missing, reordered, or duplicate keys fall back to a local
lookup. Wider, nested, or long-key shapes retain generic tracked evidence. Eligible
flat scalar values are classified from the compact `ValueView` tag with one
exhaustive switch. Only object and array tags perform the empty-container check;
strings retain the configured scalar parser and unsupported containers retain
the generic evidence path.

The inference stage may use up to the operation's effective workers and
redistributes, rather than increases, the policy's worker memory among them.
Plan complexity, packet volume, and the aggregate reserve can narrow the active
lane without reducing the operation's available maximum. Input packets retain at most
the policy's adaptive materialization row limit (at most 5,120 rows) and use the
smaller of the normal packet target and one thirty-second of the effective
worker arena, reserving for the expansion from short JSON tokens to evidence
nodes and decoded keys. A separate batch profile samples at most 256 rows only
to select a safe execution policy; it does not cap the packet. A single source
row above the byte target is processed by the reference scanner after all
earlier parallel ordinals have been reduced. Evidence packets have their own
tracked PMR arena capped at one effective worker arena, while the operation
pool remains the aggregate hard limit.

The reducer validates every row and subtree span before use, performs the shape
pass and statistics pass in order, and preserves `flattened_fields`,
`scalar_wrappings`, inferred byte counts, schema field order, and the earliest
source-order failure. Benchmarks therefore compare the exact logical-schema
payload and diagnostic JSON, not only decoded Arrow types.

## [Text-output packet heuristics](#index)

Native CSV and JSONL output share the ordinal executor used by materialization.
The coordinator validates each Arrow batch before dispatch, assigns contiguous
row ordinals, writes the CSV header once, commits fragment bytes in order,
updates statistics, selects the earliest ordinal failure, and performs the final
flush. Workers only read immutable Arrow arrays and encode private strings; they
never mutate the output stream or shared statistics.

The output stage derives its limits from the same operation policy rather than
adding public controls. It may use up to the operation's effective workers,
retains at most one unfinished fragment per active worker, and caps each packet
at 2,048 rows. The byte target is the native packet target derived from
`memory_limit_bytes`. A capped
recursive estimator accounts for worst-case JSON escaping, CSV quoting, binary
expansion, dictionaries, lists, maps, and structs. A row that reaches the target
is isolated instead of allowing later rows to enlarge the fragment. For flat
fixed-cost JSONL structs whose validated Arrow batch reports exact zero null
counts, the conservative row estimate is prepared once from the schema and then
reused in O(1) per row. It is identical to the recursive estimate for eligible
rows, so packet boundaries do not change. Null-bearing, unknown-null,
variable-width, nested, dictionary, and CSV batches retain row-aware estimation.
Released fragment buffers are overwritten when hardened cleanup is enabled.

For flat JSONL structs with at least 24 fixed-cost scalar fields, operations
with more than eight effective workers use up to half of the operation arena,
with a minimum ceiling of four. Schemas wider than 96 fields halve that ceiling
again; for example, a 64-worker operation admits up to 32 output workers for 96
or fewer fields and 16 for a wider fixed-cost schema. That lane is admitted
once at its complete bounded width from the first packet; workers still start
lazily, so a short batch starts only as many threads as it has packets. This
avoids a full ordered executor drain/recreation between early Arrow batches.
Hosts with eight workers or fewer, variable-width fields, and nested output
retain adaptive per-batch admission.

The effective CPU ceiling is derived internally before worker counts are
calculated. It uses the minimum available signal among hardware concurrency,
process affinity, Linux cgroup v2/v1 CPU quota, the Windows process affinity
mask, and macOS active CPUs, with a floor of one. This keeps containers and
CPU-restricted processes from oversubscribing the host without adding a public
worker-count parameter or environment-variable override.

`single` executes the identical packet callable inline and creates no native
thread. `multi` pays a fixed pool/coordination cost, so it is intended for
substantial row counts, escaping, nesting, or wider records. The benchmark
harness records the observed crossover instead of embedding an unstable public
row threshold. Native Parquet now performs that separation: workers collect and
encode independent leaf columns into private artifacts, while one coordinator
assigns physical offsets, commits column chunks and page indexes in schema order,
and writes the footer/trailer once. The adaptive route remains serial for small
nested groups, narrow scalar groups, low-memory operations, or one effective
worker.

Path-based CSV, JSONL, and Parquet outputs are written to a unique sibling staging
file. The destination is replaced only after the writer and final flush/close
succeed. An existing destination and its permissions are preserved until that
commit point; failures remove the staging file rather than truncating the valid
output. This is atomic publication on the destination filesystem, not a promise
of crash-durable `fsync` semantics.

## [Parquet route and storage heuristics](#index)

Local Parquet input prefers the native Arrow C Stream reader when the file
satisfies its contract. PyArrow Dataset is the compatibility fallback;
unfiltered local reads can additionally fall back to
`ParquetFile.iter_batches`. Filtered reads fail closed if Dataset cannot apply
the filter.

The native writer bounds rows, estimated uncompressed column bytes, and staged
page bytes. Row-group rows, row-group bytes, page bytes, reader windows, and
footer retention are derived together from the operation's single
`memory_limit_bytes`. They are intentionally not independent tuning knobs: this
keeps the derived limits mutually consistent and prevents one subsystem from
consuming the full operation budget in isolation.

In `multi`, Parquet may prepare up to the minimum of the operation's effective
workers, leaf-column count, and memory-supported compression candidates. The
row-group target is narrowed only for schemas likely to use the arena, reserving
space for collected values, prepared artifacts, and compression candidates.
Nested groups below 16,384 rows, flat groups with four or fewer leaves below
65,536 rows, and operations below the 96 MiB parallel reserve stay serial.
Wider scalar schemas use progressively lower row thresholds because independent
column compression amortizes the executor sooner. These thresholds are internal
safety/performance policy, not public tuning options.

Pages choose encodings by compressed size. Profitable repeated scalars use
dictionary encoding; signed integer and temporal pages may use
`DELTA_BINARY_PACKED`; high-cardinality string/binary pages may use
`DELTA_LENGTH_BYTE_ARRAY`; non-dictionary floats use `BYTE_STREAM_SPLIT`;
otherwise plain encoding is retained. GZIP is the public default, with Snappy
and uncompressed output also supported.

Detailed native-reader contract diagnostics and certification helpers are
implementation-facing adapter concerns rather than public contracts.

## [Remote staging heuristics](#index)

Remote conversion uses provider-native asynchronous clients rather than
`pyarrow.fs`. Single files stream into replayable local spools. Directory
children are listed non-recursively, ordered deterministically, and packetized
by both file count and known bytes. In `multi`, one lazy operation context owns
the event-loop host from initial listing through staged downloads and final
remote output upload. Compatible aiohttp, S3, and Azure clients are pooled on that host for the
complete operation, while directory staging shares one global transfer
semaphore. Incompatible HTTP header sets or Azure accounts receive distinct
pool entries. The paired registry route
retains a policy-bounded probe prefix and transfers ownership to materialization;
a directory that fits in that prefix is downloaded once, while later packets are
replayed by a second bounded pass. Every staged packet reserves known or
estimated bytes from the operation-owned temporary-storage pool before prefetch,
resizes that lease to its exact on-disk size, and releases it only when the
consumer closes or cancellation drains the packet. Final remote-output spools
hold an exact lease through upload. S3 switches to bounded multipart parts for
large spools, GCS uses sequential resumable ranges and reconciles the durable
provider offset after a lost response, and Azure passes a memory-bounded
concurrency window to its block-blob uploader. Small objects and generic HTTP
remain single-request paths. Multipart completion or resumable finalization is
the only publication point; failure drains active work and aborts partial remote
state while preserving the completed local spool until cleanup.

Known-size packet targets reserve only a fair share of the operation budget
across the configured lookahead. A file larger than the target is isolated as a
single packet. Unknown sizes use a deterministic fair-share estimate and remain
file-count bounded; actual transfer-size checks still apply.

### [Partition source lookahead](#index)

Static partition pipelines in `multi` may prepare exactly one immutable
source for partition `N + 1` while `N` is converting or publishing. This is
a dedicated one-slot preparation executor, not another general worker pool,
and it is enabled only when the derived policy has more than one effective
worker. The child partition context shares the operation temporary-storage
permits and remote coordinator, but captures its own fixed timestamp.

The trigger is deliberately conservative. Fully prepared local inputs may
start lookahead before CPU conversion. Lazy remote-native inputs wait until
the current writer has consumed them. Remote outputs wait until their exact
spool reservation is held. The lookahead never performs schema inference,
registry mutation, callbacks, or publication for the next partition. A
failure is retained until that partition reaches its ordinal, and `N + 2`
is never submitted early. If the shared temporary-storage window is occupied,
preparation is deferred and retried synchronously at the correct ordinal.
Callable per-partition option factories and all `single` executions remain
fully sequential so evaluation order and the one-host-thread contract do not
change.

Provider routing is URI-based: GCS uses its JSON API and Google ADC, S3 uses the
normal AWS credential chain through `aiobotocore`, Azure uses asynchronous Blob
clients and `DefaultAzureCredential`, and HTTP(S) supports single files but not
portable directory listing. File outputs are staged locally and uploaded only
after successful conversion.

## [Flat JSONL inference parsing](#index)

Flat JSONL inference uses a worker-private single-pass root-object visitor when
one packet remains eligible for the bounded scalar aggregate. The visitor does
not calculate key hashes or construct numeric values because inference needs
only key order, scalar category, and decoded string content. Integer tokens are
classified against exact signed 64-bit lexical bounds; floats and out-of-range
integers still use strict floating validation. Empty containers contribute no
evidence, while non-empty nested values force the canonical generic fallback.
This is an internal execution heuristic and does not change public parsing or
resource options.

## [Bounded output-lane progress](#index)

The shared operation arena treats output as a bounded scheduling hint, not a
separate unbounded priority pool. Above eight workers the established high-core
policy may promote one dedicated upper-lane output task before returning to
FIFO. On four-to-five-worker arenas, the same one-bypass rule is enabled only
for explicitly classified output in shallow queues of at most four tasks.
Six-through-eight-worker arenas retain FIFO. Remote helpers inspect only the
front of the already-selected victim queue. This keeps scheduling memory fixed,
avoids global queue scans, and prevents output progress from starving upstream
materialization.

## [Validation-certified positional JSONL materialization](#index)

For flat JSONL direct-scalar plans, parallel validation may certify that one
row's unescaped root keys exactly match the compiled plan in name, count, and
order. Materialization then consumes the already validated value tokens by
position and skips key decoding, hashing, `FieldRef` construction, and plan
lookup. The certificate is row-local and packet-bounded. Escaped, missing,
reordered, duplicate, nested, or variant-bearing rows use the canonical lookup
path. This optimization does not change error policy, ordering, memory limits,
or public options.

## [Direct lexical positional JSONL materialization](#index)

After validation certifies that a flat JSONL row exactly matches a direct-scalar
compiled plan, materialization may convert lexical tokens directly when their
form already matches the destination type. Exact null, boolean, integer,
floating, unescaped UTF-8, and integer temporal values bypass generic value
construction. Any lexical/type mismatch, escape, coercion, nested value, or
error condition returns to the canonical parser and conversion policy. The fast
path borrows only from the packet-owned validated row for the duration of one
append and does not add cross-row state or public tuning options.

## [Scalable arena packet metadata](#index)

The operation arena has no fixed worker-count ceiling. Effective width is
derived from available CPUs, process pressure, and the operation memory budget.
Queue membership uses a scalable bitmap and lane bounds retain machine-sized
arithmetic, so machines wider than 32 workers neither truncate eligible workers
nor enter a compatibility path. Queue memory grows with admitted workers and
remains charged to the operation policy.
