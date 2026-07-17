# Deterministic threading architecture TODO

Status: planned, not implemented.

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

Remote discovery/download may remain asynchronous in multi mode, but results
must be delivered to the source plan in canonical order. Single mode uses a
window of one, no project-owned `ThreadPoolExecutor`, native worker count one,
and `use_threads=False` for PyArrow fallbacks. Optional dependencies may manage
their own runtime threads internally; they must still honor the ordered data
contract.

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

- [ ] Add cross-mode golden helpers that compare schema, ordered rows, canonical
  registry JSON, drift JSON, diagnostics, exceptions, and logical file contents.
- [ ] Build a fixed-clock equivalence matrix for CSV, JSON, JSON array, JSONL,
  NDJSON, XML, native/fallback Parquet, directory inputs, nested shapes, schema
  versions, all error policies, strict/additive registries, and warm-up flows.
- [ ] Add `threading_mode` to the native option catalog, public signatures,
  Python normalization, serialized option contract, README, examples, and API
  option matrices.
- [ ] Introduce one immutable execution policy derived from `threading_mode`,
  `memory_limit_bytes`, CPU availability, and hard ceilings; report requested
  mode, effective workers, queue bounds, and fallback-to-one-worker reason.
- [ ] Make remote discovery, remote staging, source-plan prefetch, and PyArrow
  fallback threading consume that shared policy. Prove single mode creates no
  project-owned worker pool.
- [ ] Add native ordinal packet, inline executor, bounded worker-pool, reorder
  buffer, cancellation, and per-worker memory-arena primitives below the ABI3
  layer, releasing the GIL while native CPU work runs.
- [ ] Split inference into packet-local evidence plus one ordered reducer and
  prove its registry/drift output matches the existing serial inference across
  the regression and fuzz corpora.
- [ ] Split materialization into private packet batches plus one ordered commit;
  keep Arrow builders, diagnostics, registries, and file writers single-owner.
- [ ] Route native Parquet row groups/pages through the same policy without
  changing projected row order, null semantics, encoding decisions, or logical
  output.
- [ ] Add cancellation, earliest-error, interrupt, partial-output cleanup, and
  memory-pressure tests under forced out-of-order completion.
- [ ] Run ThreadSanitizer, ASan/UBSan, native fuzzing, and repeated cross-mode
  differential tests in CI on Linux, Windows, macOS x86-64, and macOS arm64.
- [ ] Benchmark both modes by format, width, nesting, source count, and memory
  limit. Do not change the default until multi mode is measurably useful and all
  equivalence gates pass.
