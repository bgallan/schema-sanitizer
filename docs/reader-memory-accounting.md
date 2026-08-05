# Reader memory accounting

`memory_limit_bytes` is the single public resident-resource input for one
Schema-Sanitizer operation. The resolved value is fixed at operation start and
backs one native atomic ledger shared by Python-owned staging resources and the
C++ reader, inference, materialization, and writer pools.

The operation ledger is paired with an exact process-wide resident pool. Every
scalable Python reservation and native allocation is charged to both the owning
operation and the shared process ceiling, so overlapping calls cannot each spend
the full safe host allowance. This is not a promise that process RSS equals the
counter: interpreter/runtime overhead, thread stacks, allocator bookkeeping,
third-party client internals, and filesystem page cache remain outside it and
are covered by the automatic-sizing safety reserve.

## Charged domains

| Domain | Ledger behavior |
|---|---|
| Native parsing, inference, materialization, writer buffers, reorder windows, Arrow construction, and native workers | Actual upstream allocation size, including native allocation headers, alignment padding, and guards, is reserved atomically before the parent allocator is called and released on every failure or free path. All child workers use the same ledger. |
| Materialized in-memory input | Text, bytes, and memory views receive a conservative lifetime lease before native execution. The lease remains until the output stream or operation closes. |
| Registry-generated metadata streams | Path, Python-stream, and in-memory-text wrappers receive the same ledger through prepared native options; they never recalculate a host-memory allowance. |
| Local and remote directory discovery | Requested URIs, retained file records, and directory associations are conservatively charged before retention. Synchronous, asynchronous, grouped, and concurrent provider paths share the operation ledger. |
| Remote HTTP control responses | Source bytes, transient immutable/Unicode copies, and the returned body/text wrapper are reserved before allocation. The retained wrapper keeps only its final exact charge until release or collection. |
| Remote transfer and upload windows | HTTP, S3, GCS, and Azure staging reserve a bounded chunk window before each blocking or asynchronous read. Multipart uploads reserve both the file-read bytes and retained provider payload before materialization. Concurrent transfers compete against the same operation and process ledgers. |
| Schema-registry warm-up and partition workflows | Timestamp-distinct child contexts share the root workflow ledger, directory metadata budget, temporary-storage permits, and remote coordinator. |

Directory metadata also has a format-specific sublimit. A fixed 64 KiB runtime
allowance prevents that sublimit from becoming unusably small, but the shared
operation ledger remains authoritative: a smaller public operation limit still
wins.

Temporary files use a separate disk-permit hierarchy because on-disk bytes are
not resident allocations. Each operation has a spool ceiling and all operations
share a process-wide reservation governor for the target filesystem, including a
fixed emergency free-space margin. Their in-memory transfer windows remain
charged to the resident ledger.

## Deliberately outside the ledger

- Python interpreter/import machinery, extension-loader state, thread stacks,
  garbage-collector metadata, and opaque standard-library or third-party SDK
  implementation overhead.
- Caller objects and provider-client state created before the operation. When an
  existing text/bytes input is retained by an operation, Schema-Sanitizer still
  takes an equivalent conservative ledger lease for its lifetime.
- Filesystem page cache and the contents of temporary files on disk.
- Bounded option objects, path wrappers, exception objects, and privacy-safe
  diagnostics whose size is independent of hostile payload cardinality.
- Analytical results after ownership transfers to Python, PyArrow, pandas,
  Polars, DuckDB, or another Arrow consumer. Retaining yielded batches is also
  caller-owned memory after transfer.

## Invariants

- No charged domain may reserve beyond `memory_limit_bytes`, even when Python
  tasks and native workers race.
- The sum of charged bytes across concurrent operations may not cross the current
  safe process resident ceiling.
- A failed reservation occurs before the corresponding scalable allocation or
  blocking read.
- Reservations unwind on parser failure, cancellation, staging failure, output
  close, and normal completion.
- Closing an operation prevents new Python reservations while allowing existing
  leases and native allocations to drain; diagnostics retain the historical
  peak and report zero current bytes after final release.
- Stage-specific sublimits may reject earlier, but they never enlarge the
  operation-wide ceiling.

Regression coverage in `tests/test_operation_memory_ledger.py` and
`tests/test_concurrency_memory_hardening_pass*.py` exercises atomic concurrency,
Python/native coexistence, cross-operation admission, finalizer cleanup,
temporary-storage contention, retained HTTP bodies, multipart buffers, remote
deadlines, and staged-result ownership. The shared native resident pool is also
stressed repeatedly under ASan/UBSan and TSan.
