# Pass 66 — concurrency and memory hardening

This pass applies the pass-65 audit findings across external runtime ownership,
post-fork safety, result/finalizer ownership, completion-memory ownership, and
format-pair concurrency certification.

## Implemented hardening

- External runtime `shrink_to()` now mirrors every committed native/borrow/logical
  release into its prearmed finalizer state immediately, making partial failures
  retry-exact and preventing double release.
- Lease mutators validate owner PID before acquiring inherited mutexes, so a
  post-fork child fails fast instead of deadlocking on a vanished thread owner.
- PyArrow-table -> Polars admission uses `polars.thread_pool_size()` via the
  runtime-aware external-thread helper.
- Process-global external runtimes now have shared logical and physical pool
  authorities. Concurrent operations hold independent claims while the process
  owns only the maximum live physical/logical width. Live pools are monotonic:
  later overlapping requests share an already constrained width rather than
  re-expanding it.
- Borrowed-operation and standalone claims interoperate without leaking a native
  claim when a pre-existing process-global pool is narrower.
- Added bounded diagnostics with `external_runtime_pool_snapshot()`.
- Complex finalizer graphs use named typed state for external runtime leases,
  `Result`, and Parquet dataset lifetime ownership instead of positional
  `arg0..argN` mirrors.
- Native completion/result memory is represented by move-only
  `CompletionMemoryLease`. The raw quantity-based `ReleaseCompletionBytes()` API
  was removed; ordered completion slots consume/reset the ownership object.
- The 8x7 release gate now additionally requires per-pair format-specific stage
  observations. Stage evidence is published at parser/open, actual adapter/table
  materialization, or decode->writer boundaries rather than merely at public
  wrapper return.
- Added adversarial pass-66 tests for native/borrow/logical partial failures,
  GC retry exactness, real post-fork inherited-lock behavior, global-pool sharing,
  monotonic shrinking, mixed borrowed/standalone ownership, Polars pool sizing,
  move-only completion memory ownership, and release-stage certification.

## Validation performed in this environment

- `tests/memory/test_memory_safety_pass62.py` through `pass66.py`: 64 passed.
- `tests/memory/test_memory_safety_pass45.py`: 17 passed.
- Modified pass48 active->completion transfer regression: passed.
- `python -m compileall -q src`: passed.
- `g++ -std=c++20 -fsyntax-only` for `operation_task_arena.cc`: passed.
- `g++ -std=c++20 -fsyntax-only` for a TU including `ordered_executor.hh`: passed.

A complete CMake build cannot be configured in this container because the
project requires CMake 4.3+ while the available version is 3.31.6.
