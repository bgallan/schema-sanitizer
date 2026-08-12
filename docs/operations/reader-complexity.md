# Reader complexity contract

## Index

- [Overview](#overview)

## [Overview](#index)

Reader hardening uses bounded, iterative scanners. For a fixed projection and
fixed configured safety limits, accepted and rejected inputs must require
`O(input bytes + decoded output bytes)` work.

- CSV frames each byte once, validates UTF-8 once, and performs at most two
  additional passes over a quoted field to size and decode it.
- JSON performs bounded top-level framing followed by canonical validation or
  materialization. Optimized and skipped-field paths still validate the same
  slice; no retry restarts from the beginning of the source.
- XML tokenization, name validation, entity sizing, and entity decoding advance
  monotonically. Element construction and release are iterative and each node
  is linked and released once.
- Parquet validates the footer and selected page ranges once. Decompression,
  repetition levels, and definition levels are bounded by both declared sizes
  and the operation budget; corrupt pages do not trigger repeated fallback
  decoding.

The contract permits constant-factor rescans described above and work
proportional to bounded decoded output. It does not permit backtracking,
unbounded entity expansion, recursive retries, or restarting a record after a
chunk boundary. `benchmarks.readers.linear_scaling` exercises hostile
but valid CSV, JSONL, and XML patterns at increasing sizes in serial and
parallel modes. The normalized time-growth gate is intentionally generous to
absorb scheduler and filesystem noise while detecting super-linear regressions.
Parquet's corresponding guarantee is enforced structurally through range,
page, decompression, and level-expansion limits plus sanitizer fuzzing.
