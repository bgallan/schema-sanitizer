# Reader hardening A/B benchmark

`bench_reader_hardening_ab.py` compares two independently built source trees in
fresh Python subprocesses. Both native extensions must use the same build type.
The harness exercises valid JSON Lines, CSV, XML row-tag conversion through the
public JSONL sink and valid Parquet native preflight without requiring PyArrow.

Example:

```bash
PYTHONPATH=src python benchmarks/bench_reader_hardening_ab.py \
  --baseline-root /path/to/baseline \
  --candidate-root /path/to/candidate \
  --rows 2000 --width 8 --repeats 3 \
  --output benchmarks/reader_hardening_ab.json
```

The retained `reader_hardening_pass2_ab.json` report compares Release builds of
the first and second hardening passes. It is evidence, not a performance waiver:
CSV and JSONL regressions recorded there keep the corresponding TODO item open.
The XML fixture uses a 256 MiB operation budget so the benchmark measures valid
throughput rather than the separately documented 64 MiB early-release limit.

## Linear-scaling gate

`bench_reader_linear_scaling.py` grows hostile-but-valid CSV, JSONL, and XML
fixtures and compares elapsed-time growth with input-byte growth in serial and
parallel modes. The retained `reader_linear_scaling.json` report uses repeated
Release measurements; CI also runs a smaller, deliberately noise-tolerant smoke
on every supported wheel platform. The algorithmic contract and the structural
Parquet argument are documented in `docs/reader-complexity.md`.

## Fourth-pass validation

`reader_hardening_pass4_validation.json` records the targeted pytest matrix,
the explicit registry-metadata budget propagation regression, concurrent
pressure campaigns, sanitizer campaigns, remaining environment limitations,
and the exact three TODO items that remain open.
