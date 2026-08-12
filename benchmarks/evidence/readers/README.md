# Reader benchmark evidence

`benchmarks.readers.hardening_ab` compares two independently built source trees in
fresh Python subprocesses. Both native extensions must use the same build type.
The harness exercises valid JSON Lines, CSV, XML row-tag conversion through the
public JSONL sink and valid Parquet native preflight without requiring PyArrow.

Example:

```bash
PYTHONPATH=src python -m benchmarks.readers.hardening_ab \
  --baseline-root /path/to/baseline \
  --candidate-root /path/to/candidate \
  --rows 2000 --width 8 --repeats 3 \
  --output artifacts/reader-hardening-ab.json
```

The retained [`hardening-ab.json`](hardening-ab.json) report is the Release A/B
dataset referenced by the validation record dated 2026-08-01. It is historical
evidence, not a claim about the current checkout. The accepted envelope is stored
separately in [`performance-budget.json`](performance-budget.json), so changes to
measurements cannot silently change policy. The XML fixture uses a 256 MiB
operation budget so the benchmark measures valid throughput rather than the
separately documented 64 MiB early-release limit.

## Linear-scaling gate

`benchmarks.readers.linear_scaling` grows hostile-but-valid CSV, JSONL, and XML
fixtures and compares elapsed-time growth with input-byte growth in serial and
parallel modes. The retained [`linear-scaling.json`](linear-scaling.json) report
uses repeated Release measurements; CI also runs a smaller, deliberately
noise-tolerant smoke on every supported wheel platform. The algorithmic contract
and the structural Parquet argument are documented in
[`docs/operations/reader-complexity.md`](../../../docs/operations/reader-complexity.md).

## Validation record

[`validation.json`](validation.json) consolidates the dated validation records
for registry-metadata budget propagation, the shared operation ledger, focused
pytest matrices, sanitizer campaigns, release checks, and the limitations of
the environments where those observations were made. Counts and limitations
remain attached to their original dates; they are not rewritten as current
results.
