# Benchmarks

This package separates executable benchmark harnesses by domain from retained
evidence and native concurrency probes. Run commands from the repository root
so module execution uses the current source tree and build.

## Layout

- `ingestion/` contains ingestion cases, fixtures, timing, reporting, and its CLI.
- `readers/`, `pipeline/`, and `remote/` contain focused benchmark entrypoints.
- `concurrency/threading/` contains cross-platform threading matrices.
- `concurrency/telemetry/` contains telemetry collection and high-core evidence tools.
- `evidence/concurrency/` groups retained concurrency measurements by stable
  subsystem: `scheduler/`, `lifecycle/`, `telemetry/`, `layout/`, and `safety/`.
  Its [manifest](evidence/concurrency/manifest.json) is the authoritative mapping
  between stable contract identifiers, evidence, and probes.
- `evidence/readers/` contains reader measurements, validation records, budgets,
  and the [reader hardening guide](evidence/readers/README.md).
- `probes/concurrency/` uses the same stable subsystem hierarchy for standalone
  benchmark and TSan sources.

Retained evidence is an auditable record, not scratch output. Write new local
reports to `artifacts/` or another ignored directory and move them into
`evidence/` only as part of a reviewed change. Python caches and build products
do not belong here. Filenames describe the measured contract rather than the
implementation pass that introduced it; chronology belongs in Git history.

## Common commands

Run the ingestion smoke used by CI:

```bash
python -m benchmarks.ingestion.cli --rows 8 --width 2 --repeats 1
```

Exercise the cross-platform threading matrix:

```bash
python -m benchmarks.concurrency.threading.matrix \
  --profile ci --rows 256 --warmups 0 --repeats 1 \
  --only parquet --output artifacts/threading-matrix.json
```

Check reader growth against the CI envelope:

```bash
python -m benchmarks.readers.linear_scaling \
  --sizes 1024,2048 --repeats 3 --maximum-normalized-growth 8 \
  --latency-budget benchmarks/readers/linear_scaling_budget.json \
  --output artifacts/reader-linear-scaling.json
```

The command gates both scaling shape and absolute median latency. The static
policy is independent from generated reports, is calibrated from a named
healthy run across every supported platform, and the output identifies the
commit and native-extension digest that were measured.

Aggregate fuzz evidence and privacy-safe production counters for a manual
reader-limit review:

```bash
python -m benchmarks.readers.review_limits \
  --fuzz-evidence artifacts/fuzz-summary.json \
  --telemetry artifacts/reader-stats.jsonl \
  --output artifacts/reader-limit-review.json
```

This report never changes security ceilings automatically; it only prepares
evidence for maintainer review.

Inspect a concurrency placement plan without running the measured workload:

```bash
python -m benchmarks.concurrency.telemetry.cli \
  --plan-only --workers 1,2,4,8
```

Use repeated measurements, fixed affinity, identical build modes, and fresh
processes for performance claims. Every benchmark that compares execution modes
must also verify logical or byte-level output equivalence.
