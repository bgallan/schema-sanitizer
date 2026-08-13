# Native fuzz inputs

This tree contains byte-exact inputs for the native CSV, JSON, XML, and
Parquet fuzz targets in `cpp/fuzz`.

- `corpus/<target>/` contains small, descriptive seeds chosen to exercise
  useful parser grammar and boundary conditions.
- `regressions/<target>/` contains minimized failures that must execute once
  before every mutation campaign.

Keep both layouts flat. The regression runner intentionally discovers regular
files directly inside each target directory; nested directories are not part
of the regression contract. A seed and a regression may contain identical
bytes while serving different purposes, so do not remove those semantic
duplicates.

## Byte integrity

Names made of 40 lowercase hexadecimal characters are libFuzzer SHA-1 content
identifiers. Their bytes must hash to the filename. Descriptive filenames are
also allowed, but every input remains opaque data: do not format it, normalize
line endings, trim whitespace, or add a final newline. Git attributes and
pre-commit exclusions protect those bytes, while the repository checker
verifies every input through exact per-target counts and a canonical
path-and-content fingerprint:

```console
python meta/ci/fuzz/check_fuzz_corpus.py
```

Promote a minimized failure by copying it unchanged into the matching
`regressions/<target>/` directory. Deliberate corpus changes require reviewing
the complete inventory and updating the expected counts and tree fingerprint
in `meta/ci/fuzz/check_fuzz_corpus.py`; run the checker immediately afterward.

## Running regressions and campaigns

Build the native fuzz targets, then run all promoted regressions:

```console
python meta/ci/fuzz/run_fuzz_regressions.py --build-root build-fuzz/fuzz
```

Add a bounded campaign with `--campaign-runs N`. Campaigns always use a
temporary, content-deduplicated input tree: the CLI combines the curated corpus
and promoted regressions, while regression-only API callers stage just the
regressions. The temporary corpus is removed afterward, so libFuzzer cannot add
generated inputs to the source checkout.
