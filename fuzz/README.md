# Native fuzz inputs

This tree contains byte-exact inputs for the native CSV, JSON, XML, and
Parquet fuzz targets in `cpp/fuzz`.

- `corpus/<target>/` contains small, descriptive seeds chosen to exercise
  useful parser grammar and boundary conditions.
- `regressions/<target>/` contains the small descriptive fixtures shipped in
  source distributions. Content-addressed failures live in the adjacent
  deterministic `regressions/<target>.sha1.zip` archive.

Keep target directories flat. The runner presents loose and archived inputs as
one sorted logical collection and stages their exact bytes outside the checkout
before execution. A seed and a regression may contain identical bytes while
serving different purposes, so do not remove those semantic duplicates.

## Byte integrity

Names made of 40 lowercase hexadecimal characters are legacy identifiers
assigned by libFuzzer. Descriptive filenames are also allowed, but every input
remains opaque data: do not format it, normalize line endings, trim whitespace,
or add a final newline. Git attributes and pre-commit exclusions protect those
bytes, while the repository checker verifies every input through exact
per-target counts and a canonical SHA-256 path-and-content fingerprint:

```console
python meta/ci/fuzz/check_fuzz_corpus.py
```

Promote a minimized failure by copying it unchanged, under its lowercase
40-character libFuzzer artifact name, into the matching
`regressions/<target>/` directory, then repack and validate it:

```console
python meta/ci/fuzz/pack_fuzz_regressions.py --remove-loose
python meta/ci/fuzz/check_fuzz_corpus.py
```

The packer validates names and bytes, updates all four archives atomically, and
is safe to rerun. The canonical SHA-256 logical-tree fingerprint protects every
archived and descriptive input, so deliberate additions or removals also require
reviewing the expected counts and fingerprint in the checker.

## Running regressions and campaigns

Build the native fuzz targets, then run all promoted regressions:

```console
meta/ci/fuzz/run_fuzz_regressions.sh --build-root build-fuzz/fuzz
```

Add a bounded campaign with `--campaign-runs N`. Campaigns always use a
temporary, content-deduplicated input tree: the planner combines the curated
corpus and promoted regressions, then the shell executes its validated argv
plan. The temporary corpus is removed afterward, so libFuzzer cannot add
generated inputs to the source checkout.
