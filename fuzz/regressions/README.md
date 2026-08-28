# Native fuzz regressions

Place a new minimized libFuzzer crash input in the directory matching its
parser target: `json`, `csv`, `xml`, or `parquet`. Run
`python meta/ci/fuzz/pack_fuzz_regressions.py --remove-loose` to merge the
content-addressed input into the adjacent deterministic archive.

`meta/ci/fuzz/run_fuzz_regressions.py` executes every stored input once against the
instrumented target before a new fuzz campaign starts. A discovered crash is
therefore promoted by copying its minimized artifact here unchanged. Keep its
40-character lowercase libFuzzer SHA-1 name. Descriptive stable filenames are
reserved for the small hand-maintained distribution fixtures that remain loose.
Never normalize or format the bytes.

See [`../README.md`](../README.md) for the layout, integrity checker, and local
commands.
