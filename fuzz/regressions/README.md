# Native fuzz regressions

Place every minimized libFuzzer crash input in the directory matching its
parser target: `json`, `csv`, `xml`, or `parquet`.

`meta/ci/fuzz/run_fuzz_regressions.py` executes every stored input once against the
instrumented target before a new fuzz campaign starts. A discovered crash is
therefore promoted by copying its minimized artifact here unchanged. Keep its
40-character libFuzzer SHA-1 name, or assign a descriptive stable filename for
a hand-maintained distribution fixture. Never normalize or format the bytes.

See [`../README.md`](../README.md) for the layout, integrity checker, and local
commands.
