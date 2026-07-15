# Native fuzz regressions

Place every minimized libFuzzer crash input in the directory matching its
parser target: `json`, `csv`, `xml`, or `parquet`.

`meta/ci/run_fuzz_regressions.py` executes every stored input once against the
instrumented target before a new fuzz campaign starts. A discovered crash is
therefore promoted by copying its minimized artifact here with a descriptive,
stable filename. Keep the original bytes unchanged.
