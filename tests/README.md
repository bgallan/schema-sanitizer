# Test suite layout

The test suite is organized by product domain. Run the complete suite with
`pytest`, or pass one directory to work on a focused area.

| Directory | Scope |
|---|---|
| `concurrency/` | Threading, schedulers, task arenas, lifecycle, and concurrency scaling |
| `memory/` | Global budgets, accounting, limits, permits, and memory hardening |
| `parquet/` | Native Parquet readers, recursive layouts, fallbacks, and contract gates |
| `io/` | Public APIs, input readers, parsing policies, and reader hardening |
| `remote/` | Cloud transports, HTTP fault handling, sessions, and multipart transfers |
| `pipeline/` | Discovery, window planning, lookahead, warm-up, and registry state |
| `sinks/` | CSV, JSONL, and Parquet output paths and writer lifecycle |
| `schema/` | Options, registries, result contexts, and generated metadata |
| `examples/` | Executable examples and notebooks |
| `quality/` | CI, packaging, source layout, maintenance, and risk-coverage contracts |

`conftest.py` and the `*_shared.py`, `*_support.py`, and `*_helpers.py` modules
remain at the test root because they are shared by more than one domain. The
test root is included in pytest's import path so those helpers have one stable
location without package facades or duplicated fixtures.

Examples:

```bash
pytest tests/concurrency
pytest tests/parquet
pytest
```
