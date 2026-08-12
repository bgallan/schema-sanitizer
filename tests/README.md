# Test suite layout

The test suite is organized by product domain. Run the complete suite with
`pytest`, or pass one directory to work on a focused area.

| Directory | Scope |
|---|---|
| `concurrency/` | Threading, schedulers, task arenas, lifecycle, and scaling contracts |
| `memory/` | Global budgets, accounting, limits, permits, and ownership contracts |
| `parquet/` | Native Parquet readers, recursive layouts, fallbacks, and contract gates |
| `io/` | Public APIs, input readers, parsing policies, and reader hardening |
| `remote/` | Cloud transports, HTTP fault handling, sessions, and multipart transfers |
| `pipeline/` | Discovery, window planning, lookahead, warm-up, and registry state |
| `sinks/` | CSV, JSONL, and Parquet output paths and writer lifecycle |
| `schema/` | Options, registries, result contexts, and generated metadata |
| `examples/` | Executable examples and notebooks |
| `quality/` | CI, packaging, source layout, ownership, and risk-coverage contracts |
| `_support/` | Explicitly imported helpers shared across test modules and domains |

`conftest.py` at the test root owns cross-domain fixtures, hooks, and explicit
test-support plugins. Reusable test-only modules live in the `_support` package;
the opt-in isolated native stub used by memory lifecycle tests is registered
from there so a domain-local `conftest.py` cannot shadow root helpers. The test
root is included in pytest's import path so this support package has one stable
location without duplicated helpers.

Test modules and cases use stable, behavior-oriented names. Chronological
labels such as `passNN`, `phaseNN`, `vNN`, `partNN`, and numbered maintenance
files belong in Git history rather than in the active suite. The layout gate
enforces this rule without imposing a module-count target that would reward
fragmentation.

Examples:

```bash
pytest tests/concurrency
pytest tests/parquet
pytest
```
