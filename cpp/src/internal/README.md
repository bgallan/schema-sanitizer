# Internal modules

Internal implementation is grouped by responsibility. Public C++ headers remain
under `cpp/src/sanitize/`; nothing in this directory is a compatibility layer.

- `abi/`: internal helpers shared by the C and Python ABI bindings.
- `arrow_c/`: Arrow C Data and C Stream construction, export, and release callbacks.
- `arrow_text/`: scalar Arrow-to-text formatting for delimited outputs.
- `csv/`: CSV stream serialization internals.
- `inference/`: shape discovery, statistics, and inferred-schema construction.
- `json_encoding/`: JSON token and escaping primitives used by diagnostics and metadata.
- `json_output/`: JSON Lines stream and nested-value serialization.
- `materialization/`: row-to-column conversion and Arrow batch construction.
- `memory/`: arenas, pool accounting, and memory-resource adapters.
- `parquet/`: native Parquet footer reading and stream writing.
- `parsing/`: JSON/XML parsing plus incremental CSV/JSON/XML scanners.
- `planning/`: option serialization, field-name matching, and compiled-plan helpers.

Frontend-specific state belongs beside its frontend under `cpp/src/frontends/`.
Core `ValueView` helpers belong under `cpp/src/core/`, next to the implementation
they extend. This keeps ownership visible and avoids generic internal catch-all
folders.
