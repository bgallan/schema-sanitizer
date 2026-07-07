# sanitize Modules

This directory is organized by API surface and runtime concern:

- `abi/`: Arrow C Data/C Stream bridge handle types.
- `core/`: Fundamental runtime value/status/diagnostics/schema stream types.
- `ingest/`: Ingest-facing source/typing/entry-point abstractions.
- `options/`: Options model and serialization/preparation interfaces.
- `planning/`: Plan schema and compilation interfaces.
- `registry/`: Built-in frontend dispatch interfaces.
- `runtime/`: Execution-context interface and runtime composition.
- `detail/`: Low-level utility headers used by public/internal modules.
