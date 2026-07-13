// Declares private ingestion preparation phases.

#pragma once

#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/ingest_types.hh"
#include "sanitize/options/options.hh"

namespace sanitize {
class ExecutionContext;

namespace ingest_internal {

// Infers one logical schema by consuming frontend batches.
sanitize::Result<LogicalSchema>
infer_schema_from_frontend(FrontendHandle &frontend,
                           const PreparedOptions &opts,
                           IngestDiagnostics *diagnostics, bool *out_consumed,
                           ExecutionContext *execution_context);

// Resolves the final logical schema from a contract and inferred schema.
sanitize::Result<LogicalSchema>
resolve_ingest_logical_schema(const PreparedOptions &opts,
                              const LogicalSchema &inferred_schema);

} // namespace ingest_internal
} // namespace sanitize
