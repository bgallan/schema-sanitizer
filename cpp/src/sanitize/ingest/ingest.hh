// Declares ingestion preparation and stream execution entry points.

#pragma once

#include "sanitize/core/status.hh"

#include <string_view>

#include "sanitize/ingest/ingest_types.hh"

namespace sanitize {

// ---------------- Staged ingest --------------------------------------------

// Infers or evolves the input schema and compiles the materialization plan.
//
// If ctx is null, the returned PreparedIngest owns a fresh ExecutionContext.
sanitize::Result<PreparedIngest>
prepare_ingest(std::string_view frontend_name, FrontendHandle frontend,
               PreparedOptionsPtr opts, ExecutionContext *ctx = nullptr);

// Converts a prepared ingest plan into an Arrow C Stream result.
sanitize::Result<IngestStream> ingest_to_stream(PreparedIngest prepared);

} // namespace sanitize
