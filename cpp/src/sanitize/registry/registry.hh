// Declares built-in frontend registry dispatch entry points.
// Stable names resolve to CSV, JSON, JSONL, or XML factories without exposing
// concrete frontend classes to ingestion orchestration.

#pragma once

#include <string_view>

#include "sanitize/core/row_stream.hh"
#include "sanitize/ingest/chunk_source.hh"
#include "sanitize/options/options.hh"

namespace sanitize {

/// Creates a built-in frontend by registry name.
FrontendHandle make_builtin_frontend(std::string_view name, ChunkSourcePtr src,
                                     const Options &opts);

} // namespace sanitize
