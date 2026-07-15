// Defines prepared ingest and stream result ownership types.

#pragma once

#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

#include <memory>
#include <string>

namespace sanitize {

class ExecutionContext;

// Staged ingest state (frontend + compiled plan + prepared options).
//
// When callers omit a context, prepare_* helpers create a fresh
// ExecutionContext and store it in owned_ctx so the entire pipeline (especially
// streaming) keeps it alive.
struct PreparedIngest {
  std::string frontend_name;
  FrontendHandle frontend;

  // Optional owned context (set when ctx is omitted).
  std::shared_ptr<ExecutionContext> owned_ctx;

  // Non-owning pointer to the context used by this pipeline. Always non-null
  // after preparation.
  ExecutionContext *ctx = nullptr;

  // Operation-scoped pool with an independent quota. It delegates to the
  // context pool, which aggregates concurrent operation usage.
  std::shared_ptr<void> operation_memory_pool;

  std::shared_ptr<const CompiledPlan> plan;
  PreparedOptionsPtr opts;
  std::shared_ptr<IngestDiagnostics> diagnostics;
  LogicalSchema logical_schema;
  bool inference_consumed = false;
};

struct IngestStream {
  UniqueCStream stream;
  std::shared_ptr<IngestDiagnostics> diagnostics;
};

} // namespace sanitize
