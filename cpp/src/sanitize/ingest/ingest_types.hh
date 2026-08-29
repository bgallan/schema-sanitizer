// Defines prepared-ingest state and stream-result ownership types.
// Move-only handles keep compiled plans, frontends, execution resources, and
// Arrow C stream releases authoritative across pipeline handoffs.

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

namespace sanitize::internal {
class OperationTaskArena;
class PerformanceTelemetry;
} // namespace sanitize::internal

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

  // Operation-wide native workers reused by inference, materialization, and
  // compatible sinks. Single mode owns an inline arena with no helper thread.
  std::shared_ptr<internal::OperationTaskArena> task_arena;

  // Operation-local timing, queue, worker, and memory telemetry.
  std::shared_ptr<internal::PerformanceTelemetry> telemetry;

  std::shared_ptr<const CompiledPlan> plan;
  PreparedOptionsPtr opts;
  std::shared_ptr<IngestDiagnostics> diagnostics;
  LogicalSchema logical_schema;
  // Best-effort local source size used only for internal stage-width policy.
  std::int64_t input_size_hint_bytes = 0;
  bool inference_consumed = false;
};

struct IngestStream {
  UniqueCStream stream;
  std::shared_ptr<IngestDiagnostics> diagnostics;
};

} // namespace sanitize
