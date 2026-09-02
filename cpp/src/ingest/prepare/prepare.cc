// Prepares ingestion by resolving schemas and compiling materialization plans.
// The phases combine inferred or supplied schemas with options before compiling
// the execution plan.

#include "sanitize/ingest/ingest.hh"

#include "sanitize/runtime/execution_context.hh"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "ingest/prepare/prepare_internal.hh"
#include "internal/materialization/stream.hh"
#include "internal/memory/memory_budget.hh"
#include "internal/planning/plan_compile.hh"
#include "internal/runtime/execution_policy.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/performance_telemetry.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/ingest_types.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize {

/// Infers or resolves the input schema and compiles its materialization plan.
/// Creates and owns an execution context when the caller does not supply one.
sanitize::Result<PreparedIngest> prepare_ingest(std::string_view frontend_name,
                                                FrontendHandle frontend,
                                                PreparedOptionsPtr opts,
                                                ExecutionContext *ctx) {
  if (!frontend) {
    return sanitize::Status::Invalid("prepare_ingest: frontend is null");
  }
  if (!opts) {
    return sanitize::Status::Invalid("prepare_ingest: opts is null");
  }

  std::shared_ptr<ExecutionContext> owned_ctx;
  if (!ctx) {
    owned_ctx = std::make_shared<ExecutionContext>();
    ctx = owned_ctx.get();
  }
  if (!ctx) {
    return sanitize::Status::Invalid("prepare_ingest: ctx is null");
  }
  auto operation_memory_pool = ctx->make_operation_memory_pool_handle(
      opts->spec.memory_limit_bytes, opts->operation_memory_ledger);
  if (!operation_memory_pool) {
    return sanitize::Status::OutOfMemory(
        "prepare_ingest: operation memory pool allocation failed");
  }
  frontend.set_memory_pool(operation_memory_pool);
  const auto execution_policy = internal::execution_policy_from(
      opts->spec.threading_mode, opts->spec.memory_limit_bytes);
  auto telemetry = ctx->begin_performance_telemetry(
      operation_memory_pool, opts->spec.memory_limit_bytes,
      execution_policy.effective_workers,
      opts->spec.threading_mode == sanitize::ThreadingMode::kMulti);
  internal::PerformancePhaseScope prepare_scope(
      telemetry, internal::PerformancePhase::kPrepare);
  internal::PerformanceCompletionScope finish_on_prepare_error(telemetry);
  SAN_ASSIGN_OR_RAISE(auto task_arena,
                      internal::OperationTaskArena::Make(
                          static_cast<std::size_t>(std::max<std::int64_t>(
                              1, execution_policy.effective_workers)),
                          telemetry));
  const auto arena_budget =
      internal::memory_budget_from_limit(opts->spec.memory_limit_bytes);
  task_arena->SetBackpressureTimeoutMillis(
      internal::backpressure_timeout_millis_from(arena_budget));
  task_arena->SetBackpressureDeadlineMillis(
      internal::backpressure_deadline_millis_from(arena_budget));
  frontend.set_task_arena(task_arena);

  const bool has_contract = static_cast<bool>(opts->spec.arrow_schema_contract);
  if (opts->spec.schema_evolution == SchemaEvolutionMode::kStrict &&
      !has_contract) {
    return sanitize::Status::Invalid(
        "Strict schema evolution requires a schema contract");
  }
  const bool strict_fast_path =
      has_contract &&
      opts->spec.schema_evolution == SchemaEvolutionMode::kStrict;
  const bool need_inference = !strict_fast_path;

  auto diagnostics = std::make_shared<IngestDiagnostics>();
  diagnostics->bind_operation_memory_pool(operation_memory_pool);
  LogicalSchema inferred_schema;
  bool inference_consumed = false;
  if (need_inference) {
    internal::PerformancePhaseScope inference_scope(
        telemetry, internal::PerformancePhase::kInference);
    SAN_ASSIGN_OR_RAISE(inferred_schema,
                        ingest_internal::infer_schema_from_frontend(
                            frontend_name, frontend, *opts, diagnostics.get(),
                            &inference_consumed, ctx, operation_memory_pool,
                            task_arena));
  }

  LogicalSchema final_schema;
  SAN_ASSIGN_OR_RAISE(
      final_schema,
      ingest_internal::resolve_ingest_logical_schema(*opts, inferred_schema));
  CompiledPlan compiled;
  {
    internal::PerformancePhaseScope plan_scope(
        telemetry, internal::PerformancePhase::kPlanCompile);
    SAN_ASSIGN_OR_RAISE(compiled, compile_plan(final_schema));
  }
  auto plan = std::make_shared<CompiledPlan>(std::move(compiled));

  frontend.set_plan(plan.get());
  if (need_inference && inference_consumed) {
    frontend.reset();
  }

  PreparedIngest out;
  out.frontend_name = std::string(frontend_name);
  out.frontend = std::move(frontend);
  out.owned_ctx = std::move(owned_ctx);
  out.ctx = ctx;
  out.operation_memory_pool = std::move(operation_memory_pool);
  out.task_arena = std::move(task_arena);
  out.telemetry = std::move(telemetry);
  out.plan = std::move(plan);
  out.opts = std::move(opts);
  out.diagnostics = std::move(diagnostics);
  out.logical_schema = std::move(final_schema);
  out.inference_consumed = inference_consumed;
  finish_on_prepare_error.Dismiss();
  return out;
}

} // namespace sanitize
