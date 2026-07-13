// Prepares ingestion by resolving schemas and compiling materialization plans.

#include "sanitize/ingest/ingest.hh"

#include "sanitize/runtime/execution_context.hh"

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "ingest/prepare/prepare_internal.hh"
#include "internal/materialization/stream.hh"
#include "internal/planning/plan_compile.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/ingest_types.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize {

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
  LogicalSchema inferred_schema;
  bool inference_consumed = false;
  if (need_inference) {
    SAN_ASSIGN_OR_RAISE(
        inferred_schema,
        ingest_internal::infer_schema_from_frontend(
            frontend, *opts, diagnostics.get(), &inference_consumed, ctx));
  }

  LogicalSchema final_schema;
  SAN_ASSIGN_OR_RAISE(
      final_schema,
      ingest_internal::resolve_ingest_logical_schema(*opts, inferred_schema));
  SAN_ASSIGN_OR_RAISE(CompiledPlan compiled, compile_plan(final_schema));
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
  out.plan = std::move(plan);
  out.opts = std::move(opts);
  out.diagnostics = std::move(diagnostics);
  out.logical_schema = std::move(final_schema);
  out.inference_consumed = inference_consumed;
  return out;
}

} // namespace sanitize
