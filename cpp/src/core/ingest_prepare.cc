// Prepares ingestion by inferring schemas and compiling materialization plans.
//
// Handles schema inference/evolution, plan compilation, and construction of
// PreparedIngest.
#include "sanitize/ingest/ingest.hh"

#include "sanitize/runtime/execution_context.hh"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "internal/pipeline/batch_sizing.hh"
#include "internal/pipeline/infer.hh"
#include "internal/pipeline/materialize.hh"
#include "internal/planning/field_name_sanitizer.hh"
#include "internal/planning/plan_compile.hh"
#include "internal/planning/schema_evolution.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/ingest_types.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize {

namespace {

struct InferenceProgress {
  int64_t rows = 0;
  int64_t bytes = 0;
};

// Records one inferred row using saturating counters.
static void record_inferred_row(InferenceProgress *progress,
                                const RowRef &row) noexcept {
  if (progress->rows < std::numeric_limits<int64_t>::max()) {
    progress->rows += 1;
  }
  const auto row_bytes = static_cast<uint64_t>(row.raw.size());
  const auto remaining = static_cast<uint64_t>(
      std::numeric_limits<int64_t>::max() - progress->bytes);
  progress->bytes = row_bytes > remaining
                        ? std::numeric_limits<int64_t>::max()
                        : progress->bytes + static_cast<int64_t>(row_bytes);
}

// Scans one frontend batch into inference shape and statistics state.
static sanitize::Status
scan_inference_batch(internal::InferenceContext *ctx, const RowBatch &batch,
                     const PreparedOptions &opts, IngestDiagnostics *diag,
                     InferenceProgress *progress, ExecutionContext *exec_ctx) {
  std::size_t interrupt_countdown = 0;
  for (const auto &row : batch.rows) {
    if (exec_ctx && (interrupt_countdown++ & std::size_t{1023}) == 0) {
      SAN_RETURN_NOT_OK(exec_ctx->CheckInterrupt());
    }
    SAN_RETURN_NOT_OK(internal::scan_shapes_row(ctx, row, opts, diag));
    SAN_RETURN_NOT_OK(internal::update_stats_row(ctx, row, opts, diag));
    record_inferred_row(progress, row);
  }
  return sanitize::Status::OK();
}

// Copies inference counters to diagnostics when diagnostics are requested.
static void apply_inference_diagnostics(const InferenceProgress &progress,
                                        IngestDiagnostics *diag) noexcept {
  if (!diag) {
    return;
  }
  diag->inferred_rows = progress.rows;
  diag->inferred_bytes = progress.bytes;
}

// Infers schema from frontend.
static sanitize::Result<sanitize::LogicalSchema>
infer_schema_from_frontend(FrontendHandle &frontend,
                           const PreparedOptions &opts, IngestDiagnostics *diag,
                           bool *out_consumed, ExecutionContext *exec_ctx) {
  internal::InferenceContext ctx;
  ctx.set_default_key(opts.spec.default_key_name);
  InferenceProgress progress;

  const int64_t want =
      internal::rows_per_batch_from_memory_limit(opts.spec.memory_limit_bytes);

  while (true) {
    if (exec_ctx) {
      SAN_RETURN_NOT_OK(exec_ctx->CheckInterrupt());
    }
    SAN_ASSIGN_OR_RAISE(RowBatch batch, frontend.next_batch(want));
    if (batch.rows.empty())
      break;

    SAN_RETURN_NOT_OK(
        scan_inference_batch(&ctx, batch, opts, diag, &progress, exec_ctx));
  }

  apply_inference_diagnostics(progress, diag);

  if (out_consumed)
    *out_consumed = (progress.rows > 0);

  auto schema = internal::infer_logical_schema(ctx, opts);
  if (diag) {
    diag->arrow_schema_depth = arrow_schema_depth(schema);
    diag->parquet_schema_depth = parquet_schema_depth(schema);
  }
  return schema;
}

} // namespace

sanitize::Result<PreparedIngest> prepare_ingest(std::string_view frontend_name,
                                                FrontendHandle frontend,
                                                PreparedOptionsPtr opts,
                                                ExecutionContext *ctx) {
  if (!frontend)
    return sanitize::Status::Invalid("prepare_ingest: frontend is null");
  if (!opts)
    return sanitize::Status::Invalid("prepare_ingest: opts is null");

  std::shared_ptr<ExecutionContext> owned_ctx;
  if (!ctx) {
    owned_ctx = std::make_shared<ExecutionContext>();
    ctx = owned_ctx.get();
  }
  if (!ctx)
    return sanitize::Status::Invalid("prepare_ingest: ctx is null");

  auto diag = std::make_shared<IngestDiagnostics>();

  // Determine whether we need to infer anything from input.
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

  sanitize::LogicalSchema inferred_logical;
  bool inference_consumed = false;

  if (need_inference) {
    SAN_ASSIGN_OR_RAISE(inferred_logical,
                        infer_schema_from_frontend(frontend, *opts, diag.get(),
                                                   &inference_consumed, ctx));
  }

  // Build final schema in the logical layer, then convert to Arrow at the
  // boundary.
  sanitize::LogicalSchema contract_logical;
  if (has_contract)
    contract_logical = internal::sanitize_logical_schema_field_names(
        *opts->spec.arrow_schema_contract, *opts);

  sanitize::LogicalSchema final_logical;
  if (has_contract) {
    if (strict_fast_path) {
      if (contract_logical.fields.empty()) {
        return sanitize::Status::Invalid(
            "Strict schema evolution requires a non-empty schema contract");
      }
      final_logical = internal::reorder_schema_fields(
          contract_logical, &contract_logical, opts->spec.field_order);
    } else {
      SAN_ASSIGN_OR_RAISE(final_logical, internal::evolve_schema(
                                             contract_logical, inferred_logical,
                                             opts->spec.schema_evolution,
                                             opts->spec.field_order));
    }
  } else {
    final_logical = internal::reorder_schema_fields(inferred_logical, nullptr,
                                                    opts->spec.field_order);
  }

  SAN_ASSIGN_OR_RAISE(CompiledPlan compiled, compile_plan(final_logical));

  auto plan = std::make_shared<CompiledPlan>(std::move(compiled));

  // Let frontend precompute extraction filters.
  frontend.set_plan(plan.get());

  // Reset for materialization only if inference consumed input.
  if (need_inference && inference_consumed) {
    frontend.reset();
  }

  PreparedIngest out;
  out.frontend_name = std::string(frontend_name);
  out.frontend = std::move(frontend);
  out.owned_ctx = std::move(owned_ctx);
  out.ctx = ctx;
  out.plan = plan;
  out.opts = std::move(opts);
  out.diagnostics = std::move(diag);
  out.logical_schema = std::move(final_logical);
  out.inference_consumed = inference_consumed;

  return out;
}

} // namespace sanitize
