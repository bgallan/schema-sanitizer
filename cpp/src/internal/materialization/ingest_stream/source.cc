// Implements construction and Arrow stream callbacks for ingestion.

#include "internal/materialization/ingest_stream/source.hh"

#include "internal/arrow_c/cdata_schema_builder.hh"
#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/materialization/batch_appender.hh"
#include "internal/materialization/direct_rows.hh"
#include "internal/materialization/ingest_stream/source_internal.hh"
#include "internal/memory/pool_resource.hh"
#include "sanitize/core/status.hh"

#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize::internal {
namespace {

std::vector<RuntimeFieldLayout>
derive_runtime_fields_from_plan(const sanitize::CompiledPlan &plan) {
  std::vector<RuntimeFieldLayout> fields;
  fields.reserve(plan.columns.size());
  for (const auto &column : plan.columns) {
    RuntimeFieldLayout field;
    field.name = column.name;
    field.nullable = column.nullable;
    field.logical_type = column.logical_type;
    fields.push_back(std::move(field));
  }
  return fields;
}

} // namespace

IngestStreamSource::IngestStreamSource(IngestStreamInit init)
    : fields_(std::move(init.fields)), frontend_(std::move(init.frontend)),
      plan_keepalive_(std::move(init.plan)), opts_(std::move(init.opts)),
      diagnostics_(std::move(init.diagnostics)),
      owned_ctx_keepalive_(std::move(init.owned_ctx)),
      operation_memory_pool_keepalive_(
          std::move(init.operation_memory_pool)),
      app_(std::move(init.app)), pool_keepalive_(std::move(init.pool)),
      direct_(std::move(init.direct)) {}

sanitize::Status IngestStreamSource::GetSchema(struct ArrowSchema *out) {
  return export_fields_as_struct_schema(fields_, out,
                                        opts_->spec.timestamp_precision);
}

sanitize::Status IngestStreamSource::GetNext(struct ArrowArray *out) {
  if (!out) {
    return sanitize::Status::Invalid(
        "IngestStreamSource::GetNext: out is null");
  }
  SAN_RETURN_NOT_OK(check_interrupt());

  if (eof_) {
    sanitize::internal::cdata_stream::clear_array(out);
    return sanitize::Status::OK();
  }

  const BatchLimits limits = batch_limits();
  for (;;) {
    SAN_RETURN_NOT_OK(batch_appender_reset(app_.get()));
    SAN_RETURN_NOT_OK(fill_appender(limits));

    if (batch_appender_length(app_.get()) == 0) {
      if (eof_) {
        sanitize::internal::cdata_stream::clear_array(out);
        return sanitize::Status::OK();
      }
      continue;
    }

    const auto batch_rows = batch_appender_length(app_.get());
    const auto batch_bytes = batch_appender_bytes(app_.get());
    if (batch_rows > 0) {
      const auto sample = std::max<int64_t>(1, batch_bytes / batch_rows);
      observed_bytes_per_row_ =
          has_observed_batch_size_
              ? std::max<int64_t>(1, (observed_bytes_per_row_ * 3 + sample) / 4)
              : sample;
      has_observed_batch_size_ = true;
    }
    SAN_RETURN_NOT_OK(batch_appender_finish(app_.get(), out));
    record_finished_batch(out);
    return sanitize::Status::OK();
  }
}

sanitize::Status IngestStreamSource::Close() {
  closed_ = true;
  return sanitize::Status::OK();
}

sanitize::Result<std::shared_ptr<sanitize::ExportBatchSource>>
make_ingest_stream_source(
    std::string_view frontend_name, FrontendHandle frontend,
    std::shared_ptr<const CompiledPlan> plan, PreparedOptionsPtr opts,
    std::shared_ptr<IngestDiagnostics> diagnostics,
    std::shared_ptr<sanitize::ExecutionContext> owned_ctx,
    std::shared_ptr<void> operation_memory_pool) {
  if (!plan) {
    return sanitize::Status::Invalid("make_ingest_stream_source: plan is null");
  }
  if (!opts) {
    return sanitize::Status::Invalid("make_ingest_stream_source: opts is null");
  }

  auto runtime_fields = derive_runtime_fields_from_plan(*plan);
  auto pool = std::make_shared<PoolResource>(operation_memory_pool);
  SAN_ASSIGN_OR_RAISE(auto app, make_batch_appender(*plan, pool));
  SAN_ASSIGN_OR_RAISE(auto direct,
                      make_direct_materializer(frontend_name, pool.get()));

  std::shared_ptr<sanitize::ExportBatchSource> source =
      std::make_shared<IngestStreamSource>(IngestStreamInit{
          .fields = std::move(runtime_fields),
          .frontend = std::move(frontend),
          .plan = std::move(plan),
          .opts = std::move(opts),
          .diagnostics = std::move(diagnostics),
          .owned_ctx = std::move(owned_ctx),
          .operation_memory_pool = std::move(operation_memory_pool),
          .app = std::move(app),
          .pool = std::move(pool),
          .direct = std::move(direct),
      });
  return source;
}

} // namespace sanitize::internal
