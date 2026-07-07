// Implements ingest Arrow C stream source construction helpers.

#include "internal/pipeline/ingest_stream_source.hh"

#include "internal/build/build.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/pipeline/batch_sizing.hh"
#include "internal/pipeline/cdata_export_internal.hh"
#include "internal/pipeline/cdata_schema_builder.hh"
#include "internal/pipeline/cdata_stream_utils.hh"
#include "internal/pipeline/direct_materializer.hh"

#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/runtime/execution_context.hh"

#include "nanoarrow/nanoarrow.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace sanitize::internal {
namespace {

using RuntimeFieldLayout = CDataFieldLayout;

// Derives runtime fields from plan.
std::vector<RuntimeFieldLayout>
derive_runtime_fields_from_plan(const sanitize::CompiledPlan &plan) {
  std::vector<RuntimeFieldLayout> fields;
  fields.reserve(plan.columns.size());
  for (const auto &col : plan.columns) {
    RuntimeFieldLayout field;
    field.name = col.name;
    field.nullable = col.nullable;
    field.logical_type = col.logical_type;
    fields.push_back(std::move(field));
  }
  return fields;
}

// Bundles owned dependencies needed to construct an ingest stream.
struct IngestStreamInit {
  std::vector<RuntimeFieldLayout> fields;
  FrontendHandle frontend;
  std::shared_ptr<const CompiledPlan> plan;
  PreparedOptionsPtr opts;
  std::shared_ptr<IngestDiagnostics> diagnostics;
  std::shared_ptr<sanitize::ExecutionContext> owned_ctx;
  BatchAppenderPtr app;
  std::shared_ptr<PoolResource> pool;
  std::unique_ptr<DirectMaterializer> direct;
};

class IngestStream final : public sanitize::ExportBatchSource {
public:
  // Creates an ingest stream from prepared frontend and materialization state.
  explicit IngestStream(IngestStreamInit init)
      : fields_(std::move(init.fields)), frontend_(std::move(init.frontend)),
        plan_keepalive_(std::move(init.plan)), opts_(std::move(init.opts)),
        diagnostics_(std::move(init.diagnostics)),
        owned_ctx_keepalive_(std::move(init.owned_ctx)),
        app_(std::move(init.app)), pool_keepalive_(std::move(init.pool)),
        direct_(std::move(init.direct)) {}

  // Exports the Arrow schema for materialized output batches.
  sanitize::Status GetSchema(struct ArrowSchema *out) override {
    return export_fields_as_struct_schema(fields_, out,
                                          opts_->spec.timestamp_precision);
  }

  // Exports the next materialized ArrowArray batch.
  sanitize::Status GetNext(struct ArrowArray *out) override {
    if (!out) {
      return sanitize::Status::Invalid("IngestStream::GetNext: out is null");
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

      SAN_RETURN_NOT_OK(batch_appender_finish(app_.get(), out));
      record_finished_batch(out);
      return sanitize::Status::OK();
    }
  }

  // Closes the object state.
  sanitize::Status Close() override {
    if (!closed_) {
      closed_ = true;
    }
    return sanitize::Status::OK();
  }

private:
  struct BatchLimits {
    int64_t max_rows = 0;
    int64_t max_bytes = 0;
    int64_t capacity = 0;
  };

  // Returns batch limits derived from prepared options.
  [[nodiscard]] BatchLimits batch_limits() const {
    const int64_t memory_limit = opts_ ? opts_->spec.memory_limit_bytes : -1;
    const int64_t max_rows = rows_per_batch_from_memory_limit(memory_limit);
    return BatchLimits{
        .max_rows = max_rows, .max_bytes = memory_limit, .capacity = max_rows};
  }

  // Returns whether the appender has reached a pre-row append limit.
  [[nodiscard]] bool appender_is_full(const BatchLimits &limits) const {
    const int64_t cur_len = batch_appender_length(app_.get());
    if (limits.max_rows > 0 && cur_len >= limits.max_rows) {
      return true;
    }
    return limits.max_bytes > 0 && cur_len > 0 &&
           batch_appender_bytes(app_.get()) >= limits.max_bytes;
  }

  // Returns whether byte limits require ending the batch after an append.
  [[nodiscard]] bool byte_limit_reached(const BatchLimits &limits) const {
    return limits.max_bytes > 0 && batch_appender_length(app_.get()) > 0 &&
           batch_appender_bytes(app_.get()) >= limits.max_bytes;
  }

  // Loads the next frontend batch when the current batch is exhausted.
  sanitize::Result<bool> ensure_current_row(const BatchLimits &limits) {
    if (cur_i_ < cur_.rows.size()) {
      return true;
    }

    SAN_RETURN_NOT_OK(check_interrupt());
    auto next = frontend_.next_batch(limits.capacity);
    if (!next.ok()) {
      sanitize::Status status = next.status();
      return status;
    }
    cur_ = std::move(next).ValueOrDie();
    cur_i_ = 0;
    if (cur_.rows.empty()) {
      eof_ = true;
      return false;
    }
    return true;
  }

  // Appends one row through the direct or planned materialization path.
  sanitize::Result<AppendRowResult> append_current_row(const RowRef &row) {
    const bool raw_only =
        (row.flags & std::to_underlying(RowFlags::kRawOnly)) != 0;
    if (raw_only) {
      if (!direct_) {
        return sanitize::Status::Invalid(
            "raw-only row encountered but frontend has no direct materializer");
      }
      return direct_->AppendRaw(app_.get(), row, *opts_, diagnostics_.get());
    }
    return append_row(app_.get(), row, *opts_, diagnostics_.get());
  }

  // Polls any context-provided interrupt hook.
  sanitize::Status check_interrupt() const {
    if (!owned_ctx_keepalive_) {
      return sanitize::Status::OK();
    }
    return owned_ctx_keepalive_->CheckInterrupt();
  }

  // Appends source rows until a batch limit or EOF is reached.
  sanitize::Status fill_appender(const BatchLimits &limits) {
    std::size_t interrupt_countdown = 0;
    while (!appender_is_full(limits)) {
      if ((interrupt_countdown++ & std::size_t{1023}) == 0) {
        SAN_RETURN_NOT_OK(check_interrupt());
      }

      SAN_ASSIGN_OR_RAISE(bool has_row, ensure_current_row(limits));
      if (!has_row) {
        break;
      }

      const RowRef &row = cur_.rows[cur_i_++];
      const int64_t rows_before = batch_appender_length(app_.get());
      const int64_t skipped_before =
          diagnostics_ ? diagnostics_->skipped_rows : 0;
      auto result = append_current_row(row);
      if (!result.ok()) {
        return result.status();
      }
      (void)result.ValueOrDie();
      if (diagnostics_ &&
          opts_->spec.on_error == sanitize::OnErrorPolicy::kSkipRow &&
          batch_appender_length(app_.get()) == rows_before &&
          diagnostics_->skipped_rows == skipped_before) {
        diagnostics_->skipped_rows += 1;
      }
      row_index_++;

      if (byte_limit_reached(limits)) {
        break;
      }
    }
    return sanitize::Status::OK();
  }

  // Records diagnostics after exporting a non-empty ArrowArray batch.
  void record_finished_batch(const ArrowArray *out) {
    if (diagnostics_) {
      diagnostics_->batches += 1;
      diagnostics_->materialized_rows += out->length;
    }
  }

  std::vector<RuntimeFieldLayout> fields_;
  FrontendHandle frontend_;
  std::shared_ptr<const CompiledPlan> plan_keepalive_;
  PreparedOptionsPtr opts_;
  std::shared_ptr<IngestDiagnostics> diagnostics_;
  std::shared_ptr<sanitize::ExecutionContext> owned_ctx_keepalive_;

  BatchAppenderPtr app_;
  std::shared_ptr<PoolResource> pool_keepalive_;
  std::unique_ptr<DirectMaterializer> direct_;

  RowBatch cur_;
  std::size_t cur_i_ = 0;
  int64_t row_index_ = 0;
  bool eof_ = false;
  bool closed_ = false;
};

} // namespace

sanitize::Result<std::shared_ptr<sanitize::ExportBatchSource>>
make_ingest_stream_source(
    std::string_view frontend_name, FrontendHandle frontend,
    std::shared_ptr<const CompiledPlan> plan, PreparedOptionsPtr opts,
    std::shared_ptr<IngestDiagnostics> diagnostics,
    std::shared_ptr<sanitize::ExecutionContext> owned_ctx) {
  if (!plan) {
    return sanitize::Status::Invalid("make_ingest_stream_source: plan is null");
  }
  if (!opts) {
    return sanitize::Status::Invalid("make_ingest_stream_source: opts is null");
  }

  auto runtime_fields = derive_runtime_fields_from_plan(*plan);
  SAN_ASSIGN_OR_RAISE(auto app, make_batch_appender(*plan));

  auto pool = std::make_shared<PoolResource>();
  SAN_ASSIGN_OR_RAISE(auto direct,
                      make_direct_materializer(frontend_name, pool.get()));

  auto src = std::make_shared<IngestStream>(IngestStreamInit{
      .fields = std::move(runtime_fields),
      .frontend = std::move(frontend),
      .plan = std::move(plan),
      .opts = std::move(opts),
      .diagnostics = std::move(diagnostics),
      .owned_ctx = std::move(owned_ctx),
      .app = std::move(app),
      .pool = std::move(pool),
      .direct = std::move(direct),
  });
  return src;
}

} // namespace sanitize::internal
