// Public entry points for private Arrow C Data batch building.

#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/materialization/batch_appender_internal.hh"

#include <cstring>
#include <memory>
#include <new>
#include <utility>
#include <vector>

#include "sanitize/core/logical_schema.hh"

namespace sanitize::internal {

BatchAppender::BatchAppender(const sanitize::CompiledPlan &plan,
                             std::shared_ptr<PoolResource> pool)
    : plan_(&plan), pool_(std::move(pool)) {}

sanitize::Status BatchAppender::init() {
  SAN_ASSIGN_OR_RAISE(root_, make_root_builder(*plan_, pool_));
  return sanitize::Status::OK();
}

const sanitize::CompiledPlan &BatchAppender::plan() const noexcept {
  return *plan_;
}

sanitize::Status BatchAppender::reset() {
  if (!root_)
    return sanitize::Status::Invalid("BatchAppender::reset: root is null");
  return root_->reset();
}

int64_t BatchAppender::length() const noexcept {
  return root_ ? root_->length() : 0;
}

int64_t BatchAppender::bytes() const noexcept {
  return root_ ? root_->bytes() : 0;
}

sanitize::Status BatchAppender::finish(ArrowArray *out) {
  if (!root_)
    return sanitize::Status::Invalid("BatchAppender::finish: root is null");
  return root_->finish(out);
}

std::vector<Cell> &BatchAppender::prepare_row_cells(std::size_t size) {
  // Reuse the outer vector allocation while destroying nested/string payloads
  // from the previous row. This removes one heap allocation per input row
  // without retaining exceptional nested values.
  row_cells_.clear();
  row_cells_.resize(size);
  return row_cells_;
}

sanitize::Status BatchAppender::append_prepared_cells() {
  if (!root_)
    return sanitize::Status::Invalid(
        "BatchAppender::append_prepared_cells: root is null");
  Cell root;
  root.is_null = false;
  root.kind = sanitize::LogicalKind::kStruct;
  root.children = std::move(row_cells_);
  auto status = root_->append(root);
  row_cells_ = std::move(root.children);
  return status;
}

std::vector<sanitize::FieldRef> &
BatchAppender::prepare_field_refs(std::size_t reserve) {
  field_refs_.clear();
  if (field_refs_.capacity() < reserve) {
    field_refs_.reserve(reserve);
  }
  return field_refs_;
}

sanitize::Status BatchAppender::append_null_row() {
  if (!root_)
    return sanitize::Status::Invalid(
        "BatchAppender::append_null_row: root is null");
  return root_->append_null();
}

void BatchAppenderDeleter::operator()(BatchAppender *app) const noexcept {
  delete app;
}

sanitize::Result<BatchAppenderPtr>
make_batch_appender(const sanitize::CompiledPlan &plan,
                    std::shared_ptr<PoolResource> pool) {
  if (!pool) {
    return sanitize::Status::Invalid("make_batch_appender: pool is null");
  }
  auto app =
      BatchAppenderPtr(new (std::nothrow) BatchAppender(plan, std::move(pool)));
  if (!app)
    return sanitize::Status::OutOfMemory("make_batch_appender: OOM appender");
  auto st = app->init();
  if (!st.ok())
    return st;
  return app;
}

sanitize::Status batch_appender_reset(BatchAppender *app) {
  if (!app)
    return sanitize::Status::Invalid("batch_appender_reset: app is null");
  return app->reset();
}

int64_t batch_appender_length(const BatchAppender *app) noexcept {
  return app ? app->length() : 0;
}

int64_t batch_appender_bytes(const BatchAppender *app) noexcept {
  return app ? app->bytes() : 0;
}

sanitize::Status batch_appender_finish(BatchAppender *app, ArrowArray *out) {
  if (!app)
    return sanitize::Status::Invalid("batch_appender_finish: app is null");
  if (!out)
    return sanitize::Status::Invalid("batch_appender_finish: out is null");
  std::memset(out, 0, sizeof(*out));
  sanitize::Status st = app->finish(out);
  if (!st.ok()) {
    sanitize::internal::cdata_stream::release_array_nothrow(out);
    std::memset(out, 0, sizeof(*out));
  }
  return st;
}

sanitize::Result<AppendRowResult>
append_row(BatchAppender *app, const sanitize::RowRef &row,
           const sanitize::PreparedOptions &opts,
           sanitize::IngestDiagnostics *diagnostics) {
  return append_materialized_row(app, row, opts, diagnostics);
}

} // namespace sanitize::internal
