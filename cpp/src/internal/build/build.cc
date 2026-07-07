// Public facade for private Arrow C Data batch building.

#include "internal/build/build_internal.hh"

#include <cstring>
#include <memory>
#include <new>
#include <utility>
#include <vector>

#include "sanitize/core/logical_schema.hh"

namespace sanitize::internal {

BatchAppender::BatchAppender(const sanitize::CompiledPlan &plan)
    : plan_(&plan) {}

sanitize::Status BatchAppender::init() {
  SAN_ASSIGN_OR_RAISE(root_, make_root_builder(*plan_));
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

sanitize::Status BatchAppender::append_cells(std::vector<Cell> cells) {
  if (!root_)
    return sanitize::Status::Invalid(
        "BatchAppender::append_cells: root is null");
  Cell root;
  root.is_null = false;
  root.kind = sanitize::LogicalKind::kStruct;
  root.children = std::move(cells);
  return root_->append(root);
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
make_batch_appender(const sanitize::CompiledPlan &plan) {
  auto app = BatchAppenderPtr(new (std::nothrow) BatchAppender(plan));
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
    if (out->release)
      out->release(out);
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
