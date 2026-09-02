// Provides public entry points for private Arrow C Data batch building. The
// code converts validated rows into memory-accounted Arrow C Data batches for
// ordered ingestion.

#include "internal/arrow_c/cdata_stream_callbacks.hh"
#include "internal/materialization/batch_appender_internal.hh"

#include <algorithm>
#include <cstring>
#include <memory>
#include <new>
#include <utility>
#include <vector>

#include "sanitize/core/logical_schema.hh"

namespace sanitize::internal {

BatchAppender::BatchAppender(const sanitize::CompiledPlan &plan,
                             std::shared_ptr<PoolResource> pool)
    : plan_(&plan), pool_(std::move(pool)) {
  variable_width_columns_ = static_cast<int64_t>(std::count_if(
      plan.columns.begin(), plan.columns.end(), [](const auto &column) {
        return column.logical_type.kind == sanitize::LogicalKind::kUtf8;
      }));
  supports_direct_scalar_rows_ =
      std::ranges::all_of(plan.columns, [](const sanitize::ColumnPlan &column) {
        const auto kind = column.logical_type.kind;
        return !column.has_variant_sibling &&
               kind != sanitize::LogicalKind::kStruct &&
               kind != sanitize::LogicalKind::kList;
      });
}

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

sanitize::Status BatchAppender::reserve(int64_t rows, int64_t source_bytes) {
  if (!root_) {
    return sanitize::Status::Invalid("BatchAppender::reserve: root is null");
  }
  if (rows < 0 || source_bytes < 0) {
    return sanitize::Status::Invalid(
        "BatchAppender::reserve: negative capacity");
  }
  const int64_t variable_bytes =
      variable_width_columns_ > 0 ? source_bytes / variable_width_columns_ : 0;
  return root_->reserve(rows, variable_bytes);
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

sanitize::Status BatchAppender::append_array(const ArrowArray &array) {
  if (!root_)
    return sanitize::Status::Invalid(
        "BatchAppender::append_array: root is null");
  return root_->append_array(array);
}

std::vector<sanitize::FieldRef> &
BatchAppender::prepare_field_refs(std::size_t reserve) {
  field_refs_.clear();
  if (field_refs_.capacity() < reserve) {
    field_refs_.reserve(reserve);
  }
  return field_refs_;
}

RowFieldSnapshot &BatchAppender::prepare_row_snapshot() noexcept {
  return row_snapshot_;
}

bool BatchAppender::supports_direct_scalar_rows() const noexcept {
  return supports_direct_scalar_rows_;
}

std::vector<DirectScalarValue> &BatchAppender::prepare_direct_scalars() {
  if (direct_scalars_.size() != plan_->columns.size()) {
    direct_scalars_.resize(plan_->columns.size());
  }
  for (std::size_t index = 0; index < direct_scalars_.size(); ++index) {
    direct_scalars_[index].reset(plan_->columns[index].logical_type.kind);
  }
  return direct_scalars_;
}

sanitize::Status BatchAppender::append_direct_scalars() {
  if (!root_) {
    return sanitize::Status::Invalid(
        "BatchAppender::append_direct_scalars: root is null");
  }
  return root_->append_direct_row(direct_scalars_);
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

sanitize::Status batch_appender_reserve(BatchAppender *app, int64_t rows,
                                        int64_t source_bytes) {
  if (!app) {
    return sanitize::Status::Invalid("batch_appender_reserve: app is null");
  }
  return app->reserve(rows, source_bytes);
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

sanitize::Status batch_appender_append_array(BatchAppender *app,
                                             const ArrowArray *array) {
  if (!app)
    return sanitize::Status::Invalid(
        "batch_appender_append_array: app is null");
  if (!array)
    return sanitize::Status::Invalid(
        "batch_appender_append_array: array is null");
  return app->append_array(*array);
}

sanitize::Result<PreparedRow>
prepare_row(const sanitize::CompiledPlan &plan, const sanitize::RowRef &row,
            const sanitize::PreparedOptions &opts,
            sanitize::IngestDiagnostics *diagnostics) {
  return prepare_materialized_row(plan, row, opts, diagnostics);
}

sanitize::Status append_prepared_row(BatchAppender *app, PreparedRow row) {
  if (!app) {
    return sanitize::Status::Invalid("append_prepared_row: app is null");
  }
  switch (row.action) {
  case PreparedRowAction::kAppendCells:
    return app->append_cells(std::move(row.cells));
  case PreparedRowAction::kAppendNull:
    return app->append_null_row();
  case PreparedRowAction::kSkip:
    return sanitize::Status::OK();
  }
  return sanitize::Status::Invalid("append_prepared_row: invalid action");
}

sanitize::Result<AppendRowResult>
append_row(BatchAppender *app, const sanitize::RowRef &row,
           const sanitize::PreparedOptions &opts,
           sanitize::IngestDiagnostics *diagnostics) {
  if (!app) {
    return sanitize::Status::Invalid("append_row: app is null");
  }
  SAN_ASSIGN_OR_RAISE(auto prepared, prepare_materialized_row(
                                         app->plan(), row, opts, diagnostics));
  const auto result = prepared.result;
  SAN_RETURN_NOT_OK(append_prepared_row(app, std::move(prepared)));
  return result;
}

} // namespace sanitize::internal
