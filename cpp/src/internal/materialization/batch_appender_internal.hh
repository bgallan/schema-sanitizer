// Private implementation details for Arrow C Data batch building.

#pragma once

#include "internal/materialization/batch_appender.hh"

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "internal/memory/pool_resource.hh"
#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/value_view.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

struct DirectScalarValue {
  bool is_null = true;
  bool borrows_utf8 = false;
  sanitize::LogicalKind kind = sanitize::LogicalKind::kNull;
  bool b = false;
  int64_t i64 = 0;
  double f64 = 0.0;
  std::string owned_utf8;
  std::string_view borrowed_utf8;

  // Resets the scalar scratch while retaining owned UTF-8 capacity.
  void reset(sanitize::LogicalKind logical_kind) noexcept {
    is_null = true;
    borrows_utf8 = false;
    kind = logical_kind;
    b = false;
    i64 = 0;
    f64 = 0.0;
    owned_utf8.clear();
    borrowed_utf8 = {};
  }

  // Returns the UTF-8 bytes represented by this direct scalar.
  [[nodiscard]] std::string_view utf8() const noexcept {
    return borrows_utf8 ? borrowed_utf8 : std::string_view(owned_utf8);
  }
};

class ColumnBuilder {
public:
  // Destroys the ColumnBuilder.
  virtual ~ColumnBuilder() = default;
  virtual sanitize::Status reset() = 0;
  virtual sanitize::Status reserve(int64_t rows,
                                   int64_t variable_bytes_per_column) {
    (void)rows;
    (void)variable_bytes_per_column;
    return sanitize::Status::OK();
  }
  virtual sanitize::Status append(const Cell &cell) = 0;
  virtual sanitize::Status append_direct(const DirectScalarValue &) {
    return sanitize::Status::NotImplemented(
        "column builder does not support direct scalar append");
  }
  virtual sanitize::Status
  append_direct_row(std::span<const DirectScalarValue>) {
    return sanitize::Status::NotImplemented(
        "column builder does not support direct row append");
  }
  virtual sanitize::Status append_null() = 0;
  virtual sanitize::Status append_array(const ArrowArray &) {
    return sanitize::Status::NotImplemented(
        "column builder does not support bulk Arrow append");
  }
  virtual sanitize::Status finish(ArrowArray *out) = 0;
  [[nodiscard]] virtual int64_t length() const noexcept = 0;
  [[nodiscard]] virtual int64_t bytes() const noexcept = 0;
};

struct CoerceError {
  sanitize::DiagnosticCode code = sanitize::DiagnosticCode::kUnknown;
  uint32_t path_id = 0;
  std::string detail;
};

struct ConvertCtx {
  const sanitize::PreparedOptions &opts;
  sanitize::IngestDiagnostics *diagnostics = nullptr;
  CoerceError *error = nullptr;
};

struct FieldLookup {
  const sanitize::RowRef *row = nullptr;

  // Finds the object state.
  sanitize::Result<sanitize::ValueView>
  find(std::string_view key, const sanitize::PreparedOptions &opts,
       bool *found) const;
  // Returns whether a non-empty unplanned field is present.
  sanitize::Result<bool>
  has_unplanned_field(const sanitize::StructLayout &layout,
                      const sanitize::PreparedOptions &opts,
                      std::string *name) const;
};

struct RowFieldSnapshot {
  const sanitize::RowRef *row = nullptr;
  std::vector<int32_t> column_field_indices;

  // Materializes row fields and pre-resolves source keys to root columns.
  [[nodiscard]] sanitize::Status build(const sanitize::RowRef &row,
                                       const sanitize::CompiledPlan &plan,
                                       const sanitize::PreparedOptions &opts);
  // Finds the value for one planned root column after build() has completed.
  [[nodiscard]] bool find(std::size_t column_index,
                          sanitize::ValueView *out) const;
};

// Performs the unflattened name operation.
std::string_view unflattened_name(std::string_view value) noexcept;

// Converts null.
sanitize::Status convert_null(const sanitize::ColumnPlan &plan, Cell *out);
// Converts value.
sanitize::Status convert_value(const sanitize::ColumnPlan &plan,
                               sanitize::ValueView value, ConvertCtx &ctx,
                               Cell *out);

// Creates root builder.
sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_root_builder(const sanitize::CompiledPlan &plan,
                  const std::shared_ptr<PoolResource> &pool);

class BatchAppender {
public:
  // Creates a BatchAppender.
  BatchAppender(const sanitize::CompiledPlan &plan,
                std::shared_ptr<PoolResource> pool);

  // Initializes the object state.
  sanitize::Status init();
  // Returns the compiled plan.
  [[nodiscard]] const sanitize::CompiledPlan &plan() const noexcept;
  // Resets the object state.
  sanitize::Status reset();
  // Returns the current row count.
  [[nodiscard]] int64_t length() const noexcept;
  // Returns the current byte usage.
  [[nodiscard]] int64_t bytes() const noexcept;
  // Reserves one packet's predictable scalar storage.
  sanitize::Status reserve(int64_t rows, int64_t source_bytes);
  // Finishes the current output.
  sanitize::Status finish(ArrowArray *out);
  // Returns reusable row conversion scratch sized for the current schema.
  std::vector<Cell> &prepare_row_cells(std::size_t size);
  // Appends the reusable row conversion scratch.
  sanitize::Status append_prepared_cells();
  // Appends one externally prepared row and takes ownership of its cells.
  sanitize::Status append_cells(std::vector<Cell> cells);
  // Appends one compatible Arrow struct array in bulk.
  sanitize::Status append_array(const ArrowArray &array);
  // Returns reusable FieldRef scratch for direct text frontends.
  std::vector<sanitize::FieldRef> &prepare_field_refs(std::size_t reserve);
  // Returns reusable root-field lookup scratch for direct append paths.
  RowFieldSnapshot &prepare_row_snapshot() noexcept;
  // Returns whether this appender can consume prevalidated scalar rows.
  [[nodiscard]] bool supports_direct_scalar_rows() const noexcept;
  // Returns reusable direct-scalar scratch sized for the current schema.
  std::vector<DirectScalarValue> &prepare_direct_scalars();
  // Appends the prevalidated direct-scalar scratch.
  sanitize::Status append_direct_scalars();
  // Appends null row.
  sanitize::Status append_null_row();

private:
  const sanitize::CompiledPlan *plan_ = nullptr;
  std::shared_ptr<PoolResource> pool_;
  std::unique_ptr<ColumnBuilder> root_;
  std::vector<Cell> row_cells_;
  std::vector<sanitize::FieldRef> field_refs_;
  RowFieldSnapshot row_snapshot_;
  std::vector<DirectScalarValue> direct_scalars_;
  int64_t variable_width_columns_ = 0;
  bool supports_direct_scalar_rows_ = false;
};

// Converts one parsed row into cells under the frozen compiled plan.
sanitize::Result<PreparedRow>
prepare_materialized_row(const sanitize::CompiledPlan &plan,
                         const sanitize::RowRef &row,
                         const sanitize::PreparedOptions &opts,
                         sanitize::IngestDiagnostics *diagnostics);

// Converts and appends one row through appender-owned reusable cell scratch.
sanitize::Result<AppendRowResult>
append_materialized_row_reuse(BatchAppender *app, const sanitize::RowRef &row,
                              const sanitize::PreparedOptions &opts,
                              sanitize::IngestDiagnostics *diagnostics);

// Applies the configured row-error policy after direct scalar conversion.
sanitize::Result<std::optional<AppendRowResult>>
handle_direct_scalar_conversion_error(BatchAppender *app,
                                      const sanitize::PreparedOptions &opts,
                                      sanitize::IngestDiagnostics *diagnostics,
                                      const CoerceError &error,
                                      const sanitize::Status &status);

// Appends a flat scalar row without materializing owning Cell strings.
sanitize::Result<std::optional<AppendRowResult>>
try_append_direct_scalar_row(BatchAppender *app, const sanitize::RowRef &row,
                             const sanitize::PreparedOptions &opts,
                             sanitize::IngestDiagnostics *diagnostics);

} // namespace sanitize::internal
