// Private implementation details for Arrow C Data batch building.

#pragma once

#include "internal/materialization/batch_appender.hh"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "internal/memory/pool_resource.hh"
#include "nanoarrow/nanoarrow.h"
#include "sanitize/core/value_view.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

struct Cell {
  bool is_null = true;
  sanitize::LogicalKind kind = sanitize::LogicalKind::kNull;
  bool b = false;
  int64_t i64 = 0;
  double f64 = 0.0;
  std::string str;
  std::vector<Cell> children;
  std::vector<Cell> elements;

  // Creates a null value view.
  static Cell Null(sanitize::LogicalKind kind) {
    Cell cell;
    cell.is_null = true;
    cell.kind = kind;
    return cell;
  }
};

class ColumnBuilder {
public:
  // Destroys the ColumnBuilder.
  virtual ~ColumnBuilder() = default;
  virtual sanitize::Status reset() = 0;
  virtual sanitize::Status append(const Cell &cell) = 0;
  virtual sanitize::Status append_null() = 0;
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
  std::vector<sanitize::FieldRef> fields;
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
  // Finishes the current output.
  sanitize::Status finish(ArrowArray *out);
  // Returns reusable row conversion scratch sized for the current schema.
  std::vector<Cell> &prepare_row_cells(std::size_t size);
  // Appends the reusable row conversion scratch.
  sanitize::Status append_prepared_cells();
  // Returns reusable FieldRef scratch for direct text frontends.
  std::vector<sanitize::FieldRef> &prepare_field_refs(std::size_t reserve);
  // Appends null row.
  sanitize::Status append_null_row();

private:
  const sanitize::CompiledPlan *plan_ = nullptr;
  std::shared_ptr<PoolResource> pool_;
  std::unique_ptr<ColumnBuilder> root_;
  std::vector<Cell> row_cells_;
  std::vector<sanitize::FieldRef> field_refs_;
};

// Appends materialized row.
sanitize::Result<AppendRowResult>
append_materialized_row(BatchAppender *app, const sanitize::RowRef &row,
                        const sanitize::PreparedOptions &opts,
                        sanitize::IngestDiagnostics *diagnostics);

} // namespace sanitize::internal
