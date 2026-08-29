// Declares private implementation details for Arrow C Data batch building. The
// code converts validated rows into memory-accounted Arrow C Data batches for
// ordered ingestion.

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

  /// Resets the scalar scratch while retaining owned UTF-8 capacity.
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

  /// Returns the UTF-8 bytes represented by this direct scalar.
  [[nodiscard]] std::string_view utf8() const noexcept {
    return borrows_utf8 ? borrowed_utf8 : std::string_view(owned_utf8);
  }
};

class ColumnBuilder {
public:
  /// Enables polymorphic destruction of concrete Arrow column builders.
  virtual ~ColumnBuilder() = default;
  /// Clears appended values while retaining reusable builder capacity.
  virtual sanitize::Status reset() = 0;
  /// Accepts an optional capacity hint; the base builder intentionally retains
  /// its current size.
  virtual sanitize::Status reserve(int64_t rows,
                                   int64_t variable_bytes_per_column) {
    (void)rows;
    (void)variable_bytes_per_column;
    return sanitize::Status::OK();
  }
  /// Appends one converted cell to the concrete Arrow column builder.
  virtual sanitize::Status append(const Cell &cell) = 0;
  /// Appends one preconverted scalar when the concrete builder supports direct
  /// input.
  virtual sanitize::Status append_direct(const DirectScalarValue &) {
    return sanitize::Status::NotImplemented(
        "column builder does not support direct scalar append");
  }
  /// Appends a complete row of preconverted scalars when supported by the root
  /// builder.
  virtual sanitize::Status
  append_direct_row(std::span<const DirectScalarValue>) {
    return sanitize::Status::NotImplemented(
        "column builder does not support direct row append");
  }
  /// Appends one null value while preserving the column's logical length.
  virtual sanitize::Status append_null() = 0;
  /// Imports one Arrow array when the concrete builder supports bulk input.
  virtual sanitize::Status append_array(const ArrowArray &) {
    return sanitize::Status::NotImplemented(
        "column builder does not support bulk Arrow append");
  }
  /// Finalizes the column and transfers its buffers to an Arrow array.
  virtual sanitize::Status finish(ArrowArray *out) = 0;
  /// Returns the number of logical values currently appended.
  [[nodiscard]] virtual int64_t length() const noexcept = 0;
  /// Returns retained bytes currently owned by the builder.
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

  /// Finds the first non-empty row field matching a planned output key.
  sanitize::Result<sanitize::ValueView>
  find(std::string_view key, const sanitize::PreparedOptions &opts,
       bool *found) const;
  /// Returns whether a non-empty unplanned field is present.
  sanitize::Result<bool>
  has_unplanned_field(const sanitize::StructLayout &layout,
                      const sanitize::PreparedOptions &opts,
                      std::string *name) const;
};

struct RowFieldSnapshot {
  const sanitize::RowRef *row = nullptr;
  std::vector<int32_t> column_field_indices;

  /// Materializes row fields and pre-resolves source keys to root columns.
  [[nodiscard]] sanitize::Status build(const sanitize::RowRef &row,
                                       const sanitize::CompiledPlan &plan,
                                       const sanitize::PreparedOptions &opts);
  /// Finds the value for one planned root column after build() has completed.
  [[nodiscard]] bool find(std::size_t column_index,
                          sanitize::ValueView *out) const;
};

/// Recovers the original source name encoded in a flattened output name.
std::string_view unflattened_name(std::string_view value) noexcept;

/// Initializes a null cell tree matching the planned logical type.
sanitize::Status convert_null(const sanitize::ColumnPlan &plan, Cell *out);
/// Converts a value into a planned scalar or recursively nested cell.
sanitize::Status convert_value(const sanitize::ColumnPlan &plan,
                               sanitize::ValueView value, ConvertCtx &ctx,
                               Cell *out);

/// Constructs a recursive root builder hierarchy for the compiled plan.
sanitize::Result<std::unique_ptr<ColumnBuilder>>
make_root_builder(const sanitize::CompiledPlan &plan,
                  const std::shared_ptr<PoolResource> &pool);

class BatchAppender {
public:
  /// Records the compiled plan and reusable pool, including direct-append
  /// capabilities.
  BatchAppender(const sanitize::CompiledPlan &plan,
                std::shared_ptr<PoolResource> pool);

  /// Constructs the recursive root builder for the compiled plan.
  sanitize::Status init();
  /// Returns the compiled plan.
  [[nodiscard]] const sanitize::CompiledPlan &plan() const noexcept;
  /// Clears the current batch while retaining reusable builder storage.
  sanitize::Status reset();
  /// Returns the current row count.
  [[nodiscard]] int64_t length() const noexcept;
  /// Returns retained builder-buffer capacity in bytes.
  [[nodiscard]] int64_t bytes() const noexcept;
  /// Reserves one packet's predictable scalar storage.
  sanitize::Status reserve(int64_t rows, int64_t source_bytes);
  /// Finalizes the current batch and transfers its buffers to an Arrow array.
  sanitize::Status finish(ArrowArray *out);
  /// Returns reusable row conversion scratch sized for the current schema.
  std::vector<Cell> &prepare_row_cells(std::size_t size);
  /// Appends the reusable row conversion scratch.
  sanitize::Status append_prepared_cells();
  /// Appends one externally prepared row and takes ownership of its cells.
  sanitize::Status append_cells(std::vector<Cell> cells);
  /// Appends one compatible Arrow struct array in bulk.
  sanitize::Status append_array(const ArrowArray &array);
  /// Returns reusable FieldRef scratch for direct text frontends.
  std::vector<sanitize::FieldRef> &prepare_field_refs(std::size_t reserve);
  /// Returns reusable root-field lookup scratch for direct append paths.
  RowFieldSnapshot &prepare_row_snapshot() noexcept;
  /// Returns whether this appender can consume prevalidated scalar rows.
  [[nodiscard]] bool supports_direct_scalar_rows() const noexcept;
  /// Returns reusable direct-scalar scratch sized for the current schema.
  std::vector<DirectScalarValue> &prepare_direct_scalars();
  /// Appends the prevalidated direct-scalar scratch.
  sanitize::Status append_direct_scalars();
  /// Extends every root column with one null value.
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

/// Converts one parsed row into cells under the frozen compiled plan.
sanitize::Result<PreparedRow>
prepare_materialized_row(const sanitize::CompiledPlan &plan,
                         const sanitize::RowRef &row,
                         const sanitize::PreparedOptions &opts,
                         sanitize::IngestDiagnostics *diagnostics);

/// Converts and appends one row through appender-owned reusable cell scratch.
sanitize::Result<AppendRowResult>
append_materialized_row_reuse(BatchAppender *app, const sanitize::RowRef &row,
                              const sanitize::PreparedOptions &opts,
                              sanitize::IngestDiagnostics *diagnostics);

/// Applies the configured row-error policy after direct scalar conversion.
sanitize::Result<std::optional<AppendRowResult>>
handle_direct_scalar_conversion_error(BatchAppender *app,
                                      const sanitize::PreparedOptions &opts,
                                      sanitize::IngestDiagnostics *diagnostics,
                                      const CoerceError &error,
                                      const sanitize::Status &status);

/// Appends a flat scalar row without materializing owning Cell strings.
sanitize::Result<std::optional<AppendRowResult>>
try_append_direct_scalar_row(BatchAppender *app, const sanitize::RowRef &row,
                             const sanitize::PreparedOptions &opts,
                             sanitize::IngestDiagnostics *diagnostics);

} // namespace sanitize::internal
