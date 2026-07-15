// Declares CSV column projection state for the built-in CSV frontend.
//
// Tracks header names, numeric fallback keys, plan keep masks, and direct
// materialization column indexes derived from a compiled plan.

#pragma once

#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "internal/parsing/csv_direct.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/string_lookup.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

class CsvColumnProjection {
public:
  // Creates projection state from CSV options and the resolved delimiter.
  CsvColumnProjection(const sanitize::Options &opts, char delimiter);

  // Installs the active compiled plan and rebuilds no-header direct mapping.
  void set_plan(const sanitize::CompiledPlan *plan) noexcept;

  // Invalidates header-derived state after a source reset.
  void reset_header() noexcept;

  // Returns whether the CSV input is configured with a header row.
  [[nodiscard]] bool has_header() const noexcept;

  // Returns whether the configured header has been read or skipped.
  [[nodiscard]] bool header_ready() const noexcept;

  // Returns whether the current row can use direct CSV materialization.
  [[nodiscard]] bool can_use_raw_only() const noexcept;

  // Returns direct CSV materialization context when available.
  [[nodiscard]] const CsvDirectContext *direct_context() const noexcept;

  // Returns the best known column count for per-batch reservations.
  [[nodiscard]] std::size_t column_count_hint() const noexcept;

  // Rejects header fields absent from a strict compiled schema.
  [[nodiscard]] sanitize::Status
  validate_header_cells(const std::vector<std::string_view> &cells) const;

  // Stores parsed header cells and updates derived lookup state.
  void set_header_cells(const std::vector<std::string_view> &cells);

  // Returns whether parsed header cells match the first stored header.
  [[nodiscard]] bool
  header_cells_equal(const std::vector<std::string_view> &cells) const;

  // Appends projected parsed cells into the current flat row.
  void append_parsed_cells(FlatRowBatch *batch,
                           const std::vector<std::string_view> &cells);

private:
  // Performs one uncached root field lookup.
  [[nodiscard]] const sanitize::FieldIndex *
  find_root_field_uncached(std::string_view key) const noexcept;

  // Builds direct materialization column indexes from header names.
  void build_direct_from_headers(const std::vector<std::string_view> &cells);

  // Returns the header or numeric fallback key for a CSV column.
  [[nodiscard]] std::string_view column_key(std::size_t index);

  // Ensures numeric fallback keys exist up to the requested column count.
  void ensure_numeric_keys(std::size_t count);

  // Ensures source-key hashes exist up to the requested column count.
  void ensure_column_hashes(std::size_t column_count);

  // Resolves each source column once without duplicating owned key strings.
  void ensure_resolved_fields(std::size_t column_count);

  // Ensures the per-column keep mask matches the active plan.
  void ensure_keep_mask(std::size_t column_count);

  bool has_header_ = true;
  bool strict_schema_ = false;
  bool raw_only_ = false;
  bool direct_ready_ = false;
  bool header_ready_ = false;
  bool keep_mask_ready_ = false;

  std::string field_name_policy_;
  CsvDirectContext direct_;
  const sanitize::CompiledPlan *plan_ = nullptr;
  std::vector<std::string> headers_;
  std::vector<std::string> numeric_keys_;
  std::vector<std::uint64_t> column_hashes_;
  std::vector<const sanitize::FieldIndex *> resolved_fields_;
  std::vector<uint8_t> keep_mask_;
};

} // namespace sanitize::internal
