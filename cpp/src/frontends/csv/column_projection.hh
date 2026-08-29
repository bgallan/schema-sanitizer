// Declares CSV column projection state for the built-in CSV frontend.
//
// Exact mode retains one canonical mutable header. Union mode consumes shared
// immutable per-source projections selected by TextSlice::source_index.

#pragma once

#include "sanitize/core/status.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "frontends/csv/source_projection.hh"
#include "internal/parsing/csv_direct.hh"
#include "internal/parsing/flat_row_batch.hh"
#include "internal/string_lookup.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {

class CsvColumnProjection {
public:
  /// Creates projection state from CSV options and optional pre-read headers.
  CsvColumnProjection(const sanitize::Options &opts, char delimiter,
                      CsvSourceProjectionSetPtr source_projections = nullptr);

  /// Installs the active compiled plan and rebuilds exact-mode direct mapping.
  void set_plan(const sanitize::CompiledPlan *plan) noexcept;

  /// Invalidates header-derived state after a source reset.
  void reset_header() noexcept;

  /// Returns whether the CSV input is configured with a header row.
  [[nodiscard]] bool has_header() const noexcept;

  /// Returns whether the exact-mode header has been read or skipped.
  [[nodiscard]] bool header_ready() const noexcept;

  /// Returns whether union mode owns pre-read immutable source projections.
  [[nodiscard]] bool has_source_projections() const noexcept;

  /// Returns whether rows can use exact-mode direct CSV materialization.
  [[nodiscard]] bool can_use_raw_only() const noexcept;

  /// Returns direct CSV materialization context when available.
  [[nodiscard]] const CsvDirectContext *direct_context() const noexcept;

  /// Returns the best known column count for per-batch reservations.
  [[nodiscard]] std::size_t column_count_hint() const noexcept;

  /// Rejects duplicate, colliding, or strict-schema-incompatible fields.
  [[nodiscard]] sanitize::Status
  validate_header_cells(std::span<const std::string_view> cells) const;

  /// Validates one runtime header against its pre-read immutable source header.
  [[nodiscard]] sanitize::Status
  validate_source_header(std::size_t source_index,
                         std::span<const std::string_view> cells) const;

  /// Stores the exact-mode parsed header and updates derived lookup state.
  void set_header_cells(std::span<const std::string_view> cells);

  /// Returns whether parsed header cells match the exact-mode stored header.
  [[nodiscard]] bool
  header_cells_equal(std::span<const std::string_view> cells) const;

  /// Appends projected cells into the current flat row.
  [[nodiscard]] sanitize::Status append_parsed_cells(
      FlatRowBatch *batch, std::span<const std::string_view> cells,
      std::size_t source_index = 0, bool has_source_index = false);

private:
  /// Resolves a header key against the compiled root layout without using
  /// derived caches.
  [[nodiscard]] const sanitize::FieldIndex *
  find_root_field_uncached(std::string_view key) const noexcept;

  /// Rebuilds the planned-column mapping from header-derived field resolutions.
  void build_direct_from_headers(std::size_t column_count);

  /// Returns the header label or generated positional key for a physical
  /// column.
  [[nodiscard]] std::string_view column_key(std::size_t index);

  /// Extends the generated positional-key cache through the requested column
  /// count.
  void ensure_numeric_keys(std::size_t count);

  /// Computes hashes for physical columns not already present in the cache.
  void ensure_column_hashes(std::size_t column_count);

  /// Resolves newly observed physical columns against the active compiled plan.
  void ensure_resolved_fields(std::size_t column_count);

  /// Extends the mask that identifies physical columns retained by the plan.
  void ensure_keep_mask(std::size_t column_count);

  /// Returns the immutable projection selected by a valid source index, if
  /// available.
  [[nodiscard]] const CsvSourceProjection *
  source_projection(std::size_t source_index,
                    bool has_source_index) const noexcept;

  /// Appends a union-mode row through its immutable source projection while
  /// omitting unplanned fields.
  [[nodiscard]] sanitize::Status
  append_union_cells(FlatRowBatch *batch,
                     std::span<const std::string_view> cells,
                     std::size_t source_index, bool has_source_index) const;

  bool has_header_ = true;
  bool strict_schema_ = false;
  bool union_mode_ = false;
  bool raw_only_ = false;
  bool direct_ready_ = false;
  bool header_ready_ = false;
  bool keep_mask_ready_ = false;

  std::string field_name_policy_;
  CsvDirectContext direct_;
  const sanitize::CompiledPlan *plan_ = nullptr;
  CsvSourceProjectionSetPtr source_projections_;
  std::vector<std::string> headers_;
  std::vector<std::string> numeric_keys_;
  std::vector<std::uint64_t> column_hashes_;
  std::vector<const sanitize::FieldIndex *> resolved_fields_;
  std::vector<uint8_t> keep_mask_;
};

} // namespace sanitize::internal
