// Defines flat row batches emitted by text frontends.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <vector>

#include "sanitize/core/row_stream.hh"

namespace sanitize::internal {

inline constexpr std::size_t kMaxMaterializedFieldsPerRow = 65'536;

// Helper for frontends that want to build a batch of RowRef without per-row
// allocations. All FieldRef for the batch are stored in one contiguous vector.
//
// Lifetime: RowRef pointers are valid until the next reset().
class FlatRowBatch {
public:
  explicit FlatRowBatch(
      std::pmr::memory_resource *resource = std::pmr::get_default_resource())
      : fields_(resource), rows_(resource) {}

  // Clears the batch and reserves storage for an expected row count.
  void reset(int64_t capacity) {
    rows_.clear();
    fields_.clear();
    if (capacity > 0) {
      // `capacity` is an upper bound derived from the operation budget and can
      // be much larger than the rows available in a source. Reserving to a
      // normal frontend window avoids charging unused metadata while still
      // making ordinary batches allocation-free after construction.
      constexpr std::size_t kInitialRowMetadataReserve = 4096;
      rows_.reserve(std::min(static_cast<std::size_t>(capacity),
                             kInitialRowMetadataReserve));
    }
  }

  // Starts a row and records its raw input slice metadata.
  void start_row(std::string_view raw = {}, std::size_t base_offset = 0,
                 uint8_t flags = 0, const void *direct_ctx = nullptr,
                 std::string_view source_file = {}) {
    rows_.push_back(RowMetadata{.field_offset = fields_.size(),
                                .field_count = 0,
                                .raw = raw,
                                .base_offset = base_offset,
                                .direct_ctx = direct_ctx,
                                .source_file = source_file,
                                .flags = flags});
  }

  // Appends one field reference to the current row.
  void push(FieldRef f) { fields_.push_back(f); }

  // Replaces one field slot relative to the current row start.
  [[nodiscard]] bool set_current_row_field(std::size_t index,
                                           FieldRef field) noexcept {
    if (rows_.empty() || index >= fields_.size() - rows_.back().field_offset) {
      return false;
    }
    fields_[rows_.back().field_offset + index] = field;
    return true;
  }

  // Returns the first field offset of the row currently being built.
  [[nodiscard]] std::size_t current_row_offset() const noexcept {
    return rows_.empty() ? fields_.size() : rows_.back().field_offset;
  }

  // Removes all fields appended since start_row() for the current row.
  void truncate_current_row_fields() {
    if (!rows_.empty()) {
      fields_.erase(fields_.begin() +
                        static_cast<std::ptrdiff_t>(rows_.back().field_offset),
                    fields_.end());
    }
  }

  // Replaces the flags of the row currently being built.
  void set_current_row_flags(uint8_t flags) noexcept {
    if (!rows_.empty()) {
      rows_.back().flags = flags;
    }
  }

  // Abandons the row currently being built and restores batch metadata.
  void abort_current_row() noexcept {
    if (rows_.empty() || rows_.back().complete) {
      return;
    }
    fields_.erase(fields_.begin() +
                      static_cast<std::ptrdiff_t>(rows_.back().field_offset),
                  fields_.end());
    rows_.pop_back();
  }

  // Finishes the current row.
  void end_row() {
    auto &row = rows_.back();
    row.field_count = fields_.size() - row.field_offset;
    row.complete = true;
  }

  // Exports stable row views backed by this batch.
  void export_rows(std::vector<RowRef> *out) const {
    if (!out)
      return;
    out->clear();
    out->reserve(rows_.size());
    const FieldRef *base = fields_.data();
    for (const auto &meta : rows_) {
      RowRef row;
      row.fields = base + meta.field_offset;
      row.size = meta.field_count;
      row.raw = meta.raw;
      row.base_offset = meta.base_offset;
      row.direct_ctx = meta.direct_ctx;
      row.source_file = meta.source_file;
      row.flags = meta.flags;
      out->push_back(row);
    }
  }

private:
  struct RowMetadata {
    std::size_t field_offset = 0;
    std::size_t field_count = 0;
    std::string_view raw;
    std::size_t base_offset = 0;
    const void *direct_ctx = nullptr;
    std::string_view source_file;
    uint8_t flags = 0;
    bool complete = false;
  };

  std::pmr::vector<FieldRef> fields_;
  std::pmr::vector<RowMetadata> rows_;
};
} // namespace sanitize::internal
