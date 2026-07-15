// Defines flat row batches emitted by text frontends.

#pragma once

#include <cstddef>
#include <cstdint>
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
  // Clears the batch and reserves storage for an expected row count.
  void reset(int64_t capacity) {
    offsets_.clear();
    sizes_.clear();
    raw_.clear();
    flags_.clear();
    base_offsets_.clear();
    direct_ctx_.clear();
    source_files_.clear();
    fields_.clear();
    if (capacity > 0) {
      const auto cap = static_cast<std::size_t>(capacity);
      offsets_.reserve(cap);
      sizes_.reserve(cap);
      raw_.reserve(cap);
      flags_.reserve(cap);
      base_offsets_.reserve(cap);
      direct_ctx_.reserve(cap);
      source_files_.reserve(cap);
    }
  }

  // Starts a row and records its raw input slice metadata.
  void start_row(std::string_view raw = {}, std::size_t base_offset = 0,
                 uint8_t flags = 0, const void *direct_ctx = nullptr,
                 std::string_view source_file = {}) {
    offsets_.push_back(fields_.size());
    raw_.push_back(raw);
    base_offsets_.push_back(base_offset);
    flags_.push_back(flags);
    direct_ctx_.push_back(direct_ctx);
    source_files_.push_back(source_file);
  }

  // Appends one field reference to the current row.
  void push(FieldRef f) { fields_.push_back(f); }

  // Finishes the current row.
  void end_row() {
    const std::size_t off = offsets_.back();
    sizes_.push_back(fields_.size() - off);
  }

  // Exports stable row views backed by this batch.
  void export_rows(std::vector<RowRef> *out) const {
    if (!out)
      return;
    out->clear();
    out->reserve(sizes_.size());
    const FieldRef *base = fields_.data();
    for (std::size_t i = 0; i < sizes_.size(); ++i) {
      RowRef r;
      r.fields = base + offsets_[i];
      r.size = sizes_[i];
      r.raw = raw_[i];
      r.base_offset = base_offsets_[i];
      r.direct_ctx = direct_ctx_[i];
      r.source_file = source_files_[i];
      r.flags = flags_[i];
      out->push_back(r);
    }
  }

private:
  std::vector<FieldRef> fields_;
  std::vector<std::size_t> offsets_;
  std::vector<std::size_t> sizes_;
  std::vector<std::string_view> raw_;
  std::vector<uint8_t> flags_;
  std::vector<std::size_t> base_offsets_;
  std::vector<const void *> direct_ctx_;
  std::vector<std::string_view> source_files_;
};

} // namespace sanitize::internal
