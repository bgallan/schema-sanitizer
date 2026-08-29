// Defines scanned text slices shared by streaming scanners.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

namespace sanitize::internal {

struct TextSlice {
  std::string_view view;
  std::size_t base_offset = 0;

  // Keep the backing bytes alive when `view` aliases an external buffer
  // (e.g. a file chunk). When empty, the bytes are owned by the RowBatch owner.
  std::shared_ptr<const void> owner;

  // Optional display source backing this slice.
  std::shared_ptr<const std::string> source_file_owner;
  std::string_view source_file;
  std::size_t source_index = 0;
  bool has_source_index = false;
};

/// Creates a text slice with optional backing storage ownership.
inline TextSlice make_text_slice(
    std::string_view view, std::size_t base_offset,
    const std::shared_ptr<const void> &owner = {},
    const std::shared_ptr<const std::string> &source_file_owner = {},
    std::string_view source_file = {}, std::size_t source_index = 0,
    bool has_source_index = false) {
  return {
      .view = view,
      .base_offset = base_offset,
      .owner = owner,
      .source_file_owner = source_file_owner,
      .source_file = source_file,
      .source_index = source_index,
      .has_source_index = has_source_index,
  };
}

/// Returns whether a byte is JSON/CSV whitespace.
inline bool is_ws(unsigned char c) noexcept {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

} // namespace sanitize::internal
