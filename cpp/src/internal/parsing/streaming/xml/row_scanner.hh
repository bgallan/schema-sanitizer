// Declares streaming XML row-tag slicing.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "sanitize/core/status.hh"
#include "sanitize/ingest/chunk_source.hh"

namespace sanitize::internal {

/// Streams matching direct-child XML row elements from a chunk source.
class XmlRowTagScanner {
public:
  struct RowSlice {
    // Valid until the next next_row() or Reset() call.
    std::string_view text;
    std::size_t base_offset = 0;
  };

  /// Store stream settings for row-tag scanning.
  XmlRowTagScanner(ChunkSourcePtr src, std::string row_tag, int64_t chunk_bytes,
                   int64_t memory_limit_bytes);

  /// Reset source state and scanner bookkeeping to the beginning.
  sanitize::Status Reset();

  /// Return the next XML row slice or an empty slice at EOF.
  sanitize::Result<RowSlice> next_row();

private:
  static constexpr std::size_t npos = std::string::npos;

  sanitize::Status invalid(std::string_view message) const;
  sanitize::Status enforce_buffer_limit(std::size_t incoming) const;
  [[nodiscard]] bool should_compact_before_refill() const noexcept;
  [[nodiscard]] std::size_t retained_buffer_limit() const noexcept;
  void discard_buffer();
  void compact_buffer();
  sanitize::Result<bool> ensure_data();
  sanitize::Status refill();
  sanitize::Status read_more_or_fail(std::string_view eof_message);
  sanitize::Status handle_text(std::string_view text);
  sanitize::Result<std::size_t> find_markup_end(std::size_t lt);
  sanitize::Result<std::string> parse_markup_name(std::size_t pos,
                                                  std::size_t gt) const;
  bool tag_is_self_closing(std::size_t lt, std::size_t gt) const;
  sanitize::Result<RowSlice> make_row(std::size_t end_pos);
  sanitize::Result<RowSlice> handle_markup(std::size_t lt, std::size_t gt);

  ChunkSourcePtr src_;
  std::string row_tag_;
  int64_t chunk_bytes_ = int64_t{1} << 20;
  int64_t memory_limit_bytes_ = -1;

  std::string buffer_;
  std::size_t buffer_start_offset_ = 0;
  std::size_t scan_pos_ = 0;
  bool eof_ = false;
  bool done_ = false;
  bool root_open_ = false;
  bool root_closed_ = false;
  int depth_ = 0;
  std::vector<std::string> stack_;
  std::size_t row_start_pos_ = npos;
  int row_parent_depth_ = 0;
};

} // namespace sanitize::internal
