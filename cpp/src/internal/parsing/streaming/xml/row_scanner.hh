// Declares hardened streaming XML row-tag slicing.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

#include "internal/parsing/xml_entities.hh"
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

  /// Store stream settings, budgeted resource, and row-tag selector.
  XmlRowTagScanner(
      ChunkSourcePtr src, std::string row_tag, int64_t chunk_bytes,
      int64_t memory_limit_bytes,
      std::pmr::memory_resource *resource = std::pmr::get_default_resource());

  /// Reset source state and scanner bookkeeping to the beginning.
  sanitize::Status Reset();

  /// Return the next XML row slice or an empty slice at EOF.
  sanitize::Result<RowSlice> next_row();

private:
  enum class PendingMarkupKind : std::uint8_t {
    kNone,
    kComment,
    kProcessingInstruction,
    kCdata,
    kTag,
  };

  static constexpr std::size_t npos = std::string::npos;

  sanitize::Status invalid(std::string_view message) const;
  sanitize::Status invalid_at(std::size_t buffer_position,
                              std::string_view message) const;
  sanitize::Status enforce_buffer_limit(std::size_t incoming) const;
  [[nodiscard]] bool should_compact_before_refill() const noexcept;
  [[nodiscard]] std::size_t retained_buffer_limit() const noexcept;
  void discard_buffer();
  void compact_buffer();
  void reset_pending_markup() noexcept;
  sanitize::Result<bool> ensure_data();
  sanitize::Status refill();
  sanitize::Status read_more_or_fail(std::string_view eof_message);
  sanitize::Status handle_text(std::string_view text,
                               std::size_t buffer_position);
  sanitize::Status finish_text_token();
  sanitize::Result<std::size_t> find_markup_end(std::size_t lt);
  sanitize::Result<std::pmr::string> parse_markup_name(std::size_t pos,
                                                       std::size_t gt) const;
  sanitize::Status validate_start_markup(std::size_t lt, std::size_t gt,
                                         std::string_view expected_name,
                                         bool *self_closing) const;
  sanitize::Status
  validate_closing_markup(std::size_t lt, std::size_t gt,
                          std::string_view expected_name) const;
  sanitize::Status validate_comment_markup(std::size_t lt,
                                           std::size_t gt) const;
  sanitize::Status validate_processing_instruction(std::size_t lt,
                                                   std::size_t gt) const;
  sanitize::Result<RowSlice> make_row(std::size_t end_pos);
  sanitize::Result<RowSlice> handle_markup(std::size_t lt, std::size_t gt);

  ChunkSourcePtr src_;
  std::pmr::memory_resource *resource_ = std::pmr::get_default_resource();
  std::pmr::string row_tag_;
  int64_t chunk_bytes_ = int64_t{1} << 20;
  int64_t memory_limit_bytes_ = -1;

  std::pmr::string buffer_;
  std::size_t buffer_start_offset_ = 0;
  std::size_t scan_pos_ = 0;
  bool eof_ = false;
  bool done_ = false;
  bool root_open_ = false;
  bool root_closed_ = false;
  std::uint32_t depth_ = 0;
  std::pmr::vector<std::pmr::string> stack_;
  std::size_t row_start_pos_ = npos;
  std::uint32_t row_parent_depth_ = 0;

  PendingMarkupKind pending_markup_kind_ = PendingMarkupKind::kNone;
  std::size_t pending_markup_lt_ = npos;
  std::size_t pending_markup_resume_ = 0;
  bool pending_markup_in_quote_ = false;
  char pending_markup_quote_ = '\0';

  XmlUtf8StreamValidator utf8_validator_;
  bool text_entity_open_ = false;
  std::size_t text_entity_offset_ = 0;
  std::pmr::string text_entity_;
  std::uint8_t text_closing_brackets_ = 0;
};

} // namespace sanitize::internal
