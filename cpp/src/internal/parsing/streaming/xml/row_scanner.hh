// Declares hardened streaming XML row-tag slicing.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

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

  /// Builds a parse error at the scanner's current absolute byte offset.
  sanitize::Status invalid(std::string_view message) const;
  /// Builds a parse error at a specified position in the retained buffer.
  sanitize::Status invalid_at(std::size_t buffer_position,
                              std::string_view message) const;
  /// Rejects a refill that would exceed the configured retained-byte limit.
  sanitize::Status enforce_buffer_limit(std::size_t incoming) const;
  /// Reports whether consumed bytes should be removed before the next refill.
  [[nodiscard]] bool should_compact_before_refill() const noexcept;
  /// Computes the capacity threshold above which an empty buffer is released.
  [[nodiscard]] std::size_t retained_buffer_limit() const noexcept;
  /// Clears buffered XML and releases or scrubs storage when policy requires
  /// it.
  void discard_buffer();
  /// Removes consumed bytes while rebasing row, scan, and markup positions.
  void compact_buffer();
  /// Clears state used to continue markup scanning across chunk boundaries.
  void reset_pending_markup() noexcept;
  /// Ensures an unconsumed byte is buffered and reports clean end of input.
  sanitize::Result<bool> ensure_data();
  /// Compacts when useful, then appends one validated source chunk.
  sanitize::Status refill();
  /// Refills incomplete markup or reports the supplied error at end of input.
  sanitize::Status read_more_or_fail(std::string_view eof_message);
  /// Validates text placement, entity references, and forbidden CDATA
  /// terminators.
  sanitize::Status handle_text(std::string_view text,
                               std::size_t buffer_position);
  /// Rejects an entity reference left open at a markup boundary or end of
  /// input.
  sanitize::Status finish_text_token();
  /// Locates a complete tag, comment, instruction, or CDATA terminator
  /// incrementally.
  sanitize::Result<std::size_t> find_markup_end(std::size_t lt);
  /// Parses and validates an XML name within a buffered markup range.
  sanitize::Result<std::pmr::string> parse_markup_name(std::size_t pos,
                                                       std::size_t gt) const;
  /// Validates an opening tag and reports whether it is self-closing.
  sanitize::Status validate_start_markup(std::size_t lt, std::size_t gt,
                                         std::string_view expected_name,
                                         bool *self_closing) const;
  /// Validates a closing tag against the currently open element.
  sanitize::Status
  validate_closing_markup(std::size_t lt, std::size_t gt,
                          std::string_view expected_name) const;
  /// Rejects malformed comments and forbidden double hyphens in comment
  /// content.
  sanitize::Status validate_comment_markup(std::size_t lt,
                                           std::size_t gt) const;
  /// Validates an instruction target and restricts XML declarations to byte
  /// zero.
  sanitize::Status validate_processing_instruction(std::size_t lt,
                                                   std::size_t gt) const;
  /// Returns the completed buffered row and advances scanner state past it.
  sanitize::Result<RowSlice> make_row(std::size_t end_pos);
  /// Applies one markup token to nesting state and emits a completed selected
  /// row.
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
