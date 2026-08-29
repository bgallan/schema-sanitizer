// Implements on-demand JSON document lifetime, cursor, and span helpers.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#include "internal/parsing/json/ondemand/document.hh"

#include "internal/parsing/json/ondemand/scan.hh"
#include "sanitize/core/status.hh"

#include <cassert>
#include <string>
#include <string_view>

namespace sanitize::internal {

using json_scan::Cursor;
using json_scan::skip_value;
using json_scan::skip_ws;

JsonOnDemandDoc::JsonOnDemandDoc(std::pmr::memory_resource *upstream)
    : arena_(upstream) {
  // Crash-fast if the pipeline violated allocator invariants.
  // A null upstream is a logic error and must not be masked.
  assert(upstream != nullptr);
}

void *JsonOnDemandDoc::ArenaAlloc(std::size_t n, std::size_t align) {
  return arena_.allocate(n, align);
}

char *JsonOnDemandDoc::ArenaAllocChars(std::size_t n) {
  return static_cast<char *>(ArenaAlloc(n, alignof(char)));
}

sanitize::Status JsonOnDemandDoc::ParseError(std::string_view msg,
                                             std::size_t offset) {
  return sanitize::Status::Invalid(std::string(msg), " at byte ",
                                   std::to_string(offset));
}

json_scan::Cursor
JsonOnDemandDoc::MakeCursor(std::string_view text,
                            std::size_t base_offset) noexcept {
  Cursor cursor;
  cursor.p = text.data();
  cursor.end = text.data() + text.size();
  cursor.base = base_offset;
  cursor.text_begin = text.data();
  return cursor;
}

sanitize::Status JsonOnDemandDoc::ExpectEnd(Cursor &cursor) {
  skip_ws(cursor);
  if (cursor.p != cursor.end) {
    return ParseError("JSON parse error: trailing characters", cursor.offset());
  }
  return sanitize::Status::OK();
}

sanitize::Result<std::size_t> json_skip_value(std::string_view text,
                                              std::size_t start,
                                              std::size_t base_offset) {
  if (start > text.size()) {
    return sanitize::Status::Invalid("json_skip_value: start out of range");
  }
  Cursor cursor;
  cursor.p = text.data() + start;
  cursor.end = text.data() + text.size();
  cursor.base = base_offset;
  cursor.text_begin = text.data();

  skip_ws(cursor);
  SAN_RETURN_NOT_OK(skip_value(cursor));
  return static_cast<std::size_t>(cursor.p - text.data());
}

} // namespace sanitize::internal
