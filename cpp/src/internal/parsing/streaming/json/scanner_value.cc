// Scans one JSON value from the current chunk position.

#include "internal/parsing/streaming/json/scanner.hh"

#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/streaming/json/value_span_scanner.hh"

namespace sanitize::internal {

sanitize::Result<TextSlice> JsonStreamingScanner::scan_value(BumpArena *arena) {
  SAN_RETURN_NOT_OK(ensure_chunk());
  if (chunk_.data.empty()) {
    return make_text_slice(std::string_view{}, eof_offset());
  }

  const std::string_view data = chunk_.data;
  const std::size_t start = pos_;
  auto end = json_skip_value(data, start, chunk_.base_offset);
  if (end.ok() && *end <= data.size()) {
    pos_ = *end;
    return make_text_slice(data.substr(start, *end - start),
                           chunk_.base_offset + start, chunk_.owner,
                           chunk_.source_name_owner, chunk_.source_name,
                           chunk_.source_index, chunk_.has_source_index);
  }
  return scan_json_value_span(*this, arena);
}

} // namespace sanitize::internal
