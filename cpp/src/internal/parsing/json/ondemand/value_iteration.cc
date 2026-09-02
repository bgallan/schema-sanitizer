// Parses nested JSON value slices for on-demand iterators.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#include "internal/parsing/json/ondemand/document.hh"

#include <string_view>

#include "sanitize/core/status.hh"

namespace sanitize::internal {

sanitize::Result<ValueView> JsonOnDemandDoc::ParseChildValue(
    json_scan::Cursor &cursor, std::string_view text, std::size_t base_offset) {
  const char *value_start = cursor.p;
  SAN_RETURN_NOT_OK(json_scan::skip_value(cursor));
  const char *value_end = cursor.p;
  return ParseValue(
      std::string_view(value_start,
                       static_cast<std::size_t>(value_end - value_start)),
      base_offset + static_cast<std::size_t>(value_start - text.data()));
}

} // namespace sanitize::internal
