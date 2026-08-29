// Implements scalar parsing for on-demand JSON ValueView values.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#include "internal/parsing/json/ondemand/document.hh"

#include "internal/parsing/json/ondemand/scan.hh"
#include "internal/parsing/json/string_decode.hh"
#include "sanitize/core/primitives.hh"

#include <charconv>
#include <string_view>
#include <system_error>

namespace sanitize::internal {

using json_scan::Cursor;
using json_scan::scan_string;
using json_scan::skip_literal;
using json_scan::skip_number;
using json_string_decode::decode_json_string_slice;
using json_string_decode::DecodeErrors;

namespace {

constexpr DecodeErrors kStringDecodeErrors{
    .truncated_escape = "JSON parse error: truncated escape",
    .incomplete_unicode_escape = "JSON parse error: incomplete \\uXXXX escape",
    .invalid_unicode_hex = "JSON parse error: invalid hex in \\uXXXX",
    .missing_low_surrogate = "JSON parse error: missing low surrogate",
    .invalid_low_surrogate_hex =
        "JSON parse error: invalid hex in low surrogate",
    .invalid_low_surrogate_range =
        "JSON parse error: invalid low surrogate range",
    .unexpected_low_surrogate = "JSON parse error: unexpected low surrogate",
    .invalid_escape = "JSON parse error: invalid escape",
};

} // namespace

sanitize::Result<ValueView>
JsonOnDemandDoc::ParseLiteralValue(Cursor &cursor, const char *literal,
                                   std::size_t literal_size, ValueView value) {
  SAN_RETURN_NOT_OK(skip_literal(cursor, literal, literal_size));
  SAN_RETURN_NOT_OK(ExpectEnd(cursor));
  return value;
}

sanitize::Result<ValueView>
JsonOnDemandDoc::ParseStringValue(Cursor &cursor, std::string_view text,
                                  std::size_t base_offset) {
  const char *begin = nullptr;
  const char *end = nullptr;
  bool has_escapes = false;
  SAN_RETURN_NOT_OK(scan_string(cursor, begin, end, has_escapes));
  SAN_RETURN_NOT_OK(ExpectEnd(cursor));
  if (!has_escapes) {
    return ValueView::String(
        std::string_view(begin, static_cast<std::size_t>(end - begin)));
  }
  const auto input_size = static_cast<std::size_t>(end - begin);
  char *out = ArenaAllocChars(input_size ? input_size : 1);
  SAN_ASSIGN_OR_RAISE(
      auto decoded, decode_json_string_slice(out, begin, end, text, base_offset,
                                             kStringDecodeErrors));
  return ValueView::String(decoded);
}

sanitize::Result<ValueView>
JsonOnDemandDoc::ParseNumberValue(Cursor &cursor, std::string_view text,
                                  std::size_t base_offset) {
  const char *start = cursor.p;
  SAN_RETURN_NOT_OK(skip_number(cursor));
  const char *end = cursor.p;
  SAN_RETURN_NOT_OK(ExpectEnd(cursor));

  std::string_view number(start, static_cast<std::size_t>(end - start));
  bool is_float = false;
  for (const char character : number) {
    if (character == '.' || character == 'e' || character == 'E') {
      is_float = true;
      break;
    }
  }
  if (!is_float) {
    int64_t value = 0;
    auto result =
        std::from_chars(number.data(), number.data() + number.size(), value);
    if (result.ec == std::errc() &&
        result.ptr == number.data() + number.size()) {
      return ValueView::Int(value);
    }
    // Fallback to float if an integer literal is outside int64 range.
    is_float = true;
  }
  if (!is_float) {
    return ParseError("JSON parse error: invalid number",
                      base_offset +
                          static_cast<std::size_t>(start - text.data()));
  }

  double value = 0.0;
  if (!parse_ascii_float64_strict(number, &value)) {
    return ParseError("JSON parse error: invalid float",
                      base_offset +
                          static_cast<std::size_t>(start - text.data()));
  }
  return ValueView::Float(value);
}

} // namespace sanitize::internal
