// Implements on-demand JSON object iteration.

#include "internal/parsing/json/ondemand/document.hh"

#include "internal/parsing/json/string_decode.hh"

#include <cstdint>
#include <string_view>

#include "sanitize/core/status.hh"
#include "sanitize/detail/hash.hh"

namespace sanitize::internal {

namespace {

constexpr json_string_decode::DecodeErrors kKeyDecodeErrors{
    .truncated_escape = "JSON parse error: truncated escape in key",
    .incomplete_unicode_escape = "JSON parse error: incomplete \\uXXXX in key",
    .invalid_unicode_hex = "JSON parse error: invalid hex in key \\uXXXX",
    .missing_low_surrogate = "JSON parse error: missing low surrogate in key",
    .invalid_low_surrogate_hex =
        "JSON parse error: invalid low surrogate hex in key",
    .invalid_low_surrogate_range =
        "JSON parse error: invalid low surrogate range in key",
    .unexpected_low_surrogate =
        "JSON parse error: unexpected low surrogate in key",
    .invalid_escape = "JSON parse error: invalid escape in key",
};

} // namespace

sanitize::Result<std::string_view> JsonOnDemandDoc::ParseObjectKey(
    json_scan::Cursor &cursor, std::string_view text, std::size_t base_offset) {
  if (cursor.p >= cursor.end || *cursor.p != '"') {
    return ParseError("JSON parse error: expected string key", cursor.offset());
  }

  const char *begin = nullptr;
  const char *end = nullptr;
  bool has_escapes = false;
  SAN_RETURN_NOT_OK(json_scan::scan_string(cursor, begin, end, has_escapes));
  if (!has_escapes) {
    return std::string_view(begin, static_cast<std::size_t>(end - begin));
  }

  const auto input_size = static_cast<std::size_t>(end - begin);
  char *out = ArenaAllocChars(input_size ? input_size : 1);
  SAN_ASSIGN_OR_RAISE(
      auto key, json_string_decode::decode_json_string_slice(
                    out, begin, end, text, base_offset, kKeyDecodeErrors));
  return key;
}

sanitize::Status JsonOnDemandDoc::EmitObjectField(json_scan::Cursor &cursor,
                                                  std::string_view text,
                                                  void *ctx,
                                                  ValueView::ObjectEachFn fn,
                                                  std::size_t base_offset) {
  SAN_ASSIGN_OR_RAISE(auto key, ParseObjectKey(cursor, text, base_offset));
  const uint64_t key_hash = sanitize::detail::hash_key64(key);

  json_scan::skip_ws(cursor);
  SAN_RETURN_NOT_OK(json_scan::expect(cursor, ':'));
  json_scan::skip_ws(cursor);

  SAN_ASSIGN_OR_RAISE(auto value, ParseChildValue(cursor, text, base_offset));
  return fn(ctx, key, key_hash, value);
}

sanitize::Status JsonOnDemandDoc::EnterObjectIterator(json_scan::Cursor &cursor,
                                                      bool *done) {
  *done = false;
  json_scan::skip_ws(cursor);
  if (cursor.p >= cursor.end || *cursor.p != '{') {
    return ParseError("JSON parse error: expected object", cursor.offset());
  }

  SAN_RETURN_NOT_OK(json_scan::expect(cursor, '{'));
  json_scan::skip_ws(cursor);
  if (cursor.p < cursor.end && *cursor.p == '}') {
    ++cursor.p;
    *done = true;
  }
  return sanitize::Status::OK();
}

sanitize::Status
JsonOnDemandDoc::AdvanceObjectIterator(json_scan::Cursor &cursor, bool *done) {
  *done = false;
  json_scan::skip_ws(cursor);
  if (cursor.p >= cursor.end) {
    return ParseError("JSON parse error: unterminated object", cursor.offset());
  }
  if (*cursor.p == ',') {
    ++cursor.p;
    json_scan::skip_ws(cursor);
    return sanitize::Status::OK();
  }
  if (*cursor.p == '}') {
    ++cursor.p;
    *done = true;
    return sanitize::Status::OK();
  }
  return ParseError("JSON parse error: expected ',' or '}'", cursor.offset());
}

sanitize::Status
JsonOnDemandDoc::ForEachObjectFieldC(std::string_view text, void *ctx,
                                     ValueView::ObjectEachFn fn,
                                     std::size_t base_offset) {
  if (!fn) {
    return sanitize::Status::Invalid("ForEachObjectFieldC: callback is null");
  }

  auto cursor = MakeCursor(text, base_offset);
  bool done = false;
  SAN_RETURN_NOT_OK(EnterObjectIterator(cursor, &done));
  while (!done) {
    SAN_RETURN_NOT_OK(EmitObjectField(cursor, text, ctx, fn, base_offset));
    SAN_RETURN_NOT_OK(AdvanceObjectIterator(cursor, &done));
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal
