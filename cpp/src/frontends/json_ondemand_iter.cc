// Implements on-demand JSON object and array iteration helpers.
//
// Keeps nested iteration mechanics separate from top-level ValueView parsing
// while sharing JsonOnDemandDoc's arena-backed string decoding.

#include "internal/parsing/json_ondemand.hh"

#include "internal/parsing/json_string_decode.hh"

#include <cstdint>
#include <string_view>

#include "sanitize/core/status.hh"
#include "sanitize/detail/hash.hh"

namespace sanitize::internal {

using json_scan::Cursor;
using json_scan::expect;
using json_scan::scan_string;
using json_scan::skip_value;
using json_scan::skip_ws;
using json_string_decode::decode_json_string_slice;
using json_string_decode::DecodeErrors;

namespace {

constexpr DecodeErrors kKeyDecodeErrors{
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

sanitize::Result<std::string_view>
JsonOnDemandDoc::ParseObjectKey(Cursor &cursor, std::string_view text,
                                std::size_t base_offset) {
  if (cursor.p >= cursor.end || *cursor.p != '"') {
    return ParseError("JSON parse error: expected string key", cursor.offset());
  }

  const char *begin = nullptr;
  const char *end = nullptr;
  bool has_escapes = false;
  SAN_RETURN_NOT_OK(scan_string(cursor, begin, end, has_escapes));
  if (!has_escapes) {
    return std::string_view(begin, static_cast<std::size_t>(end - begin));
  }

  const auto input_size = static_cast<std::size_t>(end - begin);
  char *out = ArenaAllocChars(input_size ? input_size : 1);
  SAN_ASSIGN_OR_RAISE(auto key,
                      decode_json_string_slice(out, begin, end, text,
                                               base_offset, kKeyDecodeErrors));
  return key;
}

sanitize::Result<ValueView>
JsonOnDemandDoc::ParseChildValue(Cursor &cursor, std::string_view text,
                                 std::size_t base_offset) {
  const char *value_start = cursor.p;
  SAN_RETURN_NOT_OK(skip_value(cursor));
  const char *value_end = cursor.p;
  return ParseValue(
      std::string_view(value_start,
                       static_cast<std::size_t>(value_end - value_start)),
      base_offset + static_cast<std::size_t>(value_start - text.data()));
}

sanitize::Status JsonOnDemandDoc::EmitObjectField(Cursor &cursor,
                                                  std::string_view text,
                                                  void *ctx,
                                                  ValueView::ObjectEachFn fn,
                                                  std::size_t base_offset) {
  SAN_ASSIGN_OR_RAISE(auto key, ParseObjectKey(cursor, text, base_offset));
  const uint64_t key_hash = sanitize::detail::hash_key64(key);

  skip_ws(cursor);
  SAN_RETURN_NOT_OK(expect(cursor, ':'));
  skip_ws(cursor);

  SAN_ASSIGN_OR_RAISE(auto value, ParseChildValue(cursor, text, base_offset));
  return fn(ctx, key, key_hash, value);
}

sanitize::Status JsonOnDemandDoc::EnterObjectIterator(Cursor &cursor,
                                                      bool *done) {
  *done = false;
  skip_ws(cursor);
  if (cursor.p >= cursor.end || *cursor.p != '{') {
    return ParseError("JSON parse error: expected object", cursor.offset());
  }

  SAN_RETURN_NOT_OK(expect(cursor, '{'));
  skip_ws(cursor);
  if (cursor.p < cursor.end && *cursor.p == '}') {
    ++cursor.p;
    *done = true;
  }
  return sanitize::Status::OK();
}

sanitize::Status JsonOnDemandDoc::EnterArrayIterator(Cursor &cursor,
                                                     bool *done) {
  *done = false;
  skip_ws(cursor);
  if (cursor.p >= cursor.end || *cursor.p != '[') {
    return ParseError("JSON parse error: expected array", cursor.offset());
  }

  SAN_RETURN_NOT_OK(expect(cursor, '['));
  skip_ws(cursor);
  if (cursor.p < cursor.end && *cursor.p == ']') {
    ++cursor.p;
    *done = true;
  }
  return sanitize::Status::OK();
}

sanitize::Status JsonOnDemandDoc::AdvanceObjectIterator(Cursor &cursor,
                                                        bool *done) {
  *done = false;
  skip_ws(cursor);
  if (cursor.p >= cursor.end) {
    return ParseError("JSON parse error: unterminated object", cursor.offset());
  }
  if (*cursor.p == ',') {
    ++cursor.p;
    skip_ws(cursor);
    return sanitize::Status::OK();
  }
  if (*cursor.p == '}') {
    ++cursor.p;
    *done = true;
    return sanitize::Status::OK();
  }
  return ParseError("JSON parse error: expected ',' or '}'", cursor.offset());
}

sanitize::Status JsonOnDemandDoc::AdvanceArrayIterator(Cursor &cursor,
                                                       bool *done) {
  *done = false;
  skip_ws(cursor);
  if (cursor.p >= cursor.end) {
    return ParseError("JSON parse error: unterminated array", cursor.offset());
  }
  if (*cursor.p == ',') {
    ++cursor.p;
    skip_ws(cursor);
    return sanitize::Status::OK();
  }
  if (*cursor.p == ']') {
    ++cursor.p;
    *done = true;
    return sanitize::Status::OK();
  }
  return ParseError("JSON parse error: expected ',' or ']'", cursor.offset());
}

sanitize::Status
JsonOnDemandDoc::ForEachObjectFieldC(std::string_view text, void *ctx,
                                     ValueView::ObjectEachFn fn,
                                     std::size_t base_offset) {
  if (!fn) {
    return sanitize::Status::Invalid("ForEachObjectFieldC: callback is null");
  }

  Cursor c = MakeCursor(text, base_offset);
  bool done = false;
  SAN_RETURN_NOT_OK(EnterObjectIterator(c, &done));
  if (done) {
    return sanitize::Status::OK();
  }

  while (true) {
    SAN_RETURN_NOT_OK(EmitObjectField(c, text, ctx, fn, base_offset));

    done = false;
    SAN_RETURN_NOT_OK(AdvanceObjectIterator(c, &done));
    if (done) {
      return sanitize::Status::OK();
    }
  }
}

sanitize::Status
JsonOnDemandDoc::ForEachArrayElementC(std::string_view text, void *ctx,
                                      ValueView::ArrayEachFn fn,
                                      std::size_t base_offset) {
  if (!fn) {
    return sanitize::Status::Invalid("ForEachArrayElementC: callback is null");
  }

  Cursor c = MakeCursor(text, base_offset);
  bool done = false;
  SAN_RETURN_NOT_OK(EnterArrayIterator(c, &done));
  if (done) {
    return sanitize::Status::OK();
  }

  while (true) {
    SAN_ASSIGN_OR_RAISE(auto value, ParseChildValue(c, text, base_offset));
    SAN_RETURN_NOT_OK(fn(ctx, value));

    done = false;
    SAN_RETURN_NOT_OK(AdvanceArrayIterator(c, &done));
    if (done) {
      return sanitize::Status::OK();
    }
  }
}

} // namespace sanitize::internal
