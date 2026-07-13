// Implements on-demand JSON array iteration.

#include "internal/parsing/json/ondemand/document.hh"

#include <string_view>

#include "sanitize/core/status.hh"

namespace sanitize::internal {

sanitize::Status JsonOnDemandDoc::EnterArrayIterator(json_scan::Cursor &cursor,
                                                     bool *done) {
  *done = false;
  json_scan::skip_ws(cursor);
  if (cursor.p >= cursor.end || *cursor.p != '[') {
    return ParseError("JSON parse error: expected array", cursor.offset());
  }

  SAN_RETURN_NOT_OK(json_scan::expect(cursor, '['));
  json_scan::skip_ws(cursor);
  if (cursor.p < cursor.end && *cursor.p == ']') {
    ++cursor.p;
    *done = true;
  }
  return sanitize::Status::OK();
}

sanitize::Status
JsonOnDemandDoc::AdvanceArrayIterator(json_scan::Cursor &cursor, bool *done) {
  *done = false;
  json_scan::skip_ws(cursor);
  if (cursor.p >= cursor.end) {
    return ParseError("JSON parse error: unterminated array", cursor.offset());
  }
  if (*cursor.p == ',') {
    ++cursor.p;
    json_scan::skip_ws(cursor);
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
JsonOnDemandDoc::ForEachArrayElementC(std::string_view text, void *ctx,
                                      ValueView::ArrayEachFn fn,
                                      std::size_t base_offset) {
  if (!fn) {
    return sanitize::Status::Invalid("ForEachArrayElementC: callback is null");
  }

  auto cursor = MakeCursor(text, base_offset);
  bool done = false;
  SAN_RETURN_NOT_OK(EnterArrayIterator(cursor, &done));
  while (!done) {
    SAN_ASSIGN_OR_RAISE(auto value, ParseChildValue(cursor, text, base_offset));
    SAN_RETURN_NOT_OK(fn(ctx, value));
    SAN_RETURN_NOT_OK(AdvanceArrayIterator(cursor, &done));
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal
