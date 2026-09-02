// Drives top-level JSON stream and array traversal.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#include "internal/parsing/streaming/json/scanner.hh"

namespace sanitize::internal {
namespace {

/// Reports whether a byte is legal JSON whitespace in streaming framing logic.
bool json_stream_is_ws(unsigned char c) {
  return c == ' ' || c == '\n' || c == '\r' || c == '\t';
}

} // namespace

sanitize::Result<TextSlice> JsonStreamingScanner::next_value(BumpArena *arena) {
  if (!arena) {
    return sanitize::Status::Invalid("JSON scanner: arena is null");
  }

  for (;;) {
    if (state_ == State::kDone) {
      return make_text_slice(std::string_view{}, eof_offset());
    }

    SAN_ASSIGN_OR_RAISE(bool has, skip_ws_before_value());
    if (!has) {
      return make_text_slice(std::string_view{}, eof_offset());
    }

    SAN_RETURN_NOT_OK(enter_initial_mode());
    if (state_ == State::kStream) {
      return next_stream_value(arena);
    }

    SAN_ASSIGN_OR_RAISE(TextSlice value, next_array_value(arena));
    if (value.view.empty() && state_ != State::kDone) {
      continue;
    }
    return value;
  }
}

sanitize::Result<bool> JsonStreamingScanner::skip_ws() {
  for (;;) {
    SAN_RETURN_NOT_OK(ensure_chunk());
    while (pos_ < chunk_.data.size() &&
           json_stream_is_ws(static_cast<unsigned char>(chunk_.data[pos_]))) {
      ++pos_;
    }
    if (pos_ < chunk_.data.size()) {
      return true;
    }
    if (eof_) {
      return false;
    }
    SAN_RETURN_NOT_OK(refill());
  }
}

sanitize::Result<bool> JsonStreamingScanner::skip_ws_before_value() {
  SAN_ASSIGN_OR_RAISE(bool has, skip_ws());
  if (has) {
    return true;
  }
  if (state_ == State::kArray) {
    return sanitize::Status::Invalid("JSON parse error: unterminated array");
  }
  state_ = State::kDone;
  return false;
}

sanitize::Status JsonStreamingScanner::enter_initial_mode() {
  if (state_ != State::kInit) {
    return sanitize::Status::OK();
  }
  if (line_delimited_) {
    state_ = State::kStream;
    return sanitize::Status::OK();
  }
  if (peek() == '[') {
    consume();
    state_ = State::kArray;
    return sanitize::Status::OK();
  }
  if (require_top_level_array_) {
    return sanitize::Status::Invalid("json_array input must be a JSON array");
  }
  state_ = State::kStream;
  return sanitize::Status::OK();
}

sanitize::Result<TextSlice>
JsonStreamingScanner::next_stream_value(BumpArena *arena) {
  SAN_ASSIGN_OR_RAISE(TextSlice value, line_delimited_ ? scan_line_value(arena)
                                                       : scan_value(arena));
  if (value.view.empty() && eof_) {
    state_ = State::kDone;
  }
  return value;
}

sanitize::Result<TextSlice>
JsonStreamingScanner::next_array_value(BumpArena *arena) {
  SAN_ASSIGN_OR_RAISE(bool has, skip_ws());
  if (!has) {
    return sanitize::Status::Invalid("JSON parse error: unterminated array");
  }

  if (peek() == ']') {
    const std::size_t end_offset = chunk_.base_offset + pos_;
    consume();
    SAN_RETURN_NOT_OK(finish_array());
    return make_text_slice(std::string_view{}, end_offset);
  }

  SAN_ASSIGN_OR_RAISE(TextSlice value, scan_value(arena));
  SAN_ASSIGN_OR_RAISE(bool has_separator, skip_ws());
  if (!has_separator) {
    return sanitize::Status::Invalid("JSON parse error: unterminated array");
  }

  const char separator = peek();
  if (separator == ',') {
    consume();
    return value;
  }
  if (separator == ']') {
    consume();
    SAN_RETURN_NOT_OK(finish_array());
    return value;
  }
  return sanitize::Status::Invalid(
      "JSON parse error: expected ',' or ']' in array");
}

sanitize::Status JsonStreamingScanner::finish_array() {
  SAN_ASSIGN_OR_RAISE(bool has_trailing, skip_ws());
  if (has_trailing) {
    return sanitize::Status::Invalid(
        "JSON parse error: trailing characters after top-level array");
  }
  state_ = State::kDone;
  return sanitize::Status::OK();
}

} // namespace sanitize::internal
