// Scans JSON values that cross chunk boundaries.

#include "internal/parsing/streaming/json/value_span_scanner.hh"

namespace sanitize::internal {

namespace {

// Returns whether a byte is JSON whitespace.
bool is_json_ws(unsigned char c) {
  return c == ' ' || c == '\n' || c == '\r' || c == '\t';
}

} // namespace

JsonValueSpanScanner::JsonValueSpanScanner(JsonStreamingScanner &scanner,
                                           BumpArena *arena)
    : scanner_(scanner), arena_(arena),
      start_abs_(scanner.chunk_.base_offset + scanner.pos_),
      start_pos_(scanner.pos_), start_owner_(scanner.chunk_.owner),
      start_source_file_owner_(scanner.chunk_.source_name_owner),
      start_source_file_(scanner.chunk_.source_name),
      start_source_index_(scanner.chunk_.source_index),
      start_has_source_index_(scanner.chunk_.has_source_index),
      seg_start_pos_(start_pos_), seg_owner_(scanner.chunk_.owner) {
  segments_.reserve(4);
  stack_.reserve(8);
}

sanitize::Result<TextSlice> JsonValueSpanScanner::scan() {
  SAN_RETURN_NOT_OK(initialize_mode());
  for (;;) {
    if (mode_ == Mode::kPrimitive) {
      SAN_ASSIGN_OR_RAISE(bool done, scan_primitive());
      if (done) {
        return finish();
      }
      continue;
    }

    SAN_ASSIGN_OR_RAISE(bool done, scan_string_or_composite_byte());
    if (done) {
      return finish();
    }
  }
}

bool JsonValueSpanScanner::is_primitive_delim(char ch) {
  return is_json_ws(static_cast<unsigned char>(ch)) || ch == ',' || ch == ']' ||
         ch == '}';
}

sanitize::Status JsonValueSpanScanner::initialize_mode() {
  for (;;) {
    if (scanner_.pos_ < scanner_.chunk_.data.size()) {
      break;
    }
    if (scanner_.eof_) {
      return sanitize::Status::Invalid("JSON parse error: unexpected EOF");
    }
    SAN_RETURN_NOT_OK(need_more());
  }

  const char c0 = scanner_.chunk_.data[scanner_.pos_];
  if (c0 == '"') {
    mode_ = Mode::kString;
    in_string_ = true;
    escape_ = false;
    ++scanner_.pos_;
  } else if (c0 == '{') {
    mode_ = Mode::kComposite;
    stack_.push_back('}');
    ++scanner_.pos_;
  } else if (c0 == '[') {
    mode_ = Mode::kComposite;
    stack_.push_back(']');
    ++scanner_.pos_;
  } else {
    mode_ = Mode::kPrimitive;
    ++scanner_.pos_;
  }
  return sanitize::Status::OK();
}

sanitize::Result<bool> JsonValueSpanScanner::scan_primitive() {
  for (;;) {
    if (scanner_.pos_ >= scanner_.chunk_.data.size()) {
      if (scanner_.eof_) {
        return true;
      }
      SAN_RETURN_NOT_OK(need_more());
      continue;
    }
    const char ch = scanner_.chunk_.data[scanner_.pos_];
    if (is_primitive_delim(ch)) {
      return true;
    }
    ++scanner_.pos_;
  }
}

sanitize::Result<bool> JsonValueSpanScanner::scan_string_or_composite_byte() {
  if (scanner_.pos_ >= scanner_.chunk_.data.size()) {
    if (scanner_.eof_) {
      return sanitize::Status::Invalid(
          "JSON parse error: unexpected end of input");
    }
    SAN_RETURN_NOT_OK(need_more());
    return false;
  }

  const char ch = scanner_.chunk_.data[scanner_.pos_++];
  if (in_string_) {
    return scan_string_byte(ch);
  }
  return scan_composite_byte(ch);
}

sanitize::Result<bool> JsonValueSpanScanner::scan_string_byte(char ch) {
  if (escape_) {
    escape_ = false;
    return false;
  }
  if (ch == '\\') {
    escape_ = true;
    return false;
  }
  if (ch == '"') {
    in_string_ = false;
    return mode_ == Mode::kString;
  }
  return false;
}

sanitize::Result<bool> JsonValueSpanScanner::scan_composite_byte(char ch) {
  if (ch == '"') {
    in_string_ = true;
    escape_ = false;
    return false;
  }
  if (ch == '{') {
    stack_.push_back('}');
    return false;
  }
  if (ch == '[') {
    stack_.push_back(']');
    return false;
  }
  if (ch != '}' && ch != ']') {
    return false;
  }
  if (stack_.empty() || ch != stack_.back()) {
    return sanitize::Status::Invalid(
        "JSON parse error: mismatched closing bracket");
  }
  stack_.pop_back();
  return stack_.empty() && mode_ == Mode::kComposite;
}

sanitize::Result<TextSlice> scan_json_value_span(JsonStreamingScanner &scanner,
                                                 BumpArena *arena) {
  JsonValueSpanScanner span_scanner(scanner, arena);
  return span_scanner.scan();
}

} // namespace sanitize::internal
