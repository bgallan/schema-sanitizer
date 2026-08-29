// Scans one JSON value from the current chunk position.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#include "internal/parsing/streaming/json/scanner.hh"

#include "internal/parsing/json/ondemand/document.hh"
#include "internal/parsing/streaming/json/value_span_scanner.hh"

#include <array>
#include <optional>

namespace sanitize::internal {

namespace {

constexpr std::size_t kWorkerFramingInlineDepth = 64;

/// Returns the JSON framing delimiter expected after a worker-bounded value.
[[nodiscard]] bool worker_frame_delimiter(char ch) noexcept {
  return json_scan::is_ws(ch) || ch == ',' || ch == ']' || ch == '}';
}

/// Finds the framing delimiter that terminates one worker-assigned JSON value.
[[nodiscard]] std::optional<std::size_t>
find_worker_framed_value_end(std::string_view data,
                             std::size_t start) noexcept {
  if (start >= data.size()) {
    return std::nullopt;
  }

  const char first = data[start];
  if (first == '"') {
    bool escaped = false;
    for (std::size_t pos = start + 1; pos < data.size(); ++pos) {
      const char ch = data[pos];
      if (escaped) {
        escaped = false;
      } else if (ch == '\\') {
        escaped = true;
      } else if (ch == '"') {
        return pos + 1;
      }
    }
    return std::nullopt;
  }

  if (first != '{' && first != '[') {
    for (std::size_t pos = start + 1; pos < data.size(); ++pos) {
      if (worker_frame_delimiter(data[pos])) {
        return pos;
      }
    }
    return std::nullopt;
  }

  // Most data is shallow. Deeper values fall back to the canonical parser.
  std::array<char, kWorkerFramingInlineDepth> expected;
  std::size_t depth = 1;
  expected[0] = first == '{' ? '}' : ']';
  bool in_string = false;
  bool escaped = false;

  for (std::size_t pos = start + 1; pos < data.size(); ++pos) {
    const char ch = data[pos];
    if (in_string) {
      if (escaped) {
        escaped = false;
      } else if (ch == '\\') {
        escaped = true;
      } else if (ch == '"') {
        in_string = false;
      }
      continue;
    }
    if (ch == '"') {
      in_string = true;
      continue;
    }
    if (ch == '{' || ch == '[') {
      if (depth >= expected.size()) {
        return std::nullopt;
      }
      expected[depth++] = ch == '{' ? '}' : ']';
      continue;
    }
    if (ch != '}' && ch != ']') {
      continue;
    }
    if (depth == 0 || ch != expected[depth - 1]) {
      return std::nullopt;
    }
    --depth;
    if (depth == 0) {
      return pos + 1;
    }
  }
  return std::nullopt;
}
} // namespace

sanitize::Result<TextSlice> JsonStreamingScanner::scan_value(BumpArena *arena) {
  SAN_RETURN_NOT_OK(ensure_chunk());
  if (chunk_.data.empty()) {
    return make_text_slice(std::string_view{}, eof_offset());
  }

  const std::string_view data = chunk_.data;
  const std::size_t start = pos_;
  if (worker_authoritative_framing_) {
    if (const auto framed_end = find_worker_framed_value_end(data, start)) {
      pos_ = *framed_end;
      return make_text_slice(data.substr(start, *framed_end - start),
                             chunk_.base_offset + start, chunk_.owner,
                             chunk_.source_name_owner, chunk_.source_name,
                             chunk_.source_index, chunk_.has_source_index);
    }
  }
  auto end = json_skip_value(data, start, chunk_.base_offset);
  const char first = data[start];
  const bool primitive = first != '"' && first != '{' && first != '[';
  if (end.ok() && *end == data.size() && !eof_ && primitive) {
    return scan_json_value_span(*this, arena);
  }
  if (end.ok() && *end <= data.size()) {
    pos_ = *end;
    return make_text_slice(data.substr(start, *end - start),
                           chunk_.base_offset + start, chunk_.owner,
                           chunk_.source_name_owner, chunk_.source_name,
                           chunk_.source_index, chunk_.has_source_index);
  }
  if (!end.ok() && eof_) {
    return end.status();
  }
  return scan_json_value_span(*this, arena);
}

} // namespace sanitize::internal
