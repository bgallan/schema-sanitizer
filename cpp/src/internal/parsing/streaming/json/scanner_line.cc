// Scans one JSON Lines record using newline search instead of structural JSON
// traversal. The actual JSON parse remains worker-local during parallel
// materialization.

#include "internal/parsing/streaming/json/scanner.hh"

#include <cstring>
#include <memory>
#include <memory_resource>
#include <string_view>
#include <vector>

#include "internal/memory/pool_resource.hh"

namespace sanitize::internal {
namespace {

constexpr std::size_t kMaxJsonLineBytes = std::size_t{128} << 20;
constexpr std::size_t kMaxJsonLineSegments = 65'536;

struct LineSegment {
  std::shared_ptr<const void> owner;
  std::string_view view;
};

/// Removes only the format-defined whitespace from JSON line end without
/// allocating.
[[nodiscard]] std::size_t trim_json_line_end(std::string_view view) noexcept {
  std::size_t end = view.size();
  while (end > 0 && is_ws(static_cast<unsigned char>(view[end - 1]))) {
    --end;
  }
  return end;
}

} // namespace

sanitize::Result<TextSlice>
JsonStreamingScanner::scan_line_value(BumpArena *arena) {
  SAN_RETURN_NOT_OK(ensure_chunk());
  if (!arena) {
    return sanitize::Status::Invalid("JSON Lines scanner: arena is null");
  }
  if (chunk_.data.empty()) {
    return make_text_slice(std::string_view{}, eof_offset());
  }

  const std::size_t start_abs = chunk_.base_offset + pos_;
  const std::size_t start_pos = pos_;

  // Keep the overwhelmingly common one-chunk record path allocation-free.
  // In particular, do not copy shared_ptr owners or construct the segment
  // vector until a record actually crosses an input chunk boundary.
  if (pos_ < chunk_.data.size()) {
    const char *begin = chunk_.data.data() + pos_;
    const std::size_t remaining = chunk_.data.size() - pos_;
    const void *found = std::memchr(begin, '\n', remaining);
    if (found) {
      const auto newline_pos = static_cast<std::size_t>(
          static_cast<const char *>(found) - chunk_.data.data());
      std::string_view line =
          chunk_.data.substr(start_pos, newline_pos - start_pos);
      line = line.substr(0, trim_json_line_end(line));
      pos_ = newline_pos + 1;
      return make_text_slice(line, start_abs, chunk_.owner,
                             chunk_.source_name_owner, chunk_.source_name,
                             chunk_.source_index, chunk_.has_source_index);
    }
    pos_ = chunk_.data.size();
  }

  if (eof_) {
    std::string_view line = chunk_.data.substr(start_pos);
    line = line.substr(0, trim_json_line_end(line));
    return make_text_slice(line, start_abs, chunk_.owner,
                           chunk_.source_name_owner, chunk_.source_name,
                           chunk_.source_index, chunk_.has_source_index);
  }

  const auto source_file_owner = chunk_.source_name_owner;
  const std::string_view source_file = chunk_.source_name;
  const std::size_t source_index = chunk_.source_index;
  const bool has_source_index = chunk_.has_source_index;
  PoolResource segment_resource(arena->pool());
  std::pmr::vector<LineSegment> segments(&segment_resource);
  std::size_t segment_start = start_pos;
  std::size_t total_bytes = 0;

  auto push_segment = [&](std::size_t end_pos) -> sanitize::Status {
    if (end_pos <= segment_start) {
      return sanitize::Status::OK();
    }
    const std::string_view part =
        chunk_.data.substr(segment_start, end_pos - segment_start);
    if (part.size() > kMaxJsonLineBytes ||
        total_bytes > kMaxJsonLineBytes - part.size()) {
      return sanitize::Status::Invalid(
          "JSON Lines record exceeds max buffered size");
    }
    if (segments.size() >= kMaxJsonLineSegments) {
      return sanitize::Status::Invalid(
          "JSON Lines record spans too many input chunks");
    }
    total_bytes += part.size();
    segments.push_back(LineSegment{.owner = chunk_.owner, .view = part});
    return sanitize::Status::OK();
  };

  SAN_RETURN_NOT_OK(push_segment(chunk_.data.size()));
  SAN_RETURN_NOT_OK(refill());
  segment_start = 0;
  for (;;) {
    if (pos_ < chunk_.data.size()) {
      const char *begin = chunk_.data.data() + pos_;
      const std::size_t remaining = chunk_.data.size() - pos_;
      const void *found = std::memchr(begin, '\n', remaining);
      if (found) {
        const auto newline_pos = static_cast<std::size_t>(
            static_cast<const char *>(found) - chunk_.data.data());
        SAN_RETURN_NOT_OK(push_segment(newline_pos));
        pos_ = newline_pos + 1;
        break;
      }
      pos_ = chunk_.data.size();
    }
    SAN_RETURN_NOT_OK(push_segment(chunk_.data.size()));
    if (eof_) {
      break;
    }
    SAN_RETURN_NOT_OK(refill());
    segment_start = 0;
  }

  char *destination =
      static_cast<char *>(arena->alloc(total_bytes, alignof(char)));
  if (!destination && total_bytes != 0) {
    return sanitize::Status::OutOfMemory(
        "JSON Lines scanner: arena allocation failed");
  }
  std::size_t written = 0;
  for (const auto &segment : segments) {
    std::memcpy(destination + written, segment.view.data(),
                segment.view.size());
    written += segment.view.size();
  }
  std::string_view line(destination, written);
  line = line.substr(0, trim_json_line_end(line));
  return make_text_slice(line, start_abs, {}, source_file_owner, source_file,
                         source_index, has_source_index);
}

} // namespace sanitize::internal
