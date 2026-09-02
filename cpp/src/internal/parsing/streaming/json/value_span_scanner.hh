// Declares the buffered JSON value scanner used across chunk boundaries.
// The parser validates bounded input while preserving offsets, zero-copy views,
// and deterministic diagnostics.

#pragma once

#include "internal/parsing/json/ondemand/scan.hh"
#include "internal/parsing/streaming/json/scanner.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

#include "internal/memory/pool_resource.hh"

namespace sanitize::internal {

class JsonValueSpanScanner {
public:
  JsonValueSpanScanner(JsonStreamingScanner &scanner, BumpArena *arena);

  /// Scans one value and returns a text slice over buffered bytes.
  sanitize::Result<TextSlice> scan();

private:
  struct Segment {
    std::shared_ptr<const void> owner;
    std::string_view view;
  };

  enum class Mode : uint8_t { kString = 0, kComposite = 1, kPrimitive = 2 };

  static constexpr std::size_t kMaxValueBytes = std::size_t{128} << 20;
  static constexpr std::size_t kMaxSegments = 65'536;

  /// Returns whether a byte terminates a primitive JSON value.
  static bool is_primitive_delim(char ch);
  /// Appends the current chunk segment to the buffered span.
  sanitize::Status push_segment(std::size_t end_pos);
  /// Buffers the current chunk tail and refills the scanner.
  sanitize::Status need_more();
  /// Initializes scanner mode from the first value byte.
  sanitize::Status initialize_mode();
  /// Scans primitive bytes until a delimiter or EOF.
  sanitize::Result<bool> scan_primitive();
  /// Processes one string or composite byte and returns whether the value
  /// ended.
  sanitize::Result<bool> scan_string_or_composite_byte();
  /// Processes one byte while inside a JSON string.
  sanitize::Result<bool> scan_string_byte(char ch);
  /// Processes one byte while inside a JSON composite value.
  sanitize::Result<bool> scan_composite_byte(char ch);
  /// Builds the final text slice from one chunk or copied buffered segments.
  sanitize::Result<TextSlice> finish();

  JsonStreamingScanner &scanner_;
  BumpArena *arena_ = nullptr;
  PoolResource pmr_pool_;
  std::pmr::vector<Segment> segments_;
  std::size_t start_abs_ = 0;
  std::size_t start_pos_ = 0;
  std::shared_ptr<const void> start_owner_;
  std::shared_ptr<const std::string> start_source_file_owner_;
  std::string_view start_source_file_;
  std::size_t start_source_index_ = 0;
  bool start_has_source_index_ = false;
  bool multi_ = false;
  std::size_t seg_start_pos_ = 0;
  std::shared_ptr<const void> seg_owner_;
  std::size_t total_bytes_ = 0;
  Mode mode_ = Mode::kPrimitive;
  std::pmr::vector<char> stack_;
  bool in_string_ = false;
  bool escape_ = false;
};

/// Scans a JSON value that may span multiple input chunks.
sanitize::Result<TextSlice> scan_json_value_span(JsonStreamingScanner &scanner,
                                                 BumpArena *arena);

} // namespace sanitize::internal
