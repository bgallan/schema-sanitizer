// Implements JSON value scanning for spans that cross chunk boundaries.
//
// The main streaming scanner first tries a fast single-chunk parse; this file
// owns the slower buffered path used only when a value continues into later
// chunks.

#include "internal/parsing/streaming/json_streaming_scanner.hh"

#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

namespace sanitize::internal {

namespace {

struct Segment {
  std::shared_ptr<const void> owner;
  std::string_view view;
};

// Returns whether a byte is JSON whitespace.
bool is_json_ws(unsigned char c) {
  return c == ' ' || c == '\n' || c == '\r' || c == '\t';
}

} // namespace

class JsonValueSpanScanner {
public:
  // Creates a scanner for one value that may cross chunk boundaries.
  JsonValueSpanScanner(JsonStreamingScanner &scanner, BumpArena *arena)
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

  // Scans one value and returns a text slice over buffered bytes.
  sanitize::Result<TextSlice> scan() {
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

private:
  enum class Mode : uint8_t { kString = 0, kComposite = 1, kPrimitive = 2 };

  static constexpr std::size_t kMaxValueBytes = std::size_t{128} << 20;

  // Returns whether a byte terminates a primitive JSON value.
  static bool is_primitive_delim(char ch) {
    return is_json_ws(static_cast<unsigned char>(ch)) || ch == ',' ||
           ch == ']' || ch == '}';
  }

  // Appends the current chunk segment to the buffered span.
  sanitize::Status push_segment(std::size_t end_pos) {
    if (end_pos <= seg_start_pos_) {
      return sanitize::Status::OK();
    }
    std::string_view part =
        scanner_.chunk_.data.substr(seg_start_pos_, end_pos - seg_start_pos_);
    if (part.size() > kMaxValueBytes ||
        total_bytes_ > kMaxValueBytes - part.size()) {
      return sanitize::Status::Invalid("JSON value exceeds max buffered size");
    }
    total_bytes_ += part.size();
    segments_.push_back(Segment{.owner = seg_owner_, .view = part});
    return sanitize::Status::OK();
  }

  // Buffers the current chunk tail and refills the scanner.
  sanitize::Status need_more() {
    SAN_RETURN_NOT_OK(push_segment(scanner_.chunk_.data.size()));
    multi_ = true;
    SAN_RETURN_NOT_OK(scanner_.refill());
    seg_start_pos_ = 0;
    seg_owner_ = scanner_.chunk_.owner;
    return sanitize::Status::OK();
  }

  // Initializes scanner mode from the first value byte.
  sanitize::Status initialize_mode() {
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

  // Scans primitive bytes until a delimiter or EOF.
  sanitize::Result<bool> scan_primitive() {
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

  // Processes one string or composite byte and returns whether the value ended.
  sanitize::Result<bool> scan_string_or_composite_byte() {
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

  // Processes one byte while inside a JSON string.
  sanitize::Result<bool> scan_string_byte(char ch) {
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

  // Processes one byte while inside a JSON composite value.
  sanitize::Result<bool> scan_composite_byte(char ch) {
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

  // Builds the final text slice from one chunk or copied buffered segments.
  sanitize::Result<TextSlice> finish() {
    if (!multi_) {
      const std::string_view view =
          scanner_.chunk_.data.substr(start_pos_, scanner_.pos_ - start_pos_);
      return make_text_slice(view, start_abs_, start_owner_,
                             start_source_file_owner_, start_source_file_,
                             start_source_index_, start_has_source_index_);
    }

    SAN_RETURN_NOT_OK(push_segment(scanner_.pos_));

    char *dst = static_cast<char *>(arena_->alloc(total_bytes_, alignof(char)));
    if (!dst && total_bytes_) {
      return sanitize::Status::Invalid("JSON scanner: arena alloc failed");
    }

    std::size_t w = 0;
    for (const auto &segment : segments_) {
      std::memcpy(dst + w, segment.view.data(), segment.view.size());
      w += segment.view.size();
    }

    return make_text_slice(std::string_view(dst, total_bytes_), start_abs_, {},
                           start_source_file_owner_, start_source_file_,
                           start_source_index_, start_has_source_index_);
  }

  JsonStreamingScanner &scanner_;
  BumpArena *arena_ = nullptr;
  std::vector<Segment> segments_;
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
  std::vector<char> stack_;
  bool in_string_ = false;
  bool escape_ = false;
};

sanitize::Result<TextSlice> scan_json_value_span(JsonStreamingScanner &scanner,
                                                 BumpArena *arena) {
  JsonValueSpanScanner span_scanner(scanner, arena);
  return span_scanner.scan();
}

} // namespace sanitize::internal
